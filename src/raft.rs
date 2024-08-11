use anyhow::Error;
use flashmap::{self};
use heartbeat_reply::HeartbeatType;
use rand::prelude::*;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{
    watch::{self},
    RwLock,
};

mod data;
mod requests;
use crate::{
    grpc::{self, *},
    HeartbeatRequest, Object, RequestVoteReply, RequestVoteRequest,
};

pub use self::data::{RaftCore, RaftState};
pub use self::requests::RaftManagementRequest;
use self::requests::{send_heartbeat, send_vote_request};

impl LogEntry {
    pub fn get_type(&self) -> LogType {
        LogType::try_from(self.ltype).expect("Invalid Logtype")
    }
    pub fn get_index(&self) -> usize {
        self.index as usize
    }
    pub fn get_term(&self) -> usize {
        self.term as usize
    }
    pub fn set_term(&mut self, term: usize) {
        self.term = term as u64;
    }
    pub fn create_insert(index: usize, term: usize, key: String, value: Object) -> Self {
        LogEntry {
            ltype: LogType::Insert as i32,
            index: index as u64,
            term: term as u64,
            key,
            value: Some(value),
            old_value: None,
        }
    }
    pub fn create_cas(
        index: usize,
        term: usize,
        key: String,
        old_value: Object,
        value: Object,
    ) -> Self {
        LogEntry {
            ltype: LogType::Cas as i32,
            index: index as u64,
            term: term as u64,
            key,
            value: Some(value),
            old_value: Some(old_value),
        }
    }
    pub fn create_delete(index: usize, term: usize, key: String) -> Self {
        LogEntry {
            ltype: LogType::Delete as i32,
            index: index as u64,
            term: term as u64,
            key,
            value: None,
            old_value: None,
        }
    }
}

impl HeartbeatReply {
    pub fn get_type(&self) -> HeartbeatType {
        HeartbeatType::try_from(self.htype).expect("Invalid HeartbeatType")
    }
}

pub async fn log_manager(
    log: Arc<RwLock<Vec<LogEntry>>>,
    data: flashmap::WriteHandle<String, Object>,
    core: Arc<RwLock<RaftCore>>,
) {
    let mut current_index = 0;
    let mut writer = data;
    loop {
        let c = core.read().await;
        let new_index = c.max_committed;
        drop(c);

        // Check if there are new log entries that need to be committed to the data store and apply them
        if new_index > current_index {
            let log_handle = log.read().await;
            let new_entries = log_handle[current_index..new_index].to_vec();
            let log_len = log_handle.len();
            drop(log_handle);

            let mut write_guard = writer.guard();

            for e in new_entries {
                match e.get_type() {
                    LogType::Insert => {
                        if e.key == "foo" {
                            eprintln!("logged foo at: {:?}", SystemTime::now());
                        }
                        write_guard.insert(e.key, e.value.unwrap_or(Object::default()));
                    }
                    LogType::Cas => {
                        if let Some(current) = write_guard.get(&e.key) {
                            if e.value.is_none() || e.old_value.is_none() {
                                continue;
                            } else if *current == e.old_value.unwrap() {
                                write_guard.insert(e.key.to_owned(), e.value.unwrap().to_owned());
                            }
                        }
                    }
                    LogType::Delete => {
                        write_guard.remove(e.key);
                    }
                }
            }
            write_guard.publish();
            current_index = new_index.min(log_len);
        }
    }
}

pub async fn raft_state_manager(
    raft_sender: watch::Sender<RaftState>,
    raft_receiver: watch::Receiver<RaftState>,
    core: Arc<RwLock<RaftCore>>,
    log: Arc<RwLock<Vec<LogEntry>>>,
) -> Result<(), std::io::Error> {
    let core_copy = core.clone();
    let log_copy = log.clone();
    tokio::task::spawn(async move {
        loop {
            let state = raft_receiver.borrow().clone();
            match state {
                RaftState::Canidate(term) => {
                    let won_election = tokio::time::timeout(
                        tokio::time::Duration::from_millis(100),
                        run_election(core_copy.clone()),
                    )
                    .await;

                    let current_state = raft_receiver.borrow().clone();
                    eprintln!("Finished Election");

                    if let Ok(Ok(true)) = won_election {
                        eprintln!("Won Election");
                        raft_sender.send(RaftState::Leader(term));
                        let c = core_copy.read().await;
                        let mut l = log_copy.write().await;

                        // Will need to update this once log compaction is implemented as this will no longer work appropriately
                        let start = (c.max_committed.checked_sub(1).unwrap_or(0));
                        if start < l.len() {
                            for log_entry in l[..].iter_mut() {
                                if term > log_entry.get_term()
                                    && c.max_committed < log_entry.get_index()
                                {
                                    log_entry.set_term(term);
                                }
                            }
                        }
                        eprintln!("State is {:?}. Changing to Leader", state);
                        continue;
                    } else if let RaftState::Canidate(_) = current_state {
                        let mut c = core_copy.write().await;
                        c.current_term += 1;
                        let term = c.current_term;
                        raft_sender.send(RaftState::Canidate(term));
                    }
                    eprintln!("Lost Election");
                    let r = (random::<u64>() % 200) + 300;
                    tokio::time::sleep(tokio::time::Duration::from_millis(r)).await;
                }
                RaftState::Leader(term) => {
                    let h = send_global_heartbeat(core_copy.clone(), log_copy.clone());
                    let t = h.await;
                    if let Ok(t) = t {
                        if t > term {
                            let mut c = core_copy.write().await;
                            c.current_term = t;
                            raft_sender.send(RaftState::Follower(t, None, true));
                        }
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                RaftState::Follower(term, addr, should_continue) => {
                    if !should_continue {
                        let mut c = core_copy.write().await;
                        c.current_term += 1;
                        raft_sender.send(RaftState::Canidate(c.current_term));
                    } else {
                        raft_sender.send(RaftState::Follower(term, addr, false));
                        let r = (random::<u64>() % 200) + 300;
                        tokio::time::sleep(tokio::time::Duration::from_millis(r)).await;
                    }
                }
            }
        }
    });
    Ok(())
}

async fn run_election(core: Arc<RwLock<RaftCore>>) -> Result<bool, Error> {
    let mut c = core.write().await;

    // Return early if the node has already voted in the current term
    if c.last_voted >= c.current_term {
        c.current_term = c.last_voted;
        return Ok(false);
    }
    let members = c.members.clone();
    let target_votes = (members.len() + 1) / 2 + 1;
    let mut current_votes = 1;
    let request = RequestVoteRequest {
        current_term: c.current_term as u64,
        max_received: c.max_received as u64,
    };

    // Spawn vote requests as individual tasks
    let mut pool = tokio::task::JoinSet::new();
    for address in members {
        pool.spawn(send_vote_request(address, request.clone()));
    }

    // Wait for the vote requests to complete in any order, returning early if the result of the election can be determined
    while !pool.is_empty() {
        if let Some(Ok(Ok(response))) = pool.join_next().await {
            if let RequestVoteReply {
                current_term: None,
                max_received: None,
            } = response
            {
                current_votes += 1;
            } else {
                eprintln!("Vote was rejected, updating term");
                let term_check = c.current_term;
                c.current_term = c
                    .current_term
                    .max(response.current_term.unwrap_or(0) as usize)
                    .max(response.max_received.unwrap_or(0) as usize);
                if term_check != c.current_term {
                    return Ok(false);
                }
            }

            if current_votes >= target_votes {
                eprintln!("0:Votes {}:{}", current_votes, target_votes);
                return Ok(true);
            } else if current_votes + pool.len() < target_votes {
                eprintln!("1:Votes {}:{}", current_votes, target_votes);
                return Ok(false);
            }
        }
    }
    eprintln!("2:Votes {}:{}", current_votes, target_votes);
    return Ok(current_votes >= target_votes);
}

async fn send_global_heartbeat(
    core: Arc<RwLock<RaftCore>>,
    log: Arc<RwLock<Vec<LogEntry>>>,
) -> Result<usize, Error> {
    let l = log.read().await;
    let mut c = core.write().await;
    let mut max_recieved_members = Vec::new();

    let addresses = c.members.clone();

    // Node is running in a cluster of 1, so does not need to coordinate with any other nodes
    if addresses.len() == 0 {
        c.max_received = l.len();
        c.max_committed = c.max_received;
        return Ok(c.current_term);
    }

    let new_logs = l[c.max_received..].to_vec();
    let last;

    if let Some(max) = c.max_received.checked_sub(1) {
        last = l.get(max).cloned();
    } else {
        last = None;
    }

    if let Some(last_entry) = l.last() {
        c.max_received = last_entry.get_index();
    }

    drop(l);

    let mut state = c.clone();
    let addr = state.address.clone();
    drop(c);

    let request = HeartbeatRequest {
        latest_sent: last,
        current_term: state.current_term as u64,
        commit_to: state.max_committed as u64,
        log_entries: new_logs,
        address: addr.to_string(),
    };

    // Spawn heartbeats as individual tasks
    let mut pool = tokio::task::JoinSet::new();
    for address in addresses {
        eprintln!("Sending Heartbeat: {:?}", address);
        let request = request.clone();
        pool.spawn(async move {
            let x = send_heartbeat(address, request.clone()).await;
            return (x, address);
        });
    }

    // Wait for the heartbeats to complete in any order, adding a new request to the pool in the instance of a heartbeat add-one response
    let mut request;
    while !pool.is_empty() {
        if let Some(Ok((Ok(response), addr))) = pool.join_next().await {
            match response.get_type() {
                HeartbeatType::AddOne => {
                    if let HeartbeatReply {
                        htype: _,
                        max_received: Some(max_received),
                        current_term: _,
                    } = response
                    {
                        let log = log.clone();
                        let l = log.read().await;

                        // Safe because assertion above guarantees that max_received is Some
                        let latest_sent = l
                            .get((max_received.checked_sub(2).unwrap_or(0)) as usize)
                            .cloned();
                        if let Some(latest) = latest_sent {
                            let entries = l
                                .clone()
                                .into_iter()
                                .filter(|x| x.get_index() >= latest.get_index())
                                .collect();

                            request = HeartbeatRequest {
                                latest_sent: Some(latest),
                                current_term: state.current_term as u64,
                                commit_to: state.max_committed as u64,
                                log_entries: entries,
                                address: addr.to_string(),
                            }
                        } else {
                            let entries = l.clone();
                            request = HeartbeatRequest {
                                latest_sent: None,
                                current_term: state.current_term as u64,
                                commit_to: state.max_committed as u64,
                                log_entries: entries,
                                address: addr.to_string(),
                            };
                        }
                        eprintln!("Sent HeartbeatAddOne");
                        pool.spawn(async move {
                            let x = send_heartbeat(addr, request.clone()).await;
                            return (x, addr);
                        });
                    }
                }
                HeartbeatType::Ok => {
                    eprintln!("heartbeat ok");
                    max_recieved_members.push(response.max_received.unwrap_or(0) as usize);
                }
                HeartbeatType::Rejected => {
                    eprintln!("heartbeat reject");
                    state.current_term = state
                        .current_term
                        .max(response.current_term.unwrap_or(0) as usize);
                }
            }
        }
    }

    // Calculate the median of the max_recieved_members and update the max_committed value
    let mut c = core.write().await;
    max_recieved_members.push(c.max_received);
    let loc = c.members.len() / 2;
    eprintln!("responses: {:?}", max_recieved_members);
    while max_recieved_members.len() < c.members.len() {
        max_recieved_members.push(0);
    }

    let (_, median, _) = max_recieved_members.select_nth_unstable(loc);

    c.max_committed = *median.min(&mut (c.max_received.clone()));

    return Ok(state.current_term.max(c.current_term));
}

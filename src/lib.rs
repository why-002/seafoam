use bytes::buf;
use futures_util::Future;
use http::{request, response};
use serde::{Serialize, Deserialize};
use core::time;
use std::{any::Any, array, collections::{BTreeMap, HashMap, HashSet}, ops::Add, os::linux::raw::stat, process::Output, thread::{current, spawn}, time::{Duration, SystemTime}, usize::MIN};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::{mpsc::{self, *}, watch::{self, *}, RwLock}};
use std::sync::{Arc};
use flashmap::{self, new};
use std::net::SocketAddr;
use tokio::net::{TcpListener};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Data{
    String(String),
    Int(u32),
    Array(Vec<Data>),
    Map(HashMap<String, Data>)
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Req {
    S{
        key: String,
        value: Data,
        msg_id: u32
    },
    G{
        key: String,
        msg_id: u32
    },
    GOk{
        value: Data,
        in_reply_to: u32
    },
    SOk{
        in_reply_to: u32
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogEntry{
    Insert{
        key: String,
        data: Data,
        index: usize,
        term: usize
    },
    Delete{
        key: String,
        index: usize,
        term: usize
    }
}

impl LogEntry {
    pub fn get_index(&self) -> usize{
        match self {
            LogEntry::Insert { key, data, index, term } => *index,
            LogEntry::Delete { key, index, term } =>  *index
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum RaftState{
    Leader(usize),
    Canidate(usize),
    Follower(usize, Option<SocketAddr>, bool)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftCore {
    pub max_committed: usize,
    pub max_received: usize,
    pub current_term: usize,
    pub members: Vec<SocketAddr>,
    pub address: SocketAddr,
    pub last_voted: usize
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftManagementRequest{
    Heartbeat{
        latest_sent: Option<LogEntry>,
        current_term: usize,
        commit_to: usize,
        log_entries: Vec<LogEntry>,
        address: SocketAddr
    },
    RequestVote{
        current_term: usize,
        max_received: usize
    },

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftManagementResponse{
    HeartbeatOk{
        max_received: usize,
        current_term: usize
    },
    HeartbeatRejected{
        current_term: usize
    },
    HeartbeatAddOne{
        max_received: usize
    },
    VoteOk{

    },
    VoteRejected{
        current_term: usize,
        max_received: usize
    }
}

// impl RaftManagementRequest{
//     send(){

//     }
// }

impl RaftManagementResponse {
     async fn send_over_tcp_and_shutdown(&self, socket: &mut TcpStream){
        let response = serde_json::to_vec(&self)
            .expect("Failed to serialize Raft request");
        socket.write_all(&response).await.expect("Failed to send RaftManagementResponse");
        socket.shutdown().await.expect("Failed on shutdown of socket");
     }
 }

pub async fn log_manager(log: Arc<RwLock<Vec<LogEntry>>>, data: flashmap::WriteHandle<String, Data>, core: Arc<RwLock<RaftCore>>){
    let mut current_index = 0;
    let mut writer = data;
    loop {
        let c = core.read().await;
        let new_index = c.max_committed;
        drop(c);
        if new_index > current_index {
            let log_handle = log.read().await;
            let new_entries = log_handle.clone().into_iter().filter(|x| {
                match x {
                    LogEntry::Insert { key: _, data: _, index, term: _ } => *index > current_index && *index <= new_index,
                    LogEntry::Delete { key: _, index, term: _ } => *index > current_index && *index <= new_index,
                }
            });
            let mut write_guard = writer.guard();
            for e in new_entries {
                match e {
                    LogEntry::Insert { key, data, index, term } => {
                        if key == "foo"{
                            eprintln!("logged foo at: {:?}", SystemTime::now());
                        }
                        write_guard.insert(key, data)
                    },
                    LogEntry::Delete { key, index, term } => write_guard.remove(key)
                };
            }
            write_guard.publish();
            current_index = new_index.min(log_handle.len());
        }
    }
}

//TODO: Write as a tcp connection, dont bother with http.




//TODO: write canidate as non-blocking and have it keep looking out for follower stuff
// Fix writing for TCP
// Fix the response logic for heartbeats and maybe votes too
// need a fix for the manager to realize that it can no longer stay a follower, trying to used changed for this, but this caused a deadlock last night
pub async fn raft_state_manager(state_ref: watch::Receiver<RaftState>, state_updater: watch::Sender<RaftState>, core: Arc<RwLock<RaftCore>>, log: Arc<RwLock<Vec<LogEntry>>>, port: u16) -> Result<(), std::io::Error> {
    let addr = SocketAddr::from(([0,0,0,0], port));
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);

    let core_copy = core.clone();
    let log_copy = log.clone();
    let state_owner = Arc::new(RwLock::new(state_updater));
    let state_copy = state_owner.clone();
    let state_ref_copy = state_ref.clone();
    tokio::task::spawn(async move {
        let state_ref_copy = state_ref_copy;
        let state_copy = state_copy;
        loop{
            let state_updater = state_copy.write().await;
            let state = state_updater.borrow().clone();
            match state {
                RaftState::Canidate(term) => {
                    drop(state_updater);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    if let Ok(won_election) = tokio::time::timeout(tokio::time::Duration::from_secs(1), run_election(core_copy.clone())).await {
                        eprintln!("Finished election");
                        if won_election {
                            eprintln!("won");
                            let state_updater = state_copy.write().await;
                            state_updater.send(RaftState::Leader(term));
                            eprintln!("State is {:?}. Changing to Leader", state);
                        }
                        else {
                            let mut c = core_copy.write().await;
                            c.current_term += 1;
                            let term = c.current_term;
                            let state_updater = state_copy.write().await;
                            let current_state = state_updater.borrow().to_owned();
                            match  current_state {
                                RaftState::Canidate(_) => {
                                    state_updater.send(RaftState::Canidate(term));
                                },
                                _ => {}
                            }
                            eprintln!("Lost Election");
                        }
                    } //Rewrite this to use a 150-300ms timeout
                    else {
                        eprintln!("election timed out");
                        let state_updater = state_copy.write().await;
                        let current_state = state_updater.borrow().to_owned();
                        match  current_state {
                            RaftState::Canidate(_) => {
                                let mut c = core_copy.write().await;
                                c.current_term += 1;
                                let term = c.current_term;
                                state_updater.send(RaftState::Canidate(term));
                            },
                            _ => {}
                        }
                    }
                }
                RaftState::Leader(term) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    //eprintln!("I'm the leader.");
                    let h = tokio::task::spawn(send_global_heartbeat(core_copy.clone(), log_copy.clone()));
                    let t = h.await;
                    if let Ok(t) = t {
                        if t > term {
                            let mut c = core_copy.write().await;
                            c.current_term += 1;
                            state_updater.send(RaftState::Canidate(term + 1));
                        }
                    } 
                }
                RaftState::Follower(term, addr, should_continue) => {
                    if !should_continue {
                        eprintln!("matched");
                        let mut c = core_copy.write().await;
                        eprintln!("got core");
                        c.current_term += 1;
                        eprintln!("got changed");
                        state_updater.send(RaftState::Canidate(c.current_term));
                        continue;
                    }
                    else{
                        state_updater.send(RaftState::Follower(term, addr, false));
                    }
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }
        });


    loop {
        if let Ok((mut socket, _)) = listener.accept().await {
            let mut buf = Vec::new();
            socket.read_to_end(&mut buf).await
                .expect("Failed to read message from socket");
            let req = serde_json::from_slice(&buf)
                .expect("Failed to deserialize raft management request");

            match req {
                RaftManagementRequest::Heartbeat { latest_sent, current_term, commit_to, mut log_entries, address} => {
                    let mut c = core.write().await;
                    
                    if c.current_term > current_term {
                        eprintln!("Rejected heartbeat, Self: {} Other: {}", c.current_term, current_term);
                        let response = RaftManagementResponse::HeartbeatRejected { current_term: c.current_term };
                        response.send_over_tcp_and_shutdown(&mut socket).await;
                        continue;
                    }

                    let state_updater = state_owner.write().await;
                    state_updater.send(RaftState::Follower(current_term, Some(address), true));

                    if c.current_term < current_term {
                        c.current_term = current_term;
                    }

                    let old_received = c.max_received;
                    let current_term = c.current_term;
                    let mut new_received: usize;

                    let mut l = log.write().await;
                    if let Some(latest_sent) = latest_sent {  
                        if let Some(last_entry) = l.last(){
                            if latest_sent != *last_entry {
                                if let Some(n) = log_entries.last() {
                                    new_received = n.get_index();
                                    c.max_received = new_received;
                                    c.max_committed = commit_to;
                                }
                                l.append(&mut log_entries);
                                let response = &RaftManagementResponse::HeartbeatAddOne { max_received: old_received };
                                eprintln!("Responding with {:?}", response);
                                
                                response.send_over_tcp_and_shutdown(&mut socket).await;
                            }
                            else{
                                if let Some(n) = log_entries.last() {
                                    new_received = n.get_index();
                                    c.max_received = new_received;
                                    c.max_committed = commit_to;
                                }
                                else{
                                    new_received = c.max_received;
                                    c.max_received = new_received;
                                    c.max_committed = commit_to;
                                }
                                l.append(&mut log_entries);
                                let response = RaftManagementResponse::HeartbeatOk { max_received: new_received, current_term: current_term };
                                eprintln!("Responding with {:?}", response);
                                response.send_over_tcp_and_shutdown(&mut socket).await;
                            }
                        }
                    }
                    else{
                        if let Some(n) = log_entries.last() {
                            new_received = n.get_index();
                            c.max_received = new_received;
                            c.max_committed = commit_to;
                        }
                        else {
                            new_received = c.max_received;
                        }
                        l.append(&mut log_entries);
                        let response = RaftManagementResponse::HeartbeatOk { max_received: new_received, current_term: current_term };
                        eprintln!("Responding with {:?}", response);
                        response.send_over_tcp_and_shutdown(&mut socket).await;
                    }
                    eprintln!("Accepted heartbeat");
                },
                RaftManagementRequest::RequestVote { current_term, max_received } => {
                    let mut c = core.write().await;
                    eprintln!("responding to vote");
                    if current_term <= c.last_voted {
                        let response = RaftManagementResponse::VoteRejected { current_term: c.current_term, max_received: c.max_received };
                        response.send_over_tcp_and_shutdown(&mut socket).await;
                    }
                    else if c.current_term > current_term {
                        let response = RaftManagementResponse::VoteRejected { current_term: c.current_term, max_received: c.max_received };
                        response.send_over_tcp_and_shutdown(&mut socket).await;
                    }
                    else if c.max_received > max_received {
                        let response = RaftManagementResponse::VoteRejected { current_term: c.current_term, max_received: c.max_received };
                        response.send_over_tcp_and_shutdown(&mut socket).await;
                    }
                    else {
                        c.current_term = current_term;
                        c.last_voted = current_term;
                        let response = RaftManagementResponse::VoteOk {  };
                        response.send_over_tcp_and_shutdown(&mut socket).await;
                    }
                }
            }
        }
    }      
}

//TODO: finish and implement a lock preventing a node from voting twice, when voting, mark the term you are voting in
pub async fn run_election(core: Arc<RwLock<RaftCore>>) -> bool {
    let mut c = core.write().await;
    if c.last_voted >= c.current_term {
        c.current_term = c.last_voted;
        return false;
    }
    let members = c.members.clone();
    let target_votes = (members.len() + 1) / 2 + 1;
    let mut current_votes = 1;

    let request = serde_json::to_vec(&RaftManagementRequest::RequestVote { current_term: c.current_term, max_received: c.max_received })
    .expect("failed to convert request to json");
    
    eprintln!("{:?}", members);
    for address in members {
        // Make request and increment current_votes if it voted for you
        let stream = TcpStream::connect(address).await;
        if let Ok(mut stream) = stream {
            let i = tokio::time::timeout(time::Duration::from_millis(50), stream.write_all(request.as_slice())).await;
            let j = tokio::time::timeout(time::Duration::from_millis(50),  stream.shutdown()).await;
            if let Err(_) = j {
                eprintln!("shutdown failed");
                continue;
            }
            if let Err(_) = i {
                eprintln!("write failed");
                continue;
            }
            let mut buf = Vec::new();
            if let Ok(_) = tokio::time::timeout(time::Duration::from_millis(50),stream.read_to_end(&mut buf)).await {
                if let Ok(response) = serde_json::from_slice(&buf) {
                    match response {
                        RaftManagementResponse::VoteOk {  } => {current_votes += 1},
                        RaftManagementResponse::VoteRejected { current_term, max_received: _ } => {
                            eprintln!("Vote was rejected, updating term");
                            if c.current_term < current_term {
                                c.current_term = current_term;
                                return false
                            }
                        },
                        _ => panic!("Received a heartbeat response instead of a vote response")
                    }
                }
                else {
                    eprintln!("Failed to parse vote response");
                }
            }
            else{
                eprintln!("vote response stream invalid");
            }
        }
        else {
            eprintln!("Failed to connect to {:?}", address);
        }
    }
    eprintln!("Votes {}:{}", current_votes, target_votes);
    return current_votes >= target_votes;
}

//TODO: rewrite so the heartbeats are not sync with each other
pub async fn send_global_heartbeat(core: Arc<RwLock<RaftCore>>, log: Arc<RwLock<Vec<LogEntry>>>) -> usize {
    let l = log.read().await;
    let mut c = core.write().await;
    let mut max_recieved_members = Vec::new();
    
    let addresses = c.members.clone();

    if addresses.len() == 0 {
        return c.current_term;
    }

    let new_logs = l.clone().into_iter().filter(|x| {
        x.get_index() > c.max_received
    }).collect::<Vec<LogEntry>>();

    let last = l.get(c.max_received - 1).cloned();

    if let Some(last_entry) = l.last() {
        c.max_received = last_entry.get_index();
    }
    drop(l);

    let mut state = c.clone();

    let request = serde_json::to_vec(&RaftManagementRequest::Heartbeat { latest_sent: last, current_term: state.current_term, commit_to: state.max_committed, log_entries: new_logs, address: c.address })
        .expect("failed to convert request to json");

    for address in addresses {
        if let Ok(mut stream) = TcpStream::connect(address).await {
            let (mut read, mut write) = stream.split();
            write.write_all(request.as_slice()).await;
            write.shutdown().await;
            let mut buf = Vec::new();
            if let Ok(_) = read.read_to_end(&mut buf).await {
                if let Ok(response) = serde_json::from_slice(&buf) {
                    match response {
                        RaftManagementResponse::HeartbeatAddOne { max_received } => {todo!("Not implemented add-one responses")},
                        RaftManagementResponse::HeartbeatOk { max_received, current_term } => {
                            max_recieved_members.push(max_received);
                        },
                        RaftManagementResponse::HeartbeatRejected { current_term } => {
                            state.current_term = state.current_term.max(current_term);
                        }
                        _ => panic!("Received a voting response instead of a heartbeat response")
                    }
                }
            }
        }
    }
    max_recieved_members.push(c.max_received);
    let loc = max_recieved_members.len() / 2 - 1;

    if loc <= max_recieved_members.len() - 1 && max_recieved_members.len() != 0 {
        let (_, median, _) = max_recieved_members.select_nth_unstable(loc);
        c.max_committed = *median;
    }

    return state.current_term.max(c.current_term);
}
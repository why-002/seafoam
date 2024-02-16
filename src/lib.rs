use futures_util::{future::poll_fn, Future, FutureExt};
use http::{request, response};
use serde::{Serialize, Deserialize};
use core::time;
use std::{any::Any, arch::x86_64::CpuidResult, array, collections::{hash_map::RandomState, BTreeMap, HashMap, HashSet}, ops::Add, process::Output, task::Poll, thread::{current, spawn}, time::{Duration, SystemTime}, usize::MIN};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::{mpsc::{self, *}, watch::{self, *}, RwLock}};
use std::sync::{Arc};
use flashmap::{self, new};
use std::net::SocketAddr;
use tokio::net::{TcpListener};
use anyhow::Error;
use rand::prelude::*;

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
    VoteRejected{
        current_term: usize,
        max_received: usize
    },
    VoteOk{

    }
}

impl RaftManagementRequest{
    async fn send_over_tcp_and_shutdown(&self, socket: &mut TcpStream) -> Result<(), Error> {
        let response = serde_json::to_vec(&self)?;
        socket.write_all(&response).await?;
        socket.shutdown().await?;
        Ok(())
    }
}

impl RaftManagementResponse {
    async fn send_over_tcp_and_shutdown(&self, socket: &mut TcpStream) -> Result<(), Error> {
        let response = serde_json::to_vec(&self)?;
        socket.write_all(&response).await?;
        socket.shutdown().await?;
        Ok(())
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

async fn handle_management_request(mut socket: &mut TcpStream, core: Arc<RwLock<RaftCore>>, state_owner: Arc<RwLock<watch::Sender<RaftState>>>, log: Arc<RwLock<Vec<LogEntry>>>){
    let mut buf = Vec::new();
            socket.read_to_end(&mut buf).await
                .expect("Failed to read message from socket");
            let req = serde_json::from_slice(&buf)
                .expect("Failed to deserialize raft management request");

            match req { //Heartbeat needs to be reworked
                RaftManagementRequest::Heartbeat { latest_sent, current_term: message_current_term, commit_to, mut log_entries, address} => {
                    let c = core.read().await;
                    
                    if c.current_term > message_current_term {
                        eprintln!("Rejected heartbeat, Self: {} Other: {}", c.current_term, message_current_term);
                        let response = RaftManagementResponse::HeartbeatRejected { current_term: c.current_term };
                        response.send_over_tcp_and_shutdown(&mut socket).await;
                        return;
                    }

                    let state_updater = state_owner.write().await;
                    state_updater.send(RaftState::Follower(message_current_term, Some(address), true));
                    drop(state_updater);

                    let old_received = c.max_received;
                    let mut current_term = c.current_term;
                    drop(c);

                    if current_term < message_current_term {
                        let mut c = core.write().await;
                        c.current_term = message_current_term;
                        current_term = c.current_term;
                        drop(c);
                    }

                    let new_received: usize;

                    let mut l = log.write().await;
                    
                    if let Some(latest_sent) = latest_sent {
                        let last_match = l.get(latest_sent.get_index() - 1).cloned();
                        if let Some(last_match) = last_match {
                            if latest_sent !=  last_match { // latest_sent did not equal the same spot in current log, need to add-one
                                let response = RaftManagementResponse::HeartbeatAddOne { max_received: latest_sent.get_index() - 1 };
                                eprintln!("Responding with {:?}", response);
                                response.send_over_tcp_and_shutdown(&mut socket).await;
                            }
                            else { // They did match at that location, drain and then append
                                l.drain(latest_sent.get_index()..);
                                l.append(&mut log_entries);
                                let new_received = l.len();
                                let mut c = core.write().await;
                                c.max_received = new_received;
                                c.max_committed = commit_to;
                                let response = RaftManagementResponse::HeartbeatOk { max_received: new_received, current_term: current_term };
                                eprintln!("Responding with {:?}", response);
                                response.send_over_tcp_and_shutdown(&mut socket).await;
                            }
                        }
                        else { // The entry did not have a corresponding match, need to call for a heartbeat add-one
                            let response = RaftManagementResponse::HeartbeatAddOne { max_received: (old_received - 1).min(0) };
                            eprintln!("Responding with {:?}", response);
                            response.send_over_tcp_and_shutdown(&mut socket).await;
                        }
                    }
                    else{ // There are no entries that have been sent in the past, this should cause a complete rewrite of log
                        if let Some(last) = log_entries.last() { // Leader is sending at least 1 entry
                            new_received = last.get_index();
                            l.append(&mut log_entries);
                            let mut c = core.write().await;
                            c.max_received = new_received;
                            c.max_committed = commit_to;
                            let response = RaftManagementResponse::HeartbeatOk { max_received: new_received, current_term: current_term };
                            eprintln!("Responding with {:?}", response);
                            response.send_over_tcp_and_shutdown(&mut socket).await;
                        }
                        else { // heartbeat is empty
                            let response = RaftManagementResponse::HeartbeatOk { max_received: old_received, current_term: current_term };
                            eprintln!("Responding with {:?}", response);
                            response.send_over_tcp_and_shutdown(&mut socket).await;
                        }
                    }
                    eprintln!("Accepted heartbeat");
                },
                RaftManagementRequest::RequestVote { current_term, max_received } => {
                    let mut c = core.write().await;
                    eprintln!("responding to vote");
                    let response = RaftManagementResponse::VoteRejected { current_term: c.current_term, max_received: c.max_received };
                    if current_term <= c.last_voted {
                        response.send_over_tcp_and_shutdown(&mut socket).await;
                    }
                    else if c.current_term > current_term {
                        response.send_over_tcp_and_shutdown(&mut socket).await;
                    }
                    else if c.max_received > max_received {
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


// Fix the response logic for heartbeats and maybe votes too
// Heartbeats need to accurately replicate taking in heartbeat AddOnes
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
                    if let won_election = run_election(core_copy.clone()).await {
                        eprintln!("Finished election");
                        if let Ok(true) = won_election {
                            eprintln!("won");
                            let state_updater = state_copy.write().await;
                            state_updater.send(RaftState::Leader(term));
                            eprintln!("State is {:?}. Changing to Leader", state);
                            continue;
                        }
                        else {
                            let state_updater = state_copy.write().await;
                            let current_state = state_updater.borrow().to_owned();
                            match current_state {
                                RaftState::Canidate(_) => {
                                    let mut c = core_copy.write().await;
                                    c.current_term += 1;
                                    let term = c.current_term;
                                    state_updater.send(RaftState::Canidate(term));
                                },
                                _ => {}
                            }
                            eprintln!("Lost Election");
                            drop(state_updater);
                            let r = (random::<u64>() % 200) + 300;
                            tokio::time::sleep(tokio::time::Duration::from_millis(r)).await;
                        }
                    } //Rewrite this to use a 150-300ms timeout
                    else {
                        eprintln!("election timed out");
                        let state_updater = state_copy.write().await;
                        let current_state = state_updater.borrow().clone();
                        match  current_state {
                            RaftState::Canidate(_) => {
                                let mut c = core_copy.write().await;
                                c.current_term += 1;
                                let term = c.current_term;
                                state_updater.send(RaftState::Canidate(term));
                            },
                            _ => {}
                        }
                        drop(state_updater);
                        let r = (random::<u64>() % 200) + 300;
                        tokio::time::sleep(tokio::time::Duration::from_millis(r)).await;
                    }
                }
                RaftState::Leader(term) => {
                    //eprintln!("I'm the leader.");
                    let h = tokio::task::spawn(send_global_heartbeat(core_copy.clone(), log_copy.clone()));
                    let t = h.await;
                    if let Ok(Ok(t)) = t {
                        if t > term {
                            let mut c = core_copy.write().await;
                            c.current_term = t;
                            state_updater.send(RaftState::Follower(t, None, true));
                        }
                    }
                    drop(state_updater);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                RaftState::Follower(term, addr, should_continue) => {
                    if !should_continue {
                        let mut c = core_copy.write().await;
                        c.current_term += 1;
                        state_updater.send(RaftState::Canidate(c.current_term));
                    }
                    else{
                        state_updater.send(RaftState::Follower(term, addr, false));
                    }
                    drop(state_updater);
                    let r = (random::<u64>() % 200) + 300;
                    tokio::time::sleep(tokio::time::Duration::from_millis(r)).await;
                }
            }
        }
        });

    loop {
        if let Ok((mut socket, _)) = listener.accept().await {
            let core = core.clone();
            let state_owner = state_owner.clone();
            let log = log.clone();
            tokio::task::spawn(async move {
                handle_management_request(&mut socket, core.clone(), state_owner.clone(), log.clone()).await
            });
        }
    }      
}


// TODO: use is_finished() on spawned threads in order to do async, probably write a threadpool struct
pub async fn run_election(core: Arc<RwLock<RaftCore>>) -> Result<bool, Error> {
    let mut c = core.write().await;
    if c.last_voted >= c.current_term {
        c.current_term = c.last_voted;
        return Ok(false);
    }
    let members = c.members.clone();
    let target_votes = (members.len() + 1) / 2 + 1;
    let mut current_votes = 1;

    let request = RaftManagementRequest::RequestVote { current_term: c.current_term, max_received: c.max_received };

    eprintln!("{:?}", members);
    for address in members {
        // Make request and increment current_votes if it voted for you
        if let Ok(response) = send_vote_request(address, request.clone()).await {
            match response {
                RaftManagementResponse::VoteOk {  } => {current_votes += 1},
                RaftManagementResponse::VoteRejected { current_term, max_received: _ } => {
                    eprintln!("Vote was rejected, updating term");
                    if c.current_term < current_term {
                        c.current_term = current_term;
                        return Ok(false);
                    }
                },
                _ => panic!("Received a heartbeat response instead of a vote response")
            }
        }
    }
    eprintln!("Votes {}:{}", current_votes, target_votes);
    return Ok(current_votes >= target_votes);
}

//TODO: rewrite so the heartbeats are not sync with each other
// Rewrite so it loops sending lower lasts until it succeded (requires them to be async from each other)
pub async fn send_global_heartbeat(core: Arc<RwLock<RaftCore>>, log: Arc<RwLock<Vec<LogEntry>>>) -> Result<usize, Error> {
    let l = log.read().await;
    let mut c = core.write().await;
    let mut max_recieved_members = Vec::new();
    
    let addresses = c.members.clone();

    if addresses.len() == 0 {
        c.max_received = l.len();
        c.max_committed = c.max_received;
        return Ok(c.current_term);
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

    let request = RaftManagementRequest::Heartbeat { latest_sent: last, current_term: state.current_term, commit_to: state.max_committed, log_entries: new_logs, address: c.address };

    for address in addresses {
        eprintln!("{:?}", address);
        if let Ok(mut response) = send_heartbeat(address, request.clone()).await {
            match response {
                RaftManagementResponse::HeartbeatAddOne { max_received } => {
                    let l = log.clone();
                    let request = RaftManagementRequest::Heartbeat { latest_sent: None, current_term: state.current_term, commit_to: state.max_committed, log_entries: l.read().await.clone(), address: c.address };
                    send_heartbeat(address, request.clone()).await;
                    // eprintln!("heartbeat add-one");
                    // let mut should_break = false;
                    // while let RaftManagementResponse::HeartbeatAddOne { max_received } = response {
                    //     if max_received == 0 {
                    //         if should_break {
                    //             break;
                    //         }
                    //         should_break = true;
                    //         continue;
                    //     }
                    //     //todo!("not finished add-one");
                    //     eprintln!("Add-one triggered");
                    //     if let RaftManagementRequest::Heartbeat { latest_sent, current_term, commit_to, log_entries, address } = request.clone() {
                    //         let l = log.read().await;
                    //         let new_last = l.get(max_received - 2).cloned();
                    //         let mut new_logs = Vec::new();
                            
                    //         if let Some(last) = new_last.clone() {
                    //             new_logs = l.clone().into_iter().filter(|x| {
                    //                 x.get_index() >= last.get_index()
                    //             }).collect();
                    //         }
                    //         else {
                    //             new_logs = l.clone();
                    //         }

                    //         let new_request = RaftManagementRequest::Heartbeat { latest_sent: new_last, current_term: current_term, commit_to: commit_to, log_entries: new_logs, address: address };
                    //         let mut socket = TcpStream::connect(address).await.unwrap();
                    //         new_request.send_over_tcp_and_shutdown(&mut socket).await.unwrap();
                    //         let mut buff = Vec::new();
                    //         socket.read_to_end(&mut buff).await;
                    //         response = serde_json::from_slice(&buff).unwrap_or(RaftManagementResponse::HeartbeatAddOne { max_received: (max_received - 1).min(0) });
                    //     }
                    // }
                },
                RaftManagementResponse::HeartbeatOk { max_received, current_term } => {
                    eprintln!("heartbeat ok");
                    max_recieved_members.push(max_received);
                },
                RaftManagementResponse::HeartbeatRejected { current_term } => {
                    eprintln!("heartbeat reject");
                    state.current_term = state.current_term.max(current_term);
                }
                _ => panic!("Received a voting response instead of a heartbeat response")
            }
        }
        else {
            eprintln!("Failed to get heartbeat");
        }
    }

    max_recieved_members.push(c.max_received);
    let loc = max_recieved_members.len() / 2 - 1;
    eprintln!("{:?}", max_recieved_members);
    if loc <= max_recieved_members.len() - 1 && max_recieved_members.len() != 0 {
        let (_, median, _) = max_recieved_members.select_nth_unstable(loc);
        c.max_committed = *median;
    }

    return Ok(state.current_term.max(c.current_term));
}

pub async fn send_heartbeat(address: SocketAddr, request: RaftManagementRequest) -> Result<RaftManagementResponse, Error> {
    let result = tokio::time::timeout(Duration::from_millis(75), async {
        let mut stream = TcpStream::connect(address).await?;
        request.send_over_tcp_and_shutdown(&mut stream).await?;
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await?;
        let response: RaftManagementResponse = serde_json::from_slice(&buf)?;
        return Ok(response);
    }).await?;

    return result;
}

pub async fn send_vote_request(address: SocketAddr, request: RaftManagementRequest) -> Result<RaftManagementResponse, Error> {
    let result = tokio::time::timeout(Duration::from_millis(75), async {
        let mut stream = TcpStream::connect(address).await?;
        request.send_over_tcp_and_shutdown(&mut stream).await?;
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await?;
        let response: RaftManagementResponse = serde_json::from_slice(&buf)?;
        return Ok(response);
    }).await?;

    return result;
}
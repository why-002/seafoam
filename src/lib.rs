use bytes::buf;
use futures_util::Future;
use http::{request, response};
use serde::{Serialize, Deserialize};
use std::{any::Any, array, collections::{BTreeMap, HashMap, HashSet}, ops::Add, os::linux::raw::stat, process::Output, thread::spawn, usize::MIN};
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
    Follower(usize)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftCore {
    pub max_committed: usize,
    pub max_received: usize,
    pub current_term: usize,
    pub members: Vec<SocketAddr>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftManagementRequest{
    Heartbeat{
        latest_sent: Option<LogEntry>,
        current_term: usize,
        commit_to: usize,
        log_entries: Vec<LogEntry>,
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
        socket.write_all(&response).await;
        socket.shutdown().await;
     }
 }

pub async fn log_manager(log: Arc<RwLock<Vec<LogEntry>>>, data: flashmap::WriteHandle<String, Data>, core: Arc<RwLock<RaftCore>>){
    let mut current_index = 0;
    let mut writer = data;
    loop {
        let new_index = core.read().await.max_committed;
        eprintln!("new Index: {}, log: {:?}", new_index, log.read().await);
        if new_index > current_index {
            let log_handle = log.read().await;
            eprintln!("Log: {:?}", log_handle);
            let new_entries = log_handle.clone().into_iter().filter(|x| {
                println!("Index of entry {:?}, current: {}, new: {}", *x, current_index, new_index);
                match x {
                    LogEntry::Insert { key: _, data: _, index, term: _ } => *index > current_index && *index <= new_index,
                    LogEntry::Delete { key: _, index, term: _ } => *index > current_index && *index <= new_index,
                }
            });
            let mut write_guard = writer.guard();
            for e in new_entries {
                eprintln!("Writing entry: {:?}", e);
                match e {
                    LogEntry::Insert { key, data, index, term } => write_guard.insert(key, data),
                    LogEntry::Delete { key, index, term } => write_guard.remove(key)
                };
            }
            write_guard.publish();
            current_index = new_index.min(log_handle.len());
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
}

//TODO: Write as a tcp connection, dont bother with http.




//TODO: write canidate as non-blocking and have it keep looking out for follower stuff
// Fix writing for TCP
pub async fn raft_state_manager(state_ref: watch::Receiver<RaftState>, state_updater: watch::Sender<RaftState>, core: Arc<RwLock<RaftCore>>, log: Arc<RwLock<Vec<LogEntry>>>, port: u16) -> Result<(), std::io::Error> {
    let addr = SocketAddr::from(([0,0,0,0], port));
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);
    loop{
        let state = *state_ref.borrow();
        match state {
            RaftState::Follower(term) => { //Rewrite to have a random 150-300ms timeout and use that impl for canidate
                if let Ok(Ok((mut socket, _))) = tokio::time::timeout(tokio::time::Duration::from_secs(1), listener.accept()).await {
                    let mut buf = Vec::new();
                    socket.read_to_end(&mut buf).await
                        .expect("Failed to read message from socket");
                    let req = serde_json::from_slice(&buf)
                        .expect("Failed to deserialize raft management request");

                    match req {
                        RaftManagementRequest::Heartbeat { latest_sent, current_term, commit_to, mut log_entries } => {
                            let mut c = core.write().await;
                            
                            if c.current_term > current_term {
                                let response = RaftManagementResponse::HeartbeatRejected { current_term: c.current_term };
                                response.send_over_tcp_and_shutdown(&mut socket).await;
                                continue;
                            }

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
                                        let response = &RaftManagementResponse::HeartbeatAddOne { max_received: *&old_received };
                                        eprintln!("Responding with {:?}", response);
                                        response.send_over_tcp_and_shutdown(&mut socket).await;
                                        continue;
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
                            drop(l);
                            eprintln!("Accepted heartbeat");
                        },
                        RaftManagementRequest::RequestVote { current_term, max_received } => {
                            let c = core.read().await;
                            eprintln!("responding to vote");
                            if c.current_term > current_term {
                                let response = RaftManagementResponse::VoteRejected { current_term: c.current_term, max_received: c.max_received };
                                response.send_over_tcp_and_shutdown(&mut socket).await;
                            }
                            else if c.max_received > max_received {
                                let response = RaftManagementResponse::VoteRejected { current_term: c.current_term, max_received: c.max_received };
                                response.send_over_tcp_and_shutdown(&mut socket).await;
                            }
                            else {
                                drop(c);
                                let mut c = core.write().await;
                                c.current_term = current_term;
                                let response = RaftManagementResponse::VoteOk {  };
                                response.send_over_tcp_and_shutdown(&mut socket).await;
                            }
                        }
                    }
                }
                else{
                    eprintln!("Changing to Canidate");
                    let mut c = core.write().await;
                    c.current_term += 1;
                    state_updater.send(RaftState::Canidate(c.current_term));
                }
            }
            RaftState::Canidate(term) => {
                if let Ok(won_election) = tokio::time::timeout(tokio::time::Duration::from_secs(1), run_election(core.clone())).await {
                    eprintln!("Finished election");
                    if won_election {
                        state_updater.send(RaftState::Leader(term));
                        eprintln!("State is {:?}. Changing to Leader", state);
                    }
                    else {
                        let c = core.read().await;
                        state_updater.send(RaftState::Follower(c.current_term));
                        eprintln!("State is {:?}. Failed to win election", state);
                    }
                } //Rewrite this to use a 150-300ms timeout
            }
            RaftState::Leader(term) => {
                //eprintln!("I'm the leader.");
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                let h = tokio::task::spawn(send_global_heartbeat(core.clone(), log.clone()));
                let t = h.await?;
                if t > term{
                    state_updater.send(RaftState::Follower(t));
                    let mut c = core.write().await;
                    c.current_term = t;
                }
            }
            }
        }              
}

//TODO: finish and implement a lock preventing a node from voting twice, perhaps something like keeping track of most recent eleciton voted in
pub async fn run_election(core: Arc<RwLock<RaftCore>>) -> bool {
    let c = core.write().await;
    let members = c.members.clone();
    let target_votes = (members.len() + 1) / 2 + 1;
    let mut current_votes = 1;

    let request = serde_json::to_vec(&RaftManagementRequest::RequestVote { current_term: c.current_term, max_received: c.max_received })
    .expect("failed to convert request to json");
    
    drop(c);
    eprintln!("{:?}", members);
    for address in members {
        // Make request and increment current_votes if it voted for you
        let stream = TcpStream::connect(address).await;
        if let Ok(mut stream) = stream {
            stream.write_all(request.as_slice()).await;
            stream.shutdown().await;
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).await
                .expect("Failed to read vote response");
            let response = serde_json::from_slice(&buf)
                .expect("Failed to parse vote response");
            match response {
                RaftManagementResponse::VoteOk {  } => {current_votes += 1},
                RaftManagementResponse::VoteRejected { current_term, max_received: _ } => {
                    let mut c = core.write().await;
                    if c.current_term < current_term {
                        c.current_term = current_term;
                        return false
                    }
                },
                _ => panic!("Received a heartbeat response instead of a vote response")
            }
        }
        else {
            eprintln!("Failed to connect to {:?}", address);
            continue;
        }
    }
    eprintln!("Votes {}:{}", current_votes, target_votes);
    return current_votes >= target_votes;
}

//TODO: rewrite so the heartbeats are not sync with each other
// TODO: need to fix leader not sending previous max as latest_sent
pub async fn send_global_heartbeat(core: Arc<RwLock<RaftCore>>, log: Arc<RwLock<Vec<LogEntry>>>) -> usize {
    let l = log.read().await;
    let mut c = core.write().await;
    let mut max_recieved_members = Vec::new();
    
    let addresses = c.members.clone();

    let new_logs = l.clone().into_iter().filter(|x| {
        x.get_index() > c.max_received
    }).collect::<Vec<LogEntry>>();

    let last = l.get(c.max_received - 1).cloned();

    if let Some(last_entry) = l.last() {
        c.max_received = last_entry.get_index();
    }
    drop(l);

    let state = c.clone();
    drop(c);

    let request = serde_json::to_vec(&RaftManagementRequest::Heartbeat { latest_sent: last, current_term: state.current_term, commit_to: state.max_committed, log_entries: new_logs })
        .expect("failed to convert request to json");

    for address in addresses {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let (mut read, mut write) = stream.split();
        write.write_all(request.as_slice()).await;
        write.shutdown().await;
        let mut buf = Vec::new();
        read.read_to_end(&mut buf).await
            .expect("Failed to read heartbeat response");
        let response = serde_json::from_slice(&buf)
            .expect("Failed to parse heartbeat response");
        match response {
            RaftManagementResponse::HeartbeatAddOne { max_received } => todo!("Not implemented add-one responses"),
            RaftManagementResponse::HeartbeatOk { max_received, current_term } => {
                max_recieved_members.push(max_received);
            },
            RaftManagementResponse::HeartbeatRejected { current_term } => {
                let c = core.read().await;
                if c.current_term < current_term {
                    return current_term;
                }
            }
            _ => panic!("Received a voting response instead of a heartbeat response")
        }
    }
    let mut c = core.write().await;
    max_recieved_members.push(c.max_received);
    let loc = max_recieved_members.len() / 2 - 1;
    let (_, median, _) = max_recieved_members.select_nth_unstable(loc);
    

    c.max_committed = *median;

    return state.current_term;
}

//TODO if needed write a function that tries to set something to true every 150-300ms and will change state if it hits true while having the main process unset true
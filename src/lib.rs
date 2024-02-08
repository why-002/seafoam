use bytes::buf;
use futures_util::Future;
use serde::{Serialize, Deserialize};
use std::{any::Any, array, collections::{BTreeMap, HashMap, HashSet}, os::linux::raw::stat, process::Output};
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
        latest_sent: LogEntry,
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

pub async fn log_manager(log: Arc<RwLock<Vec<LogEntry>>>, data: flashmap::WriteHandle<String, Data>, core: Arc<RwLock<RaftCore>>){
    let mut current_index = 0;
    let mut writer = data;
    loop {
        let new_index = core.read().await.max_committed;
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
                    LogEntry::Insert { key, data, index, term } => write_guard.insert(key, data),
                    LogEntry::Delete { key, index, term } => write_guard.remove(key)
                };
            }
            write_guard.publish();
            current_index = [new_index, log_handle.len() - 1].into_iter().min().unwrap();
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
}

//TODO: Write as a tcp connection, dont bother with http
pub async fn raft_state_manager(state_ref: watch::Receiver<RaftState>, state_updater: watch::Sender<RaftState>, core: Arc<RwLock<RaftCore>>, log: Arc<RwLock<Vec<LogEntry>>>) -> Result<(), std::io::Error> {
    let addr = SocketAddr::from(([0,0,0,0], 3010));
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);
    loop{
        let state = *state_ref.borrow();
        match state {
            RaftState::Follower(term) => { //Rewrite to have a random 150-300ms timeout and use that impl for canidate
                if let Ok(Ok((mut socket, _))) = tokio::time::timeout(tokio::time::Duration::from_secs(1), listener.accept()).await {
                    let mut buf = [0;1000];
                    socket.read(&mut buf);
                    let req = serde_json::from_slice(&buf)
                        .expect("Failed to deserialize raft management request");
                    match req {
                        RaftManagementRequest::Heartbeat { latest_sent, current_term, commit_to, mut log_entries } => {
                            let mut c = core.write().await;
                            
                            if c.current_term > current_term {
                                let response = serde_json::to_vec(&RaftManagementResponse::HeartbeatRejected { current_term: c.current_term }).expect("Failed to convert response to json");
                                socket.write(response.as_slice());
                                continue;
                            }

                            if c.current_term < current_term {
                                c.current_term = current_term;
                            }

                            c.max_received = latest_sent.get_index();
                            c.max_committed = commit_to;

                            let received = c.max_received;
                            let current_term = c.current_term;
                            drop(c);

                            let mut l = log.write().await;
                            if let Some(last_entry) = l.last(){
                                if latest_sent != *last_entry { //
                                    let response = serde_json::to_vec(&RaftManagementResponse::HeartbeatAddOne { max_received: *&received })
                                        .expect("Failed to convert response to json");
                                    socket.write(response.as_slice());
                                    continue;
                                }
                                l.append(&mut log_entries);
                            }
                            drop(l);

                            let response = serde_json::to_vec(&RaftManagementResponse::HeartbeatOk { max_received: received, current_term: current_term })
                                .expect("Failed to convert response to json");
                            socket.write(response.as_slice());
                        },
                        RaftManagementRequest::RequestVote { current_term, max_received } => {
                            let c = core.read().await;
                            if c.current_term > current_term {
                                todo!()
                            }

                            if c.max_received > max_received {
                                todo!()
                            }
                        }
                    }
                }
                else{
                    eprintln!("Changing to Canidate");
                    state_updater.send(RaftState::Canidate(term + 1));
                }
            }
            RaftState::Canidate(term) => {
                eprintln!("State is {:?}. Changing to Leader", state);
                state_updater.send(RaftState::Leader(term));
                // if let Ok(won_election) = tokio::time::timeout(tokio::time::Duration::from_secs(1), run_election(core.read().await.members.clone())).await {
                //     if won_election {
                //         state_updater.send(RaftState::Leader(term));
                //     }
                // } //Rewrite this to use a 150-300ms timeout
            }
            RaftState::Leader(term) => {
                //eprintln!("I'm the leader.");
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                let addresses = core.read().await.members.clone();
                //tokio::task::spawn(send_global_heartbeat(addresses, core.clone(), log.clone()));
            }
            }
        }              
}

//TODO: finish and implement a lock preventing a node from voting twice, perhaps something like keeping track of most recent eleciton voted in
pub async fn run_election(members: Vec<SocketAddr>) -> bool {
    let target_votes = members.len() / 2 + 1;
    let mut current_votes = 1;
    for member in members {
        // Make request and increment current_votes if it voted for you
    }

    if current_votes >= target_votes {
        return true;
    }
    return false;
}

//TODO: rewrite so the heartbeats are not sync with each other
pub async fn send_global_heartbeat(addresses: Vec<SocketAddr>, core: Arc<RwLock<RaftCore>>, log: Arc<RwLock<Vec<LogEntry>>>) {
    let l = log.read().await;
    let mut c = core.write().await;

    let new_logs = l.clone().into_iter().filter(|x| {
        x.get_index() > c.max_received
    }).collect::<Vec<LogEntry>>();

    let last = l.last().unwrap().clone();
    drop(l);

    c.max_received = last.get_index();
    let state = c.clone();
    drop(c);

    let request = serde_json::to_vec(&RaftManagementRequest::Heartbeat { latest_sent: last, current_term: state.current_term, commit_to: state.max_committed, log_entries: new_logs })
        .expect("failed to convert request to json");

    for address in addresses.into_iter() {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write(request.as_slice());
        stream.read(todo!());
    }
}

//TODO if needed write a function that tries to set something to true every 150-300ms and will change state if it hits true while having the main process unset true
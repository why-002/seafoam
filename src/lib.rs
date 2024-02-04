use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet, BTreeMap};
use tokio::{io::AsyncReadExt, sync::{RwLock, watch::*, watch, mpsc::{self, *}}};
use std::sync::{Arc};
use flashmap::{self, new};
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogEntry{
    Insert{
        key: String,
        data: Data,
        index: u32,
        term: u32
    },
    Delete{
        key: String,
        index: u32,
        term: u32
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum RaftState{
    Leader,
    Canidate,
    Follower
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct RaftCore {
    max_committed: u32,
    max_received: u32,
    current_term: u32,
}

async fn log_manager(log: Arc<RwLock<Vec<LogEntry>>>, mut data: flashmap::WriteHandle<String, Data>, core: Arc<RwLock<RaftCore>>){
    let mut current_index = 0;
    loop{
        let mut write_guard = data.guard();
        let new_index = core.read().await.max_committed;

        if (new_index > current_index) {
            let log_handle = log.read().await;
            let new_entries = log_handle.clone().into_iter().filter(|x| {
                match x {
                    LogEntry::Insert { key, data, index, term } => *index > current_index && *index <= new_index,
                    LogEntry::Delete { key, index, term } => *index > current_index && *index <= new_index,
                }
            });
            for e in new_entries {
                match e {
                    LogEntry::Insert { key, data, index, term } => write_guard.insert(key, data),
                    LogEntry::Delete { key, index, term } => write_guard.remove(key)
                };
            }
            write_guard.publish();
            current_index = new_index;
        }
        else{
            eprintln!("Failed to get index update");
        }
    }
}

//TODO: Write as a tcp connection, dont bother with http
async fn raft_state_manager(state_ref: watch::Receiver<RaftState>, state_updater: watch::Sender<RaftState>) -> Result<(), std::io::Error> {
    let addr = SocketAddr::from(([0,0,0,0], 3010));
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);
    loop{
        let state = *state_ref.borrow();
        match state {
            RaftState::Follower => {
                if let Ok(Ok((socket, _))) = tokio::time::timeout(tokio::time::Duration::from_secs(1), listener.accept()).await {
                    todo!("yay, state was {:?}", state);
                    
                    //let io = TokioIo::new(stream);
                }
                else{
                    eprintln!("Changing to Canidate");
                    state_updater.send(RaftState::Canidate);
                }
            }
            RaftState::Canidate => {
                eprintln!("State is {:?}. Changing to Leader", state);
                state_updater.send(RaftState::Leader);

            }
            RaftState::Leader => {
                //eprintln!("I'm the leader.");
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
            }
        }              
}
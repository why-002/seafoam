use anyhow::Error;
use flashmap::{self};
use rand::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::net::TcpListener;
use tokio::sync::{
    watch::{self},
    RwLock,
};

mod data;
mod requests;
mod responses;
pub use self::data::{Data, LogEntry, RaftCore, RaftState};
pub use self::requests::RaftManagementRequest;
use self::requests::{send_heartbeat, send_vote_request};
use self::responses::handle_management_request;
pub use self::responses::RaftManagementResponse;

pub async fn log_manager(
    log: Arc<RwLock<Vec<LogEntry>>>,
    data: flashmap::WriteHandle<String, Data>,
    core: Arc<RwLock<RaftCore>>,
) {
    let mut current_index = 0;
    let mut writer = data;
    loop {
        let c = core.read().await;
        let new_index = c.max_committed;
        drop(c);
        if new_index > current_index {
            let log_handle = log.read().await;
            let new_entries = log_handle.clone().into_iter().filter(|x| match x {
                LogEntry::Insert {
                    key: _,
                    data: _,
                    index,
                    term: _,
                } => *index > current_index && *index <= new_index,
                LogEntry::Delete {
                    key: _,
                    index,
                    term: _,
                } => *index > current_index && *index <= new_index,
            });
            let mut write_guard = writer.guard();
            for e in new_entries {
                match e {
                    LogEntry::Insert {
                        key,
                        data,
                        index: _,
                        term: _,
                    } => {
                        if key == "foo" {
                            eprintln!("logged foo at: {:?}", SystemTime::now());
                        }
                        write_guard.insert(key, data)
                    }
                    LogEntry::Delete {
                        key,
                        index: _,
                        term: _,
                    } => write_guard.remove(key),
                };
            }
            write_guard.publish();
            current_index = new_index.min(log_handle.len());
        }
    }
}

pub async fn raft_state_manager(
    state_ref: watch::Receiver<RaftState>,
    state_updater: watch::Sender<RaftState>,
    core: Arc<RwLock<RaftCore>>,
    log: Arc<RwLock<Vec<LogEntry>>>,
    port: u16,
) -> Result<(), std::io::Error> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
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
        loop {
            let state_ref_copy = state_ref_copy.clone();
            let state = state_ref_copy.borrow().clone();
            match state {
                RaftState::Canidate(term) => {
                    let won_election = tokio::time::timeout(
                        tokio::time::Duration::from_millis(100),
                        run_election(core_copy.clone()),
                    )
                    .await;
                    let current_state = state_ref_copy.borrow().clone();
                    eprintln!("Finished election");
                    if let Ok(Ok(true)) = won_election {
                        eprintln!("won");
                        let state_updater = state_copy.write().await;
                        state_updater.send(RaftState::Leader(term));
                        eprintln!("State is {:?}. Changing to Leader", state);
                        continue;
                    } else if let RaftState::Canidate(_) = current_state {
                        let state_updater = state_copy.write().await;
                        let mut c = core_copy.write().await;
                        c.current_term += 1;
                        let term = c.current_term;
                        state_updater.send(RaftState::Canidate(term));
                        drop(state_updater);
                    }
                    eprintln!("Lost Election");
                    let r = (random::<u64>() % 200) + 300;
                    tokio::time::sleep(tokio::time::Duration::from_millis(r)).await;
                }
                RaftState::Leader(term) => {
                    let state_updater = state_copy.write().await;
                    let h = tokio::time::timeout(
                        tokio::time::Duration::from_millis(100),
                        send_global_heartbeat(core_copy.clone(), log_copy.clone()),
                    );
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
                    let state_updater = state_copy.write().await;
                    if !should_continue {
                        let mut c = core_copy.write().await;
                        c.current_term += 1;
                        state_updater.send(RaftState::Canidate(c.current_term));
                    } else {
                        state_updater.send(RaftState::Follower(term, addr, false));
                        drop(state_updater);
                        let r = (random::<u64>() % 200) + 300;
                        tokio::time::sleep(tokio::time::Duration::from_millis(r)).await;
                    }
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
                handle_management_request(
                    &mut socket,
                    core.clone(),
                    state_owner.clone(),
                    log.clone(),
                )
                .await
            });
        }
    }
}

// TODO: use is_finished() on spawned threads in order to do async, probably write a threadpool struct
async fn run_election(core: Arc<RwLock<RaftCore>>) -> Result<bool, Error> {
    let mut c = core.write().await;
    if c.last_voted >= c.current_term {
        c.current_term = c.last_voted;
        return Ok(false);
    }
    let members = c.members.clone();
    let target_votes = (members.len() + 1) / 2 + 1;
    let mut current_votes = 1;

    let request = RaftManagementRequest::RequestVote {
        current_term: c.current_term,
        max_received: c.max_received,
    };

    let mut pool = tokio::task::JoinSet::new();
    for address in members {
        pool.spawn(send_vote_request(address, request.clone()));
    }

    while !pool.is_empty() {
        if let Some(Ok(response)) = pool.join_next().await {
            match response {
                Ok(RaftManagementResponse::VoteOk {}) => current_votes += 1,
                Ok(RaftManagementResponse::VoteRejected {
                    current_term,
                    max_received: _,
                }) => {
                    eprintln!("Vote was rejected, updating term");
                    if c.current_term < current_term {
                        c.current_term = current_term;
                        return Ok(false);
                    }
                }
                _ => continue,
            }
            if current_votes >= target_votes {
                eprintln!("Votes {}:{}", current_votes, target_votes);
                return Ok(true);
            } else if current_votes + pool.len() < target_votes {
                eprintln!("Votes {}:{}", current_votes, target_votes);
                return Ok(false);
            }
        }
    }
    eprintln!("Votes {}:{}", current_votes, target_votes);
    return Ok(current_votes >= target_votes);
}

//TODO: rewrite so the heartbeats are not sync with each other
// Rewrite so it loops sending lower lasts until it succeded (requires them to be async from each other)
async fn send_global_heartbeat(
    core: Arc<RwLock<RaftCore>>,
    log: Arc<RwLock<Vec<LogEntry>>>,
) -> Result<usize, Error> {
    let l = log.read().await;
    let mut c = core.write().await;
    let mut max_recieved_members = Vec::new();

    let addresses = c.members.clone();

    if addresses.len() == 0 {
        c.max_received = l.len();
        c.max_committed = c.max_received;
        return Ok(c.current_term);
    }

    let new_logs = l
        .clone()
        .into_iter()
        .filter(|x| x.get_index() > c.max_received)
        .collect::<Vec<LogEntry>>();

    let last = l.get(c.max_received - 1).cloned();

    if let Some(last_entry) = l.last() {
        c.max_received = last_entry.get_index();
    }

    drop(l);

    let mut state = c.clone();

    let request = RaftManagementRequest::Heartbeat {
        latest_sent: last,
        current_term: state.current_term,
        commit_to: state.max_committed,
        log_entries: new_logs,
        address: c.address,
    };

    let mut pool = tokio::task::JoinSet::new();
    for address in addresses {
        let request = request.clone();

        /*
        returning the address with the response in order to have accesss in the while loop. Looking for a
            more idiomatic way to solve it, but for now this can work.
         */
        pool.spawn(async move {
            let x = send_heartbeat(address, request.clone()).await;
            return (x, address);
        });
    }

    while !pool.is_empty() {
        if let Some(Ok(response)) = pool.join_next().await {
            match response {
                (Ok(RaftManagementResponse::HeartbeatAddOne { max_received }), address) => {
                    let request;
                    let log = log.clone();
                    let l = log.read().await;
                    let latest_sent = l.get(max_received - 2).cloned();
                    if let Some(latest) = latest_sent {
                        let entries = l
                            .clone()
                            .into_iter()
                            .filter(|x| x.get_index() >= latest.get_index())
                            .collect();
                        request = RaftManagementRequest::Heartbeat {
                            latest_sent: Some(latest),
                            current_term: state.current_term,
                            commit_to: state.max_committed,
                            log_entries: entries,
                            address: c.address,
                        };
                    } else {
                        let entries = l.clone();
                        request = RaftManagementRequest::Heartbeat {
                            latest_sent: None,
                            current_term: state.current_term,
                            commit_to: state.max_committed,
                            log_entries: entries,
                            address: c.address,
                        };
                    }
                    eprintln!("Sent HeartbeatAddOne: {:?}", request);
                    pool.spawn(async move {
                        let x = send_heartbeat(address, request.clone()).await;
                        return (x, address);
                    });
                }
                (
                    Ok(RaftManagementResponse::HeartbeatOk {
                        max_received,
                        current_term,
                    }),
                    _,
                ) => {
                    eprintln!("heartbeat ok");
                    max_recieved_members.push(max_received);
                }
                (Ok(RaftManagementResponse::HeartbeatRejected { current_term }), _) => {
                    eprintln!("heartbeat reject");
                    state.current_term = state.current_term.max(current_term);
                }
                _ => continue,
            }
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

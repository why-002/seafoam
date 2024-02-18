use anyhow::Error;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::watch::{self},
};

use super::{Arc, LogEntry, RaftCore, RaftManagementRequest, RaftState, RwLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftManagementResponse {
    HeartbeatOk {
        max_received: usize,
        current_term: usize,
    },
    HeartbeatRejected {
        current_term: usize,
    },
    HeartbeatAddOne {
        max_received: usize,
    },
    VoteRejected {
        current_term: usize,
        max_received: usize,
    },
    VoteOk {},
}

impl RaftManagementResponse {
    pub async fn send_over_tcp_and_shutdown(&self, socket: &mut TcpStream) -> Result<(), Error> {
        let response = serde_json::to_vec(&self)?;
        socket.write_all(&response).await?;
        socket.shutdown().await?;
        Ok(())
    }
}

pub async fn handle_management_request(
    mut socket: &mut TcpStream,
    core: Arc<RwLock<RaftCore>>,
    state_owner: Arc<RwLock<watch::Sender<RaftState>>>,
    log: Arc<RwLock<Vec<LogEntry>>>,
) {
    let mut buf = Vec::new();
    socket
        .read_to_end(&mut buf)
        .await
        .expect("Failed to read message from socket");
    let req = serde_json::from_slice(&buf).expect("Failed to deserialize raft management request");

    match req {
        //Heartbeat needs to be reworked
        RaftManagementRequest::Heartbeat {
            latest_sent,
            current_term: message_current_term,
            commit_to,
            mut log_entries,
            address,
        } => {
            let c = core.read().await;

            if c.current_term > message_current_term {
                eprintln!(
                    "Rejected heartbeat, Self: {} Other: {}",
                    c.current_term, message_current_term
                );
                let response = RaftManagementResponse::HeartbeatRejected {
                    current_term: c.current_term,
                };
                response.send_over_tcp_and_shutdown(&mut socket).await;
                return;
            }

            let state_updater = state_owner.write().await;
            state_updater.send(RaftState::Follower(
                message_current_term,
                Some(address),
                true,
            ));
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

            let mut l = log.write().await;

            let response = generate_heartbeat_response(
                latest_sent.clone(),
                l.last().cloned(),
                old_received,
                current_term,
                log_entries.last().cloned(),
            );

            match &response {
                &RaftManagementResponse::HeartbeatOk {
                    max_received,
                    current_term: _,
                } => {
                    if let Some(la) = latest_sent {
                        l.drain(la.get_index()..);
                    }
                    l.append(&mut log_entries);
                    let mut c = core.write().await;
                    c.max_received = max_received;
                    c.max_committed = commit_to;
                }
                _ => {}
            }

            response.send_over_tcp_and_shutdown(socket).await;
            eprintln!("Accepted heartbeat");
        }
        RaftManagementRequest::RequestVote {
            current_term,
            max_received,
        } => {
            let c = core.read().await;
            eprintln!("responding to vote");
            let response = generate_vote_response(
                c.current_term,
                c.max_received,
                c.last_voted,
                current_term,
                max_received,
            );
            drop(c);
            if let RaftManagementResponse::VoteOk {} = response {
                let mut c = core.write().await;
                c.current_term = current_term;
                c.last_voted = current_term;
            }
            response.send_over_tcp_and_shutdown(&mut socket).await;
        }
    }
}

fn generate_heartbeat_response(
    latest_sent: Option<LogEntry>,
    last_match: Option<LogEntry>,
    old_received: usize,
    current_term: usize,
    last: Option<LogEntry>,
) -> RaftManagementResponse {
    let mut new_received = old_received;
    if let Some(last) = last {
        new_received = last.get_index();
    }

    let mut response = RaftManagementResponse::HeartbeatOk {
        max_received: new_received,
        current_term: current_term,
    };

    match (latest_sent, last_match) {
        (Some(x), Some(y)) => {
            if x != y {
                // latest_sent did not equal the same spot in current log, need to add-one
                response = RaftManagementResponse::HeartbeatAddOne {
                    max_received: x.get_index() - 1,
                };
                eprintln!("Responding with {:?}", response);
            }
        }
        (Some(x), None) => {
            // The entry did not have a corresponding match, need to call for a heartbeat add-one
            response = RaftManagementResponse::HeartbeatAddOne {
                max_received: x.get_index() - 1,
            };
        }
        (_, _) => {}
    }

    return response;
}

fn generate_vote_response(
    c_current_term: usize,
    c_max_received: usize,
    c_last_voted: usize,
    current_term: usize,
    max_received: usize,
) -> RaftManagementResponse {
    let response = RaftManagementResponse::VoteRejected {
        current_term: c_current_term,
        max_received: c_max_received,
    };
    if current_term <= c_last_voted {
        return response;
    } else if c_current_term > current_term {
        return response;
    } else if c_max_received > max_received {
        return response;
    } else {
        return RaftManagementResponse::VoteOk {};
    }
}

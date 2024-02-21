use anyhow::Error;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::watch::{self},
};

use super::{Arc, LogEntry, RaftCore, RaftManagementRequest, RaftState, RwLock};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

// Consider refactoring the heartbeat rejection to also be in generate_heartbeat_response
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
        // use address from socket in order to set address to send to instead of it being a part of the message
        RaftManagementRequest::Heartbeat {
            latest_sent,
            current_term: message_current_term,
            commit_to,
            mut log_entries,
            mut address,
        } => {
            let c = core.read().await;
            let x = socket.peer_addr().unwrap();
            address.set_ip(x.ip());
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
    latest_match: Option<LogEntry>,
    old_received: usize,
    current_term: usize,
    message_last_entry: Option<LogEntry>,
) -> RaftManagementResponse {
    let mut new_received = old_received;
    if let Some(last) = message_last_entry {
        new_received = last.get_index();
    }

    let mut response = RaftManagementResponse::HeartbeatOk {
        max_received: new_received,
        current_term: current_term,
    };

    match (latest_sent, latest_match) {
        (Some(x), Some(y)) => {
            if x != y {
                // latest_sent did not equal the same spot in current log, need to add-one
                response = RaftManagementResponse::HeartbeatAddOne {
                    max_received: old_received.min(x.get_index()),
                };
                eprintln!("Responding with {:?}", response);
            }
        }
        (Some(x), None) => {
            // The entry did not have a corresponding match, need to call for a heartbeat add-one
            response = RaftManagementResponse::HeartbeatAddOne {
                max_received: old_received.min(x.get_index()),
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

#[cfg(test)]
mod test {
    use crate::raft::{responses::generate_vote_response, Data, LogEntry, RaftManagementResponse};

    use super::generate_heartbeat_response;
    #[test]
    fn heartbeat_okay_none() {
        let latest_sent = None;
        let latest_match = None;
        let old_received = 1;
        let current_term = 1;
        let message_last_entry = None;
        assert_eq!(
            generate_heartbeat_response(
                latest_sent,
                latest_match,
                old_received,
                current_term,
                message_last_entry,
            ),
            RaftManagementResponse::HeartbeatOk {
                max_received: 1,
                current_term: 1
            }
        );
    }
    #[test]
    fn heartbeat_okay_some() {
        let latest_sent = Some(LogEntry::Insert {
            key: "foo".to_string(),
            data: Data::String("Bar".to_string()),
            index: 2,
            term: 1,
        });
        let latest_match = latest_sent.clone();
        let old_received = 1;
        let current_term = 1;
        let message_last_entry = latest_sent.clone();
        assert_eq!(
            generate_heartbeat_response(
                latest_sent,
                latest_match,
                old_received,
                current_term,
                message_last_entry,
            ),
            RaftManagementResponse::HeartbeatOk {
                max_received: 2,
                current_term: 1
            }
        );
    }
    #[test]
    fn heartbeat_last_set_received() {
        let latest_sent = Some(LogEntry::Insert {
            key: "foo".to_string(),
            data: Data::String("Bar".to_string()),
            index: 2,
            term: 1,
        });
        let latest_match = latest_sent.clone();
        let old_received = 1;
        let current_term = 1;
        let message_last_entry = Some(LogEntry::Insert {
            key: "foo".to_string(),
            data: Data::String("Bar".to_string()),
            index: 3,
            term: 1,
        });
        assert_eq!(
            generate_heartbeat_response(
                latest_sent,
                latest_match,
                old_received,
                current_term,
                message_last_entry,
            ),
            RaftManagementResponse::HeartbeatOk {
                max_received: 3,
                current_term: 1
            }
        );
    }
    #[test]
    fn heartbeat_match_is_none() {
        let latest_sent = Some(LogEntry::Insert {
            key: "foo".to_string(),
            data: Data::String("Bar".to_string()),
            index: 2,
            term: 1,
        });
        let latest_match = None;
        let old_received = 1;
        let current_term = 1;
        let message_last_entry = Some(LogEntry::Insert {
            key: "foo".to_string(),
            data: Data::String("Bar".to_string()),
            index: 3,
            term: 1,
        });
        assert_eq!(
            generate_heartbeat_response(
                latest_sent,
                latest_match,
                old_received,
                current_term,
                message_last_entry,
            ),
            RaftManagementResponse::HeartbeatAddOne { max_received: 1 }
        );
    }
    #[test]
    fn heartbeat_match_not_equal() {
        let latest_sent = Some(LogEntry::Insert {
            key: "foo".to_string(),
            data: Data::String("Bar".to_string()),
            index: 2,
            term: 1,
        });
        let latest_match = Some(LogEntry::Insert {
            key: "fo".to_string(),
            data: Data::String("Bar".to_string()),
            index: 2,
            term: 1,
        });
        let old_received = 1;
        let current_term = 1;
        let message_last_entry = Some(LogEntry::Insert {
            key: "foo".to_string(),
            data: Data::String("Bar".to_string()),
            index: 3,
            term: 1,
        });
        assert_eq!(
            generate_heartbeat_response(
                latest_sent,
                latest_match,
                old_received,
                current_term,
                message_last_entry,
            ),
            RaftManagementResponse::HeartbeatAddOne { max_received: 1 }
        );
    }
    #[test]
    fn heartbeat_match_none_low_reveived() {
        let latest_sent = Some(LogEntry::Insert {
            key: "foo".to_string(),
            data: Data::String("Bar".to_string()),
            index: 2,
            term: 1,
        });
        let latest_match = None;
        let old_received = 0;
        let current_term = 1;
        let message_last_entry = Some(LogEntry::Insert {
            key: "foo".to_string(),
            data: Data::String("Bar".to_string()),
            index: 3,
            term: 1,
        });
        assert_eq!(
            generate_heartbeat_response(
                latest_sent,
                latest_match,
                old_received,
                current_term,
                message_last_entry,
            ),
            RaftManagementResponse::HeartbeatAddOne { max_received: 0 }
        );
    }
    #[test]
    fn vote_okay() {
        let c_current_term = 5;
        let c_max_received = 1;
        let c_last_voted = 5;
        let current_term = 6;
        let max_received = 1;
        assert_eq!(
            generate_vote_response(
                c_current_term,
                c_max_received,
                c_last_voted,
                current_term,
                max_received
            ),
            RaftManagementResponse::VoteOk {}
        );
    }
    #[test]
    fn term_too_low() {
        let c_current_term = 5;
        let c_max_received = 1;
        let c_last_voted = 5;
        let current_term = 5; // Term needs to be at least one higher in order to be voted for
        let max_received = 1;
        assert_eq!(
            generate_vote_response(
                c_current_term,
                c_max_received,
                c_last_voted,
                current_term,
                max_received
            ),
            RaftManagementResponse::VoteRejected {
                current_term: c_current_term,
                max_received: c_max_received
            }
        );
    }
    #[test]
    fn received_too_low() {
        let c_current_term = 5;
        let c_max_received = 1;
        let c_last_voted = 5;
        let current_term = 6;
        let max_received = 0; // Needs to have received at least as many messages in order to be a valid canidate
        assert_eq!(
            generate_vote_response(
                c_current_term,
                c_max_received,
                c_last_voted,
                current_term,
                max_received
            ),
            RaftManagementResponse::VoteRejected {
                current_term: c_current_term,
                max_received: c_max_received
            }
        );
    }
    #[test]
    fn not_first_to_request_vote() {
        let c_current_term = 5;
        let c_max_received = 1;
        let c_last_voted = 6; // Needs to be the first to request a vote in a given election
        let current_term = 6;
        let max_received = 1;
        assert_eq!(
            generate_vote_response(
                c_current_term,
                c_max_received,
                c_last_voted,
                current_term,
                max_received
            ),
            RaftManagementResponse::VoteRejected {
                current_term: c_current_term,
                max_received: c_max_received
            }
        );
    }
}

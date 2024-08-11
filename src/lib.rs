use heartbeat_reply::HeartbeatType;
use http::{request, response};
use std::{net::SocketAddr, str::FromStr, sync::Arc};
use tokio::{
    net::unix::pipe::Receiver,
    sync::{
        watch::{self},
        Mutex, RwLock,
    },
};

pub mod raft;
use raft::RaftState;

pub mod grpc {
    tonic::include_proto!("seafoam");
}
use crate::raft::RaftCore;
use flashmap::{self, ReadHandle};
use grpc::seafoam_client::SeafoamClient;
use grpc::seafoam_server::{Seafoam, SeafoamServer};
use grpc::*;
use tonic::{Request, Response};

pub struct Server {
    pub db: ReadHandle<String, Object>,
    pub log: Arc<RwLock<Vec<LogEntry>>>,
    pub core_state: Arc<RwLock<RaftCore>>,
    pub raft_receiver: watch::Receiver<RaftState>,
    pub raft_sender: watch::Sender<RaftState>,
}

#[tonic::async_trait]
impl Seafoam for Server {
    async fn get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> Result<Response<GetReply>, tonic::Status> {
        let keys: Vec<String> = request.into_inner().keys;
        let mut resp = Vec::new();
        for key in keys {
            let data = self.db.guard().get(&key).cloned();
            match data {
                Some(data) => {
                    resp.push(data);
                }
                _ => {}
            }
        }
        return Ok(Response::new(GetReply { values: resp }));
    }

    async fn set(
        &self,
        request: tonic::Request<SetRequest>,
    ) -> Result<Response<SetReply>, tonic::Status> {
        let state = self.raft_receiver.borrow().clone();
        match state {
            RaftState::Leader(term) => {
                let message = request.into_inner();
                let key = message.key;
                let value = message.value;
                let mut write_lock = self.log.write().await;

                let index = write_lock.len() + 1;

                let log_entry = LogEntry::create_insert(index, term, key, value.unwrap());

                write_lock.push(log_entry);
                drop(write_lock);

                // Figure out an optimal way to trigger a response
                // while self.internal_state.read().await.max_committed < index {
                //     tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                // }
                // Create a min heap in the executor and give it the producer end of a message channel while keeping and waiting on the consumer end

                Ok(Response::new(SetReply {}))
            }
            RaftState::Follower(_, Some(leader), _) => {
                let leader = leader.clone();
                let mut client = SeafoamClient::connect(format!("http://{}", leader))
                    .await
                    .unwrap();
                client.set(request).await
            }
            _ => Err(tonic::Status::unavailable("No leader currently elected")),
        }
    }

    async fn delete(
        &self,
        request: tonic::Request<DeleteRequest>,
    ) -> Result<Response<DeleteReply>, tonic::Status> {
        let state = self.raft_receiver.borrow().clone();
        match state {
            RaftState::Leader(term) => {
                let message = request.into_inner();
                let key = message.key;
                let mut write_lock = self.log.write().await;

                let index = write_lock.len() + 1;
                let log_entry = LogEntry::create_delete(index, term, key);

                write_lock.push(log_entry);
                drop(write_lock);

                // Note: The same consideration about optimal response triggering applies here
                // as in the set function. You might want to implement a mechanism to ensure
                // the delete has been committed before responding.

                Ok(Response::new(DeleteReply {}))
            }
            RaftState::Follower(_, Some(leader), _) => {
                let leader = leader.clone();
                let mut client = SeafoamClient::connect(format!("http://{}", leader))
                    .await
                    .unwrap();
                client.delete(request).await
            }
            _ => Err(tonic::Status::unavailable("No leader currently elected")),
        }
    }

    async fn cas(
        &self,
        request: tonic::Request<CasRequest>,
    ) -> Result<Response<CasReply>, tonic::Status> {
        let state = self.raft_receiver.borrow().clone();
        match state {
            RaftState::Leader(term) => {
                let message = request.into_inner();
                let key = message.key;
                let old_value;
                let new_value;

                if message.old_value.is_none() {
                    return Err(tonic::Status::invalid_argument(
                        "Old value must be provided",
                    ));
                } else {
                    old_value = message.old_value.unwrap();
                }

                if message.new_value.is_none() {
                    return Err(tonic::Status::invalid_argument(
                        "New value must be provided",
                    ));
                } else {
                    new_value = message.new_value.unwrap();
                }

                let mut write_lock = self.log.write().await;

                let index = write_lock.len() + 1;
                let log_entry = LogEntry::create_cas(index, term, key, old_value, new_value);

                write_lock.push(log_entry);
                drop(write_lock);

                // Note: As with set and delete, you might want to implement a mechanism
                // to ensure the CaS has been committed before responding.

                Ok(Response::new(CasReply { success: true }))
            }
            RaftState::Follower(_, Some(leader), _) => {
                let leader = leader.clone();
                let mut client = SeafoamClient::connect(format!("http://{}", leader))
                    .await
                    .unwrap();
                client.cas(request).await
            }
            _ => Err(tonic::Status::unavailable("No leader currently elected")),
        }
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, tonic::Status> {
        let req = request.into_inner();
        let c = self.core_state.read().await;

        if c.current_term > req.current_term as usize {
            eprintln!(
                "Rejected heartbeat, Self: {} Other: {}",
                c.current_term, req.current_term
            );

            return Ok(Response::new(HeartbeatReply {
                current_term: Some(c.current_term as u64),
                htype: HeartbeatType::Rejected as i32,
                max_received: Some(c.max_received as u64),
            }));
        }

        self.raft_sender.send(RaftState::Follower(
            req.current_term as usize,
            Some(SocketAddr::from_str(req.address.as_str()).unwrap()),
            true,
        ));

        let old_received = c.max_received;
        let mut current_term = c.current_term;
        drop(c);

        if current_term < req.current_term as usize {
            let mut c = self.core_state.write().await;
            c.current_term = req.current_term as usize;
            current_term = c.current_term;
            drop(c);
        }

        let mut l = self.log.write().await;

        let mut new_received = old_received;
        if let Some(last) = req.log_entries.last() {
            new_received = last.get_index();
        }

        let mut response = HeartbeatReply {
            current_term: Some(current_term as u64),
            htype: HeartbeatType::Ok as i32,
            max_received: Some(new_received as u64),
        };

        match (req.latest_sent.clone(), l.last().cloned()) {
            (Some(x), Some(y)) => {
                if x != y {
                    response = HeartbeatReply {
                        current_term: None,
                        htype: HeartbeatType::AddOne as i32,
                        max_received: Some(old_received.min(x.get_index()) as u64),
                    };
                }
            }
            (Some(x), None) => {
                // The entry did not have a corresponding match, need to call for a heartbeat add-one
                response = HeartbeatReply {
                    current_term: None,
                    htype: HeartbeatType::AddOne as i32,
                    max_received: Some(old_received.min(x.get_index()) as u64),
                };
            }
            (_, _) => {}
        }

        if let HeartbeatType::Ok = response.get_type() {
            if let Some(la) = req.latest_sent {
                l.drain(la.get_index()..);
            }
            l.append(&mut req.log_entries.clone());
        }

        Ok(Response::new(response))
    }
    async fn request_vote(
        &self,
        req: tonic::Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteReply>, tonic::Status> {
        let mut s = self.core_state.write().await;
        let req_inner = req.into_inner();

        let response = Ok(Response::new(RequestVoteReply {
            current_term: Some(s.current_term as u64),
            max_received: Some(s.max_received as u64),
        }));

        if req_inner.current_term as usize <= s.last_voted {
            return response;
        } else if s.current_term > req_inner.current_term as usize {
            return response;
        } else if s.max_received > req_inner.max_received as usize {
            return response;
        }
        s.current_term = req_inner.current_term as usize;
        s.last_voted = req_inner.current_term as usize;

        Ok(Response::new(RequestVoteReply {
            current_term: None,
            max_received: None,
        }))
    }
}

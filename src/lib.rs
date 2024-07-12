use bytes::Bytes;
use core::panic;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use serde::{Deserialize, Serialize};
use std::{ops::DerefMut, sync::Arc};
use tokio::sync::{
    watch::{self},
    RwLock,
};

pub mod raft;
use raft::{Data, LogEntry, RaftState};

pub mod grpc {
    tonic::include_proto!("seafoam");
}
use crate::raft::RaftCore;
use flashmap::{self, ReadHandle};
use grpc::seafoam_client::SeafoamClient;
use grpc::seafoam_server::{Seafoam, SeafoamServer};
use grpc::*;
use tonic::{Request, Response};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Req {
    CaS {
        key: String,
        old_value: Data,
        new_value: Data,
        msg_id: u32,
    },
    Delete {
        key: String,
        msg_id: u32,
        error_if_not_key: bool,
    },
    Set {
        key: String,
        value: Data,
        msg_id: u32,
    },
    Get {
        key: String,
        msg_id: u32,
    },
    GetOk {
        value: Data,
        in_reply_to: u32,
    },
    DeleteOk {
        in_reply_to: u32,
        key: String,
    },
    CasOk {
        in_reply_to: u32,
        new_value: Data,
    },
    SetOk {
        in_reply_to: u32,
    },
    GetFailed,
    PipeReads {
        transactions: Vec<Req>,
    },
    PipeWrites {
        transactions: Vec<Req>,
    },
    PipedOk {
        responses: Vec<Req>,
    },
}

pub struct MyServer {
    pub db: ReadHandle<String, Data>,
    pub log: Arc<RwLock<Vec<LogEntry>>>,
    pub internal_state: Arc<RwLock<RaftCore>>,
    pub state_receiver: watch::Receiver<RaftState>,
}

#[tonic::async_trait]
impl Seafoam for MyServer {
    async fn get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> Result<Response<GetReply>, tonic::Status> {
        let keys: Vec<String> = request.into_inner().keys;
        let mut resp = Vec::new();
        for key in keys {
            let data = self.db.guard().get(&key).cloned();
            match data {
                None => resp.push(String::new()),
                data => {
                    resp.push(serde_json::to_string(&data).unwrap_or(String::new()));
                }
            }
        }
        return Ok(Response::new(GetReply { values: resp }));
    }

    async fn set(
        &self,
        request: tonic::Request<SetRequest>,
    ) -> Result<Response<SetReply>, tonic::Status> {
        // Make sure to implement proper logic to forward the request to the leader of the Raft cluster
        let state = self.state_receiver.borrow().clone();
        match state {
            RaftState::Leader(term) => {
                let message = request.into_inner();
                let key = message.key;
                let value = message.value;
                let mut write_lock = self.log.write().await;

                let index = write_lock.len() + 1;
                let log_entry = LogEntry::Insert {
                    key: key,
                    data: serde_json::from_str(&value).unwrap(),
                    index: index,
                    term: term,
                };

                write_lock.push(log_entry);
                drop(write_lock);

                // Figure out an optimal way to trigger a response
                // while self.internal_state.read().await.max_committed < index {
                //     tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                // }

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
        let state = self.state_receiver.borrow().clone();
        match state {
            RaftState::Leader(term) => {
                let message = request.into_inner();
                let key = message.key;
                let mut write_lock = self.log.write().await;

                let index = write_lock.len() + 1;
                let log_entry = LogEntry::Delete {
                    key: key,
                    index: index,
                    term: term,
                };

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
        let state = self.state_receiver.borrow().clone();
        match state {
            RaftState::Leader(term) => {
                let message = request.into_inner();
                let key = message.key;
                let old_value: Data = serde_json::from_str(&message.old_value).unwrap();
                let new_value: Data = serde_json::from_str(&message.new_value).unwrap();

                let mut write_lock = self.log.write().await;

                let index = write_lock.len() + 1;
                let log_entry = LogEntry::Cas {
                    key: key,
                    old_value: old_value,
                    new_value: new_value,
                    index: index,
                    term: term,
                };

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
}

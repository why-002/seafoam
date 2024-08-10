use crate::{
    seafoam_client::SeafoamClient, HeartbeatReply, HeartbeatRequest, LogEntry, RequestVoteReply,
    RequestVoteRequest,
};
use anyhow::Error;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tonic::transport::channel;
use tonic::{client, Response};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftManagementRequest {}

pub async fn send_heartbeat(
    address: SocketAddr,
    request: HeartbeatRequest,
) -> Result<HeartbeatReply, Error> {
    let endpoint = channel::Endpoint::from_shared(address.to_string()).unwrap();
    let mut client = SeafoamClient::new(endpoint.connect().await?);
    let result = tokio::time::timeout(Duration::from_millis(75), async {
        client.heartbeat(request).await
    });

    return Ok(result.await.unwrap()?.into_inner());
}

pub async fn send_vote_request(
    address: SocketAddr,
    request: RequestVoteRequest,
) -> Result<RequestVoteReply, Error> {
    let endpoint = channel::Endpoint::from_shared(address.to_string()).unwrap();
    let mut client = SeafoamClient::new(endpoint.connect().await?);
    let result = tokio::time::timeout(Duration::from_millis(75), async {
        client.request_vote(request).await
    });
    return Ok(result.await.unwrap()?.into_inner());
}

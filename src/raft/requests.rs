use crate::{
    seafoam_client::SeafoamClient, HeartbeatReply, HeartbeatRequest, LogEntry, RequestVoteReply,
    RequestVoteRequest,
};
use anyhow::Error;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tonic::{transport::channel, Request};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftManagementRequest {}

pub async fn send_heartbeat(
    address: SocketAddr,
    request: HeartbeatRequest,
) -> Result<HeartbeatReply, Error> {
    let endpoint =
        channel::Endpoint::from_shared("http://".to_string() + &address.to_string()).unwrap();

    let mut client = SeafoamClient::new(endpoint.connect().await?);
    let result = tokio::time::timeout(tokio::time::Duration::from_millis(100), async {
        client.heartbeat(request).await
    })
    .await;

    return Ok(result??.into_inner());
}

pub async fn send_vote_request(
    address: SocketAddr,
    request: RequestVoteRequest,
) -> Result<RequestVoteReply, Error> {
    let endpoint =
        channel::Endpoint::from_shared("http://".to_string() + &address.to_string()).unwrap();

    let mut client = SeafoamClient::new(endpoint.connect().await?);
    let result = tokio::time::timeout(tokio::time::Duration::from_millis(100), async {
        client.request_vote(request).await
    })
    .await;
    return Ok(result??.into_inner());
}

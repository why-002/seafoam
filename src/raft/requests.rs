use crate::LogEntry;
use anyhow::Error;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use super::RaftManagementResponse;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftManagementRequest {
    Heartbeat {
        latest_sent: Option<LogEntry>,
        current_term: usize,
        commit_to: usize,
        log_entries: Vec<LogEntry>,
        address: SocketAddr,
    },
    RequestVote {
        current_term: usize,
        max_received: usize,
    },
}

impl RaftManagementRequest {
    pub async fn send_over_tcp_and_shutdown(&self, socket: &mut TcpStream) -> Result<(), Error> {
        let response = serde_json::to_vec(&self)?;
        socket.write_all(&response).await?;
        socket.shutdown().await?;
        Ok(())
    }
}

pub async fn send_heartbeat(
    address: SocketAddr,
    request: RaftManagementRequest,
) -> Result<RaftManagementResponse, Error> {
    let result = tokio::time::timeout(Duration::from_millis(75), async {
        let mut stream = TcpStream::connect(address).await?;
        request.send_over_tcp_and_shutdown(&mut stream).await?;
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await?;
        let response: RaftManagementResponse = serde_json::from_slice(&buf)?;
        return Ok(response);
    })
    .await?;

    return result;
}

pub async fn send_vote_request(
    address: SocketAddr,
    request: RaftManagementRequest,
) -> Result<RaftManagementResponse, Error> {
    let result = tokio::time::timeout(Duration::from_millis(75), async {
        let mut stream = TcpStream::connect(address).await?;
        request.send_over_tcp_and_shutdown(&mut stream).await?;
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await?;
        let response: RaftManagementResponse = serde_json::from_slice(&buf)?;
        return Ok(response);
    })
    .await?;

    return result;
}

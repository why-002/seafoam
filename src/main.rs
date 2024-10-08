use hyper::server::conn::http1;
use seafoam::grpc::Object;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::env;
use std::hash::Hash;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Instant;
use tonic::transport::Server;

use flashmap::{self, ReadHandle};
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{watch, Mutex, RwLock};
use tonic::Response;

use seafoam::raft::{self, log_manager, raft_state_manager, RaftCore, RaftState};

use seafoam::grpc::{seafoam_server::SeafoamServer, LogEntry};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 || args[1] == "--help" || args[1] == "-h" {
        println!(
            "Usage: {} <client port> <management port> <member1> <member2> ...",
            args[0]
        );
        return Ok(());
    }

    let client_port = args[1].parse::<u16>().expect("Invalid client port");
    for i in 2..args.len() {
        let _ = SocketAddr::from_str(&args[i]).expect("Invalid member address");
    }

    let log: Arc<RwLock<Vec<LogEntry>>> = Arc::new(RwLock::new(Vec::new()));

    let channels = watch::channel(RaftState::Follower(0, None, true));
    let raft_state_sender = channels.0.clone();
    let raft_state_receiver = channels.1.clone();

    let core_state = Arc::new(RwLock::new(RaftCore {
        max_committed: 0,
        max_received: 0,
        current_term: 1,
        members: Vec::new(),
        address: SocketAddr::from(([0, 0, 0, 0], client_port)),
        last_voted: 0,
    }));

    let addr = SocketAddr::from(([0, 0, 0, 0], client_port));

    let mut writer = core_state.write().await;
    let max = args.len();
    let mut i = 2;
    while i < max {
        writer.members.push(SocketAddr::from_str(&args[i]).unwrap());
        i += 1;
    }
    drop(writer);

    let (writer, reader) = flashmap::new();

    let l = log.clone();
    let i = core_state.clone();

    let _ = tokio::task::spawn(log_manager(l, writer, i.clone()));
    tokio::task::spawn(raft_state_manager(
        raft_state_sender.clone(),
        raft_state_receiver.clone(),
        core_state.clone(),
        log.clone(),
    ));

    let kv_store = seafoam::Server {
        db: reader.clone(),
        log: log.clone(),
        core_state: core_state.clone(),
        raft_sender: raft_state_sender.clone(),
        raft_receiver: raft_state_receiver.clone(),
    };

    println!("Listening on http://{}", addr);
    Ok(tokio::spawn(async move {
        let addr = format!("0.0.0.0:{}", client_port);
        let add = addr.parse().unwrap();
        Server::builder()
            .add_service(SeafoamServer::new(kv_store))
            .serve(add)
            .await
    })
    .await??)
}

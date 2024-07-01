use hyper::server::conn::http1;
use seafoam::raft::LogEntry;
use std::borrow::Borrow;
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Instant;
use tonic::transport::Server;

use flashmap::{self, ReadHandle};
use grpc::seafoam_client::SeafoamClient;
use hyper::service::service_fn;
use hyper::Request;
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{watch, RwLock};
use tonic::Response;

use seafoam::{
    kv_store,
    raft::{log_manager, raft_state_manager, Data, RaftCore, RaftState},
};

pub mod grpc {
    tonic::include_proto!("seafoam");
}

use grpc::seafoam_server::{Seafoam, SeafoamServer};
use grpc::*;

pub struct MyServer {
    db: ReadHandle<String, Data>,
    log: Arc<RwLock<Vec<LogEntry>>>,
    internal_state: Arc<RwLock<RaftCore>>,
    state_receiver: watch::Receiver<RaftState>,
}

#[tonic::async_trait]
impl Seafoam for MyServer {
    async fn get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> Result<Response<GetReply>, tonic::Status> {
        let key: String = request.into_inner().key;
        let start = Instant::now();
        let data = self.db.guard().get(&key).cloned();
        let end = start.elapsed().as_millis();
        if end > 10 {
            println!("{}", end);
        }

        match data {
            None => Ok(Response::new(GetReply { value: None })),
            data => {
                let resp = GetReply {
                    value: Some(serde_json::to_string(&data).unwrap()),
                };
                Ok(Response::new(resp))
            }
        }
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 || args[1] == "--help" || args[1] == "-h" {
        println!(
            "Usage: {} <client port> <management port> <member1> <member2> ...",
            args[0]
        );
        return Ok(());
    }

    let client_port = args[1].parse::<u16>().expect("Invalid client port");
    let _ = args[2].parse::<u16>().expect("Invalid management port");
    for i in 3..args.len() {
        let _ = SocketAddr::from_str(&args[i]).expect("Invalid member address");
    }

    let log: Arc<RwLock<Vec<LogEntry>>> = Arc::new(RwLock::new(Vec::new()));
    let (state_sender, state_receiver) = watch::channel(RaftState::Follower(0, None, true));

    let internal_state = Arc::new(RwLock::new(RaftCore {
        max_committed: 0,
        max_received: 0,
        current_term: 1,
        members: Vec::new(),
        address: SocketAddr::from(([0, 0, 0, 0], client_port)),
        last_voted: 0,
    }));

    let addr = SocketAddr::from(([0, 0, 0, 0], client_port));

    let mut writer = internal_state.write().await;
    let max = args.len();
    let mut i = 3;
    while i < max {
        writer.members.push(SocketAddr::from_str(&args[i]).unwrap());
        i += 1;
    }
    drop(writer);

    let (writer, reader) = flashmap::new();
    // This is wyatt's bad idea
    let greeter = MyServer {
        db: reader.clone(),
        log: log.clone(),
        internal_state: internal_state.clone(),
        state_receiver: state_receiver.clone(),
    };

    tokio::spawn(async {
        let add = "[::1]:50051".parse().unwrap();
        Server::builder()
            .add_service(SeafoamServer::new(greeter))
            .serve(add)
            .await
    })
    .await?;
    // This is normal code

    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);

    let l = log.clone();
    let i = internal_state.clone();

    let _ = tokio::task::spawn(log_manager(l, writer, i.clone()));
    tokio::task::spawn(raft_state_manager(
        state_receiver.clone(),
        state_sender,
        internal_state.clone(),
        log.clone(),
        args[2].parse::<u16>().unwrap(),
    ));

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        let new_reader = reader.clone();
        let v = log.clone();
        let s = state_receiver.clone();
        let service = service_fn(move |req: Request<hyper::body::Incoming>| {
            kv_store(req, new_reader.clone(), v.clone(), s.clone())
        });
        let http = http1::Builder::new();

        tokio::task::spawn(async move {
            if let Err(err) = http.serve_connection(io, service).await {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}

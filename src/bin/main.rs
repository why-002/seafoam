use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::body::Frame;
use hyper::server::conn::http1;
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::SystemTime;

use hyper::service::service_fn;
use hyper::{body::Body, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use flashmap;
use seafoam::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{
    mpsc::{self, *},
    watch,
    watch::*,
    RwLock,
};

async fn kv_store(
    req: Request<hyper::body::Incoming>,
    reader: flashmap::ReadHandle<String, Data>,
    v: Arc<RwLock<Vec<LogEntry>>>,
    state: watch::Receiver<RaftState>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        // Serve some instructions at /
        (&Method::GET, "/") => Ok(Response::new(full("Hello world"))),

        (&Method::POST, "/get") => {
            let frame_stream = req.into_body().map_frame(move |frame| {
                let frame = if let Ok(data) = frame.into_data() {
                    let obj: Req = serde_json::from_slice(&data).unwrap();
                    match obj {
                        Req::G { key, msg_id } => {
                            let guard = reader.guard();
                            let val = guard.get(&key);
                            if let Some(data) = val {
                                let st = serde_json::to_string(&Req::GOk {
                                    value: data.to_owned(),
                                    in_reply_to: msg_id,
                                })
                                .unwrap();
                                st.as_bytes().into_iter().map(|byte| *byte).collect()
                            } else {
                                Bytes::new()
                            }
                        }
                        _ => panic!(),
                    }
                } else {
                    Bytes::new()
                };

                Frame::data(frame)
            });

            Ok(Response::new(frame_stream.boxed()))
        }
        (&Method::POST, "/set") => {
            let mut resp = Response::builder();
            let mut body = req.into_body();
            let frame_stream = body.frame();
            let data = frame_stream.await.unwrap().unwrap().into_data().unwrap();
            let frame;
            let obj: Req = serde_json::from_slice(&data).unwrap();
            match obj {
                Req::S { key, value, msg_id } => {
                    let f = state.borrow().clone();
                    let v = v.clone();
                    if key == "foo" {
                        eprintln!("Received foo at: {:?}", SystemTime::now());
                    }
                    match f {
                        RaftState::Leader(term) => {
                            tokio::task::spawn(async move {
                                let mut writer = v.write().await;
                                let size = writer.len() + 1;
                                writer.push(LogEntry::Insert {
                                    key: key,
                                    data: value,
                                    index: size,
                                    term: term,
                                })
                            });
                            let st = serde_json::to_string(&Req::SOk {
                                in_reply_to: msg_id,
                            })
                            .unwrap();
                            frame = st.as_bytes().into_iter().map(|byte| *byte).collect();
                        }
                        RaftState::Follower(_, addr, _) => {
                            if let Some(addr) = addr {
                                resp = resp
                                    .header(
                                        "Location",
                                        "http://".to_owned() + &addr.to_string() + "/set",
                                    )
                                    .status(307);
                                frame = Bytes::new();
                            } else {
                                resp = resp.status(500);
                                frame = Bytes::new();
                            }
                        }
                        RaftState::Canidate(_) => {
                            resp = resp.status(500);
                            frame = Bytes::new();
                        }
                    }
                }
                _ => panic!("{:?}", obj),
            }
            Ok(resp
                .body(Full::new(frame).map_err(|never| match never {}).boxed())
                .unwrap())
        }

        // Return the 404 Not Found for other routes.
        _ => {
            let mut not_found = Response::new(empty());
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

fn empty() -> BoxBody<Bytes, hyper::Error> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, hyper::Error> {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args: Vec<String> = env::args().collect();
    let log = Arc::new(RwLock::new(Vec::new()));
    let (state_sender, state_receiver) = watch::channel(RaftState::Follower(0, None, true));
    let internal_state = Arc::new(RwLock::new(RaftCore {
        max_committed: 0,
        max_received: 0,
        current_term: 1,
        members: Vec::new(),
        address: SocketAddr::from(([0, 0, 0, 0], args[1].parse::<u16>().unwrap())),
        last_voted: 0,
    }));

    let addr = SocketAddr::from(([0, 0, 0, 0], args[1].parse::<u16>().unwrap()));

    let mut writer = internal_state.write().await;
    let max = args.len();
    let mut i = 3;
    while i < max {
        writer.members.push(SocketAddr::from_str(&args[i]).unwrap());
        i += 1;
    }
    drop(writer);

    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);

    let (writer, reader) = flashmap::new();

    let l = log.clone();
    let i = internal_state.clone();
    let _ = tokio::task::spawn(async move { log_manager(l, writer, i.clone()).await });

    let s = state_receiver.clone();
    tokio::task::spawn(raft_state_manager(
        s,
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

use std::net::SocketAddr;
use std::sync::mpsc::channel;
use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::body::Frame;
use hyper::server::conn::http1;

use hyper::service::service_fn;
use hyper::{body::Body, Method, Request, Response, StatusCode};
use hyper_util::server::conn::auto::Http1Builder;
use tokio::net::TcpListener;
use hyper_util::rt::TokioIo;

use std::sync::{Arc};
use tokio::sync::{RwLock, watch::*, watch, mpsc::{self, *}};
use std::collections::{HashMap, HashSet, BTreeMap};
use serde::{Serialize, Deserialize};
use flashmap;
use seafoam::*; 

async fn bar(req: Request<hyper::body::Incoming>) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    return Ok(Response::new(full("")));
}

async fn echo(
     req: Request<hyper::body::Incoming>, reader: flashmap::ReadHandle<String, Data>, v: Arc<RwLock<Vec<LogEntry>>>, state: watch::Receiver<RaftState>
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        // Serve some instructions at /
        (&Method::GET, "/") => Ok(Response::new(full(
            "Hello world",
        ))),

        (&Method::POST, "/get") => {
            let frame_stream = req.into_body().map_frame(move |frame| {
                let frame = if let Ok(data) = frame.into_data() {
                    let obj: Req = serde_json::from_slice(&data).unwrap();
                    match obj {
                        Req::G { key , msg_id} => {
                            let guard = reader.guard();
                            let val = guard.get(&key);
                            if let Some(data) = val {
                                let st = serde_json::to_string(&Req::GOk { value: data.to_owned(), in_reply_to: msg_id }).unwrap();
                                st.as_bytes().into_iter().map(|byte| *byte).collect()
                            } else {
                                Bytes::new()
                            }
                        }
                        _ => panic!()
                    }
                } else {
                    Bytes::new()
                };

                Frame::data(frame)
            });

            Ok(Response::new(frame_stream.boxed()))
        }
        (&Method::POST, "/set") => {
            let frame_stream = req.into_body().map_frame(move |frame| {
                let frame = if let Ok(data) = frame.into_data() {
                    let obj: Req = serde_json::from_slice(&data).unwrap();
                    match obj {
                        Req::S { key, value, msg_id } => {
                            let f = state.clone();
                            let v = v.clone();
                            tokio::task::spawn(async move {
                                let s = *f.borrow();
                                match &s {
                                    RaftState::Leader(term) => {
                                        let mut writer = v.write().await;
                                        let size = writer.len();
                                        writer.push(LogEntry::Insert { key: key, data: value, index: size, term: *term })
                                    }
                                    _ => {
                                        todo!();
                                    }
                                }
                            });
                            
                            let st = serde_json::to_string(&Req::SOk { in_reply_to: msg_id }).unwrap();
                            st.as_bytes().into_iter().map(|byte| *byte).collect()
                        }
                        _ => panic!("{:?}",obj )
                    }
                } else {
                    Bytes::new()
                };

                Frame::data(frame)
            });

            Ok(Response::new(frame_stream.boxed()))
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
    let addresses: Vec<String> = Vec::new();
    let log = Arc::new(RwLock::new(Vec::new()));
    let (state_sender, state_receiver) = watch::channel(RaftState::Follower(0));
    let internal_state = Arc::new(RwLock::new(RaftCore { max_committed: 0, max_received: 0, current_term: 0, members: Vec::new() }));

    let addr = SocketAddr::from(([0,0,0,0], 3000));
    
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);
    
    let (mut writer, reader) = flashmap::new();

    let l = log.clone();
    let i = internal_state.clone();
    let w = tokio::task::spawn( async move { 
        log_manager(l, writer, i.clone()).await
    });
    let s = state_receiver.clone();
    tokio::task::spawn(raft_state_manager(s, state_sender, internal_state.clone(), log.clone()));
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        let new_reader = reader.clone();
        let v = log.clone();
        let s = state_receiver.clone();
        let service = service_fn(move |x|{
            let l = v.clone();
            let r = new_reader.clone();
            let s = s.clone();
            async move{
                let a = echo( x, r, l, s).await;
                a
            }
        });
        let http = http1::Builder::new();
        tokio::task::spawn(async move {
            if let Err(err) = http
                .serve_connection(io, service)
                .await
            {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}
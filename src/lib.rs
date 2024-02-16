use bytes::Bytes;
use core::time;
use flashmap::{self, new};
use futures_util::{future::poll_fn, Future, FutureExt};
use http::{request, response};
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::body::Frame;
use hyper::server::conn::http1;
use hyper::{body::Body, Method, Request, Response, StatusCode};
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use std::{
    any::Any,
    arch::x86_64::CpuidResult,
    array,
    collections::{hash_map::RandomState, BTreeMap, HashMap, HashSet},
    ops::Add,
    process::Output,
    task::Poll,
    thread::{current, spawn},
    time::Duration,
    usize::MIN,
};
use tokio::net::TcpListener;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{
        mpsc::{self, *},
        watch::{self, *},
        RwLock,
    },
};

pub mod raft;
use raft::{Data, LogEntry, RaftState};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Req {
    S {
        key: String,
        value: Data,
        msg_id: u32,
    },
    G {
        key: String,
        msg_id: u32,
    },
    GOk {
        value: Data,
        in_reply_to: u32,
    },
    SOk {
        in_reply_to: u32,
    },
}

pub async fn kv_store(
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

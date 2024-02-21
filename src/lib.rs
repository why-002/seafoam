use bytes::Bytes;
use flashmap::{self, new};
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::body::Frame;
use hyper::{Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{
    watch::{self, *},
    RwLock,
};

pub mod raft;
use raft::{Data, LogEntry, RaftState};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Req {
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
    SetOk {
        in_reply_to: u32,
    },
    GetFailed,
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
            let mut resp = Response::builder();
            let mut body = req.into_body();
            let frame_stream = body.frame();
            let data = frame_stream.await.unwrap().unwrap().into_data().unwrap();
            let frame;
            let obj: Req = serde_json::from_slice(&data).unwrap();

            match obj {
                Req::Get { key, msg_id } => {
                    let guard = reader.guard();
                    let val = guard.get(&key);
                    if let Some(data) = val {
                        let st = serde_json::to_string(&Req::GetOk {
                            value: data.to_owned(),
                            in_reply_to: msg_id,
                        })
                        .unwrap();
                        frame = st.as_bytes().into_iter().map(|byte| *byte).collect();
                    } else {
                        frame = Bytes::new();
                        resp = resp.status(404);
                    }
                }
                _ => panic!(),
            }
            Ok(resp
                .body(Full::new(frame).map_err(|never| match never {}).boxed())
                .unwrap())
        }
        (&Method::POST, "/set") => {
            let mut resp = Response::builder();
            let mut body = req.into_body();
            let frame_stream = body.frame();
            let data = frame_stream.await.unwrap().unwrap().into_data().unwrap();
            let frame;
            let obj: Req = serde_json::from_slice(&data).unwrap();
            match obj {
                Req::Set { key, value, msg_id } => {
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
                            let st = serde_json::to_string(&Req::SetOk {
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
        (&Method::POST, "/delete") => {
            let mut resp = Response::builder();
            let mut body = req.into_body();
            let frame_stream = body.frame();
            let data = frame_stream.await.unwrap().unwrap().into_data().unwrap();
            let frame;
            let obj: Req = serde_json::from_slice(&data).unwrap();
            match obj {
                Req::Delete {
                    key,
                    msg_id,
                    error_if_not_key,
                } => {
                    let f = state.borrow().clone();
                    let v = v.clone();
                    if key == "foo" {
                        eprintln!("Received foo at: {:?}", SystemTime::now());
                    }
                    match f {
                        RaftState::Leader(term) => {
                            let k = key.clone();
                            tokio::task::spawn(async move {
                                let mut writer = v.write().await;
                                let size = writer.len() + 1;
                                writer.push(LogEntry::Delete {
                                    key: k,
                                    index: size,
                                    term: term,
                                })
                            });
                            let st = serde_json::to_string(&Req::DeleteOk {
                                in_reply_to: msg_id,
                                key: key,
                            })
                            .unwrap();
                            frame = st.as_bytes().into_iter().map(|byte| *byte).collect();
                        }
                        RaftState::Follower(_, addr, _) => {
                            if let Some(addr) = addr {
                                resp = resp
                                    .header(
                                        "Location",
                                        "http://".to_owned() + &addr.to_string() + "/delete",
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

use bytes::Bytes;
use core::panic;
use flashmap::{self};
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::{Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::{ops::DerefMut, sync::Arc};
use tokio::sync::{
    watch::{self},
    RwLock,
};

pub mod raft;
use raft::{Data, LogEntry, RaftState};

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

pub async fn kv_store(
    req: Request<hyper::body::Incoming>,
    reader: flashmap::ReadHandle<String, Data>,
    v: Arc<RwLock<Vec<LogEntry>>>,
    state: watch::Receiver<RaftState>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        // Serve some instructions at /
        (&Method::GET, "/") => Ok(Response::new(full("Hello world"))),
        (&Method::POST, "/") => {
            let mut resp = Response::builder();
            let mut body = req.into_body();
            let frame_stream = body.frame();
            let data = frame_stream.await.unwrap().unwrap().into_data().unwrap();
            let obj: Req = serde_json::from_slice(&data).unwrap();
            let f = state.borrow().clone();
            let mut frame = Bytes::new();
            match obj {
                Req::PipeReads { transactions } => {
                    let mut responses = Vec::new();
                    for obj in transactions {
                        if let Req::Get { key, msg_id } = obj {
                            let guard = reader.guard();
                            let val = guard.get(&key);
                            if let Some(data) = val {
                                responses.push(Req::GetOk {
                                    value: data.to_owned(),
                                    in_reply_to: msg_id,
                                });
                            } else {
                                responses.push(Req::GetFailed);
                            }
                        }
                    }
                    let st = serde_json::to_string(&Req::PipedOk {
                        responses: responses,
                    })
                    .unwrap();
                    frame = st.as_bytes().into_iter().map(|byte| *byte).collect();
                }
                _ => {
                    resp = resp.status(400);
                }
            }
            Ok(resp
                .body(Full::new(frame).map_err(|never| match never {}).boxed())
                .unwrap())
        }

        (&Method::POST, "/get") => {
            let mut resp = Response::builder();
            let mut body = req.into_body();
            let frame_stream = body.frame();
            let data = frame_stream.await.unwrap().unwrap().into_data().unwrap();
            let mut frame = Bytes::new();
            let obj: Req = serde_json::from_slice(&data).unwrap();

            if let Req::Get { key, msg_id } = obj {
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
            Ok(resp
                .body(Full::new(frame).map_err(|never| match never {}).boxed())
                .unwrap())
        }
        (&Method::POST, "/set") => {
            let mut resp = Response::builder();
            let mut body = req.into_body();
            let frame_stream = body.frame();
            let data = frame_stream.await.unwrap().unwrap().into_data().unwrap();
            let obj: Req = serde_json::from_slice(&data).unwrap();
            let f = state.borrow().clone();
            let mut writer = v.write().await;
            let mut frame = Bytes::new();

            if let Req::Set { key, value, msg_id } = obj.clone() {
                let (f, log) = generate_write_response(
                    obj,
                    &f,
                    "/set".to_string(),
                    &mut resp,
                    writer.len() + 1,
                );
                frame = f;
                if let Some(log) = log {
                    writer.push(log);
                }
            } else {
                resp = resp.status(400);
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
            let obj: Req = serde_json::from_slice(&data).unwrap();
            let f = state.borrow().clone();
            let mut writer = v.write().await;
            let mut frame = Bytes::new();
            if let Req::Delete {
                key,
                msg_id: _,
                error_if_not_key,
            } = obj.clone()
            {
                let (f, log) = generate_write_response(
                    obj,
                    &f,
                    "/delete".to_string(),
                    &mut resp,
                    writer.len() + 1,
                );
                frame = f;
                if let Some(log) = log {
                    if reader.guard().contains_key(&key) || !error_if_not_key {
                        writer.push(log);
                    } else {
                        resp = resp.status(400);
                        frame = Bytes::new();
                    }
                }
            } else {
                resp = resp.status(400);
            }
            Ok(resp
                .body(Full::new(frame).map_err(|never| match never {}).boxed())
                .unwrap())
        }
        (&Method::POST, "/cas") => {
            let mut resp = Response::builder();
            let mut body = req.into_body();
            let frame_stream = body.frame();
            let data = frame_stream.await.unwrap().unwrap().into_data().unwrap();
            let obj: Req = serde_json::from_slice(&data).unwrap();
            let f = state.borrow().clone();
            let mut writer = v.write().await;
            let mut frame = Bytes::new();
            if let Req::CaS {
                key,
                old_value,
                new_value,
                msg_id,
            } = obj.clone()
            {
                let (f, log) = generate_write_response(
                    obj,
                    &f,
                    "/cas".to_string(),
                    &mut resp,
                    writer.len() + 1,
                );
                frame = f;
                if let Some(log) = log {
                    if reader.guard().contains_key(&key) {
                        writer.push(log);
                    } else {
                        resp = resp.status(400);
                        frame = Bytes::new();
                    }
                }
            } else {
                resp = resp.status(400);
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

fn generate_write_response(
    req: Req,
    state: &RaftState,
    uri: String,
    mut resp: &mut http::response::Builder,
    size: usize,
) -> (Bytes, Option<LogEntry>) {
    match state {
        &RaftState::Canidate(_) => {
            *resp = Response::builder().status(500);
        }
        &RaftState::Follower(_, addr, _) => {
            if let Some(addr) = addr {
                *resp = Response::builder()
                    .header("Location", "http://".to_owned() + &addr.to_string() + &uri)
                    .status(307);
            } else {
                *resp = Response::builder().status(500);
            }
        }
        &RaftState::Leader(term) => match req {
            Req::Set { key, value, msg_id } => {
                let st = serde_json::to_string(&Req::SetOk {
                    in_reply_to: msg_id,
                })
                .unwrap();
                let bytes = st.as_bytes().into_iter().map(|byte| *byte).collect();

                return (
                    bytes,
                    Some(LogEntry::Insert {
                        key: key,
                        data: value,
                        index: size,
                        term: term,
                    }),
                );
            }
            Req::Delete {
                key,
                msg_id,
                error_if_not_key,
            } => {
                let st = serde_json::to_string(&Req::DeleteOk {
                    in_reply_to: msg_id,
                    key: key.to_owned(),
                })
                .unwrap();
                let bytes = st.as_bytes().into_iter().map(|byte| *byte).collect();
                return (
                    bytes,
                    Some(LogEntry::Delete {
                        key: key,
                        index: size,
                        term: term,
                    }),
                );
            }
            Req::CaS {
                key,
                old_value,
                new_value,
                msg_id,
            } => {
                let st = serde_json::to_string(&Req::CasOk {
                    in_reply_to: msg_id,
                    new_value: new_value.clone(),
                })
                .unwrap();
                let bytes = st.as_bytes().into_iter().map(|byte| *byte).collect();
                return (
                    bytes,
                    Some(LogEntry::Cas {
                        key: key,
                        old_value: old_value,
                        new_value: new_value,
                        index: size,
                        term: term,
                    }),
                );
            }
            _ => {}
        },
    }
    return (Bytes::new(), None);
}

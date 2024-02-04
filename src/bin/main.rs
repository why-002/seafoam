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

async fn foo(state_ref: watch::Receiver<RaftState>, state_updater: watch::Sender<RaftState>) -> Result<(), std::io::Error> {
    let addr = SocketAddr::from(([0,0,0,0], 3010));
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);
    loop{
        let state = *state_ref.borrow();
        match state {
            RaftState::Follower => {
                if let Ok(Ok((socket, _))) = tokio::time::timeout(tokio::time::Duration::from_secs(1), listener.accept()).await {
                    todo!("yay, state was {:?}", state);
                    //let io = TokioIo::new(stream);
                }
                else{
                    eprintln!("Changing to Canidate");
                    state_updater.send(RaftState::Canidate);
                }
            }
            RaftState::Canidate => {
                eprintln!("State is {:?}. Changing to Leader", state);
                state_updater.send(RaftState::Leader);

            }
            RaftState::Leader => {
                //eprintln!("I'm the leader.");
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
            }
        }              
}

async fn echo(
     req: Request<hyper::body::Incoming>, reader: flashmap::ReadHandle<String, Data>, se: tokio::sync::mpsc::Sender<(String, Data)>
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
                            let s = se.clone();
                            tokio::task::spawn( async move {
                                s.send((key, value)).await;
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
    let log: Vec<LogEntry> = Vec::new();
    let (state_sender, state_receiver) = watch::channel(RaftState::Follower);
    //let internal_state = RaftCore {  };

    let addr = SocketAddr::from(([0,0,0,0], 3000));
    
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);
    
    let (mut writer, reader) = flashmap::new();

    let (se, re) = mpsc::channel(1000);

    let t = tokio::task::spawn(async move {
        let mut w = writer;
        let mut r = re;
        loop {
            if let Some((key,data)) = r.recv().await {
                let mut w_guard= w.guard();
                w_guard.insert(key, data);
                w_guard.publish();
            }
        }
    });
    
    tokio::task::spawn(foo(state_receiver.clone(), state_sender));
    se.send(("Fizz".to_owned(), Data::String("Buzz".to_owned()))).await;
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        let new_reader = reader.clone();
        let new_se = se.clone();
        let service = service_fn(move |x|{
            let r = new_reader.clone();
            let s = new_se.clone();
            async move{
                let a = echo( x, r, s).await;
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
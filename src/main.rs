use std::future::IntoFuture;
use std::net::SocketAddr;
use std::ptr::read;
use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc::channel;
use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::body::Frame;
use hyper::server::conn::http1;

use hyper::service::service_fn;
use hyper::{body::Body, Method, Request, Response, StatusCode};
use tokio::net::TcpListener;
use hyper_util::rt::TokioIo;

use std::sync::{Arc, Mutex, RwLock};
use std::collections::{HashMap, HashSet, BTreeMap};
use serde::{Serialize, Deserialize};
use flashmap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum EchoPayload {
    Echo{
        echo: String,
        msg_id: u32
    },
    EchoOk{
        echo: String,
        in_reply_to: u32
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Req {
    S{
        key: String,
        value: String,
        msg_id: u32
    },
    G{
        key: String,
        msg_id: u32
    },
    GOk{
        value: String,
        in_reply_to: u32
    },
    SOk{
        in_reply_to: u32
    }
}

pub enum RaftState{
    Leader,
    Canidate,
    Follower
}

async fn echo(
     req: Request<hyper::body::Incoming>, reader: flashmap::ReadHandle<String, String>, se: Sender<(String, String)>
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        // Serve some instructions at /
        (&Method::GET, "/") => Ok(Response::new(full(
            reader.guard().get("Fizz").unwrap().clone(),
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
                             if let Err(err) = se.send((key, value)){
                                println!("Error, did not send: {}", err);
                             };
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
    let addr = SocketAddr::from(([0,0,0,0], 3000));
    
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);

    let map = Arc::new(RwLock::new(BTreeMap::new()));
    map.write().unwrap().insert("hi", "hello");
    let x = map.read().unwrap().get("hi").unwrap().to_string();
    println!("{}", x);
    let (mut writer, reader) = flashmap::new();
    let mut write_guard = writer.guard();

    let (se, re) = channel();
    se.send(("Fizz".to_owned(), "Buzz".to_owned()));
    let message = re.recv().unwrap();

    write_guard.insert(message.0, message.1);
    write_guard.publish();

    let t = std::thread::spawn(move || {
        let mut w = writer;
        let r = re;
        loop {
            if let Ok(msg) = r.recv(){
                println!("{:?}", msg);
                let mut w_guard= w.guard();
                w_guard.insert(msg.0, msg.1);
                w_guard.publish();
            }
        }
    });

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

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(io, service)
                .await
            {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}
use core::task;
use hello_world::seafoam_client::SeafoamClient;
use hello_world::{GetRequest, Object, SetRequest};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::*;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tonic::client;
use tonic::transport::channel;

pub mod hello_world {
    tonic::include_proto!("seafoam");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let TASKS_LIMIT = 128;
    let semaphore = Arc::new(Semaphore::new(TASKS_LIMIT));

    for _ in 0..400_000 {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        tokio::spawn(async move {
            loop {
                if let Ok(channel) = channel::Endpoint::from_static("http://0.0.0.0:3000")
                    .connect()
                    .await
                {
                    let _p = permit;
                    let mut client = SeafoamClient::new(channel);
                    for i in 0..24 {
                        let request = GetRequest {
                            keys: vec!["hello".to_string()],
                        };
                        let response = client.get(request).await;
                        println!("{:?}", response.unwrap().into_inner());
                    }
                    let request = SetRequest {
                        key: "hello".to_string(),
                        value: Some(Object {
                            fields: HashMap::new(),
                        }),
                    };
                    let response = client.set(request).await.unwrap();
                    //println!("{:?}", response.into_inner());
                    return;
                }
            }
        });
    }
    let _ = semaphore.acquire_many(TASKS_LIMIT as u32).await.unwrap();
    println!("{}", start.elapsed().as_millis());
    Ok(())
}

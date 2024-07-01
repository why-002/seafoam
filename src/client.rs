use core::task;
use hello_world::seafoam_client::SeafoamClient;
use hello_world::GetRequest;
use std::borrow::Borrow;
use std::sync::Arc;
use std::time::*;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tonic::client;
use tonic::transport::channel;

pub mod hello_world {
    tonic::include_proto!("seafoam");
}

use seafoam::raft::Data;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let TASKS_LIMIT = 128;
    let semaphore = Arc::new(Semaphore::new(TASKS_LIMIT));

    for _ in 0..4_000 {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        tokio::spawn(async move {
            loop {
                if let Ok(channel) = channel::Endpoint::from_static("http://[::1]:50051")
                    .connect()
                    .await
                {
                    let _p = permit;
                    let mut client = SeafoamClient::new(channel);
                    for i in 0..25 {
                        let request = tonic::Request::new(GetRequest {
                            key: "hello".to_string(),
                        });
                        let response = client.get(request).await.unwrap();
                    }
                    return;
                }
            }
        });
    }
    let _ = semaphore.acquire_many(TASKS_LIMIT as u32).await.unwrap();
    println!("{}", start.elapsed().as_millis());
    Ok(())
}
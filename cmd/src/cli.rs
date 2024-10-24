use futures::StreamExt;
use rstream::{cli::connect, remote_buf::StreamedBufRead};
use std::sync::Arc;

use tonic::transport::Endpoint;

#[tokio::main]
async fn main() {
    let host = "localhost";
    let port = 8888_u16;
    let addr = format!("http://{}:{}", host, port);
    let endpoint = Endpoint::from_shared(addr).unwrap();

    let cli = Arc::new(connect(endpoint).await);
    let mut stream = StreamedBufRead::new(cli);
    while let Some(data) = stream.next().await {
        println!("data: {:?}", data);
    }
    // to avoid async drop, we should close the inner buffer explicitly
    let _ = stream.as_ref().close().await;
    println!("---");
}

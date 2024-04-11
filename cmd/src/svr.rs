use rstream::{pb::data_service_server::DataServiceServer, svc::DataServiceImpl};
use tonic::transport::Server;

#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:8888".parse().unwrap();

    let svc = DataServiceImpl {};

    Server::builder()
        .add_service(DataServiceServer::new(svc))
        .serve(addr)
        .await
        .unwrap();
}

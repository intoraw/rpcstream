use std::sync::Arc;

use tonic::transport::{Channel, Endpoint};

use crate::pb::{
    data_service_client::DataServiceClient, PAckDataRequest, PAckDataResponse, PCloseRequest,
    PCloseResponse, PGetDataRequest, PGetDataResponse,
};

//pub type Client = DataServiceClient<Channel>;

pub async fn connect(endpoint: Endpoint) -> Client {
    let inner = DataServiceClient::<Channel>::connect(endpoint)
        .await
        .unwrap();
    Client::new(inner)
}

pub struct Client {
    inner: DataServiceClient<Channel>,
}

impl Client {
    pub fn new(inner: DataServiceClient<Channel>) -> Self {
        Self { inner }
    }

    pub async fn get_data(
        self: Arc<Self>,
        request: impl tonic::IntoRequest<PGetDataRequest>,
    ) -> std::result::Result<tonic::Response<PGetDataResponse>, tonic::Status> {
        self.inner.clone().get_data(request).await
    }

    pub async fn ack_data(
        self: Arc<Self>,
        request: impl tonic::IntoRequest<PAckDataRequest>,
    ) -> std::result::Result<tonic::Response<PAckDataResponse>, tonic::Status> {
        self.inner.clone().ack_data(request).await
    }

    pub async fn close(
        self: Arc<Self>,
    ) -> std::result::Result<tonic::Response<PCloseResponse>, tonic::Status> {
        let req = PCloseRequest {};
        self.inner.clone().close(req).await
    }
}

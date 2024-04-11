use tonic::transport::{Channel, Endpoint};

use crate::{
    pb::{data_service_client::DataServiceClient, PGetDataRequest},
    pb::{PAckDataRequest, PAckDataResponse, PGetDataResponse},
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
        &self,
        request: impl tonic::IntoRequest<PGetDataRequest>,
    ) -> std::result::Result<tonic::Response<PGetDataResponse>, tonic::Status> {
        self.inner.clone().get_data(request).await
    }

    pub async fn ack_data(
        &self,
        request: impl tonic::IntoRequest<PAckDataRequest>,
    ) -> std::result::Result<tonic::Response<PAckDataResponse>, tonic::Status> {
        self.inner.clone().ack_data(request).await
    }
}

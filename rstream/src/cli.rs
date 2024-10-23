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
        &self,
        request: impl tonic::IntoRequest<PGetDataRequest>,
    ) -> std::result::Result<tonic::Response<PGetDataResponse>, tonic::Status> {
        let mut x = self.inner.clone();
        x.get_data(request).await
    }

    pub async fn ack_data(
        &self,
        request: impl tonic::IntoRequest<PAckDataRequest>,
    ) -> std::result::Result<tonic::Response<PAckDataResponse>, tonic::Status> {
        self.inner.clone().ack_data(request).await
    }

    pub async fn close(
        &self,
    ) -> std::result::Result<tonic::Response<PCloseResponse>, tonic::Status> {
        let req = PCloseRequest {};
        let mut x = self.inner.clone();
        x.close(req).await
    }
}

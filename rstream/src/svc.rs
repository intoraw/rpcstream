use std::sync::atomic::AtomicI64;

use tonic::async_trait;

use crate::generated::rpcstream::data_service_server::DataService;
use crate::pb::PAckDataRequest;
use crate::pb::PAckDataResponse;
use crate::pb::PCloseRequest;
use crate::pb::PCloseResponse;
use crate::pb::PGetDataRequest;
use crate::pb::PGetDataResponse;

pub struct DataServiceImpl {
    pub capacity: i64,
    /// State
    pub acked: AtomicI64,
}

impl DataServiceImpl {
    pub fn new(capacity: i64) -> Self {
        Self {
            capacity,
            acked: Default::default(),
        }
    }
}

#[async_trait]
impl DataService for DataServiceImpl {
    async fn get_data(
        &self,
        request: tonic::Request<PGetDataRequest>,
    ) -> std::result::Result<tonic::Response<PGetDataResponse>, tonic::Status> {
        let seq = request.get_ref().seq;
        let eos = seq >= self.capacity;
        let resp = PGetDataResponse {
            seq,
            data: format!("Data {}", seq),
            eos,
        };
        println!("{request:?} {resp:?}");
        Ok(tonic::Response::new(resp))
    }

    async fn ack_data(
        &self,
        request: tonic::Request<PAckDataRequest>,
    ) -> std::result::Result<tonic::Response<PAckDataResponse>, tonic::Status> {
        self.acked
            .store(request.get_ref().seq, std::sync::atomic::Ordering::Relaxed);
        let resp = PAckDataResponse {};
        println!("{request:?} {resp:?}");
        Ok(tonic::Response::new(resp))
    }

    async fn close(
        &self,
        request: tonic::Request<PCloseRequest>,
    ) -> std::result::Result<tonic::Response<PCloseResponse>, tonic::Status> {
        let resp = PCloseResponse {};
        println!("{request:?} {resp:?}");
        Ok(tonic::Response::new(resp))
    }
}

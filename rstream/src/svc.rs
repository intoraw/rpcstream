use tonic::async_trait;

use crate::generated::rpcstream::data_service_server::DataService;
use crate::pb::PAckDataRequest;
use crate::pb::PAckDataResponse;
use crate::pb::PGetDataRequest;
use crate::pb::PGetDataResponse;

pub struct DataServiceImpl {}

#[async_trait]
impl DataService for DataServiceImpl {
    async fn get_data(
        &self,
        request: tonic::Request<PGetDataRequest>,
    ) -> std::result::Result<tonic::Response<PGetDataResponse>, tonic::Status> {
        let seq = request.get_ref().seq;
        let resp = PGetDataResponse {
            seq,
            data: format!("Data {}", seq),
        };
        println!("{request:?} {resp:?}");
        Ok(tonic::Response::new(resp))
    }

    async fn ack_data(
        &self,
        _request: tonic::Request<PAckDataRequest>,
    ) -> std::result::Result<tonic::Response<PAckDataResponse>, tonic::Status> {
        let resp = PAckDataResponse {};
        println!("{_request:?} {resp:?}");
        Ok(tonic::Response::new(resp))
    }
}

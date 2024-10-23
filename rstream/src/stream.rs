use std::{future::Future, pin::Pin, sync::Arc};

use futures::Stream;
use pin_project::pin_project;
use tonic::Status;

use crate::{
    cli::Client,
    pb::{PAckDataRequest, PAckDataResponse, PCloseResponse, PGetDataRequest, PGetDataResponse},
};

#[pin_project]
pub struct RpcReader {
    inner: Arc<Client>,
    seq: i64,
    #[pin]
    state: State,
    // data not acked
    pending: Option<String>,
    error: Option<RpcReadError>,
    eos: bool,
}

#[derive(Debug)]
pub enum RpcReadError {
    GetData,
    AckData,
    Close,
}

pub type GetDataFuture =
    Pin<Box<dyn Future<Output = Result<tonic::Response<PGetDataResponse>, Status>> + Send>>;
pub type AckDataFuture =
    Pin<Box<dyn Future<Output = Result<tonic::Response<PAckDataResponse>, Status>> + Send>>;
pub type CloseFuture =
    Pin<Box<dyn Future<Output = Result<tonic::Response<PCloseResponse>, Status>> + Send>>;

pub enum State {
    Get(GetDataFuture),
    Ack(AckDataFuture),
    Closing(CloseFuture),
    Closed,
}

impl RpcReader {
    pub fn new(cli: Arc<Client>) -> Self {
        let req = PGetDataRequest { seq: 0 };
        // let cli_ptr: *const Client = &*cli;
        // // # safety: cli_ptr will be valid as long as cli is valid
        // let fut = unsafe { (*cli_ptr).get_data(req) };
        let cli_ptr: *const Client = cli.as_ref();
        let fut = unsafe { (*cli_ptr).get_data(req) };
        Self {
            inner: cli.clone(),
            seq: 0,
            state: State::Get(Box::pin(fut)),
            pending: None,
            error: None,
            eos: false,
        }
    }
}

impl Stream for RpcReader {
    type Item = Result<String, RpcReadError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match self.state {
                State::Get(ref mut pin) => {
                    let res = futures::ready!(pin.as_mut().poll(cx));
                    match res {
                        Ok(resp) => {
                            let PGetDataResponse { seq, data, eos } = resp.into_inner();
                            let fut = {
                                let cli_ptr: *const Client = self.inner.as_ref();
                                // # safety: cli_ptr will be valid as long as cli is valid
                                let req = PAckDataRequest { seq: self.seq };
                                unsafe { (*cli_ptr).ack_data(req) }
                            };
                            self.eos = eos;
                            self.pending = Some(data);
                            self.state = State::Ack(Box::pin(fut));
                        }
                        Err(_) => {
                            // transit to closed state
                            let fut = {
                                let cli_ptr: *const Client = self.inner.as_ref();
                                // # safety: cli_ptr will be valid as long as cli is valid
                                unsafe { (*cli_ptr).close() }
                            };
                            self.error = Some(RpcReadError::GetData);
                            self.state = State::Closing(Box::pin(fut));
                        }
                    }
                }
                State::Ack(ref mut pin) => {
                    let ack_fut = pin;
                    let res = futures::ready!(ack_fut.as_mut().poll(cx));
                    match res {
                        Ok(_) => {
                            let fut = {
                                let cli_ptr: *const Client = self.inner.as_ref();
                                // # safety: cli_ptr will be valid as long as cli is valid
                                let req = PGetDataRequest { seq: self.seq };
                                unsafe { (*cli_ptr).get_data(req) }
                            };
                            self.state = State::Get(Box::pin(fut));
                            let data = self.pending.take().unwrap();
                            self.seq += 1;
                            return std::task::Poll::Ready(Some(Ok(data)));
                        }
                        Err(_) => {
                            // transit to closed state
                            let fut = {
                                let cli_ptr: *const Client = self.inner.as_ref();
                                // # safety: cli_ptr will be valid as long as cli is valid
                                unsafe { (*cli_ptr).close() }
                            };
                            self.error = Some(RpcReadError::AckData);
                            self.state = State::Closing(Box::pin(fut));
                        }
                    }
                }
                State::Closing(ref mut pin) => {
                    let res = futures::ready!(pin.as_mut().poll(cx));
                    match res {
                        Ok(_) => {
                            self.state = State::Closed;
                        }
                        Err(_) => {
                            self.state = State::Closed;
                        }
                    }
                }
                State::Closed => {
                    let error = self.error.take();
                    return match error {
                        Some(e) => std::task::Poll::Ready(Some(Err(e))),
                        None => std::task::Poll::Ready(None),
                    };
                }
            }
        }
    }
}

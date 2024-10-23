use std::{collections::LinkedList, future::Future, pin::Pin, sync::Arc, task::Poll};

use futures::{future::Remote, FutureExt, Stream};
use tonic::{Response, Status};

use crate::{
    cli::Client,
    pb::{PAckDataRequest, PAckDataResponse, PCloseResponse, PGetDataRequest, PGetDataResponse},
};

pub type Payload = String;

pub struct RpcReader {
    state: State,
    inner: Arc<Client>,
    seq: i64,
    // data not acked
    pending: Option<Payload>,
    // error information
    error: Option<RpcReadError>,
    // eos
    eos: bool,
    // data acked
    buf: LinkedList<Payload>,
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
            buf: Default::default(),
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
                    let res = pin.as_mut().poll(cx);
                    match res {
                        Poll::Pending => break,
                        Poll::Ready(Ok(resp)) => {
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
                        Poll::Ready(Err(_)) => {
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
                    let res = ack_fut.as_mut().poll(cx);
                    match res {
                        Poll::Pending => break,
                        Poll::Ready(Ok(_)) => {
                            let fut = {
                                let cli_ptr: *const Client = self.inner.as_ref();
                                // # safety: cli_ptr will be valid as long as cli is valid
                                let req = PGetDataRequest { seq: self.seq };
                                unsafe { (*cli_ptr).get_data(req) }
                            };
                            self.state = State::Get(Box::pin(fut));
                            let data = self.pending.take().unwrap();
                            self.buf.push_back(data);
                            self.seq += 1;
                        }
                        Poll::Ready(Err(_)) => {
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
                    let res = pin.as_mut().poll(cx);
                    match res {
                        Poll::Pending => break,
                        Poll::Ready(Ok(_)) => {
                            self.state = State::Closed;
                        }
                        Poll::Ready(Err(_)) => {
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

        println!("buf len {}", self.buf.len());

        if !self.buf.is_empty() {
            let data = self.buf.pop_front().unwrap();
            Poll::Ready(Some(Ok(data)))
        } else if self.eos {
            return Poll::Ready(None);
        } else if self.error.is_some() {
            let error = self.error.take().unwrap();
            return Poll::Ready(Some(Err(error)));
        } else {
            return Poll::Pending;
        }
    }
}

/// DataChannel is a resource, when dropped, should call rpc function
/// to close the channel
pub struct RemoteBuffer {
    //# Safety: user should guarantee raw_client cannot outlive the Client
    raw_cli: &'static Client,
    owned_cli: Arc<Client>,
    state: BufferState,
    eos: bool,
    seq: i64,
    pending: Option<Payload>,
    acked: LinkedList<Payload>,
}

impl RemoteBuffer {
    pub fn new(cli: Arc<Client>) -> Self {
        let owned_cli = cli;
        let raw_cli: &'static Client = unsafe { &*(owned_cli.as_ref() as *const Client) };
        Self {
            raw_cli,
            owned_cli,
            state: Default::default(),
            seq: 0,
            pending: None,
            acked: Default::default(),
            eos: false,
        }
    }

    pub async fn get(&self, seq: i64) -> Result<tonic::Response<PGetDataResponse>, Status> {
        let req = PGetDataRequest { seq };
        self.raw_cli.get_data(req).await
    }

    pub async fn ack(&self, seq: i64) -> Result<tonic::Response<PAckDataResponse>, Status> {
        let req = PAckDataRequest { seq };
        self.raw_cli.ack_data(req).await
    }

    pub async fn close(&self) -> Result<tonic::Response<PCloseResponse>, Status> {
        self.raw_cli.close().await
    }
}

// #Safety: tonic guarantees client is thread safe
unsafe impl Sync for RemoteBuffer {}

impl Drop for RemoteBuffer {
    fn drop(&mut self) {
        let cli = self.owned_cli.clone();
        tokio::spawn(async move {
            let _ = cli.close().await;
            // log error if close failed
            // we can do nothing if close is failed. maybe retry
        });
    }
}

pub enum BufferError {
    GetDataErr(Status),
    AckDataErr(Status),
    CloseErr(Status),
}

#[derive(Default)]
pub enum BufferState {
    #[default]
    Idle,
    Busy(Operation),
    Terminated,
}

pub enum Operation {
    Get(GetDataFuture),
    Ack(AckDataFuture),
}

impl Future for RemoteBuffer {
    type Output = Result<Payload, BufferError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.state {
                BufferState::Idle => {
                    let fut = { self.raw_cli.get_data(PGetDataRequest { seq: self.seq }) };
                    self.state = BufferState::Busy(Operation::Get(Box::pin(fut)))
                }
                BufferState::Busy(Operation::Get(ref mut fut)) => match fut.as_mut().poll_unpin(cx)
                {
                    Poll::Ready(Ok(resp)) => {
                        let PGetDataResponse { seq, data, eos } = resp.into_inner();
                        self.pending = Some(data);
                        self.eos |= eos;
                        let fut = { self.raw_cli.ack_data(PAckDataRequest { seq }) };
                        self.state = BufferState::Busy(Operation::Ack(Box::pin(fut)));
                    }
                    Poll::Ready(Err(status)) => {
                        self.state = BufferState::Err(BufferError::Terminated(status));
                    }
                    Poll::Pending => break,
                },
                BufferState::Busy(Operation::Ack(ref mut fut)) => match fut.as_mut().poll_unpin(cx)
                {
                    Poll::Ready(Ok(_)) => {
                        let data = self.pending.take().unwrap();
                        self.acked.push_back(data);
                        self.seq += 1;
                        if self.eos {
                            self.state = BufferState::Finished;
                        } else {
                            let fut = { self.raw_cli.get_data(PGetDataRequest { seq: self.seq }) };
                            self.state = BufferState::Busy(Operation::Get(Box::pin(fut)));
                        }
                    }
                    Poll::Ready(Err(status)) => {
                        self.state = BufferState::Err(BufferError::GetDataErr(status));
                    }
                    Poll::Pending => break,
                },
                BufferState::Finished => break,
                BufferState::Err(_) => break,
            }
        }
        todo!()
    }
}

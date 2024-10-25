use std::{collections::LinkedList, future::Future, pin::Pin, sync::Arc, task::Poll};

use futures::{future::BoxFuture, FutureExt, Stream};
use tonic::Status;

use crate::{
    cli::Client,
    pb::{PAckDataRequest, PAckDataResponse, PCloseResponse, PGetDataRequest, PGetDataResponse},
};

pub type Payload = String;

pub type GetDataResp = Result<tonic::Response<PGetDataResponse>, Status>;
pub type AckDataResp = Result<tonic::Response<PAckDataResponse>, Status>;
pub type CloseResp = Result<tonic::Response<PCloseResponse>, Status>;

pub struct RemoteBuffer {
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
        Self {
            owned_cli,
            state: Default::default(),
            seq: 0,
            pending: None,
            acked: Default::default(),
            eos: false,
        }
    }

    pub fn get(&self, seq: i64) -> BoxFuture<Result<tonic::Response<PGetDataResponse>, Status>> {
        let req = PGetDataRequest { seq };
        self.owned_cli.clone().get_data(req).boxed()
    }

    pub fn ack(&self, seq: i64) -> BoxFuture<Result<tonic::Response<PAckDataResponse>, Status>> {
        let req = PAckDataRequest { seq };
        self.owned_cli.clone().ack_data(req).boxed()
    }

    pub fn close(&self) -> BoxFuture<Result<tonic::Response<PCloseResponse>, Status>> {
        self.owned_cli.clone().close().boxed()
    }
}

// impl Drop for RemoteBuffer {
//     fn drop(&mut self) {
//         let cli = self.owned_cli.clone();
//         println!("drop");
//         tokio::spawn(async move {
//             println!("closing");
//             let _ = cli.close().await;
//             println!("closed");
//             // log error if close failed
//             // we can do nothing if close is failed. maybe retry
//         });
//     }
// }

#[derive(Clone, Debug)]
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
    Terminated(Option<BufferError>),
}

pub enum Operation {
    Get(BoxFuture<'static, GetDataResp>),
    Ack(BoxFuture<'static, AckDataResp>),
}

impl Future for RemoteBuffer {
    type Output = Option<Result<Payload, BufferError>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        loop {
            match &mut self.state {
                BufferState::Idle => {
                    let fut = {
                        let req = PGetDataRequest { seq: self.seq };
                        self.owned_cli.clone().get_data(req).boxed()
                    };
                    self.state = BufferState::Busy(Operation::Get(fut))
                }
                BufferState::Busy(Operation::Get(ref mut fut)) => match fut.as_mut().poll_unpin(cx)
                {
                    Poll::Ready(Ok(resp)) => {
                        let PGetDataResponse { seq, data, eos } = resp.into_inner();
                        self.pending = Some(data);
                        self.eos |= eos;
                        let fut = {
                            let req = PAckDataRequest { seq };
                            self.owned_cli.clone().ack_data(req).boxed()
                        };
                        self.state = BufferState::Busy(Operation::Ack(fut));
                    }
                    Poll::Ready(Err(status)) => {
                        self.state = BufferState::Terminated(Some(BufferError::GetDataErr(status)));
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
                            self.state = BufferState::Terminated(None);
                        } else {
                            let fut = {
                                let req = PGetDataRequest { seq: self.seq };
                                self.owned_cli.clone().get_data(req).boxed()
                            };
                            self.state = BufferState::Busy(Operation::Get(Box::pin(fut)));
                        }
                    }
                    Poll::Ready(Err(status)) => {
                        self.state = BufferState::Terminated(Some(BufferError::AckDataErr(status)));
                    }
                    Poll::Pending => break,
                },
                BufferState::Terminated(_) => break,
            }
        }

        // pop acked data first
        if !self.acked.is_empty() {
            let data = self.acked.pop_front();
            return Poll::Ready(Some(Ok(data.unwrap())));
        }

        // eos
        if let BufferState::Terminated(None) = self.state {
            return Poll::Ready(None);
        }

        // error
        if let BufferState::Terminated(Some(ref err)) = self.state {
            return Poll::Ready(Some(Err(err.clone())));
        }

        // pending on operation
        Poll::Pending
    }
}

pub struct StreamedBufRead {
    inner: RemoteBuffer,
}

impl StreamedBufRead {
    pub fn new(cli: Arc<Client>) -> Self {
        Self {
            inner: RemoteBuffer::new(cli),
        }
    }
}

impl AsRef<RemoteBuffer> for StreamedBufRead {
    fn as_ref(&self) -> &RemoteBuffer {
        &self.inner
    }
}

impl Stream for StreamedBufRead {
    type Item = Result<Payload, BufferError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.poll_unpin(cx)
    }
}

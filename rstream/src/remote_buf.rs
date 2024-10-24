use std::{collections::LinkedList, future::Future, pin::Pin, sync::Arc, task::Poll};

use futures::{FutureExt, Stream};
use tonic::Status;

use crate::{
    cli::Client,
    pb::{PAckDataRequest, PAckDataResponse, PCloseResponse, PGetDataRequest, PGetDataResponse},
};

pub type Payload = String;

pub type GetDataFuture =
    Pin<Box<dyn Future<Output = Result<tonic::Response<PGetDataResponse>, Status>> + Send>>;
pub type AckDataFuture =
    Pin<Box<dyn Future<Output = Result<tonic::Response<PAckDataResponse>, Status>> + Send>>;
pub type CloseFuture =
    Pin<Box<dyn Future<Output = Result<tonic::Response<PCloseResponse>, Status>> + Send>>;

pub struct RemoteBuffer {
    //# Safety: raw_cli references owned_cli, which guarantees raw_cli will not outlive owned_cli
    raw_cli: &'static Client,
    #[allow(unused)]
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
    Get(GetDataFuture),
    Ack(AckDataFuture),
}

impl Future for RemoteBuffer {
    type Output = Option<Result<Payload, BufferError>>;

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
                            let fut = { self.raw_cli.get_data(PGetDataRequest { seq: self.seq }) };
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

use futures::{ready, Stream, StreamExt};
use rstream::{
    cli::{connect, Client},
    pb::{PAckDataRequest, PGetDataRequest},
};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use tokio_util::sync::ReusableBoxFuture;
use tonic::transport::Endpoint;

#[derive(Debug)]
pub enum RespKind {
    GetData(String),
    AckData(String),
}

impl RespKind {
    pub fn get_data(self) -> String {
        match self {
            RespKind::GetData(s) => s,
            RespKind::AckData(s) => s,
        }
    }
}

pub struct FetchStream {
    inner: ReusableBoxFuture<'static, (RespKind, Client)>,
    seq: i64,
}

impl FetchStream {
    pub fn new(cli: Client) -> Self {
        Self {
            inner: ReusableBoxFuture::new(make_get_data_future(cli, 0)),
            seq: 0,
        }
    }
}

async fn make_get_data_future(cli: Client, seq: i64) -> (RespKind, Client) {
    let req = PGetDataRequest { seq };
    let res = cli.get_data(req).await.unwrap();
    (RespKind::GetData(res.get_ref().data.clone()), cli)
}

async fn make_ack_data_future(cli: Client, seq: i64, data: String) -> (RespKind, Client) {
    let req = PAckDataRequest { seq };
    let res = cli.ack_data(req).await.unwrap();
    (RespKind::AckData(data), cli)
}

impl Stream for FetchStream {
    type Item = String;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let seq = self.seq;
            let (res, cli) = ready!(self.inner.poll(cx));
            //println!("Poll res {:?}", res);

            match res {
                RespKind::GetData(s) => {
                    self.inner.set(make_ack_data_future(cli, seq, s));
                }
                RespKind::AckData(s) => {
                    self.inner.set(make_get_data_future(cli, seq + 1));
                    self.seq += 1;
                    return Poll::Ready(Some(s));
                }
            }
        }
    }

    // fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    //     let seq = self.seq;
    //     let (res, cli) = ready!(self.inner.poll(cx));
    //     println!("Poll res {:?}", res);
    //     match res {
    //         RespKind::GetData(s) => {
    //             self.inner.set(make_ack_data_future(cli, seq, s));
    //             return Poll::Pending;
    //         }
    //         RespKind::AckData(s) => {
    //             self.inner.set(make_get_data_future(cli, seq + 1));
    //             self.seq += 1;
    //             return Poll::Ready(Some(s));
    //         }
    //     }
    // }
}

// pub struct FetchExec {
//     pub cli: Client,
//     pub state: FetchState,
//     pub seq: i64,
// }

// pub enum FetchState {
//     Empty,
//     WaitResp(Pin<Box<dyn Future<Output = Result<Response<PGetDataResponse>, Status>>>>),
//     WaitAck(
//         (
//             PGetDataResponse,
//             Pin<Box<dyn Future<Output = Result<Response<PAckDataResponse>, Status>>>>,
//         ),
//     ),
// }

// impl FetchExec {
//     fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<String> {
//         let me = self.get_mut();
//         loop {
//             match me.state {
//                 FetchState::Empty => {
//                     let fut = me.cli.get_data(PGetDataRequest { seq: 0 });
//                     me.state = FetchState::WaitResp(Box::pin(fut));
//                 }
//                 FetchState::WaitResp(ref mut fut) => {
//                     let resp = ready!(fut.poll_unpin(cx)).unwrap();
//                     let fut = me.cli.ack_data(PAckDataRequest { seq: 0 });
//                     me.state = FetchState::WaitAck((resp.into_inner(), Box::pin(fut)));
//                 }
//                 FetchState::WaitAck((ref mut resp, ref mut fut)) => {
//                     let ack_resp = ready!(fut.poll_unpin(cx)).unwrap();
//                     let output = resp.data.clone();
//                     me.state = FetchState::Empty;
//                     return Poll::Ready(output);
//                 }
//             }
//         }
//     }
// }

// impl Stream for FetchExec {
//     type Item = String;

//     fn poll_next(
//         mut self: Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<Option<Self::Item>> {
//         self.poll_next(cx).map(|x| Some(x))
//     }
// }

#[tokio::main]
async fn main() {
    let host = "localhost";
    let port = 8888 as u16;
    let addr = format!("http://{}:{}", host, port);
    let endpoint = Endpoint::from_shared(addr).unwrap();

    let cli = connect(endpoint).await;

    let mut fs = FetchStream::new(cli);
    for i in 0..10 {
        let fut = fs.next().await;
        println!("{fut:?}");
    }
}

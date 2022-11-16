use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{ready, Sink, Stream};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

pub mod client;
pub mod framed;
pub mod readwriter;
pub mod serded;
pub mod server;

/// Request contains the required data to be sent to the consumer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request<T> {
    /// request id which should be maintained by the producer and used later to dispatch the response
    pub id: u64,
    /// the task body
    pub body: T,
}

/// Response contains the output for the specific task, and error message if exists.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response<T, E> {
    /// request id
    pub id: u64,

    /// the response body, or an error if the request failed.
    pub result: Result<T, E>,
}

#[derive(thiserror::Error, Debug)]
pub enum TransportError<E> {
    /// Could not ready the transport for writes.
    #[error("could not ready the transport for writes")]
    Ready(#[source] E),
    /// Could not read from the transport.
    #[error("could not read from the transport")]
    Read(#[source] E),
    /// Could not write to the transport.
    #[error("could not write to the transport")]
    Write(#[source] E),
    /// Could not flush the transport.
    #[error("could not flush the transport")]
    Flush(#[source] E),
    /// Could not close the transport.
    #[error("could not close the transport")]
    Close(#[source] E),
}

type TransportIOError = TransportError<io::Error>;

pub trait Transport<SinkItem, Item>:
    Sink<SinkItem, Error = <Self as Transport<SinkItem, Item>>::InnerError>
    + Stream<Item = Result<Item, <Self as Transport<SinkItem, Item>>::InnerError>>
{
    type InnerError: Send + Sync + 'static;
}

impl<T, SinkItem, Item, E> Transport<SinkItem, Item> for T
where
    T: ?Sized,
    T: Sink<SinkItem, Error = E>,
    T: Stream<Item = Result<Item, E>>,
    E: Send + Sync + 'static,
{
    type InnerError = E;
}

#[pin_project]
pub struct ChannelTransport<SinkItem, Item> {
    sender: mpsc::UnboundedSender<SinkItem>,
    #[pin]
    receiver: mpsc::UnboundedReceiver<Item>,
}

impl<SinkItem, Item> ChannelTransport<SinkItem, Item> {
    pub fn unbounded() -> (
        ChannelTransport<SinkItem, Item>,
        ChannelTransport<Item, SinkItem>,
    ) {
        let (send_sink_item, recv_sink_item) = mpsc::unbounded_channel();
        let (send_item, recv_item) = mpsc::unbounded_channel();
        (
            ChannelTransport {
                sender: send_sink_item,
                receiver: recv_item,
            },
            ChannelTransport {
                sender: send_item,
                receiver: recv_sink_item,
            },
        )
    }
}

impl<SinkItem, Item> Sink<SinkItem> for ChannelTransport<SinkItem, Item> {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.sender.is_closed() {
            Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn start_send(self: Pin<&mut Self>, item: SinkItem) -> io::Result<()> {
        self.sender
            .send(item)
            .map_err(|_| io::ErrorKind::BrokenPipe.into())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl<SinkItem, Item> Stream for ChannelTransport<SinkItem, Item> {
    type Item = io::Result<Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(ready!(self.project().receiver.poll_recv(cx)).map(|x| Ok(x)))
    }
}

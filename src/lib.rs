use futures::{Sink, Stream};
use serde::{Deserialize, Serialize};

pub mod client;
pub mod framed;
pub mod readwriter;
pub mod serded;
pub mod server;

pub trait Transport<SinkItem, Item>:
    Sink<SinkItem, Error = <Self as Transport<SinkItem, Item>>::TransportError>
    + Stream<Item = Result<Item, <Self as Transport<SinkItem, Item>>::TransportError>>
{
    type TransportError: Send + Sync + 'static;
}

impl<T, SinkItem, Item, E> Transport<SinkItem, Item> for T
where
    T: ?Sized,
    T: Sink<SinkItem, Error = E>,
    T: Stream<Item = Result<Item, E>>,
    E: Send + Sync + 'static,
{
    type TransportError = E;
}


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

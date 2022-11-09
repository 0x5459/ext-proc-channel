use std::io;

use futures::future::BoxFuture;
use tokio::net::{TcpStream, ToSocketAddrs};

use super::reconnect::Reconnectable;

impl<A> Reconnectable<A> for TcpStream
where
    A: ToSocketAddrs + Send + 'static,
{
    type ConnectingFut = BoxFuture<'static, io::Result<TcpStream>>;

    fn connect(ctor_arg: A) -> Self::ConnectingFut {
        Box::pin(TcpStream::connect(ctor_arg))
    }
}

use std::{
    io,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use futures::{ready, Sink, Stream};
use pin_project::pin_project;

use crate::framed::Framed;

mod bincode;
mod json;

pub use self::bincode::Bincode;
pub use json::Json;

pub trait Serializer<T> {
    type Error;

    /// Serializes `item` into a new buffer
    fn serialize(self: Pin<&mut Self>, item: &T) -> Result<Bytes, Self::Error>;
}

pub trait Deserializer<T> {
    type Error;

    /// Deserializes a value from `buf`
    fn deserialize(self: Pin<&mut Self>, src: &BytesMut) -> Result<T, Self::Error>;
}

#[pin_project]
pub struct Serded<FramedIO, Codec, Item, SinkItem> {
    #[pin]
    inner: FramedIO,
    #[pin]
    codec: Codec,
    _maker: PhantomData<(fn(SinkItem), fn() -> Item)>,
}

impl<FramedIO, Codec, Item, SinkItem> Serded<FramedIO, Codec, Item, SinkItem> {
    pub fn new(inner: FramedIO, codec: Codec) -> Self {
        Self {
            inner,
            codec,
            _maker: PhantomData,
        }
    }
}

impl<FramedIO, Codec, Item, SinkItem> Stream for Serded<FramedIO, Codec, Item, SinkItem>
where
    FramedIO: Framed,
    FramedIO::Error: Into<io::Error>,
    Codec: Deserializer<Item>,
    Codec::Error: Into<io::Error>,
{
    type Item = io::Result<Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self
            .as_mut()
            .project()
            .inner
            .poll_next(cx)
            .map_err(Into::into)?)
        {
            Some(src) => Poll::Ready(Some(
                self.as_mut()
                    .project()
                    .codec
                    .deserialize(&src)
                    .map_err(Into::into),
            )),
            None => Poll::Ready(None),
        }
    }
}

impl<FramedIO, Codec, Item, SinkItem> Sink<SinkItem> for Serded<FramedIO, Codec, Item, SinkItem>
where
    FramedIO: Framed,
    FramedIO::Error: Into<io::Error>,
    Codec: Serializer<SinkItem>,
    Codec::Error: Into<io::Error>,
{
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx).map_err(Into::into)
    }

    fn start_send(mut self: Pin<&mut Self>, item: SinkItem) -> Result<(), Self::Error> {
        let bytes = self
            .as_mut()
            .project()
            .codec
            .serialize(&item)
            .map_err(Into::into)?;

        self.as_mut()
            .project()
            .inner
            .start_send(bytes)
            .map_err(Into::into)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx).map_err(Into::into)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx).map_err(Into::into)
    }
}

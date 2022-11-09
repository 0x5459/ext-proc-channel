use bytes::{Bytes, BytesMut};
use futures::{Sink, Stream};

use crate::serded::Serded;

impl<T: ?Sized> Framed for T where T: Sink<Bytes> + Stream<Item = Result<BytesMut, Self::Error>> {}

pub trait Framed: Sink<Bytes> + Stream<Item = Result<BytesMut, Self::Error>> {}

impl<T: ?Sized> FramedExt for T where T: Framed {}

pub trait FramedExt: Framed {
    fn serded<Codec, Item, SinkItem>(
        self,
        serde_codec: Codec,
    ) -> Serded<Self, Codec, Item, SinkItem>
    where
        Self: Sized,
    {
        Serded::new(self, serde_codec)
    }
}

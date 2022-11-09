use std::{
    future::{ready, Ready},
    io,
};

use ext_proc_channel::{
    framed::FramedExt,
    readwriter::ReadWriterExt,
    serded::Bincode,
    server::{Handler, Server},
};
use tokio::net::TcpListener;
use tokio_util::codec::LengthDelimitedCodec;

pub fn fibonacci(n: usize) -> u64 {
    if n <= 1 {
        return 1;
    }

    let mut sum = 0;
    let mut last = 0;
    let mut curr = 1;
    for _i in 1..n {
        sum = last + curr;
        last = curr;
        curr = sum;
    }
    sum
}

#[derive(Clone)]
struct Fib;

impl Handler<usize, u64, String> for Fib {
    type Fut<'a> = Ready<Result<u64, String>>;

    fn serve(&self, n: usize) -> Self::Fut<'_> {
        ready(Ok(fibonacci(n)))
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8964").await?;
    loop {
        match listener.accept().await {
            Ok((tcp_stream, _)) => {
                let tp = tcp_stream
                    .framed(LengthDelimitedCodec::default())
                    .serded(Bincode::default());
                tokio::spawn(Server::new(Default::default(), tp, Fib));
            }
            Err(e) => println!("couldn't get client: {:?}", e),
        }
    }
}

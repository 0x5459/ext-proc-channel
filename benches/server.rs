use std::{convert::Infallible, fmt::Debug, io};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ext_proc_channel::{
    client::Client,
    server::{self, Server},
    ChannelTransport, Request, Response,
};
use futures::{
    future::{ready, Ready},
    SinkExt, StreamExt,
};
use tokio::{runtime::Runtime, sync::mpsc, try_join};

#[derive(Clone)]
struct Handler;

impl server::Handler<String, String, Infallible> for Handler {
    type Fut<'a> = Ready<Result<String, Infallible>>;

    fn serve(&self, req: String) -> Self::Fut<'_> {
        ready(Ok(format!("hello {}", req)))
    }
}

fn bench_server_future(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let _guard = rt.enter();

    let client_transport = server_future(Handler);

    let client = Client::new(Default::default(), client_transport).spawn();

    c.bench_function("bench server future", |b| {
        b.to_async(&rt)
            .iter(|| async { black_box(client.call(black_box("world".to_string())).await) });
    });
}

fn bench_server_async(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let _guard = rt.enter();

    let client_transport = server_async(Handler);
    let client = Client::new(Default::default(), client_transport).spawn();

    c.bench_function("bench server async", |b| {
        b.to_async(&rt)
            .iter(|| async { black_box(client.call(black_box("world".to_string())).await) });
    });
}

fn server_future<Req, Resp, E>(
    handler: impl server::Handler<Req, Resp, E> + Clone + Send + Sync + 'static,
) -> ChannelTransport<Request<Req>, Response<Resp, E>>
where
    Req: Send + 'static,
    Resp: Send + 'static,
    E: Send + Debug + 'static,
{
    let (client_transport, server_transport) = ChannelTransport::unbounded();
    tokio::spawn(Server::new(Default::default(), server_transport, handler));
    client_transport
}

fn server_async<Req, Resp, E>(
    handler: impl server::Handler<Req, Resp, E> + Clone + Send + Sync + 'static,
) -> ChannelTransport<Request<Req>, Response<Resp, E>>
where
    Req: Send + 'static,
    Resp: Send + 'static,
    E: Send + Debug + 'static,
{
    let (client_transport, server_transport) = ChannelTransport::<Request<Req>, _>::unbounded();

    tokio::spawn(async move {
        let (mut sink, mut stream) = server_transport.split();

        let config = server::Config::default();
        let (send_response, mut recv_response) = mpsc::channel(config.max_inflight_requests);

        let handle_requests = async move {
            loop {
                match stream.next().await {
                    Some(Ok(request)) => {
                        let handler_cloned = handler.clone();
                        let send_response_cloned = send_response.clone();
                        tokio::spawn(async move {
                            let _ = send_response_cloned
                                .send(Response {
                                    id: request.id,
                                    result: handler_cloned.serve(request.body).await,
                                })
                                .await;
                        });
                    }
                    Some(Err(error)) => return Err(error),
                    None => return Ok(()),
                }
            }
        };

        let send_responses = async move {
            while let Some(resp) = recv_response.recv().await {
                sink.send(resp).await?;
            }
            Ok::<_, io::Error>(())
        };

        if let Err(error) = try_join!(handle_requests, send_responses) {
            tracing::error!(err=?error, "server error");
        }
    });
    client_transport
}

criterion_group!(benches, bench_server_future, bench_server_async);
criterion_main!(benches);

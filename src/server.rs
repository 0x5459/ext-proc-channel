use std::{
    fmt::Debug,
    future::Future,
    io,
    marker::PhantomData,
    mem,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{ready, stream::Fuse, Sink, Stream, StreamExt};
use pin_project::pin_project;
use tokio::sync::mpsc;

use crate::{Request, Response, Transport, TransportError, TransportIOError};

pub trait Handler<Req, Resp, E> {
    type Fut<'a>: Future<Output = Result<Resp, E>> + Send + 'a
    where
        Self: 'a;

    fn serve(&self, req: Req) -> Self::Fut<'_>;
}

impl<Req, Resp, E, F, Fut> Handler<Req, Resp, E> for F
where
    F: Fn(Req) -> Fut,
    Fut: Future<Output = Result<Resp, E>> + Send + 'static,
{
    type Fut<'a> = Fut
    where
        Self: 'a;

    fn serve(&self, req: Req) -> Self::Fut<'_> {
        self(req)
    }
}

#[pin_project]
pub struct Server<TP, H, Req, Resp, E> {
    #[pin]
    transport: Fuse<TP>,
    handler: H,
    send_responses: mpsc::Sender<Response<Resp, E>>,
    recv_responses: mpsc::Receiver<Response<Resp, E>>,
    write_state: WriteState<Resp, E>,
    _maker: PhantomData<Req>,
}

enum WriteState<Resp, E> {
    PollPendingResponse { need_flush: bool },
    WritingResponse(Response<Resp, E>),
    Flushing,
    Closing(Option<TransportIOError>),
}

impl<Resp, E> WriteState<Resp, E> {
    fn take(&mut self, next_state: Self) -> Self {
        mem::replace(self, next_state)
    }
}

pub struct Config {
    pub max_inflight_requests: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_inflight_requests: 100,
        }
    }
}

impl<TP, H, Req, Resp, E> Server<TP, H, Req, Resp, E>
where
    TP: Transport<Response<Resp, E>, Request<Req>>,
{
    pub fn new(config: Config, transport: TP, handler: H) -> Self {
        let (send_responses, recv_responses) = mpsc::channel(config.max_inflight_requests);
        Self {
            transport: transport.fuse(),
            handler,
            send_responses,
            recv_responses,
            write_state: WriteState::PollPendingResponse { need_flush: false },
            _maker: PhantomData,
        }
    }
}

impl<TP, H, Req, Resp, E> Server<TP, H, Req, Resp, E>
where
    H: Handler<Req, Resp, E> + Clone + Send + Sync + 'static,
    TP: Transport<Response<Resp, E>, Request<Req>, InnerError = io::Error> + Send,
    Req: Send + 'static,
    Resp: Send + 'static,
    E: Send + Debug + 'static,
{
    fn handle_request(mut self: Pin<&mut Self>, req: Request<Req>) {
        let handler = self.as_mut().project().handler.clone();
        let send_responses = self.project().send_responses.clone();

        tokio::spawn(async move {
            let result = handler.serve(req.body).await;
            let _ = send_responses.send(Response { id: req.id, result }).await;
        });
    }

    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError<TP::InnerError>>> {
        while let Some(req) = ready!(self
            .as_mut()
            .transport()
            .poll_next(cx)
            .map_err(TransportError::Read)?)
        {
            self.as_mut().handle_request(req);
        }
        Poll::Ready(Ok(()))
    }

    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError<TP::InnerError>>> {
        loop {
            match self
                .as_mut()
                .project()
                .write_state
                .take(WriteState::Closing(None))
            {
                WriteState::PollPendingResponse { need_flush } => {
                    *self.as_mut().project().write_state =
                        match self.as_mut().project().recv_responses.poll_recv(cx) {
                            Poll::Ready(Some(resp)) => WriteState::WritingResponse(resp),
                            Poll::Ready(None) => WriteState::Closing(None),
                            Poll::Pending => {
                                if need_flush {
                                    WriteState::Flushing
                                } else {
                                    self.as_mut().set_write_state(
                                        WriteState::PollPendingResponse { need_flush: false },
                                    );
                                    return Poll::Pending;
                                }
                            }
                        };
                }

                WriteState::WritingResponse(response) => {
                    *self.as_mut().project().write_state = match self.as_mut().poll_ready(cx) {
                        Poll::Ready(Ok(_)) => match self.as_mut().start_send(response) {
                            Ok(_) => WriteState::PollPendingResponse { need_flush: true },
                            Err(e) => WriteState::Closing(Some(e)),
                        },
                        Poll::Ready(Err(e)) => WriteState::Closing(Some(e)),
                        Poll::Pending => {
                            self.as_mut()
                                .set_write_state(WriteState::WritingResponse(response));
                            return Poll::Pending;
                        }
                    };
                }

                WriteState::Flushing => {
                    *self.as_mut().project().write_state = match self.as_mut().poll_flush(cx) {
                        Poll::Ready(Ok(_)) => WriteState::PollPendingResponse { need_flush: false },
                        Poll::Ready(Err(e)) => WriteState::Closing(Some(e)),
                        Poll::Pending => {
                            self.as_mut().set_write_state(WriteState::Flushing);
                            return Poll::Pending;
                        }
                    }
                }

                WriteState::Closing(err_opt) => {
                    match self.as_mut().poll_close(cx) {
                        Poll::Ready(Ok(_)) => {}
                        Poll::Ready(Err(poll_close_err)) => {
                            return Poll::Ready(Err(match err_opt {
                                Some(err) => {
                                    tracing::error!(err=?poll_close_err, "poll_close");
                                    err
                                }
                                None => poll_close_err,
                            }))
                        }
                        Poll::Pending => {
                            self.as_mut().set_write_state(WriteState::Closing(err_opt));
                            return Poll::Pending;
                        }
                    }
                    return Poll::Ready(match err_opt {
                        Some(err) => Err(err),
                        None => Ok(()),
                    });
                }
            }
        }
    }

    fn set_write_state(self: Pin<&mut Self>, new_write_state: WriteState<Resp, E>) {
        *self.project().write_state = new_write_state;
    }

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError<TP::InnerError>>> {
        self.transport()
            .poll_ready(cx)
            .map_err(TransportError::Ready)
    }

    fn start_send(
        self: Pin<&mut Self>,
        response: Response<Resp, E>,
    ) -> Result<(), TransportIOError> {
        self.transport()
            .start_send(response)
            .map_err(TransportError::Write)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError<TP::InnerError>>> {
        self.transport()
            .poll_flush(cx)
            .map_err(TransportError::Flush)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError<TP::InnerError>>> {
        self.transport()
            .poll_close(cx)
            .map_err(TransportError::Close)
    }

    fn transport(self: Pin<&mut Self>) -> Pin<&mut Fuse<TP>> {
        self.project().transport
    }
}

impl<TP, H, Req, Resp, E> Future for Server<TP, H, Req, Resp, E>
where
    H: Handler<Req, Resp, E> + Clone + Send + Sync + 'static,
    TP: Transport<Response<Resp, E>, Request<Req>, InnerError = io::Error> + Send,
    Req: Send + 'static,
    Resp: Send + 'static,
    E: Send + Debug + 'static,
{
    type Output = Result<(), TransportError<TP::InnerError>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match (self.as_mut().poll_read(cx)?, self.as_mut().poll_write(cx)?) {
                (Poll::Ready(_), _) | (_, Poll::Ready(_)) => {
                    tracing::info!("Shutdown: transport closed, so shutting down.");
                    return Poll::Ready(Ok(()));
                }
                _ => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::{convert::Infallible, fmt::Debug};

    use crate::{client::Client, ChannelTransport, Request, Response};

    use super::Server;

    #[tokio::test]
    async fn test_server() {
        let (client, server_transport) = client();

        tokio::spawn(Server::new(
            Default::default(),
            server_transport,
            |req| async move { Ok::<_, Infallible>(format!("hello {}", req)) },
        ));

        let resp = client.call("world".to_string()).await.unwrap();
        assert_eq!(resp, "hello world".to_string());

        let resp = client.call("lbw".to_string()).await.unwrap();
        assert_eq!(resp, "hello lbw".to_string());
    }

    fn client<Req, Resp, E>() -> (
        Client<Req, Resp, E>,
        ChannelTransport<Response<Resp, E>, Request<Req>>,
    )
    where
        Req: Send + 'static,
        Resp: Send + 'static,
        E: Send + Debug + 'static,
    {
        let (client_transport, server_transport) = ChannelTransport::unbounded();
        (
            Client::new(Default::default(), client_transport).spawn(),
            server_transport,
        )
    }
}

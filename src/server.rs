use std::{
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{ready, stream::Fuse, Sink, Stream, StreamExt};
use pin_project::pin_project;
use tokio::sync::mpsc;

use crate::{Request, Response, Transport};

pub trait Handler<Req, Resp, E> {
    type Fut<'a>: Future<Output = Result<Resp, E>> + Send + 'a
    where
        Self: 'a;

    fn serve(&self, req: Req) -> Self::Fut<'_>;
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

#[pin_project]
pub struct Server<TP, H, Req, Resp, E> {
    #[pin]
    transport: Fuse<TP>,
    handler: H,
    send_responses: mpsc::Sender<Response<Resp, E>>,
    recv_responses: mpsc::Receiver<Response<Resp, E>>,
    write_state: WriteState,
    _maker: PhantomData<Req>,
}

enum WriteState {
    Preparing,
    Ready,
    Flushing,
    Closing,
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
            write_state: WriteState::Preparing,
            _maker: PhantomData,
        }
    }
}

impl<TP, H, Req, Resp, E> Server<TP, H, Req, Resp, E>
where
    H: Handler<Req, Resp, E> + Clone + Send + Sync + 'static,
    TP: Transport<Response<Resp, E>, Request<Req>> + Send,
    TP::TransportError: Debug,
    Req: Send + Debug + 'static,
    Resp: Send + Debug + 'static,
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
    ) -> Poll<Result<(), TransportError<TP::TransportError>>> {
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
    ) -> Poll<Result<(), TransportError<TP::TransportError>>> {
        loop {
            let mut need_flush = false;
            match self.write_state {
                WriteState::Ready => {
                    match self.as_mut().poll_write_responses(cx, &mut need_flush)? {
                        Poll::Pending if need_flush => {
                            *self.as_mut().project().write_state = WriteState::Flushing;
                        }
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(_) => {
                            *self.as_mut().project().write_state = WriteState::Closing;
                        }
                    }
                }
                WriteState::Flushing => {
                    ready!(self.as_mut().poll_flush(cx)?);
                    *self.as_mut().project().write_state = WriteState::Ready;
                }
                WriteState::Preparing => {
                    ready!(self.as_mut().poll_ready(cx)?);
                    *self.as_mut().project().write_state = WriteState::Ready;
                }

                WriteState::Closing => {
                    ready!(self.as_mut().poll_close(cx)?);
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }

    fn poll_write_responses(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        need_flush: &mut bool,
    ) -> Poll<Result<(), TransportError<TP::TransportError>>> {
        while let Some(resp) = ready!(self.as_mut().project().recv_responses.poll_recv(cx)) {
            self.as_mut()
                .project()
                .transport
                .start_send(resp)
                .map_err(TransportError::Write)?;
            *need_flush = true;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError<TP::TransportError>>> {
        self.transport()
            .poll_ready(cx)
            .map_err(TransportError::Ready)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError<TP::TransportError>>> {
        self.transport()
            .poll_flush(cx)
            .map_err(TransportError::Flush)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError<TP::TransportError>>> {
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
    TP: Transport<Response<Resp, E>, Request<Req>> + Send,
    TP::TransportError: Debug,
    Req: Send + Debug + 'static,
    Resp: Send + Debug + 'static,
    E: Send + Debug + 'static,
{
    type Output = Result<(), TransportError<TP::TransportError>>;

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

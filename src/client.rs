use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
};

use fnv::FnvHashMap;
use futures::{ready, stream::Fuse, Sink, Stream, StreamExt};
use pin_project::pin_project;
use tokio::sync::{mpsc, oneshot};

use crate::{Request, Response, Transport};

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

#[derive(thiserror::Error, Debug)]
pub enum RpcError<SE> {
    /// The client disconnected from the server.
    #[error("the client disconnected from the server")]
    Disconnected,

    /// The request exceeded its deadline.
    #[error("the request exceeded its deadline")]
    DeadlineExceeded,

    /// The error from server
    #[error("the error from server")]
    ServerError(SE),
}

pub struct Config {
    pub max_pending_requests: usize,
    pub max_inflight_requests: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_pending_requests: 100,
            max_inflight_requests: 1000,
        }
    }
}

pub struct Client<Req, Resp, E> {
    next_request_id: AtomicU64,
    send_requests: mpsc::Sender<(oneshot::Sender<Response<Resp, E>>, Request<Req>)>,
}

impl<Req, Resp, E> Client<Req, Resp, E> {
    pub fn new<TP>(
        config: Config,
        transport: TP,
    ) -> NewClient<Self, RequestDispatch<TP, Req, Resp, E>>
    where
        TP: Transport<Request<Req>, Response<Resp, E>>,
    {
        let (send_requests, recv_requests) = mpsc::channel(config.max_pending_requests);

        NewClient {
            client: Client {
                next_request_id: AtomicU64::new(1),
                send_requests,
            },
            dispatch: RequestDispatch {
                max_inflight_requests: config.max_inflight_requests,
                transport: transport.fuse(),
                pending_requests: recv_requests,
                inflight_requests: InflightRequests::new(),
                write_state: WriteState::Preparing,
            },
        }
    }

    pub async fn call(&self, req: Req) -> Result<Resp, RpcError<E>> {
        let (send_resp, wait_resp) = oneshot::channel();
        let next_request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);

        self.send_requests
            .send((
                send_resp,
                Request {
                    id: next_request_id,
                    body: req,
                },
            ))
            .await
            .map_err(|_| RpcError::Disconnected)?;

        let response = wait_resp.await.map_err(|_| RpcError::Disconnected)?;
        response.result.map_err(RpcError::ServerError)
    }
}

pub struct NewClient<C, D> {
    client: C,
    dispatch: D,
}

impl<C, D, E> NewClient<C, D>
where
    D: Future<Output = Result<(), E>> + Send + 'static,
    E: Send + 'static,
{
    pub fn spawn(self) -> C {
        tokio::spawn(self.dispatch);
        self.client
    }
}

/// Wrap FnvHashMap to make pin work
struct InflightRequests<Resp, E> {
    requests: FnvHashMap<u64, oneshot::Sender<Response<Resp, E>>>,
}

impl<Resp, E> InflightRequests<Resp, E> {
    pub fn new() -> Self {
        Self {
            requests: FnvHashMap::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    pub fn len(&self) -> usize {
        self.requests.len()
    }

    pub fn insert_request(
        &mut self,
        request_id: u64,
        send_resp: oneshot::Sender<Response<Resp, E>>,
    ) {
        self.requests.insert(request_id, send_resp);
    }

    pub fn complete_request(&mut self, resp: Response<Resp, E>) {
        if let Some(send_resp) = self.requests.remove(&resp.id) {
            self.requests.shrink_to_fit();
            let _ = send_resp.send(resp);
        }
    }
}

#[pin_project]
pub struct RequestDispatch<TP, Req, Resp, E> {
    #[pin]
    transport: Fuse<TP>,
    max_inflight_requests: usize,
    pending_requests: mpsc::Receiver<(oneshot::Sender<Response<Resp, E>>, Request<Req>)>,
    inflight_requests: InflightRequests<Resp, E>,
    write_state: WriteState,
}

enum WriteState {
    Preparing,
    Ready,
    Flushing,
    Closing,
}

impl<TP, Req, Resp, E> RequestDispatch<TP, Req, Resp, E>
where
    TP: Transport<Request<Req>, Response<Resp, E>>,
{
    fn inflight_requests(self: Pin<&mut Self>) -> &mut InflightRequests<Resp, E> {
        self.project().inflight_requests
    }

    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError<TP::TransportError>>> {
        while ready!(self.as_mut().poll_read_response(cx)?).is_some() {}
        Poll::Ready(Ok(()))
    }

    fn poll_read_response(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(), TransportError<TP::TransportError>>>> {
        self.as_mut()
            .transport()
            .poll_next(cx)
            .map_err(TransportError::Read)
            .map_ok(|resp| self.inflight_requests().complete_request(resp))
    }

    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError<TP::TransportError>>> {
        loop {
            let mut need_flush = false;
            match self.write_state {
                WriteState::Ready => {
                    match self.as_mut().poll_write_requests(cx, &mut need_flush)? {
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
                    let inflight_requests = self.as_mut().project().inflight_requests;
                    if !inflight_requests.is_empty() {
                        tracing::info!(
                            "Shutdown: write half closed, and {} requests in flight.",
                            self.inflight_requests.len()
                        );
                        return Poll::Pending;
                    }
                    tracing::info!("Shutdown: write half closed, and no requests in flight.");
                    ready!(self.as_mut().poll_close(cx)?);
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }

    fn poll_write_requests(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        need_flush: &mut bool,
    ) -> Poll<Result<(), TransportError<TP::TransportError>>> {
        while let Some((send_resp, req)) =
            ready!(self.as_mut().project().pending_requests.poll_recv(cx))
        {
            if send_resp.is_closed() {
                // TODO: span.enter();
                tracing::info!("AbortRequest");
                continue;
            }
            let request_id = req.id;
            self.as_mut()
                .transport()
                .start_send(req)
                .map_err(TransportError::Write)?;
            self.as_mut()
                .inflight_requests()
                .insert_request(request_id, send_resp);
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

impl<TP, Req, Resp, E> Future for RequestDispatch<TP, Req, Resp, E>
where
    TP: Transport<Request<Req>, Response<Resp, E>>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_call() {
        let tp = ChannelTransport::unbounded(|req: Request<String>| Response {
            id: req.id,
            result: Ok::<_, String>(format!("hello {}", req.body)),
        });
        let client = Client::new(Default::default(), tp).spawn();

        let resp = client.call("world".to_string()).await.unwrap();
        assert_eq!(resp, "hello world".to_string());

        let resp = client.call("lbw".to_string()).await.unwrap();
        assert_eq!(resp, "hello lbw".to_string());
    }

    #[pin_project]
    struct ChannelTransport<Req, Resp> {
        send_request: mpsc::UnboundedSender<Req>,
        #[pin]
        recv_response: mpsc::UnboundedReceiver<Resp>,
    }

    impl<Req, Resp> ChannelTransport<Req, Resp>
    where
        Req: Send + 'static,
        Resp: Send + 'static,
    {
        fn unbounded(handler: impl Fn(Req) -> Resp + Send + Clone + 'static) -> Self {
            let (send_request, mut recv_request) = mpsc::unbounded_channel();
            let (send_response, recv_response) = mpsc::unbounded_channel();

            tokio::spawn(async move {
                while let Some(req) = recv_request.recv().await {
                    let _ = send_response.send(handler(req));
                }
            });

            Self {
                send_request,
                recv_response,
            }
        }
    }

    impl<Req, Resp> Sink<Req> for ChannelTransport<Req, Resp> {
        type Error = ();

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, req: Req) -> Result<(), Self::Error> {
            self.send_request.send(req).map_err(|_| ())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    impl<Req, Resp> Stream for ChannelTransport<Req, Resp> {
        type Item = Result<Resp, ()>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(ready!(self.project().recv_response.poll_recv(cx)).map(|x| Ok(x)))
        }
    }
}

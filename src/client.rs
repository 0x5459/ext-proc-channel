use std::{
    fmt::Debug,
    future::Future,
    io, mem,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures::{ready, stream::Fuse, Sink, Stream, StreamExt};
use pin_project::pin_project;
use tokio::sync::{mpsc, oneshot};

use self::inflight_requests::{DeadlineExceededError, InflightRequests};
use crate::{Request, Response, Transport, TransportError, TransportIOError};

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
    send_requests: mpsc::Sender<DispatchRequest<Req, Resp, E>>,
}

impl<Req, Resp, E> Client<Req, Resp, E> {
    pub fn new<TP>(
        config: Config,
        transport: TP,
    ) -> NewClient<Self, RequestDispatcher<TP, Req, Resp, E>>
    where
        TP: Transport<Request<Req>, Response<Resp, E>, InnerError = io::Error>,
    {
        let (send_requests, recv_requests) = mpsc::channel(config.max_pending_requests);

        NewClient {
            client: Client {
                next_request_id: AtomicU64::new(1),
                send_requests,
            },

            dispatch: RequestDispatcher {
                transport: transport.fuse(),
                pending_requests: recv_requests,
                inflight_requests: InflightRequests::new(),
                write_state: WriteState::PollPendingRequest { need_flush: false },
            },
        }
    }
    pub async fn call(&self, req: Req) -> Result<Resp, RpcError<E>> {
        self.call_timeout(req, Duration::from_secs(60)).await
    }

    pub async fn call_timeout(&self, req: Req, timeout: Duration) -> Result<Resp, RpcError<E>> {
        let (response_completion, wait_resp) = oneshot::channel();
        let next_request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        self.send_requests
            .send(DispatchRequest {
                deadline: Instant::now() + timeout,
                response_completion,
                request: Request {
                    id: next_request_id,
                    body: req,
                },
            })
            .await
            .map_err(|_| RpcError::Disconnected)?;

        let response = wait_resp
            .await
            .map_err(|_| RpcError::Disconnected)?
            .map_err(|_| RpcError::DeadlineExceeded)?;
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
    E: Send + Debug + 'static,
{
    pub fn spawn(self) -> C {
        let Self { client, dispatch } = self;
        tokio::spawn(async {
            if let Err(e) = dispatch.await {
                tracing::error!(err = ?e, "dispatch");
            }
        });
        client
    }
}

struct DispatchRequest<Req, Resp, E> {
    deadline: Instant,
    response_completion: oneshot::Sender<Result<Response<Resp, E>, DeadlineExceededError>>,
    request: Request<Req>,
}

#[pin_project]
pub struct RequestDispatcher<TP, Req, Resp, E> {
    #[pin]
    transport: Fuse<TP>,
    pending_requests: mpsc::Receiver<DispatchRequest<Req, Resp, E>>,
    inflight_requests: InflightRequests<Resp, E>,
    write_state: WriteState<Req, Resp, E>,
}

enum WriteState<Req, Resp, E> {
    PollPendingRequest { need_flush: bool },
    WritingRequest(DispatchRequest<Req, Resp, E>),
    Flushing,
    Closing(Option<TransportIOError>),
}

impl<Req, Resp, E> WriteState<Req, Resp, E> {
    fn take(&mut self, next_state: Self) -> Self {
        mem::replace(self, next_state)
    }
}

impl<TP, Req, Resp, E> RequestDispatcher<TP, Req, Resp, E>
where
    TP: Transport<Request<Req>, Response<Resp, E>, InnerError = io::Error>,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportIOError>> {
        loop {
            if ready!(self
                .as_mut()
                .transport()
                .poll_next(cx)
                .map_ok(|resp| self.as_mut().inflight_requests().complete_request(resp))
                .map_err(TransportError::Read)?)
            .is_none()
            {
                return Poll::Ready(Ok(()));
            }
        }
    }

    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportIOError>> {
        loop {
            match self
                .as_mut()
                .project()
                .write_state
                .take(WriteState::Closing(None))
            {
                WriteState::PollPendingRequest { need_flush } => {
                    *self.as_mut().project().write_state =
                        match self.as_mut().project().pending_requests.poll_recv(cx) {
                            Poll::Ready(Some(dispatch_request)) => {
                                if dispatch_request.response_completion.is_closed() {
                                    // TODO: span.enter();
                                    tracing::info!("AbortRequest");
                                    WriteState::PollPendingRequest { need_flush }
                                } else {
                                    WriteState::WritingRequest(dispatch_request)
                                }
                            }
                            Poll::Ready(None) => WriteState::Closing(None),
                            Poll::Pending => {
                                if need_flush {
                                    WriteState::Flushing
                                } else {
                                    self.as_mut()
                                        .set_write_state(WriteState::PollPendingRequest {
                                            need_flush: false,
                                        });
                                    return Poll::Pending;
                                }
                            }
                        }
                }
                WriteState::WritingRequest(r) => {
                    *self.as_mut().project().write_state = match self.as_mut().poll_ready(cx) {
                        Poll::Ready(Ok(_)) => match self.as_mut().write_request(r) {
                            Ok(_) => WriteState::PollPendingRequest { need_flush: true },
                            Err(err) => WriteState::Closing(Some(err)),
                        },
                        Poll::Ready(Err(err)) => WriteState::Closing(Some(err)),
                        Poll::Pending => {
                            self.as_mut().set_write_state(WriteState::WritingRequest(r));
                            return Poll::Pending;
                        }
                    }
                }
                WriteState::Flushing => {
                    *self.as_mut().project().write_state = match self.as_mut().poll_flush(cx) {
                        Poll::Ready(Ok(_)) => WriteState::PollPendingRequest { need_flush: false },
                        Poll::Ready(Err(e)) => WriteState::Closing(Some(e)),
                        Poll::Pending => {
                            self.as_mut().set_write_state(WriteState::Flushing);
                            return Poll::Pending;
                        }
                    }
                }
                WriteState::Closing(err_opt) => {
                    let inflight_requests = self.as_mut().inflight_requests();
                    if !inflight_requests.is_empty() {
                        tracing::info!(
                            "Shutdown: write half closed, and {} requests in flight.",
                            self.inflight_requests.len()
                        );
                        self.as_mut().set_write_state(WriteState::Closing(err_opt));
                        return Poll::Pending;
                    }
                    tracing::info!("Shutdown: write half closed, and no requests in flight.");
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

    fn poll_expired(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            match ready!(self.as_mut().inflight_requests().poll_expired(cx)) {
                Some(expired) => {
                    tracing::trace!("request: {} expired", expired);
                }
                None => {
                    return Poll::Ready(());
                }
            }
        }
    }

    fn set_write_state(self: Pin<&mut Self>, new_write_state: WriteState<Req, Resp, E>) {
        *self.project().write_state = new_write_state;
    }

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportIOError>> {
        self.transport()
            .poll_ready(cx)
            .map_err(TransportError::Ready)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportIOError>> {
        self.transport()
            .poll_flush(cx)
            .map_err(TransportError::Flush)
    }

    fn write_request(
        mut self: Pin<&mut Self>,
        dispatch_request: DispatchRequest<Req, Resp, E>,
    ) -> Result<(), TransportIOError> {
        let request_id = dispatch_request.request.id;
        self.as_mut().start_send(dispatch_request.request)?;
        self.as_mut().inflight_requests().insert_request(
            request_id,
            dispatch_request.deadline,
            dispatch_request.response_completion,
        );
        Ok(())
    }

    fn start_send(self: Pin<&mut Self>, request: Request<Req>) -> Result<(), TransportIOError> {
        self.transport()
            .start_send(request)
            .map_err(TransportError::Write)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportIOError>> {
        self.transport()
            .poll_close(cx)
            .map_err(TransportError::Close)
    }

    fn transport(self: Pin<&mut Self>) -> Pin<&mut Fuse<TP>> {
        self.project().transport
    }

    fn inflight_requests(self: Pin<&mut Self>) -> &mut InflightRequests<Resp, E> {
        self.project().inflight_requests
    }
}

impl<TP, Req, Resp, E> Future for RequestDispatcher<TP, Req, Resp, E>
where
    TP: Transport<Request<Req>, Response<Resp, E>, InnerError = io::Error>,
{
    type Output = Result<(), TransportIOError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match (
            self.as_mut().poll_read(cx)?,
            self.as_mut().poll_write(cx)?,
            self.as_mut().poll_expired(cx),
        ) {
            (Poll::Ready(_), _, _) | (_, Poll::Ready(_), _) => return Poll::Ready(Ok(())),
            _ => Poll::Pending,
        }
    }
}

mod inflight_requests {
    use std::{
        collections::hash_map::Entry,
        task::{Context, Poll},
        time::Instant,
    };

    use fnv::FnvHashMap;
    use tokio::sync::oneshot;
    use tokio_util::time::{delay_queue, DelayQueue};

    use crate::Response;

    /// The request exceeded its deadline.
    #[derive(thiserror::Error, Debug)]
    #[non_exhaustive]
    #[error("the request exceeded its deadline")]
    pub struct DeadlineExceededError;

    struct RequestData<Resp, E> {
        response_completion: oneshot::Sender<Result<Response<Resp, E>, DeadlineExceededError>>,
        /// The key to remove the timer for the request's deadline.
        deadline_key: delay_queue::Key,
    }

    /// Wrap FnvHashMap to make pin work
    pub struct InflightRequests<Resp, E> {
        requests: FnvHashMap<u64, RequestData<Resp, E>>,
        deadlines: DelayQueue<u64>,
    }

    impl<Resp, E> InflightRequests<Resp, E> {
        pub fn new() -> Self {
            Self {
                requests: Default::default(),
                deadlines: Default::default(),
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
            deadline: Instant,
            response_completion: oneshot::Sender<Result<Response<Resp, E>, DeadlineExceededError>>,
        ) {
            match self.requests.entry(request_id) {
                Entry::Vacant(vacant) => {
                    let deadline_key = self.deadlines.insert_at(request_id, deadline.into());
                    vacant.insert(RequestData {
                        response_completion,
                        deadline_key,
                    });
                }
                Entry::Occupied(_) => {}
            }
        }

        pub fn complete_request(&mut self, resp: Response<Resp, E>) {
            if let Some(request_data) = self.requests.remove(&resp.id) {
                self.requests.shrink_to_fit();
                self.deadlines.remove(&request_data.deadline_key);
                let _ = request_data.response_completion.send(Ok(resp));
            }
        }

        pub fn poll_expired(&mut self, cx: &mut Context) -> Poll<Option<u64>> {
            self.deadlines.poll_expired(cx).map(|expired| {
                let request_id = expired?.into_inner();
                if let Some(request_data) = self.requests.remove(&request_id) {
                    // let _entered = request_data.span.enter();
                    tracing::error!("DeadlineExceeded");
                    self.requests.shrink_to_fit();
                    let _ = request_data
                        .response_completion
                        .send(Err(DeadlineExceededError));
                }
                Some(request_id)
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use tokio::time::sleep;

    use crate::{
        server::{self, Server},
        ChannelTransport,
    };

    use super::*;

    #[tokio::test]
    async fn test_client_call() {
        let tp = server(|req: String| async move { Ok::<_, Infallible>(format!("hello {}", req)) });
        let client = Client::new(Default::default(), tp).spawn();

        let resp = client.call("world".to_string()).await.unwrap();
        assert_eq!(resp, "hello world".to_string());

        let resp = client.call("lbw".to_string()).await.unwrap();
        assert_eq!(resp, "hello lbw".to_string());
    }

    #[tokio::test]
    async fn test_client_request_deadline() {
        let timeout = Duration::from_secs(1);
        let tp = server(move |req: String| async move {
            sleep(timeout).await;
            Ok::<_, Infallible>(format!("hello {}", req))
        });
        let client = Client::new(Default::default(), tp).spawn();

        assert!(matches!(
            client
                .call_timeout("world".to_string(), Duration::from_millis(100))
                .await,
            Err(RpcError::DeadlineExceeded)
        ));

        let resp = client
            .call_timeout("lbw".to_string(), timeout + Duration::from_millis(100))
            .await
            .unwrap();
        assert_eq!(resp, "hello lbw".to_string());
    }

    fn server<Req, Resp, E>(
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
}

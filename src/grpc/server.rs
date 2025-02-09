use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::result::Result as StdResult;
use std::sync::Arc;

use futures::Stream;
use snafu::ResultExt;
use tokio::fs;
use tokio::net::UnixListener;
use tokio::sync::mpsc::channel;
use tokio::sync::Notify;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::info;
use tracing::warn;

use super::proto;
use super::proto::shm_control_server::ShmControl;
use super::proto::shm_control_server::ShmControlServer;
use super::proto::NotifyRequest;
use super::proto::PingRequest;
use crate::consumer::session_manager::Session;
use crate::consumer::session_manager::SessionManagerRef;
use crate::error;
use crate::error::Result;

pub struct ShmCtlServer<F> {
    sock_path: PathBuf,
    notify: Arc<Notify>,
    session_manager: SessionManagerRef,
    shutdown: Option<F>,
}

impl<F> ShmCtlServer<F>
where
    F: Future<Output = ()>,
{
    pub fn with_shutdown(
        sock_path: impl Into<PathBuf>,
        notify: Arc<Notify>,
        session_manager: SessionManagerRef,
        shutdown: Option<F>,
    ) -> Self {
        ShmCtlServer {
            sock_path: sock_path.into(),
            notify,
            session_manager,
            shutdown,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        if let Some(parent) = self.sock_path.parent() {
            fs::create_dir_all(parent).await.context(error::IoSnafu)?;
        }

        if self.sock_path.metadata().is_ok() {
            fs::remove_file(&self.sock_path)
                .await
                .context(error::IoSnafu)?;

            info!("remove the unix socket file: {:?}", self.sock_path);
        }

        let listener =
            UnixListener::bind(&self.sock_path).context(error::IoSnafu)?;
        let uds_stream = UnixListenerStream::new(listener);

        let handler = ShmCtlHandler {
            notify: self.notify.clone(),
            session_manager: self.session_manager.clone(),
        };
        let shm_ctl = ShmControlServer::new(handler);

        if let Some(shutdown) = self.shutdown.take() {
            Server::builder()
                .add_service(shm_ctl)
                .serve_with_incoming_shutdown(uds_stream, shutdown)
                .await
                .context(error::ServeWithIncomingSnafu)
        } else {
            Server::builder()
                .add_service(shm_ctl)
                .serve_with_incoming(uds_stream)
                .await
                .context(error::ServeWithIncomingSnafu)
        }
    }
}

pub struct ShmCtlHandler {
    pub(crate) notify: Arc<Notify>,
    pub(crate) session_manager: SessionManagerRef,
}

#[async_trait::async_trait]
impl ShmControl for ShmCtlHandler {
    async fn notify(
        &self,
        request: Request<NotifyRequest>,
    ) -> StdResult<Response<proto::NotifyResponse>, Status> {
        let NotifyRequest { producer_id: _ } = request.into_inner();

        self.notify.notify_one();

        let resp = proto::NotifyResponse {};

        Ok(Response::new(resp))
    }

    async fn ping(
        &self,
        request: Request<PingRequest>,
    ) -> StdResult<Response<proto::PingResponse>, Status> {
        let PingRequest { producer_id } = request.into_inner();
        let mut missing_memfd = false;

        self.session_manager
            .sessions
            .entry(producer_id.clone())
            .or_insert_with(|| {
                missing_memfd = true;
                Arc::new(Session::new(producer_id))
            });

        let resp = proto::PingResponse { missing_memfd };
        return Ok(Response::new(resp));
    }

    type FetchResultStream = Pin<
        Box<
            dyn Stream<Item = StdResult<proto::ResultSet, Status>>
                + Send
                + Sync,
        >,
    >;

    async fn fetch_result(
        &self,
        request: Request<proto::FetchResultRequest>,
    ) -> StdResult<Response<Self::FetchResultStream>, Status> {
        let req = request.into_inner();
        let producer_id = req.producer_id;

        let (tx, rx) = channel::<StdResult<proto::ResultSet, Status>>(1024);

        self.session_manager.set_result_sender(&producer_id, tx);

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::FetchResultStream
        ))
    }
}

impl<F> Drop for ShmCtlServer<F> {
    fn drop(&mut self) {
        if self.sock_path.metadata().is_err() {
            return;
        }

        if std::fs::remove_file(&self.sock_path).is_err() {
            warn!("failed to remove unix socket file: {:?}", self.sock_path);
            return;
        }

        info!("remove unix socket file: {:?}", self.sock_path);
    }
}

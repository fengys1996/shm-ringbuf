use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use snafu::ResultExt;
use tokio::fs;
use tokio::net::UnixListener;
use tokio::sync::Notify;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic::Request;
use tracing::info;
use tracing::warn;

use super::proto;
use super::proto::shm_control_server::ShmControl;
use super::proto::shm_control_server::ShmControlServer;
use super::proto::NotifyRequest;
use super::proto::PingRequest;
use super::status_code::StatusCode;
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

struct ShmCtlHandler {
    notify: Arc<Notify>,
    session_manager: SessionManagerRef,
}

#[async_trait::async_trait]
impl ShmControl for ShmCtlHandler {
    async fn notify(
        &self,
        request: Request<NotifyRequest>,
    ) -> std::result::Result<
        tonic::Response<proto::NotifyResponse>,
        tonic::Status,
    > {
        let NotifyRequest { producer_id: _ } = request.into_inner();

        self.notify.notify_one();

        let resp = proto::NotifyResponse {};

        Ok(tonic::Response::new(resp))
    }

    async fn ping(
        &self,
        request: Request<PingRequest>,
    ) -> std::result::Result<tonic::Response<proto::PingResponse>, tonic::Status>
    {
        let PingRequest { producer_id } = request.into_inner();

        if self.session_manager.get(&producer_id).is_none() {
            let resp = proto::PingResponse {
                status_code: StatusCode::MissingFD as u32,
                status_message: "ringbuf was not found".to_string(),
            };

            return Ok(tonic::Response::new(resp));
        }

        let resp = proto::PingResponse {
            status_code: StatusCode::Success as u32,
            status_message: "".to_string(),
        };

        Ok(tonic::Response::new(resp))
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

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use snafu::ResultExt;
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio::sync::Notify;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::transport::Server;
use tonic::transport::Uri;
use tonic::Request;
use tower::service_fn;
use tracing::error;
use tracing::info;

use self::proto::shm_control_client::ShmControlClient;
use self::proto::shm_control_server::ShmControl;
use self::proto::shm_control_server::ShmControlServer;
use self::proto::HandShakeRequest;
use self::proto::NotifyRequest;
use self::proto::PingRequest;
use crate::consumer::RingbufStore;
use crate::error;
use crate::error::Result;
use crate::ringbuf::VERSION;

pub mod proto {
    tonic::include_proto!("shm");
}

#[derive(Clone)]
pub struct GrpcClient {
    producer_id: String,
    channel: Channel,
}

impl GrpcClient {
    pub fn new(
        producer_id: String,
        sock_path: impl Into<PathBuf>,
    ) -> GrpcClient {
        let sock_path = sock_path.into();
        // We will ignore this uri because uds do not use it
        // if your connector does use the uri it will be provided
        // as the request to the `MakeConnection`.
        let channel = Endpoint::try_from("http://[::]:50051")
            // Unwrap safety: the uri is valid.
            .unwrap()
            .connect_with_connector_lazy(service_fn(move |_: Uri| {
                UnixStream::connect(sock_path.clone())
            }));

        GrpcClient {
            producer_id,
            channel,
        }
    }
}

impl GrpcClient {
    /// Handshake with the shm server.
    pub async fn handshake(&self) -> Result<()> {
        let req = HandShakeRequest {
            version: VERSION,
            producer_id: self.producer_id.clone(),
        };
        let resp = ShmControlClient::new(self.channel.clone())
            .hand_shake(Request::new(req))
            .await
            .context(error::TonicSnafu {})?
            .into_inner();

        check_error(resp.status_code, resp.status_message)
    }

    /// Notify the server that the data has been written to shared memory.
    pub async fn notify(&self) -> Result<()> {
        let req = NotifyRequest {
            producer_id: self.producer_id.clone(),
        };

        let _resp = ShmControlClient::new(self.channel.clone())
            .notify(Request::new(req))
            .await
            .context(error::TonicSnafu {})?
            .into_inner();

        Ok(())
    }

    pub async fn ping(&self) -> Result<()> {
        let ping_req = PingRequest {
            producer_id: self.producer_id.clone(),
        };

        let resp = ShmControlClient::new(self.channel.clone())
            .ping(Request::new(ping_req))
            .await
            .context(error::TonicSnafu {})?
            .into_inner();

        check_error(resp.status_code, resp.status_message)
    }
}

fn check_error(status_code: u32, status_msg: String) -> Result<()> {
    let Ok(status) = StatusCode::try_from(status_code) else {
        return error::InvalidParameterSnafu {
            detail: format!("failed to convert {} to status code", status_code),
        }
        .fail();
    };

    match status {
        StatusCode::Success => Ok(()),
        // MissingFd requires special handling. If the client finds this error,
        // it needs to re_send memfd to the shm_server.
        StatusCode::MissingFD => error::NotFoundRingbufSnafu.fail(),
        _ => error::GenericSnafu {
            status_code,
            status_msg,
        }
        .fail(),
    }
}

pub enum StatusCode {
    Success = 0,
    VersionMismatch = 1,
    MissingFD = 2,
    RingbufRead = 3,
}

impl TryFrom<u32> for StatusCode {
    type Error = String;

    fn try_from(value: u32) -> std::result::Result<Self, String> {
        match value {
            0 => Ok(StatusCode::Success),
            1 => Ok(StatusCode::VersionMismatch),
            2 => Ok(StatusCode::MissingFD),
            3 => Ok(StatusCode::RingbufRead),
            _ => Err(format!("Invalid status code: {}", value)),
        }
    }
}

pub struct ShmCtlServer<F> {
    sock_path: PathBuf,
    notify: Arc<Notify>,
    ringbuf_store: RingbufStore,
    shutdown: Option<F>,
}

impl<F> ShmCtlServer<F>
where
    F: Future<Output = ()>,
{
    pub fn with_shutdown(
        sock_path: impl Into<PathBuf>,
        notify: Arc<Notify>,
        ringbuf_store: RingbufStore,
        shutdown: Option<F>,
    ) -> Self {
        ShmCtlServer {
            sock_path: sock_path.into(),
            notify,
            ringbuf_store,
            shutdown,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let listener =
            UnixListener::bind(&self.sock_path).context(error::IoSnafu)?;
        let uds_stream = UnixListenerStream::new(listener);

        let handler = ShmCtlHandler {
            notify: self.notify.clone(),
            ringbuf_store: self.ringbuf_store.clone(),
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
    ringbuf_store: RingbufStore,
}

#[async_trait::async_trait]
impl ShmControl for ShmCtlHandler {
    async fn hand_shake(
        &self,
        request: Request<HandShakeRequest>,
    ) -> std::result::Result<
        tonic::Response<proto::HandShakeResponse>,
        tonic::Status,
    > {
        let HandShakeRequest {
            version,
            producer_id,
        } = request.into_inner();

        if version != VERSION {
            let err_msg = format!(
                "version mismatch, producer version: {}, consumer version: {}, producer id: {}",
                version, VERSION, producer_id
            );

            error!("handshake failed: {}", err_msg);

            let resp = proto::HandShakeResponse {
                status_code: StatusCode::VersionMismatch as u32,
                status_message: err_msg,
            };

            return Ok(tonic::Response::new(resp));
        }

        info!("handshake success, producer_id: {}", producer_id);

        let resp = proto::HandShakeResponse {
            status_code: StatusCode::Success as u32,
            status_message: "".to_string(),
        };

        Ok(tonic::Response::new(resp))
    }

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

        if self.ringbuf_store.get(producer_id).is_none() {
            let resp = proto::PingResponse {
                status_code: StatusCode::MissingFD as u32,
                status_message: "The corresponding ringbuf was not found"
                    .to_string(),
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

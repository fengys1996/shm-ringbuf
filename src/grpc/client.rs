use std::path::PathBuf;

use hyper_util::rt::TokioIo;
use snafu::ResultExt;
use tokio::net::UnixStream;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::transport::Uri;
use tonic::Request;
use tower::service_fn;

use super::proto::shm_control_client::ShmControlClient;
use super::proto::NotifyRequest;
use super::proto::PingRequest;
use super::status_code::StatusCode;
use crate::error;
use crate::error::Result;

#[derive(Clone)]
pub struct GrpcClient {
    client_id: String,
    channel: Channel,
}

impl GrpcClient {
    pub fn new(
        producer_id: impl Into<String>,
        sock_path: impl Into<PathBuf>,
    ) -> GrpcClient {
        let producer_id = producer_id.into();
        let sock_path = sock_path.into();

        let connector = service_fn(move |_: Uri| {
            let sock_path = sock_path.clone();
            async move {
                Ok::<_, std::io::Error>(TokioIo::new(
                    UnixStream::connect(&sock_path).await?,
                ))
            }
        });

        // We will ignore this uri because uds do not use it
        // if your connector does use the uri it will be provided
        // as the request to the `MakeConnection`.
        let channel = Endpoint::try_from("http://[::]:50051")
            .unwrap()
            .connect_with_connector_lazy(connector);

        GrpcClient {
            client_id: producer_id,
            channel,
        }
    }
}

impl GrpcClient {
    /// Notify the consumer that the data has been written to shared memory.
    pub async fn notify(&self) -> Result<()> {
        let req = NotifyRequest {
            producer_id: self.client_id.clone(),
        };

        let _resp = ShmControlClient::new(self.channel.clone())
            .notify(Request::new(req))
            .await
            .context(error::TonicSnafu {})?
            .into_inner();

        Ok(())
    }

    /// Check if the server is available.
    pub async fn ping(&self) -> Result<()> {
        let ping_req = PingRequest {
            producer_id: self.client_id.clone(),
        };

        let resp = ShmControlClient::new(self.channel.clone())
            .ping(Request::new(ping_req))
            .await
            .context(error::TonicSnafu {})?
            .into_inner();

        check_error(resp.status_code, resp.status_message)
    }
}

fn check_error(status_code: u32, _status_msg: String) -> Result<()> {
    let Ok(status) = StatusCode::try_from(status_code) else {
        return error::InvalidParameterSnafu {
            detail: format!("convert {} to status code", status_code),
        }
        .fail();
    };

    match status {
        StatusCode::Success => Ok(()),
        // MissingFd requires special handling. If the client finds this error,
        // it needs to re_send memfd to the shm_server.
        StatusCode::MissingFD => error::NotFoundRingbufSnafu.fail(),
    }
}

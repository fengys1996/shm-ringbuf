use std::path::PathBuf;

use hyper_util::rt::TokioIo;
use snafu::ResultExt;
use tokio::net::UnixStream;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::transport::Uri;
use tonic::Request;
use tonic::Streaming;
use tower::service_fn;

use super::proto::shm_control_client::ShmControlClient;
use super::proto::FetchResultRequest;
use super::proto::NotifyRequest;
use super::proto::PingRequest;
use super::proto::PingResponse;
use super::proto::ResultSet;
use crate::error;
use crate::error::Result;

#[derive(Clone)]
pub struct GrpcClient {
    client_id: String,
    channel: Channel,
    enable_result_fetch: bool,
}

impl GrpcClient {
    pub fn new(
        producer_id: impl Into<String>,
        sock_path: impl Into<PathBuf>,
        enable_result_fetch: bool,
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
            enable_result_fetch,
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
    pub async fn ping(&self) -> Result<PingResponse> {
        let ping_req = PingRequest {
            producer_id: self.client_id.clone(),
            enable_result_fetch: self.enable_result_fetch,
        };

        let resp = ShmControlClient::new(self.channel.clone())
            .ping(Request::new(ping_req))
            .await
            .context(error::TonicSnafu {})?
            .into_inner();

        Ok(resp)
    }

    pub async fn fetch_result(&self) -> Result<Streaming<ResultSet>> {
        let req = FetchResultRequest {
            producer_id: self.client_id.clone(),
        };

        let result_stream = ShmControlClient::new(self.channel.clone())
            .fetch_result(Request::new(req))
            .await
            .context(error::TonicSnafu {})?
            .into_inner();

        Ok(result_stream)
    }
}

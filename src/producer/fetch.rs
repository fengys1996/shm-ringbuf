use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use snafu::ResultExt;
use tokio::sync::oneshot::channel;
use tokio::sync::oneshot::Receiver;
use tokio::sync::oneshot::Sender;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tonic::Streaming;
use tracing::debug;

use crate::error;
use crate::error::DataProcessResult;
use crate::error::Result;
use crate::grpc::client::GrpcClient;
use crate::grpc::proto::ResultSet;

pub type RequestId = u32;

/// The [`ResultFetcher`] has two abilities:
/// 1. Fetch the results of data processing on the consumer side.
/// 2. Send the fetched results to the corresponding subscribed producer.
pub struct ResultFetcher {
    inner: Arc<Inner>,
}

struct Inner {
    grpc_client: GrpcClient,
    retry_interval: Duration,
    normal: AtomicBool,
    subscriptions: DashMap<RequestId, Sender<DataProcessResult>>,
}

impl ResultFetcher {
    pub async fn new(
        grpc_client: GrpcClient,
        retry_interval: Duration,
    ) -> ResultFetcher {
        let normal = AtomicBool::new(false);
        let subscriptions = DashMap::new();

        let inner = Inner {
            grpc_client,
            retry_interval,
            normal,
            subscriptions,
        };
        let inner = Arc::new(inner);

        let fetcher = ResultFetcher {
            inner: inner.clone(),
        };

        // Try to fetch the result stream and update the normal flag immediately.
        let mut may_stream =
            fetcher.inner.grpc_client.fetch_result().await.ok();
        if may_stream.is_some() {
            inner.normal.store(true, Ordering::Relaxed);
        } else {
            inner.normal.store(false, Ordering::Relaxed);
        }

        tokio::spawn(async move {
            loop {
                if let Some(stream) = may_stream.take() {
                    if let Err(e) = fetcher.handle_stream(stream).await {
                        debug!("failed to handle stream, detail: {:?}", e);
                    }
                } else if let Err(e) = fetcher.fetch_result_stream().await {
                    debug!("failed to fetch result, detail: {:?}", e);
                };

                fetcher.inner.normal.store(false, Ordering::Relaxed);
                fetcher.inner.subscriptions.clear();
                sleep(fetcher.inner.retry_interval).await;
            }
        });

        ResultFetcher { inner }
    }

    /// Subscribe to the result set corresponding to the request id.
    pub fn subscribe(&self, request_id: u32) -> Receiver<DataProcessResult> {
        let (tx, rx) = channel();
        self.inner.subscriptions.insert(request_id, tx);
        rx
    }

    /// Check if the result fetcher is working normally.
    pub fn is_normal(&self) -> bool {
        self.inner.normal.load(Ordering::Relaxed)
    }

    async fn fetch_result_stream(&self) -> Result<()> {
        let result_stream = self.inner.grpc_client.fetch_result().await?;

        self.inner.normal.store(true, Ordering::Relaxed);

        self.handle_stream(result_stream).await
    }

    async fn handle_stream(
        &self,
        mut result_stream: Streaming<ResultSet>,
    ) -> Result<()> {
        while let Some(may_result) = result_stream.next().await {
            let result = may_result.context(error::TonicSnafu)?;
            self.handle_result(result).await;
        }

        Ok(())
    }

    async fn handle_result(&self, result: ResultSet) {
        for result in result.results {
            let subscription = self.inner.subscriptions.remove(&result.id);
            if let Some((_, sender)) = subscription {
                let result = DataProcessResult {
                    status_code: result.status_code,
                    message: result.message,
                };
                let _ = sender.send(result);
            }
        }
    }
}

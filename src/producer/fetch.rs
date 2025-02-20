use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

use dashmap::DashMap;
use snafu::ensure;
use snafu::ResultExt;
use tokio::sync::oneshot::channel;
use tokio::sync::oneshot::Receiver;
use tokio::sync::oneshot::Sender;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
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
    normal: AtomicBool,
    subscriptions: DashMap<RequestId, Sender<DataProcessResult>>,
    expirations: RwLock<VecDeque<(RequestId, Instant)>>,
}

impl ResultFetcher {
    pub async fn new(
        grpc_client: GrpcClient,
        reconnect_interval: Duration,
        // The interval for checking the expired result fetch subscriptions.
        expired_check_interval: Duration,
        cancel: CancellationToken,
    ) -> ResultFetcher {
        let normal = AtomicBool::new(false);
        let subscriptions = DashMap::new();
        let expirations = RwLock::new(VecDeque::new());

        let inner = Inner {
            grpc_client,
            normal,
            subscriptions,
            expirations,
        };
        let inner = Arc::new(inner);

        let fetcher = ResultFetcher {
            inner: inner.clone(),
        };

        // Start a task to check the expired result fetch subscriptions periodically.
        let inner_c = inner.clone();
        tokio::spawn(async move {
            loop {
                if cancel.is_cancelled() {
                    break;
                }
                sleep(expired_check_interval).await;
                {
                    let mut expirations = inner_c.expirations.write().unwrap();
                    clean_expired_subscriptions(
                        &inner_c.subscriptions,
                        &mut expirations,
                    );
                }
            }
        });

        // Try to fetch the result stream and update the normal flag immediately.
        let may_stream = fetcher.inner.grpc_client.fetch_result().await;
        if let Err(e) = &may_stream {
            debug!("failed to fetch result stream, detail: {:?}", e);
            inner.normal.store(false, Ordering::Relaxed);
        } else {
            inner.normal.store(true, Ordering::Relaxed);
        }

        // Start a task to fetch the result stream.
        tokio::spawn(async move {
            let mut may_stream = may_stream.ok();
            loop {
                if let Some(stream) = may_stream.take() {
                    if let Err(e) = fetcher.handle_stream(stream).await {
                        debug!("failed to handle stream, detail: {:?}", e);
                    }
                } else if let Err(e) = fetcher.fetch_result_stream().await {
                    debug!("failed to fetch result, detail: {:?}", e);
                };

                fetcher.inner.normal.store(false, Ordering::Relaxed);
                sleep(reconnect_interval).await;
            }
        });

        ResultFetcher { inner }
    }

    /// Subscribe to the result set corresponding to the request id. If the result
    /// fetcher is not ready, an error will be returned.
    pub fn subscribe(
        &self,
        request_id: u32,
    ) -> Result<Receiver<DataProcessResult>> {
        ensure!(self.is_normal(), error::ResultFetchNotReadySnafu);

        let (tx, rx) = channel();
        self.inner.subscriptions.insert(request_id, tx);
        let expired_at = Instant::now() + Duration::from_secs(5);
        self.inner
            .expirations
            .write()
            .unwrap()
            .push_back((request_id, expired_at));
        Ok(rx)
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

fn clean_expired_subscriptions<S>(
    subscriptions: &DashMap<RequestId, S>,
    expirations: &mut VecDeque<(RequestId, Instant)>,
) {
    let now = Instant::now();

    while let Some((req_id, expiration)) = expirations.front() {
        let req_id = *req_id;

        if *expiration > now {
            break;
        }

        expirations.pop_front();
        subscriptions.remove(&req_id);
    }
}

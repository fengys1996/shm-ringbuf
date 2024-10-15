use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::oneshot::channel;
use tokio::sync::oneshot::Receiver;
use tokio::sync::oneshot::Sender;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tracing::warn;

use crate::error::DataProcessResult;
use crate::grpc::client::GrpcClient;
use crate::grpc::proto::ResultSet;

pub struct ResultFetcher {
    grpc_client: GrpcClient,
    retry_interval: Duration,
    registers: DashMap<u32, Sender<DataProcessResult>>,
    is_normal: AtomicBool,
}

pub type ResultFetcherRef = std::sync::Arc<ResultFetcher>;

impl ResultFetcher {
    pub fn new(grpc_client: GrpcClient, retry_interval: Duration) -> Self {
        Self {
            grpc_client,
            retry_interval,
            registers: DashMap::new(),
            is_normal: AtomicBool::new(false),
        }
    }

    pub async fn run(&self) {
        loop {
            let mut result_set_stream =
                match self.grpc_client.fetch_result().await {
                    Ok(stream) => stream,
                    Err(e) => {
                        warn!("failed to fetch result error: {:?}", e);
                        sleep(self.retry_interval).await;
                        continue;
                    }
                };

            self.is_normal.store(true, Ordering::Relaxed);

            while let Some(result) = result_set_stream.next().await {
                match result {
                    Ok(result_set) => {
                        self.handle_result(result_set).await;
                    }
                    Err(e) => {
                        warn!("failed to fetch result error: {:?}", e);
                    }
                }
            }
            self.registers.clear();
            self.is_normal.store(false, Ordering::Relaxed);
            sleep(self.retry_interval).await;
        }
    }

    async fn handle_result(&self, result_set: ResultSet) {
        for result in result_set.results {
            if let Some((_, sender)) = self.registers.remove(&result.id) {
                let result = DataProcessResult {
                    status_code: result.status_code,
                    message: result.message.clone(),
                };
                let _ = sender.send(result);
            }
        }
    }

    pub fn register(&self, req_id: u32) -> Receiver<DataProcessResult> {
        let (tx, rx) = channel();
        self.registers.insert(req_id, tx);
        rx
    }

    pub fn is_normal(&self) -> bool {
        self.is_normal.load(Ordering::Relaxed)
    }
}

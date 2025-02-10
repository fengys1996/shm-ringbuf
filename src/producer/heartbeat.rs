use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;

use super::SessionHandleRef;
use crate::grpc::client::GrpcClient;

/// Heartbeat service is used to keep the connection between the consumer and
/// the producer.
pub struct Heartbeat {
    pub(super) online: Arc<AtomicBool>,
    pub(super) client: GrpcClient,
    pub(super) heartbeat_interval: Duration,
    pub(super) session_handle: SessionHandleRef,
}

impl Heartbeat {
    pub async fn run(&self, cancel: CancellationToken) {
        let mut tick = interval(self.heartbeat_interval);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("heartbeat canceled");
                    break;
                },
                _ = tick.tick() => {}
            }

            self.ping().await;
        }
    }

    // TODO: refactor it
    pub async fn ping(&self) {
        match self.client.ping().await {
            Ok(resp) => {
                if resp.missing_memfd {
                    if let Err(e) = self.session_handle.send().await {
                        warn!(
                            "not found ringbuf, failed to re-send session, error: {:?}",
                            e
                        );
                        self.set_online(false);
                    } else {
                        info!("not found ringbuf, re-send session success");
                        self.set_online(true);
                    }
                } else {
                    self.set_online(true);
                }
            }
            Err(e) => {
                warn!("failed to ping, error: {:?}", e);
                self.set_online(false);
            }
        }
    }

    fn set_online(&self, online: bool) {
        self.online.store(online, Ordering::Relaxed);
    }
}

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::error;
use crate::grpc::GrpcClient;

use super::SessionHandleRef;

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
                _ = tick.tick() => {}
                _ = cancel.cancelled() => info!("heartbeat canceled"),
            }

            self.ping().await;
        }
    }

    pub async fn ping(&self) {
        let Err(e) = self.client.ping().await else {
            self.set_online(true);
            return;
        };

        if matches!(e, error::Error::NotFoundRingbuf { .. }) {
            info!("not found ringbuf");

            if let Err(e) = self.session_handle.send().await {
                warn!("failed to send session, error: {:?}", e);
                self.set_online(false);
            } else {
                info!("send session success");
                self.set_online(true);
            }

            return;
        }

        warn!("failed to ping, error: {:?}", e);
        self.set_online(false);
    }

    fn set_online(&self, online: bool) {
        self.online.store(online, Ordering::Relaxed);
    }
}

#![allow(dead_code)]
use std::{sync::atomic::AtomicBool, time::Duration};

use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::grpc::client::GrpcClient;

use super::SessionHandleRef;

pub struct Connection {
    client: GrpcClient,
    check_interval: Duration,
    session_handle: SessionHandleRef,
    status: ConnStatus,
}

impl Connection {
    /// Create a [`Connection`].
    pub async fn new(
        client: GrpcClient,
        check_interval: Duration,
        session_handle: SessionHandleRef,
    ) -> Connection {
        Connection {
            client,
            check_interval,
            session_handle,
            status: ConnStatus::default(),
        }
    }

    pub async fn trigger_connect(&self) {}

    async fn run(&self, cancel: CancellationToken) {
        let mut tick = interval(self.check_interval);

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

    async fn ping(&self) {
        match self.client.ping().await {
            Ok(resp) => {
                self.status.set_online(true);
                if resp.missing_memfd {
                    self.on_missing_memfd().await;
                } else {
                    self.status.set_ringbuf_ready(true);
                }
            }
            Err(e) => {
                self.status.set_online(false);
                warn!("failed to ping, error: {:?}", e);
            }
        }
    }

    async fn on_missing_memfd(&self) {
        if let Err(e) = self.session_handle.send().await {
            self.status.set_ringbuf_ready(false);
            // If failed to send memfd, set online to false also.
            self.status.set_online(false);
            warn!("not found ringbuf, failed to send session, error: {:?}", e);
        } else {
            self.status.set_ringbuf_ready(true);
            info!("not found ringbuf, send session success");
        }
    }
}

#[derive(Debug, Default)]
pub struct ConnStatus {
    /// The producer is online.
    pub online: AtomicBool,
    /// The consumer is online.
    pub ringbuf_ready: AtomicBool,
    /// The result fetching is in progress.
    pub result_fetching: AtomicBool,
}

impl ConnStatus {
    pub fn set_online(&self, online: bool) {
        self.online
            .store(online, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn set_ringbuf_ready(&self, ready: bool) {
        self.ringbuf_ready
            .store(ready, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn set_result_fetching(&self, fetching: bool) {
        self.result_fetching
            .store(fetching, std::sync::atomic::Ordering::Relaxed);
    }
}

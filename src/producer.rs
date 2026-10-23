pub mod prealloc;
pub mod settings;

mod fetch;
mod heartbeat;

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use fetch::ResultFetcher;
use settings::ProducerSettings;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use uuid::Uuid;

use self::prealloc::PreAlloc;
use crate::error::Result;
use crate::fd_pass::send_fd;
use crate::grpc::client::GrpcClient;
use crate::memfd::Settings;
use crate::memfd::create_fd;
use crate::ringbuf::Ringbuf;

/// The producer of the ringbuf based on shared memory.
pub struct RingbufProducer {
    ringbuf: RwLock<Ringbuf>,
    grpc_client: GrpcClient,
    online: Arc<AtomicBool>,
    cancel: CancellationToken,
    req_id: AtomicU32,
    result_fetcher: Option<ResultFetcher>,
    enable_checksum: bool,
}

impl RingbufProducer {
    /// Create a [`RingbufProducer`] by the given settings.
    ///
    /// It will initially try to establish the required connection, and if fails,
    /// it will retry in the background.
    pub async fn new(settings: ProducerSettings) -> Result<RingbufProducer> {
        let ProducerSettings {
            grpc_sock_path,
            ringbuf_len,
            fdpass_sock_path,
            heartbeat_interval,
            enable_result_fetch,
            reconnect_interval,
            expired_check_interval,
            subscription_ttl,
            enable_checksum,
            #[cfg(not(any(
                target_os = "linux",
                target_os = "android",
                target_os = "freebsd"
            )))]
            backed_file_path,
        } = settings;

        let client_id = gen_client_id();

        let memfd = create_fd(Settings {
            name: client_id.clone(),
            size: ringbuf_len as u64,
            #[cfg(not(any(
                target_os = "linux",
                target_os = "android",
                target_os = "freebsd"
            )))]
            path: backed_file_path,
        })?;

        let grpc_client = GrpcClient::new(&client_id, grpc_sock_path);

        let ringbuf = Ringbuf::new(&memfd)?;
        ringbuf.set_checksum_flag(enable_checksum);
        let ringbuf = RwLock::new(ringbuf);

        let online = Arc::new(AtomicBool::new(false));
        let req_id = AtomicU32::new(0);
        let cancel = CancellationToken::new();

        let session_handle = SessionHandle {
            client_id,
            fdpass_sock_path,
            memfd,
        };

        let heartbeat = heartbeat::Heartbeat {
            client: grpc_client.clone(),
            online: online.clone(),
            heartbeat_interval,
            session_handle: Arc::new(session_handle),
        };
        heartbeat.ping().await;
        let cancel_c = cancel.child_token();
        tokio::spawn(async move { heartbeat.run(cancel_c).await });

        let cancel_c = cancel.child_token();
        let result_fetcher = if enable_result_fetch {
            Some(
                ResultFetcher::new(
                    grpc_client.clone(),
                    reconnect_interval,
                    expired_check_interval,
                    subscription_ttl,
                    cancel_c,
                )
                .await,
            )
        } else {
            None
        };

        let producer = RingbufProducer {
            ringbuf,
            grpc_client,
            online,
            cancel,
            req_id,
            result_fetcher,
            enable_checksum,
        };

        Ok(producer)
    }

    /// Reserve a [`PreAlloc`] for committing data.
    ///
    /// # Errors
    ///
    /// - If the requested space exceeds the capacity of ringbuf, an
    ///   [`crate::error::Error::ExceedCapacity`] error will be returned.
    ///
    /// - [`crate::error::Error::NotEnoughSpace`] if insufficient space remains.
    /// - [`crate::error::Error::ResultFetchNotReady`] if result fetching is enabled
    ///   but unavailable. The reserved block is discarded automatically.
    pub fn reserve(&self, bytes: usize) -> Result<PreAlloc> {
        let req_id = self.gen_req_id();
        let data_block =
            self.ringbuf.write().unwrap().reserve(bytes, req_id)?;

        let mut pre = PreAlloc {
            data_block,
            rx: None,
            enable_checksum: self.enable_checksum,
            committed: false,
        };

        pre.rx = self
            .result_fetcher
            .as_ref()
            .map(|fetcher| fetcher.subscribe(req_id))
            .transpose()?;

        Ok(pre)
    }

    /// Notify the consumer to process the data.
    ///
    /// If the accumulated data in the ringbuf exceeds the notify_threshold, will
    /// notify the consumer to process the data.
    pub async fn notify_consumer(&self, notify_threshold: Option<u32>) {
        let need_notify = notify_threshold.is_none_or(|threshold| {
            self.ringbuf.read().unwrap().written_bytes() > threshold
        });

        if !need_notify {
            debug!("no need to notify consumer");
            return;
        }

        if let Err(e) = self.grpc_client.notify().await {
            debug!("failed to notify consumer, error: {:?}", e);
            self.online.store(false, Ordering::Relaxed);
        }
    }

    /// Check if the server is online.
    pub fn server_online(&self) -> bool {
        self.online.load(Ordering::Relaxed)
    }

    /// Check if the gRPC stream which fetch execution results is created.
    /// If disabled the result fetch in producer settings, we also consider it
    /// as normal.
    pub fn result_fetch_normal(&self) -> bool {
        self.result_fetcher
            .as_ref()
            .map(|fetcher| fetcher.is_normal())
            .unwrap_or(true)
    }

    /// Generate a request id.
    fn gen_req_id(&self) -> u32 {
        self.req_id.fetch_add(1, Ordering::Relaxed)
    }
}

impl Drop for RingbufProducer {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// The [`SessionHandle`] is used to send the memfd, client id and ringbuf len
/// to the consumer.
pub(crate) struct SessionHandle {
    pub client_id: String,
    pub fdpass_sock_path: PathBuf,
    pub memfd: File,
}

pub(crate) type SessionHandleRef = Arc<SessionHandle>;

impl SessionHandle {
    pub async fn send(&self) -> Result<()> {
        send_fd(&self.fdpass_sock_path, &self.memfd, self.client_id.clone())
            .await
    }
}

fn gen_client_id() -> String {
    Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::error::Error;
    use crate::producer::settings::ProducerSettingsBuilder;
    use crate::ringbuf::data_block::BlockState;

    #[tokio::test]
    async fn test_abandoned_allocations() {
        let dir = tempfile::tempdir().unwrap();
        let settings = ProducerSettingsBuilder::new()
            .grpc_sock_path(dir.path().join("missing.sock"))
            .fdpass_sock_path(dir.path().join("missing-fd.sock"))
            .enable_result_fetch(false)
            .build();
        let producer = RingbufProducer::new(settings).await.unwrap();

        drop(producer.reserve(64).unwrap());
        let mut partial = producer.reserve(64).unwrap();
        partial.write(b"partial").unwrap();
        drop(partial);
        let mut committed = producer.reserve(64).unwrap();
        committed.write(b"complete").unwrap();
        drop(committed.commit());

        let ringbuf = producer.ringbuf.read().unwrap();
        for _ in 0..2 {
            let block = ringbuf.peek().unwrap();
            assert_eq!(block.state(), BlockState::Discarded);
            let len = block.total_len();
            unsafe { ringbuf.advance_consume_offset(len) };
        }
        let block = ringbuf.peek().unwrap();
        assert_eq!(block.state(), BlockState::Committed);
        assert_eq!(block.slice().unwrap(), b"complete");
    }

    #[tokio::test]
    async fn test_subscription_failure_discards_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let settings = ProducerSettingsBuilder::new()
            .grpc_sock_path(dir.path().join("missing.sock"))
            .fdpass_sock_path(dir.path().join("missing-fd.sock"))
            .reconnect_interval(Duration::from_secs(60))
            .build();
        let producer = RingbufProducer::new(settings).await.unwrap();

        assert!(matches!(
            producer.reserve(64),
            Err(Error::ResultFetchNotReady { .. })
        ));
        let ringbuf = producer.ringbuf.read().unwrap();
        let block = ringbuf.peek().unwrap();
        assert_eq!(block.state(), BlockState::Discarded);
    }
}

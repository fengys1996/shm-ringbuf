pub mod prealloc;
pub mod settings;

mod fetch;
mod heartbeat;

use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;

use fetch::ResultFetcher;
use settings::ProducerSettings;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use uuid::Uuid;

use self::prealloc::PreAlloc;
use crate::error::Result;
use crate::fd_pass::send_fd;
use crate::grpc::client::GrpcClient;
use crate::memfd::create_fd;
use crate::memfd::Settings;
use crate::ringbuf::Ringbuf;

/// The producer of the ringbuf based on shared memory.
pub struct RingbufProducer {
    ringbuf: RwLock<Ringbuf>,
    grpc_client: GrpcClient,
    online: Arc<AtomicBool>,
    cancel: CancellationToken,
    req_id: AtomicU32,
    result_fetcher: ResultFetcher,
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
            result_fetch_retry_interval,
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
        let cancel_c = cancel.clone();
        tokio::spawn(async move { heartbeat.run(cancel_c).await });

        let result_fetcher = ResultFetcher::new(
            grpc_client.clone(),
            result_fetch_retry_interval,
        )
        .await;

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
    /// # Errors:
    ///
    /// - If the requested space exceeds the capacity of ringbuf, an
    ///     [`crate::error::Error::ExceedCapacity`] error will be returned.
    ///
    /// - If the requested space exceeds the remaining space of ringbuf, and
    ///     not exceeds the capacity, an [`crate::error::Error::NotEnoughSpace`]
    ///     error will be returned.
    pub fn reserve(&self, bytes: usize) -> Result<PreAlloc> {
        let req_id = self.gen_req_id();
        let data_block =
            self.ringbuf.write().unwrap().reserve(bytes, req_id)?;

        let rx = self.result_fetcher.subscribe(req_id);
        let enable_checksum = self.enable_checksum;

        let pre = PreAlloc {
            data_block,
            rx,
            enable_checksum,
        };

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

    /// Check if the gRPC stream which fetch execution results is created .
    pub fn result_fetch_normal(&self) -> bool {
        self.result_fetcher.is_normal()
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

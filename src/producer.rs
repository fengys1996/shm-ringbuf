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

pub struct RingbufProducer {
    ringbuf: RwLock<Ringbuf>,
    grpc_client: GrpcClient,
    online: Arc<AtomicBool>,
    cancel: CancellationToken,
    business_id: AtomicU32,
    result_fetcher: ResultFetcher,
}

impl RingbufProducer {
    pub async fn connect_lazy(
        settings: ProducerSettings,
    ) -> Result<RingbufProducer> {
        #[cfg(not(any(
            target_os = "linux",
            target_os = "android",
            target_os = "freebsd"
        )))]
        let ProducerSettings {
            grpc_sock_path,
            ringbuf_len,
            fdpass_sock_path,
            heartbeat_interval,
            result_fetch_retry_interval,
            backed_file_path,
        } = settings;

        #[cfg(any(
            target_os = "linux",
            target_os = "android",
            target_os = "freebsd"
        ))]
        let ProducerSettings {
            grpc_sock_path,
            ringbuf_len,
            fdpass_sock_path,
            heartbeat_interval,
            result_fetch_retry_interval,
        } = settings;

        let client_id = gen_client_id();

        let memfd = create_fd(Settings {
            name: client_id.clone(),
            size: ringbuf_len as u64,
            path: backed_file_path,
        })?;
        #[cfg(any(
            target_os = "linux",
            target_os = "android",
            target_os = "freebsd"
        ))]
        let memfd = create_fd(Settings {
            name: client_id.clone(),
            size: ringbuf_len as u64,
        })?;

        let grpc_client = GrpcClient::new(&client_id, grpc_sock_path);
        let ringbuf = RwLock::new(Ringbuf::new(&memfd)?);
        let online = Arc::new(AtomicBool::new(false));
        let business_id = AtomicU32::new(0);
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
            business_id,
            result_fetcher,
        };

        Ok(producer)
    }

    pub fn reserve(&self, size: usize) -> Result<PreAlloc> {
        let business_id = self.gen_business_id();
        let data_block =
            self.ringbuf.write().unwrap().reserve(size, business_id)?;

        let rx = self.result_fetcher.subscribe(business_id);

        let pre = PreAlloc { data_block, rx };

        Ok(pre)
    }

    pub async fn notify_consumer(&self, notify_limit: Option<u32>) {
        let need_notify = notify_limit.map_or(true, |limit| {
            self.ringbuf.read().unwrap().written_bytes() > limit
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

    /// Check if the result fetcher is normal.
    pub fn result_fetch_normal(&self) -> bool {
        self.result_fetcher.is_normal()
    }

    /// Generate a business id.
    fn gen_business_id(&self) -> u32 {
        self.business_id.fetch_add(1, Ordering::Relaxed)
    }
}

impl Drop for RingbufProducer {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// The [`SessionHandle`] is used to send the memfd, client id and ringbuf len
/// to the consumer.
pub struct SessionHandle {
    pub client_id: String,
    pub fdpass_sock_path: PathBuf,
    pub memfd: File,
}

pub type SessionHandleRef = Arc<SessionHandle>;

impl SessionHandle {
    pub async fn send(&self) -> Result<()> {
        send_fd(&self.fdpass_sock_path, &self.memfd, self.client_id.clone())
            .await
    }
}

fn gen_client_id() -> String {
    Uuid::new_v4().to_string()
}

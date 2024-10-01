mod heartbeat;
pub mod prealloc;
pub mod settings;

use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;

use settings::ProducerSettings;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use self::prealloc::PreAlloc;
use crate::error::Result;
use crate::fd_pass::send_fd;
use crate::grpc::client::GrpcClient;
use crate::memfd::memfd_create;
use crate::memfd::MemfdSettings;
use crate::ringbuf::Ringbuf;

pub struct RingbufProducer {
    ringbuf: Arc<RwLock<Ringbuf>>,
    grpc_client: GrpcClient,
    online: Arc<AtomicBool>,
    cancel: CancellationToken,
}

impl RingbufProducer {
    pub async fn connect_lazy(
        settings: ProducerSettings,
    ) -> Result<RingbufProducer> {
        let ProducerSettings {
            grpc_sock_path,
            ringbuf_len,
            fdpass_sock_path,
            heartbeat_interval,
        } = settings;

        let client_id = gen_client_id();

        let memfd = memfd_create(MemfdSettings {
            name: client_id.clone(),
            size: ringbuf_len as u64,
        })?;

        let grpc_client = GrpcClient::new(&client_id, grpc_sock_path);
        let ringbuf = Arc::new(RwLock::new(Ringbuf::new(&memfd, ringbuf_len)?));
        let online = Arc::new(AtomicBool::new(false));
        let cancel = CancellationToken::new();

        let session_handle = SessionHandle {
            client_id,
            ringbuf_len,
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

        let producer = RingbufProducer {
            ringbuf,
            grpc_client,
            online,
            cancel,
        };

        Ok(producer)
    }

    pub fn reserve(&self, size: usize) -> Result<PreAlloc> {
        let mut ringbuf = self.ringbuf.write().unwrap();
        let datablock = ringbuf.reserve(size)?;

        let pre = PreAlloc {
            inner: datablock,
            notify: self.grpc_client.clone(),
            online: self.online.clone(),
            ringbuf: self.ringbuf.clone(),
        };

        Ok(pre)
    }

    /// Check if the server is online.
    pub fn server_online(&self) -> bool {
        self.online.load(Ordering::Relaxed)
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
    pub ringbuf_len: usize,
    pub fdpass_sock_path: PathBuf,
    pub memfd: File,
}

pub type SessionHandleRef = Arc<SessionHandle>;

impl SessionHandle {
    pub async fn send(&self) -> Result<()> {
        send_fd(
            &self.fdpass_sock_path,
            &self.memfd,
            self.client_id.clone(),
            self.ringbuf_len as u32,
        )
        .await
    }
}

fn gen_client_id() -> String {
    Uuid::new_v4().to_string()
}

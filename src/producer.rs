pub mod prealloc;

use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use tracing::error;
use tracing::info;
use tracing::warn;
use uuid::Uuid;

use self::prealloc::PreAlloc;
use crate::error;
use crate::error::Result;
use crate::fdpass::send_fd;
use crate::grpc::GrpcClient;
use crate::memfd::memfd_create;
use crate::memfd::MemfdSettings;
use crate::ringbuf::Ringbuf;

#[derive(Clone)]
pub struct RingbufProducer {
    client_id: String,
    settings: ProducerSettings,
    memfd: Arc<File>,
    ringbuf: Arc<RwLock<Ringbuf>>,
    grpc_client: Arc<GrpcClient>,
    online: Arc<AtomicBool>,
    stop_detect: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct ProducerSettings {
    pub control_sock_path: PathBuf,
    pub sendfd_sock_path: PathBuf,
    pub size_of_ringbuf: usize,
    pub heartbeat_interval_second: u64,
}

impl RingbufProducer {
    pub async fn connect_lazy(
        settings: ProducerSettings,
    ) -> Result<RingbufProducer> {
        let ProducerSettings {
            control_sock_path,
            size_of_ringbuf,
            ..
        } = &settings;

        let client_id = gen_client_id();

        let size_of_ringbuf = *size_of_ringbuf;
        let memfd = memfd_create(MemfdSettings {
            name: client_id.clone(),
            size: size_of_ringbuf as u64 * 2,
        })?;

        let grpc_client =
            Arc::new(GrpcClient::new(client_id.clone(), control_sock_path));

        let ringbuf =
            Arc::new(RwLock::new(Ringbuf::new(&memfd, size_of_ringbuf)?));

        let online = Arc::new(AtomicBool::new(false));

        let producer = RingbufProducer {
            memfd: Arc::new(memfd),
            client_id,
            settings,
            ringbuf: ringbuf.clone(),
            grpc_client,
            online,
            stop_detect: Arc::new(AtomicBool::new(false)),
        };

        let producer_clone = producer.clone();
        tokio::spawn(async move { producer_clone.loop_detect().await });

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

impl RingbufProducer {
    async fn loop_detect(&self) {
        let heartbeat_interval_second = self.settings.heartbeat_interval_second;

        let mut quick_detect = false;
        loop {
            if self.stop_detect.load(Ordering::Relaxed) {
                break;
            }

            if !quick_detect {
                let sleep_time = Duration::from_secs(heartbeat_interval_second);
                tokio::time::sleep(sleep_time).await;
            }

            quick_detect = self.detect().await;
        }
    }

    /// Detect if the server is online.
    ///
    /// If return true, the next detect will be quick, else the next detect will be normal.
    async fn detect(&self) -> bool {
        let prev_online = self.server_online();

        if let Err(e) = self.grpc_client.ping().await {
            warn!("failed to ping shm consumer, error: {:?}", e);

            if matches!(e, error::Error::NotFoundRingbuf { .. })
                && self.send_fd().await.is_ok()
            {
                info!("not found ringbuf in the consumer, and send fd success");
                return true;
            }

            self.set_online(false);

            return false;
        }

        if !prev_online {
            match self.re_connect().await {
                Ok(_) => {
                    info!("re_connect success, set shm consumer online");
                    self.set_online(true);
                }
                Err(e) => {
                    error!("failed to re_connect, error: {:?}", e);
                }
            }
        }

        false
    }

    async fn re_connect(&self) -> Result<()> {
        self.handshake().await?;
        self.send_fd().await
    }

    async fn handshake(&self) -> Result<()> {
        self.grpc_client.handshake().await
    }

    async fn send_fd(&self) -> Result<()> {
        send_fd(
            &self.settings.sendfd_sock_path,
            &self.memfd,
            self.client_id.clone(),
            self.settings.size_of_ringbuf as u32,
        )
        .await
    }

    fn set_online(&self, online: bool) {
        self.online.store(online, Ordering::Relaxed);
    }
}

impl Drop for RingbufProducer {
    fn drop(&mut self) {
        self.stop_detect.store(true, Ordering::Relaxed);
    }
}

fn gen_client_id() -> String {
    Uuid::new_v4().to_string()
}

pub mod prealloc;

use std::fs::File;
use std::path::PathBuf;
use std::sync::RwLock;

use uuid::Uuid;

use self::prealloc::PreAlloc;
use crate::error::Result;
use crate::fd_pass::send_fd;
use crate::memfd::memfd_create;
use crate::memfd::MemfdSettings;
use crate::ringbuf::Ringbuf;

pub struct RingbufProducer {
    client_id: String,
    settings: ProducerSettings,
    memfd: File,
    ringbuf: RwLock<Ringbuf>,
}

#[derive(Clone, Debug)]
pub struct ProducerSettings {
    /// The path of the unix socket for sending fd.
    pub fdpass_sock_path: PathBuf,

    /// The len of the ringbuf, unit is byte.
    pub ringbuf_len: usize,

    /// Whether to enable notification after commtting data.
    ///
    /// TODO: Detailed analysis of this option.
    pub enable_notify: bool,
}

impl RingbufProducer {
    pub async fn connect(
        settings: ProducerSettings,
    ) -> Result<RingbufProducer> {
        let ProducerSettings { ringbuf_len, .. } = &settings;
        let ringbuf_len = *ringbuf_len;

        // 1. generate a unique client id.
        let client_id = gen_client_id();

        // 2. create a memfd with the given settings.
        let memfd = memfd_create(MemfdSettings {
            name: &client_id,
            size: ringbuf_len as u64,
        })?;

        // 3. create a ringbuf with the memfd.
        let ringbuf = RwLock::new(Ringbuf::new(&memfd, ringbuf_len)?);

        // 4. construct the producer.
        let producer = RingbufProducer {
            memfd,
            client_id,
            settings,
            ringbuf,
        };

        // 5. send the fd to the consumer.
        producer.send_fd().await?;

        Ok(producer)
    }

    pub async fn send_fd(&self) -> Result<()> {
        send_fd(
            &self.settings.fdpass_sock_path,
            &self.memfd,
            &self.client_id,
            self.settings.ringbuf_len as u32,
        )
        .await
    }

    pub fn reserve(&self, size: usize) -> Result<PreAlloc> {
        let mut ringbuf = self.ringbuf.write().unwrap();

        let datablock = ringbuf.reserve(size)?;

        let pre = PreAlloc { inner: datablock };

        Ok(pre)
    }
}

fn gen_client_id() -> String {
    Uuid::new_v4().to_string()
}

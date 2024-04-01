use std::usize;

use uuid::Uuid;

use crate::error::Result;
use crate::fdpass::send_fd;
use crate::memfd::memfd_create;
use crate::memfd::MmapSettings;
use crate::ringbuf::PreAlloc;
use crate::ringbuf::Ringbuf;

pub struct RingbufProducer {
    inner: Ringbuf,
}

pub struct ProducerSettings {
    pub control_file_path: String,
    pub sendfd_file_path: String,
    pub size_of_ringbuf: usize,
}

impl RingbufProducer {
    pub async fn new(settings: ProducerSettings) -> Result<RingbufProducer> {
        let ProducerSettings {
            control_file_path: _,
            sendfd_file_path,
            size_of_ringbuf,
        } = settings;

        let client_id = gen_client_id();

        let memfd = memfd_create(MmapSettings {
            name: client_id.clone(),
            size: size_of_ringbuf as u64,
        })?;

        send_fd(sendfd_file_path, &memfd, client_id, size_of_ringbuf as u32)
            .await?;

        let ringbuf = Ringbuf::new(memfd, size_of_ringbuf)?;

        let producer = RingbufProducer { inner: ringbuf };

        Ok(producer)
    }

    pub fn reserve(&mut self, size: usize) -> Result<PreAlloc> {
        self.inner.reserve(size)
    }
}

fn gen_client_id() -> String {
    Uuid::new_v4().to_string()
}

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;

use tracing::warn;

use crate::data_block::DataBlock;
use crate::error::Result;
use crate::grpc::GrpcClient;
use crate::ringbuf::DropGuard;
use crate::ringbuf::Ringbuf;

pub struct PreAlloc {
    pub(super) inner: DataBlock<DropGuard>,
    pub(super) notify: Arc<GrpcClient>,
    pub(super) online: Arc<AtomicBool>,
    pub(super) ringbuf: Arc<RwLock<Ringbuf>>,
}

impl PreAlloc {
    /// Get the slice of the pre-allocated.
    pub fn slice(&self) -> &[u8] {
        self.inner.slice()
    }

    /// Write data to the pre-allocated.
    pub fn write(&mut self, data: &[u8]) -> Result<()> {
        self.inner.write(data)
    }

    /// Commit the written data.
    ///
    /// After commit, the consumer can see the written data.
    pub fn commit(&self) {
        self.inner.commit();
    }

    /// Commit the written data and notify the consumer.
    ///
    /// After commit, the consumer can see the written data.
    pub async fn commit_and_notify(self, notify_limit: u32) {
        self.inner.commit();

        let need_notify =
            self.ringbuf.read().unwrap().written_bytes() > notify_limit;

        if !need_notify {
            return;
        }

        if let Err(e) = self.notify.notify().await {
            warn!("failed to notify consumer, error: {:?}", e);
            // TODO: elegant settings online.
            self.online.store(false, Ordering::Relaxed);
        }
    }

    /// Check if the server is online.
    pub fn online(&self) -> bool {
        self.online.load(Ordering::Relaxed)
    }
}

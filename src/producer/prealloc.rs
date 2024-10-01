use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use tracing::warn;

use crate::error::Result;
use crate::grpc::client::GrpcClient;
use crate::ringbuf::data_block::DataBlock;
use crate::ringbuf::DropGuard;

pub struct PreAlloc<'a> {
    pub(super) inner: DataBlock<'a, DropGuard>,
    pub(super) notify: &'a GrpcClient,
    pub(super) online: &'a AtomicBool,
}

impl<'a> PreAlloc<'a> {
    /// Get the slice of the [`PreAlloc`].
    pub fn slice(&self) -> &[u8] {
        self.inner.slice().unwrap()
    }

    /// Write data to the [`PreAlloc`].
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
    pub async fn commit_and_notify(self) {
        self.inner.commit();

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

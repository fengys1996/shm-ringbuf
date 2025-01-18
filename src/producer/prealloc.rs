use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::FutureExt;
use tokio::sync::oneshot::Receiver;

use crate::error;
use crate::error::DataProcessResult;
use crate::error::Result;
use crate::ringbuf::data_block::DataBlock;
use crate::ringbuf::DropGuard;

/// The pre-allocated data block.
pub struct PreAlloc {
    pub(super) data_block: DataBlock<DropGuard>,
    pub(super) rx: Option<Receiver<DataProcessResult>>,
    pub(super) enable_checksum: bool,
}

impl PreAlloc {
    /// Get the slice of the [`PreAlloc`].
    pub fn slice(&self) -> &[u8] {
        self.data_block.slice().unwrap()
    }

    /// Get the capacity of the [`PreAlloc`].
    pub fn capacity(&self) -> u32 {
        self.data_block.capacity()
    }

    /// Write data to the [`PreAlloc`].
    pub fn write(&mut self, data: &[u8]) -> Result<()> {
        self.data_block.write(data)
    }

    /// Commit the written data.
    ///
    /// After commit, the consumer can see the written data.
    pub fn commit(&self) {
        if self.enable_checksum {
            let checksum = crc32fast::hash(self.slice());
            self.data_block.set_checksum(checksum);
        }

        self.data_block.commit();
    }

    /// Return a [`Handle`] to wait for the result of data processing.
    pub fn wait_result(self) -> Option<Handle> {
        self.rx.map(|rx| Handle { rx })
    }
}

pub struct Handle {
    rx: Receiver<DataProcessResult>,
}

impl Future for Handle {
    type Output = Result<DataProcessResult>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        self.rx.poll_unpin(cx).map_err(|e| error::Error::Recv {
            source: e,
            location: snafu::location!(),
        })
    }
}

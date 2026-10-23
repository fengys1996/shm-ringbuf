use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::FutureExt;
use tokio::sync::oneshot::Receiver;

use crate::error;
use crate::error::DataProcessResult;
use crate::error::Result;
use crate::ringbuf::DropGuard;
use crate::ringbuf::data_block::DataBlock;

/// A reserved block for writing data.
///
/// [`commit`](Self::commit) makes the data available to the consumer and consumes
/// this allocation. Dropping it without committing lets the consumer skip and
/// reclaim the block.
pub struct PreAlloc {
    pub(super) data_block: DataBlock<DropGuard>,
    pub(super) rx: Option<Receiver<DataProcessResult>>,
    pub(super) enable_checksum: bool,
    /// Prevents `Drop` from discarding a committed block.
    pub(super) committed: bool,
}

impl PreAlloc {
    /// Return the bytes written so far.
    pub fn slice(&self) -> &[u8] {
        self.data_block.slice().unwrap()
    }

    /// Return the payload capacity in bytes, excluding the header.
    pub fn capacity(&self) -> u32 {
        self.data_block.capacity()
    }

    /// Append data to the block, failing if insufficient capacity remains.
    pub fn write(&mut self, data: &[u8]) -> Result<()> {
        self.data_block.write(data)
    }

    /// Commit the written data, consuming the allocation.
    ///
    /// Returns a [`Handle`] to await the processing result; drop it if not needed.
    pub fn commit(mut self) -> Handle {
        if self.enable_checksum {
            let checksum = crc32fast::hash(self.slice());
            self.data_block.set_checksum(checksum);
        }

        let rx = self.rx.take();
        self.committed = true;
        self.data_block.commit();

        Handle { rx }
    }
}

impl Drop for PreAlloc {
    fn drop(&mut self) {
        if !self.committed {
            self.data_block.discard();
        }
    }
}

/// A future yielding the consumer's processing result.
///
/// Dropping it does not cancel processing. Awaiting it when result fetching is
/// disabled returns [`error::Error::ResultFetchDisabled`].
pub struct Handle {
    rx: Option<Receiver<DataProcessResult>>,
}

impl Future for Handle {
    type Output = Result<DataProcessResult>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        if let Some(rx) = self.rx.as_mut() {
            rx.poll_unpin(cx).map_err(|e| error::Error::Recv {
                source: e,
                location: snafu::location!(),
            })
        } else {
            Poll::Ready(error::ResultFetchDisabledSnafu.fail())
        }
    }
}

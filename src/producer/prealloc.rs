use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::FutureExt;
use tokio::sync::oneshot::Receiver;

use crate::error::DataProcessResult;
use crate::error::Result;
use crate::ringbuf::data_block::DataBlock;
use crate::ringbuf::DropGuard;

pub struct PreAlloc {
    pub(super) data_block: DataBlock<DropGuard>,
    pub(super) rx: Receiver<DataProcessResult>,
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
        self.data_block.commit();
    }

    pub fn wait_result(self) -> Handle {
        Handle { rx: self.rx }
    }
}

pub struct Handle {
    rx: Receiver<DataProcessResult>,
}

impl Future for Handle {
    type Output = std::result::Result<DataProcessResult, ()>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        self.rx.poll_unpin(cx).map(|res| res.map_err(|_| ()))
    }
}

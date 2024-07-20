use crate::data_block::DataBlock;
use crate::error::Result;
use crate::ringbuf::DropGuard;

pub struct PreAlloc {
    pub(super) inner: DataBlock<DropGuard>,
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
    pub async fn commit_and_notify(self) {
        self.inner.commit();
        // TODO: notify the consumer.
    }
}

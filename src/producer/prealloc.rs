use crate::error::Result;
use crate::ringbuf::data_block::DataBlock;
use crate::ringbuf::DropGuard;

pub struct PreAlloc {
    pub(super) data_block: DataBlock<DropGuard>,
}

impl PreAlloc {
    /// Get the slice of the pre-allocated.
    pub fn slice(&self) -> &[u8] {
        self.data_block.slice().unwrap()
    }

    /// Write data to the pre-allocated.
    pub fn write(&mut self, data: &[u8]) -> Result<()> {
        self.data_block.write(data)
    }

    /// Commit the written data.
    ///
    /// After commit, the consumer can see the written data.
    pub fn commit(&self) {
        self.data_block.commit();
    }
}

// TODO: remove the dead_code attribute after the implementation is completed.
#![allow(dead_code)]
use std::fs;
use std::sync::Arc;

/// The minimum allocation unit of ringbuf is 4 bytes.
pub(crate) const PAGE_SIZE: usize = 4;

/// The size of the metadata area in bytes.
pub(crate) const METADATA_LEN: usize = 4 * PAGE_SIZE;

/// The minimum size of the backed buffer in bytes.
pub(crate) const MIN_BACKED_LEN: usize = PAGE_SIZE * 10;

/// The version of the ring buffer.
pub(crate) const VERSION: u32 = 1;

/// The ringbuf data structure, which mapped to the underlying buffer, ex: share memory.
///
/// The underlying structure is as follows:
///
/// ```text
///                data_part_ptr
///                     |    
///                     v
/// +-------------------+---------------------------------------+----------------------------------------+
/// | metadata          | data part 0                           | data part 1                            |
/// +-------------------+---------------------------------------+----------------------------------------+
/// | 32 bytes          | data_part_len bytes                   | data_part_len bytes                    |
/// +-------------------+---------------------------------------+----------------------------------------+
/// ```
/// Note: data part 0 and data part 1 are mapped to the same physical memory.
pub struct Ringbuf<T> {
    /// The raw pointer to the data part.
    data_part_ptr: *mut u8,

    /// The length of the data part in ringbuf.
    data_part_len: usize,

    /// The metadata of the ring buffer.
    metadata: RingbufMetadata,

    /// The underlying buffer that the ring buffer holds. We use arc to manage the lifecycle of the buffer.
    object: Arc<T>,
}

unsafe impl<T> Send for Ringbuf<T> {}
unsafe impl<T> Sync for Ringbuf<T> {}

#[derive(Debug)]
/// The metadata of ring buffer.
///
/// The underlying structure is as follows:
/// ```text
///      metadata.produce_offset     metadata.consume_offset
///                     |                   |
///                     v                   v
/// +-------------------+-------------------+-------------------+-------------------+
/// | version           | produce_offset    | consume_offset    | reserved          |
/// +-------------------+-------------------+-------------------+-------------------+
/// | 4 bytes           | 4 bytes           | 4 bytes           | 4 bytes           |
/// +-------------------+-------------------+-------------------+-------------------+
/// ```
struct RingbufMetadata {
    /// The version of the ring buffer.
    ///
    /// Why use u32 instead of *mut u32? Because the version is a constant value, it will not be changed.
    version: u32,

    /// The raw pointer to produce_offset which is the next write position in ringbuf.
    produce_offset: *mut u32,

    /// The raw pointer to consume_offset which is the next read position in ringbuf.
    consume_offset: *mut u32,
}

impl<T> Ringbuf<T> {
    pub fn new(&self, _fd: fs::File) -> Self {
        todo!()
    }

    pub fn recover(&self, _fd: fs::File, _size: usize) -> Self {
        todo!()
    }
}

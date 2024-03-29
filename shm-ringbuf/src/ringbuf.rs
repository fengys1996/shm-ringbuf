// TODO: remove the dead_code attribute after the implementation is completed.
#![allow(dead_code)]
use std::sync::Arc;
use std::usize;

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
///        metadata.write_offset    metadata.read_offset                        data_part_ptr
///                     |                   |                                       |
///                     v                   V                                       v
/// +-------------------+-------------------+-------------------+-------------------+--------------------+
/// | metadata(version) | metadata(write)   | metadata(read)    | metadata(reserved)| data               |
/// +-------------------+-------------------+-------------------+-------------------+--------------------+
/// | 4 bytes           | 4 bytes           | 4 bytes           | 4 bytes           | data_part_len bytes|
/// +-------------------+-------------------+-------------------+-------------------+--------------------+
/// ```
pub struct Ringbuf<T> {
    /// The raw pointer to the data part.
    data_part_ptr: *mut u8,
    /// The length of the data part in ringbuf.
    data_part_len: usize,
    /// The offset of the next write, which will not be synchronized to the cunsumer side before committing data.
    current_write_offset: u32,
    /// The metadata of the ring buffer.
    metadata: RingbufMetadata,
    /// The underlying buffer that the ring buffer holds. We use arc to manage the lifecycle of the buffer.
    object: Arc<T>,
}

unsafe impl<T> Send for Ringbuf<T> {}
unsafe impl<T> Sync for Ringbuf<T> {}

#[derive(Debug)]
/// The metadata of ring buffer.
struct RingbufMetadata {
    /// The version of the ring buffer.
    ///
    /// Why use u32 instead of *mut u32? Because the version is a constant value, it will not be changed.
    version: u32,

    /// The raw pointer to write_offset in ringbuf.
    write_offset: *mut u32,

    /// The raw pointer to read_offset in ringbuf.
    read_offset: *mut u32,
}

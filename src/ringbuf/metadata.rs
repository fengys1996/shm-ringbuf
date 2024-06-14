/// The metadata of ring buffer.
///
/// ## The underlying structure
///
/// ```text
/// metadata.produce_offset metadata.consume_offset
///     |                   |
///     v                   v
///     +-------------------+-------------------+-------------------+
///     | produce_offset    | consume_offset    | reserved          |
///     +-------------------+-------------------+-------------------+
///     | 4 bytes           | 4 bytes           | n bytes           |
///     +-------------------+-------------------+-------------------+
/// ```
#[derive(Copy, Clone, Debug)]
pub struct RingbufMetadata {
    /// The raw pointer to produce_offset which is the next write position in ringbuf.
    pub(super) produce_offset_ptr: *mut u32,

    /// The raw pointer to consume_offset which is the next read position in ringbuf.
    pub(super) consume_offset_ptr: *mut u32,
}

impl RingbufMetadata {
    /// Create a new instance of `RingbufMetadata`.
    ///
    /// # Safety
    /// The `metadata_ptr` must be a valid pointer to the metadata of ring buffer.
    pub unsafe fn new(metadata_ptr: *mut u8) -> Self {
        let produce_offset_ptr = metadata_ptr as *mut u32;
        let consume_offset_ptr = unsafe { produce_offset_ptr.add(1) };

        Self {
            produce_offset_ptr,
            consume_offset_ptr,
        }
    }
}

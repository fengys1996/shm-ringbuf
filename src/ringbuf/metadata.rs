use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

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
    produce_offset_ptr: *mut u32,

    /// The raw pointer to consume_offset which is the next read position in ringbuf.
    consume_offset_ptr: *mut u32,
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

    pub fn produce_offset(&self) -> u32 {
        let ptr = self.produce_offset_ptr;

        unsafe { *ptr }
    }

    pub fn atomic_produce_offset(&self) -> u32 {
        let ptr = self.produce_offset_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.load(Ordering::Relaxed)
    }

    pub fn atomic_set_produce_offset(&self, offset: u32) {
        let ptr = self.produce_offset_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.store(offset, Ordering::Relaxed);
    }

    pub fn consume_offset(&self) -> u32 {
        let ptr = self.consume_offset_ptr;

        unsafe { *ptr }
    }

    pub fn atomic_consume_offset(&self) -> u32 {
        let ptr = self.consume_offset_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.load(Ordering::Relaxed)
    }

    pub fn atomic_set_consume_offset(&self, offset: u32) {
        let ptr = self.consume_offset_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.store(offset, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ringbuf_metadata() {
        let mut metadata = [0u8; 8];
        let metadata_ptr = metadata.as_mut_ptr();

        let ringbuf_metadata = unsafe { RingbufMetadata::new(metadata_ptr) };

        assert_eq!(ringbuf_metadata.produce_offset(), 0);
        assert_eq!(ringbuf_metadata.consume_offset(), 0);

        ringbuf_metadata.atomic_set_produce_offset(11111);
        ringbuf_metadata.atomic_set_consume_offset(22222);

        assert_eq!(ringbuf_metadata.atomic_produce_offset(), 11111);
        assert_eq!(ringbuf_metadata.atomic_consume_offset(), 22222);
        assert_eq!(ringbuf_metadata.produce_offset(), 11111);
        assert_eq!(ringbuf_metadata.consume_offset(), 22222);
    }
}

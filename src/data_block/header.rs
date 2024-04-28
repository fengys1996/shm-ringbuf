use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

/// The header of the DataBlock.
///
/// ## The underlying structure
///
/// ```text
/// header.capacity_ptr header.len_ptr      header.busy_ptr
/// |                   |                   |
/// v                   v                   v
/// +-------------------+-------------------+-------------------+-------------------+
/// | capacity          | len               | busy              | padding           |
/// +-------------------+-------------------+-------------------+-------------------+
/// | 4 bytes           | 4 bytes           | 4 bytes           | 4 bytes           |
/// +-------------------+-------------------+-------------------+-------------------+
pub struct Header {
    /// The pointer to the capacity of the DataBlock.
    capacity_ptr: *mut u32,

    /// The pointer to the length of the DataBlock.
    len_ptr: *mut u32,

    /// The pointer to the busy flag of the DataBlock.
    /// If busy is 1, it means that the producer may be writing and the consumer cannot consume the Datablock.
    /// Else it means that the consumer can reading the DataBlock.
    busy_ptr: *mut u32,
}

impl Header {
    /// Create a new instance of `Header`.
    ///
    /// # Safety
    /// The `header_ptr` must be a valid pointer to the header of DataBlock.
    pub unsafe fn new(header_ptr: *mut u8) -> Self {
        let capacity_ptr = header_ptr as *mut u32;
        let len_ptr = capacity_ptr.add(1);
        let busy_ptr = len_ptr.add(2);

        Self {
            capacity_ptr,
            len_ptr,
            busy_ptr,
        }
    }

    pub fn capacity(&self) -> u32 {
        unsafe { *self.capacity_ptr }
    }

    pub fn atomic_set_capacity(&self, capacity: u32) {
        let ptr = self.capacity_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.store(capacity, Ordering::Relaxed);
    }

    pub fn len(&self) -> u32 {
        unsafe { *self.len_ptr }
    }

    pub fn atomic_len(&self) -> u32 {
        let ptr = self.len_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.load(Ordering::Relaxed)
    }

    pub fn atomic_set_len(&self, len: u32) {
        let ptr = self.len_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.store(len, Ordering::Relaxed);
    }

    pub fn atomic_busy(&self) -> u32 {
        let ptr = self.busy_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.load(Ordering::Relaxed)
    }

    pub fn atomic_set_busy(&self, busy: u32) {
        let ptr = self.busy_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.store(busy, Ordering::Relaxed);
    }

    pub fn advance_len(&self, len: u32) {
        let ptr = self.len_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.fetch_add(len, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header() {
        let vec = [0u8; 16];
        let header = unsafe { Header::new(vec.as_ptr() as *mut u8) };

        header.atomic_set_capacity(1024);
        header.atomic_set_len(512);
        header.atomic_set_busy(1);

        assert_eq!(header.capacity(), 1024);
        assert_eq!(header.len(), 512);
        assert_eq!(header.atomic_busy(), 1);

        header.advance_len(125);
        assert_eq!(header.capacity(), 1024);
        assert_eq!(header.len(), 512 + 125);
        assert_eq!(header.atomic_busy(), 1);
    }
}

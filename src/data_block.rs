use std::mem::align_of;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use snafu::ensure;

use crate::error;
use crate::error::Result;

/// The data block structure, which is the minimum unit of data transmission between the producer
/// and the consumer.
///
/// The underlying structure is as follows:
/// ```text
///                 data_ptr
///                     |
///                     v
/// +-------------------+-----------------------------------------------+
/// | Header            | Data                                          |
/// +-------------------+-----------------------------------------------+
/// | 16 bytes          | *header.capacity_ptr bytes                    |
/// +-------------------+-----------------------------------------------+
/// ```
pub struct DataBlock<T> {
    data_ptr: *mut u8,
    header: Header,
    _object: Arc<T>,
}

pub const HEADER_LEN: usize = 4 * 4;

unsafe impl<T> Send for DataBlock<T> {}
unsafe impl<T> Sync for DataBlock<T> {}

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
pub(crate) struct Header {
    /// The pointer to the capacity of the DataBlock.
    pub(crate) capacity_ptr: *mut u32,

    /// The pointer to the length of the DataBlock.
    pub(crate) len_ptr: *mut u32,

    /// The pointer to the busy flag of the DataBlock.
    /// If busy is 1, it means that the producer may be writing and the consumer cannot consume the Datablock.
    /// Else it means that the consumer can reading the DataBlock.
    pub(crate) busy_ptr: *mut u32,
}

impl<T> DataBlock<T> {
    /// Get the slice of the DataBlock.
    pub fn slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.data_ptr,
                self.atomic_len() as usize,
            )
        }
    }

    /// Write the data to the DataBlock.
    pub fn write(&mut self, data: &[u8]) -> Result<()> {
        let data_len = data.len() as u32;
        let remain = self.capacity() - self.len();

        ensure!(
            data_len <= remain,
            error::NotEnoughSpaceSnafu {
                expected: data.len() as u32,
                remaining: remain,
            }
        );

        let write_position = unsafe { self.data_ptr.add(self.len() as usize) };

        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                write_position,
                data.len(),
            );
        }

        self.advance_len(data_len);

        Ok(())
    }

    pub async fn commit(self) {
        self.set_busy(false);
    }
}

impl<T> DataBlock<T> {
    /// Create a new DataBlock by the given `start_ptr` and `total_length`, and reset the DataBlock.
    ///
    /// # Safety
    ///
    /// The caller must ensure:
    /// 1. The `start_ptr` is a valid pointer.
    /// 2. The `total_length` is greater than `HEADER_LEN` and must be a correct value.
    pub(crate) unsafe fn new(
        start_ptr: *mut u8,
        total_length: u32,
        object: Arc<T>,
    ) -> Result<Self> {
        let data_len = total_length - HEADER_LEN as u32;
        ensure!(
            data_len > 0,
            error::InvalidParameterSnafu {
                detail: "Total_length must be greater than HEADER_LEN.",
            }
        );

        let data_block = DataBlock::from_raw(start_ptr, object);

        data_block.set_capacity(total_length - HEADER_LEN as u32);
        data_block.set_len(0);
        data_block.set_busy(true);

        Ok(data_block)
    }

    /// Create a new DataBlock from a raw pointer, and not reset the DataBlock.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the `start_ptr` is a valid pointer. And the data pointed to by the pointer
    /// is correct.
    pub(crate) unsafe fn from_raw(start_ptr: *mut u8, object: Arc<T>) -> Self {
        let header = Header {
            capacity_ptr: start_ptr as *mut u32,
            len_ptr: unsafe { start_ptr.add(4) as *mut u32 },
            busy_ptr: unsafe { start_ptr.add(8) as *mut u32 },
        };

        let data_ptr = unsafe { start_ptr.add(HEADER_LEN) };

        DataBlock {
            data_ptr,
            header,
            _object: object,
        }
    }

    pub(crate) fn total_len(&self) -> u32 {
        self.capacity() + HEADER_LEN as u32
    }

    pub(crate) fn capacity(&self) -> u32 {
        unsafe { *self.header.capacity_ptr }
    }

    pub(crate) fn len(&self) -> u32 {
        unsafe { *self.header.len_ptr }
    }

    #[allow(dead_code)]
    pub(crate) fn is_busy(&self) -> bool {
        unsafe { *self.header.busy_ptr == 1 }
    }

    #[allow(dead_code)]
    pub(crate) fn atomic_capacity(&self) -> u32 {
        let ptr = self.header.capacity_ptr;
        debug_assert!(ptr.is_aligned_to(align_of::<u32>()));

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.load(Ordering::Relaxed)
    }

    pub(crate) fn atomic_len(&self) -> u32 {
        let ptr = self.header.len_ptr;
        debug_assert!(ptr.is_aligned_to(align_of::<u32>()));

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.load(Ordering::Relaxed)
    }

    pub(crate) fn atomic_is_busy(&self) -> bool {
        let ptr = self.header.busy_ptr;
        debug_assert!(ptr.is_aligned_to(align_of::<u32>()));

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.load(Ordering::Relaxed) == 1
    }

    pub(crate) fn set_busy(&self, busy: bool) {
        let ptr = self.header.busy_ptr;
        debug_assert!(ptr.is_aligned_to(align_of::<u32>()));

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.store(busy as u32, Ordering::Relaxed);
    }

    /// Set the capacity of the DataBlock.
    ///
    /// ## Note
    /// Capacity can only be set when creating datablock.
    fn set_capacity(&self, capacity: u32) {
        let ptr = self.header.capacity_ptr;
        debug_assert!(ptr.is_aligned_to(align_of::<u32>()));

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.store(capacity, Ordering::Relaxed);
    }

    pub(crate) fn set_len(&self, len: u32) {
        let ptr = self.header.len_ptr;
        debug_assert!(ptr.is_aligned_to(align_of::<u32>()));

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.store(len, Ordering::Relaxed);
    }

    pub(crate) fn advance_len(&self, len: u32) {
        let ptr = self.header.len_ptr;
        debug_assert!(ptr.is_aligned_to(align_of::<u32>()));

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.fetch_add(len, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::DataBlock;
    use super::HEADER_LEN;
    use crate::error;
    use crate::ringbuf::METADATA_LEN;

    #[test]
    fn test_new_data_block_error() {
        let data = vec![0u8; 1024];

        let data_ptr = data.as_ptr() as *mut u8;

        let result = unsafe {
            DataBlock::new(data_ptr, HEADER_LEN as u32, Arc::new(()))
        };

        assert!(matches!(result, Err(error::Error::InvalidParameter { .. })));

        let result = unsafe {
            DataBlock::new(data_ptr, HEADER_LEN as u32 + 1, Arc::new(()))
        };
        assert!(result.is_ok());
    }

    #[test]
    fn test_data_block() {
        let data = vec![0u8; 1024];

        let data_ptr = data.as_ptr() as *mut u8;

        let data_block =
            unsafe { DataBlock::new(data_ptr, 1024, Arc::new(())) }.unwrap();

        assert_eq!(data_block.capacity(), 1024 - HEADER_LEN as u32);
        assert_eq!(data_block.len(), 0);
        assert!(data_block.is_busy());

        assert_eq!(data_block.atomic_capacity(), 1024 - HEADER_LEN as u32);
        assert_eq!(data_block.atomic_len(), 0);
        assert!(data_block.atomic_is_busy());

        let data_block = unsafe { DataBlock::from_raw(data_ptr, Arc::new(())) };

        assert_eq!(data_block.capacity(), 1024 - HEADER_LEN as u32);
        assert_eq!(data_block.len(), 0);
        assert!(data_block.is_busy());

        assert_eq!(data_block.atomic_capacity(), 1024 - HEADER_LEN as u32);
        assert_eq!(data_block.atomic_len(), 0);
        assert!(data_block.atomic_is_busy());

        data_block.set_len(10);
        assert_eq!(data_block.len(), 10);

        data_block.set_busy(false);
        assert!(!data_block.is_busy());
    }

    #[test]
    fn test_data_block_write() {
        let data = vec![0u8; 1024];

        let data_ptr = data.as_ptr() as *mut u8;

        let mut data_block =
            unsafe { DataBlock::new(data_ptr, 1024, Arc::new(())) }.unwrap();

        let data1 = vec![1u8; 10];

        data_block.write(&data1).unwrap();

        assert_eq!(data_block.len(), 10);

        let data2 = vec![2u8; 1024 - METADATA_LEN - 10];

        data_block.write(&data2).unwrap();

        assert_eq!(&data_block.slice()[..10], data1);
        assert_eq!(&data_block.slice()[10..], data2);

        assert_eq!(data_block.len(), 1024 - METADATA_LEN as u32);

        let result = data_block.write(&[1u8; 1]);
        assert!(matches!(result, Err(error::Error::NotEnoughSpace { .. })));
    }
}

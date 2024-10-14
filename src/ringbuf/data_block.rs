use std::ptr;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use snafu::ensure;
use snafu::ResultExt;

use crate::convert_num;
use crate::error;
use crate::error::Result;

/// The [`DataBlock`] is the minimum unit of data transmission.
///
/// The underlying structure is as follows:
/// ```text
///                 data_ptr
///                     |
///                     v
/// +-------------------+-----------------------------------------------+
/// | Header            | Data                                          |
/// +-------------------+-----------------------------------------------+
/// | 16 bytes          | *(header.capacity_ptr) bytes                  |
/// +-------------------+-----------------------------------------------+
/// ```
pub struct DataBlock<T> {
    header: Header,
    data_ptr: *mut u8,
    _object: Arc<T>,
}

// Unit is byte.
pub const HEADER_LEN: usize = 4 * 4;

unsafe impl<T> Send for DataBlock<T> {}
unsafe impl<T> Sync for DataBlock<T> {}

impl<T> DataBlock<T> {
    /// Get the slice of the written data.
    pub fn slice(&self) -> Result<&[u8]> {
        unsafe {
            let written_len = convert_num!(self.written_len(), usize)?;

            Ok(std::slice::from_raw_parts(self.data_ptr, written_len))
        }
    }

    /// Write the data to the DataBlock.
    pub fn write(&mut self, data: &[u8]) -> Result<()> {
        let data_len = convert_num!(data.len(), u32)?;

        let remain = self.capacity() - self.written_len();

        ensure!(
            data_len <= remain,
            error::NotEnoughSpaceSnafu {
                expected: data_len,
                remaining: remain,
            }
        );

        let written_len = convert_num!(self.written_len(), usize)?;

        let write_position = unsafe { self.data_ptr.add(written_len) };

        unsafe {
            ptr::copy_nonoverlapping(data.as_ptr(), write_position, data.len());
        }

        self.header.advance_len(data_len);
        Ok(())
    }

    /// Commit the DataBlock. The consumer can read data after the [`DataBlock`]
    /// is committed.
    pub fn commit(&self) {
        self.header.set_busy(false);
    }

    pub fn is_busy(&self) -> bool {
        self.header.busy()
    }

    pub fn id(&self) -> u32 {
        self.header.id()
    }
}

impl<T> DataBlock<T> {
    /// Create a new [`DataBlock`] by the given `start_ptr` and `total_length`.
    ///
    /// # Safety
    ///
    /// The caller must ensurea that `start_ptr` and `len` identify a valid
    /// [`DataBlock`].
    pub(crate) unsafe fn new(
        start_ptr: *mut u8,
        len: u32,
        object: Arc<T>,
    ) -> Result<Self> {
        let header_len_u32 = convert_num!(HEADER_LEN, u32)?;
        let data_len = len - header_len_u32;

        ensure!(
            data_len > 0,
            error::InvalidParameterSnafu {
                detail: "Total_length must be greater than HEADER_LEN.",
            }
        );

        let header = Header::fow_raw(start_ptr);
        header.set_capacity(len - header_len_u32);
        header.set_written(0);
        header.set_busy(true);

        let data_ptr = unsafe { start_ptr.add(HEADER_LEN) };

        let data_block = DataBlock {
            data_ptr,
            header,
            _object: object,
        };

        Ok(data_block)
    }

    /// Recover a [`DataBlock`] from a raw pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensurea that `start_ptr` identifies a valid [`DataBlock`].
    pub(crate) unsafe fn from_raw(start_ptr: *mut u8, object: Arc<T>) -> Self {
        let header = Header::fow_raw(start_ptr);

        let data_ptr = unsafe { start_ptr.add(HEADER_LEN) };

        DataBlock {
            data_ptr,
            header,
            _object: object,
        }
    }

    pub fn total_len(&self) -> u32 {
        self.capacity() + HEADER_LEN as u32
    }

    fn capacity(&self) -> u32 {
        self.header.capacity()
    }

    fn written_len(&self) -> u32 {
        self.header.written_len()
    }
}

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
struct Header {
    /// The pointer to the capacity.
    capacity_ptr: *mut u32,

    /// The pointer to the length of the data to be written.
    len_ptr: *mut u32,

    /// The pointer to the busy flag.
    ///
    /// If busy flag is 1, it means that the producer is writing and the consumer
    /// cannot consume the [`DataBlock`]. Else it means that the consumer can
    /// reading the [`DataBlock`].
    busy_ptr: *mut u32,

    id_ptr: *mut u32,
}

impl Header {
    /// Recover a [`Header`] from a raw pointer.
    ///
    /// # Safety
    ///
    /// The `header_ptr` must be a valid pointer to the [`Header`].
    unsafe fn fow_raw(header_ptr: *mut u8) -> Self {
        let capacity_ptr = header_ptr as *mut u32;
        let len_ptr = capacity_ptr.add(1);
        let busy_ptr = len_ptr.add(2);
        let id_ptr = busy_ptr.add(1);

        Self {
            capacity_ptr,
            len_ptr,
            busy_ptr,
            id_ptr,
        }
    }

    fn capacity(&self) -> u32 {
        let ptr = self.capacity_ptr;
        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.load(Ordering::Relaxed)
    }

    fn set_capacity(&self, capacity: u32) {
        let ptr = self.capacity_ptr;
        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.store(capacity, Ordering::Relaxed);
    }

    fn written_len(&self) -> u32 {
        let ptr = self.len_ptr;
        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.load(Ordering::Relaxed)
    }

    fn set_written(&self, len: u32) {
        let ptr = self.len_ptr;
        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.store(len, Ordering::Relaxed);
    }

    fn busy(&self) -> bool {
        let ptr = self.busy_ptr;
        let atomic = unsafe { AtomicU32::from_ptr(ptr) };

        atomic.load(Ordering::Relaxed) == 1
    }

    fn set_busy(&self, busy: bool) {
        let ptr = self.busy_ptr;
        let atomic = unsafe { AtomicU32::from_ptr(ptr) };

        if busy {
            atomic.store(1, Ordering::Relaxed);
        } else {
            atomic.store(0, Ordering::Relaxed);
        }
    }

    fn advance_len(&self, len: u32) {
        let ptr = self.len_ptr;
        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.fetch_add(len, Ordering::Relaxed);
    }

    fn id(&self) -> u32 {
        let ptr = self.id_ptr;
        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::DataBlock;
    use super::Header;
    use super::HEADER_LEN;
    use crate::error;

    #[test]
    fn test_header() {
        let vec = [0u8; 16];
        let header = unsafe { Header::fow_raw(vec.as_ptr() as *mut u8) };

        header.set_capacity(1024);
        header.set_written(512);
        header.set_busy(true);

        assert_eq!(header.capacity(), 1024);
        assert_eq!(header.written_len(), 512);
        assert!(header.busy());

        header.advance_len(125);
        assert_eq!(header.capacity(), 1024);
        assert_eq!(header.written_len(), 512 + 125);
        assert!(header.busy());
    }

    #[test]
    fn test_new_data_block_error() {
        let data = vec![0u8; 1024];

        let data_ptr = data.as_ptr() as *mut u8;

        let small_len = HEADER_LEN as u32;

        let result =
            unsafe { DataBlock::new(data_ptr, small_len, Arc::new(())) };

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
        assert_eq!(data_block.written_len(), 0);
        assert!(data_block.is_busy());

        assert_eq!(data_block.capacity(), 1024 - HEADER_LEN as u32);
        assert_eq!(data_block.written_len(), 0);
        assert!(data_block.is_busy());

        let data_block = unsafe { DataBlock::from_raw(data_ptr, Arc::new(())) };

        assert_eq!(data_block.capacity(), 1024 - HEADER_LEN as u32);
        assert_eq!(data_block.written_len(), 0);
        assert!(data_block.is_busy());

        assert_eq!(data_block.capacity(), 1024 - HEADER_LEN as u32);
        assert_eq!(data_block.written_len(), 0);
        assert!(data_block.is_busy());

        data_block.header.set_written(10);
        assert_eq!(data_block.written_len(), 10);

        data_block.header.set_busy(false);
        assert!(!data_block.is_busy());
    }
}

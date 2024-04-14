pub mod metadata;

use std::ffi::c_void;
use std::fs;
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::Arc;
use std::usize;

use nix::libc::_SC_PAGESIZE;
use nix::sys::mman;
use nix::sys::mman::MapFlags;
use nix::sys::mman::ProtFlags;
use snafu::ensure;
use snafu::ResultExt;
use tracing::error;
use tracing::info;

use self::metadata::RingbufMetadata;
use crate::data_block::DataBlock;
use crate::data_block::HEADER_LEN;
use crate::error;
use crate::error::Result;

pub(crate) const PAGE_SIZE: usize = 4;

pub(crate) const METADATA_LEN: usize = 4 * PAGE_SIZE;

/// The version of the ring buffer.
pub(crate) const VERSION: u32 = 1;

/// The ringbuf data structure, which mapped to the underlying buffer, ex: share memory.
///
/// ## The underlying structure
///
/// ```text
///                data_part_ptr
///                     |    
///                     v
/// +-------------------+---------------------------------------+----------------------------------------+
/// | metadata          | data part 0                           | data part 1                            |
/// +-------------------+---------------------------------------+----------------------------------------+
/// ```
///
/// ## Note
///
/// 1. data part 0 and data part 1 are mapped to the same physical memory.
/// 2. The len of metadata part is align(METADATA_LEN, page_size).
#[derive(Clone)]
pub struct Ringbuf {
    /// The raw pointer to the data part.
    data_part_ptr: *mut u8,

    /// The length of the data part in ringbuf.
    data_part_len: usize,

    /// The metadata of the ring buffer.
    metadata: RingbufMetadata,

    /// The drop guard of the ring buffer, which is used to munmap when all Ringbuf and
    /// releated DataBlock is dropped.
    drop_guard: Arc<DropGuard>,
}

unsafe impl Send for Ringbuf {}
unsafe impl Sync for Ringbuf {}

impl Ringbuf {
    /// Create a new ring buffer from the file, and reset the metadata.
    pub fn new(file: &fs::File, data_size: usize) -> Result<Self> {
        let ringbuf = Self::from_raw(file, data_size)?;

        ringbuf.metadata.atomic_set_consume_offset(0);
        ringbuf.metadata.atomic_set_produce_offset(0);

        Ok(ringbuf)
    }

    /// Create a new ring buffer from the raw file.
    ///
    /// ## Note
    ///
    /// 1. The length paramter is not the actual length of the data part.
    /// This length parameter will be referenced when creating ringbuf.
    /// 2. create a ringbuf, but not reset the metadata.
    pub fn from_raw(file: &fs::File, length: usize) -> Result<Self> {
        ensure!(
            length > 0,
            error::InvalidParameterSnafu {
                detail: "The data_size must be greater than 0.",
            }
        );

        let align_metadata_size = page_align_size(METADATA_LEN);
        let align_data_size = page_align_size(length);

        info!(
            "actual metadata size: {}, actual data_part size: {}",
            align_metadata_size, align_data_size
        );

        let rw_prot = ProtFlags::PROT_READ | ProtFlags::PROT_WRITE;
        let none_prot = ProtFlags::PROT_NONE;

        let private_flags = MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS;
        let public_flags = MapFlags::MAP_SHARED | MapFlags::MAP_FIXED;

        let total_size =
            NonZeroUsize::new(align_metadata_size + align_data_size * 2)
                .unwrap();

        let anchor_ptr = unsafe {
            mman::mmap_anonymous(None, total_size, none_prot, private_flags)
                .context(error::MmapAnonymousSnafu)?
        };

        let anchor_addr = anchor_ptr.addr();
        let mmap_len =
            NonZeroUsize::new(align_metadata_size + align_data_size).unwrap();

        let metadata_ptr = unsafe {
            mman::mmap(
                Some(anchor_addr),
                mmap_len,
                rw_prot,
                public_flags,
                file,
                0,
            )
            .context(error::MmapSnafu)?
        };

        let mmap_len = NonZeroUsize::new(align_data_size).unwrap();
        let start_addr = unsafe {
            anchor_ptr.add(align_metadata_size + align_data_size).addr()
        };
        let offset = align_metadata_size as i64;

        let _ = unsafe {
            mman::mmap(
                Some(start_addr),
                mmap_len,
                rw_prot,
                public_flags,
                file,
                offset,
            )
            .context(error::MmapSnafu)?
        };

        let data_part_ptr =
            unsafe { anchor_ptr.add(align_metadata_size).as_ptr() as *mut u8 };
        let data_part_len = align_data_size;
        let metadata =
            unsafe { RingbufMetadata::new(metadata_ptr.as_ptr() as *mut u8) };

        let drop_guard = Arc::new(DropGuard {
            mmap_ptr: anchor_ptr,
            mmap_len: total_size.get(),
        });

        let ringbuf = Ringbuf {
            data_part_ptr,
            data_part_len,
            metadata,
            drop_guard,
        };

        Ok(ringbuf)
    }

    pub fn reserve(&mut self, bytes: usize) -> Result<DataBlock<DropGuard>> {
        let bytes = (bytes + 3) / 4 * 4;
        let actual_alloc_bytes = (bytes + HEADER_LEN) as u32;

        ensure!(
            actual_alloc_bytes <= self.remain_bytes(),
            error::NotEnoughSpaceSnafu {
                remaining: self.remain_bytes(),
                expected: actual_alloc_bytes,
            }
        );

        let produce_offset = self.produce_offset();
        let start_ptr =
            unsafe { self.data_part_ptr.add(produce_offset as usize) };
        let drop_guard = self.drop_guard.clone();

        let data_block = unsafe {
            DataBlock::new(start_ptr, actual_alloc_bytes, drop_guard)?
        };

        unsafe {
            self.advance_produce_offset(actual_alloc_bytes);
        }

        Ok(data_block)
    }

    pub fn peek(&self) -> Option<DataBlock<DropGuard>> {
        let consume_offset = self.consume_offset();
        let produce_offset = self.atomic_produce_offset();

        if consume_offset == produce_offset {
            return None;
        }

        let start_ptr =
            unsafe { self.data_part_ptr.add(consume_offset as usize) };

        let data_block =
            unsafe { DataBlock::from_raw(start_ptr, self.drop_guard.clone()) };

        Some(data_block)
    }

    /// Advance the produce offset with no boundary detection.
    ///
    /// # Safety
    /// This function is unsafe because it does not check the boundary of the ring buffer.
    pub unsafe fn advance_produce_offset(&mut self, len: u32) {
        let pre_produce_offset = self.produce_offset();

        let produce_offset =
            (pre_produce_offset + len) % self.data_part_len as u32;

        self.atomic_set_produce_offset(produce_offset);
    }

    /// Advance the consume offset with no boundary detection.
    ///
    /// # Safety
    /// This function is unsafe because it does not check the boundary of the ring buffer.
    pub unsafe fn advance_consume_offset(&mut self, len: u32) {
        let pre_consume_offset = self.consume_offset();

        let consume_offset =
            (pre_consume_offset + len) % self.data_part_len as u32;

        self.atomic_set_consume_offset(consume_offset);
    }

    pub fn remain_bytes(&self) -> u32 {
        self.capacity() - self.written_bytes()
    }

    pub fn capacity(&self) -> u32 {
        self.data_part_len as u32 - 1
    }

    pub fn written_bytes(&self) -> u32 {
        let produce_offset = self.produce_offset();
        let consumer_offset = self.atomic_consume_offset();
        if produce_offset >= consumer_offset {
            produce_offset - consumer_offset
        } else {
            self.data_part_len as u32 - (consumer_offset - produce_offset)
        }
    }

    pub fn consume_offset(&self) -> u32 {
        self.metadata.consume_offset()
    }

    pub fn atomic_consume_offset(&self) -> u32 {
        self.metadata.atomic_consume_offset()
    }

    fn atomic_set_consume_offset(&self, offset: u32) {
        self.metadata.atomic_set_consume_offset(offset)
    }

    pub fn produce_offset(&self) -> u32 {
        self.metadata.produce_offset()
    }

    pub fn atomic_produce_offset(&self) -> u32 {
        self.metadata.atomic_produce_offset()
    }

    fn atomic_set_produce_offset(&self, offset: u32) {
        self.metadata.atomic_set_produce_offset(offset)
    }
}

pub struct DropGuard {
    mmap_ptr: NonNull<c_void>,
    mmap_len: usize,
}

unsafe impl Send for DropGuard {}
unsafe impl Sync for DropGuard {}

impl Drop for DropGuard {
    fn drop(&mut self) {
        if let Err(e) = unsafe { mman::munmap(self.mmap_ptr, self.mmap_len) } {
            error!(
                "munmap failed: {:?}, mmap_addr: {:?}, mmap_len: {}",
                e, self.mmap_ptr, self.mmap_len
            );
        }
    }
}

fn page_align_size(size: usize) -> usize {
    // Note: sys_page_size always power of 2.
    let mask = sys_page_size() - 1;
    (size + mask) & !mask
}

fn sys_page_size() -> usize {
    unsafe { nix::libc::sysconf(_SC_PAGESIZE) as usize }
}

#[cfg(test)]
mod tests {
    use crate::ringbuf::page_align_size;
    use crate::ringbuf::{self};

    #[test]
    fn test_ringbuf_metadata() {
        let file = tempfile::tempfile().unwrap();
        file.set_len(1024).unwrap();

        let mut ringbuf_producer = ringbuf::Ringbuf::new(&file, 1024).unwrap();
        ringbuf_producer.atomic_set_consume_offset(4);
        ringbuf_producer.atomic_set_produce_offset(8);

        unsafe {
            ringbuf_producer.advance_produce_offset(4);
        }

        let mut ringbuf_consumer =
            ringbuf::Ringbuf::from_raw(&file, 1024).unwrap();
        assert_eq!(ringbuf_consumer.atomic_consume_offset(), 4);
        assert_eq!(ringbuf_consumer.atomic_produce_offset(), 12);

        unsafe {
            ringbuf_consumer.advance_consume_offset(4);
        }

        assert_eq!(ringbuf_consumer.atomic_produce_offset(), 12);
        assert_eq!(ringbuf_producer.atomic_produce_offset(), 12);
    }

    #[test]
    fn test_ringbuf_remain_bytes() {
        let file = tempfile::tempfile().unwrap();
        file.set_len(1024).unwrap();

        let mut ringbuf_producer = ringbuf::Ringbuf::new(&file, 1024).unwrap();
        let actual_alloc_bytes = page_align_size(1024) as u32;

        ringbuf_producer.atomic_set_consume_offset(4);
        ringbuf_producer.atomic_set_produce_offset(8);

        assert_eq!(ringbuf_producer.remain_bytes(), actual_alloc_bytes - 4 - 1);
        unsafe {
            ringbuf_producer.advance_produce_offset(4);
        }
        assert_eq!(ringbuf_producer.remain_bytes(), actual_alloc_bytes - 8 - 1);

        ringbuf_producer.atomic_set_consume_offset(8);
        ringbuf_producer.atomic_set_produce_offset(4);
        assert_eq!(ringbuf_producer.remain_bytes(), 3);
    }

    #[test]
    fn test_ringbuf_advance() {
        let file = tempfile::tempfile().unwrap();
        file.set_len(1024).unwrap();

        let mut ringbuf_producer = ringbuf::Ringbuf::new(&file, 1024).unwrap();
        let actual_alloc_bytes = page_align_size(1024) as u32;

        ringbuf_producer.atomic_set_consume_offset(actual_alloc_bytes - 8);
        unsafe {
            ringbuf_producer.advance_consume_offset(9);
        }
        assert_eq!(ringbuf_producer.consume_offset(), 1);

        ringbuf_producer.atomic_set_produce_offset(actual_alloc_bytes);
        unsafe {
            ringbuf_producer.advance_produce_offset(9);
        }
        assert_eq!(ringbuf_producer.produce_offset(), 9);
    }
}

use std::ffi::c_void;
use std::fs;
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use nix::libc::_SC_PAGESIZE;
use nix::sys::mman;
use nix::sys::mman::MapFlags;
use nix::sys::mman::ProtFlags;
use snafu::ensure;
use snafu::ResultExt;
use tracing::error;
use tracing::info;

use crate::data_block::DataBlock;
use crate::data_block::HEADER_LEN;
use crate::error;
use crate::error::Result;

// Unit is byte.
pub(crate) const METADATA_LEN: usize = 4 * 4;

/// The ringbuf data structure, which mapped to the share memory.
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
/// The underlying memory is divided into two parts: metadata and data part.
///
/// The metadata is used to store the information of the data part, include the
/// produce offset and consume offset. The len of Metadata is align(METADATA_LEN
/// , page_size).
///
/// The data part is used to store the data, which organized as a ring buffer.
/// Note: data part 0 and data part 1 are mapped to the same physical memory.
#[derive(Clone)]
pub struct Ringbuf {
    /// The raw pointer to the data part.
    data_part_ptr: *mut u8,

    /// The length of the data part in ringbuf, include data part 0 and data
    /// part 1.
    data_part_len: usize,

    /// The metadata of the ring buffer.
    metadata: RingbufMetadata,

    /// The drop guard of the ring buffer, which is used to munmap when all
    /// [Ringbuf] and releated [DataBlock] is dropped.
    drop_guard: Arc<DropGuard>,
}

unsafe impl Send for Ringbuf {}
unsafe impl Sync for Ringbuf {}

impl Ringbuf {
    /// Creates a new ringbuf based on the given file.
    ///
    /// Note: it will reset the metadata part.
    pub fn new(
        file: &fs::File,
        expected_data_part_bytes: usize,
    ) -> Result<Self> {
        let ringbuf = Self::from(file, expected_data_part_bytes)?;

        ringbuf.atomic_set_consume_offset(0);
        ringbuf.atomic_set_produce_offset(0);

        Ok(ringbuf)
    }

    /// Recover a ring buffer from the given file.
    ///
    /// Note: it does not modify the metadata part and the data part.
    pub fn from(
        file: &fs::File,
        expected_data_part_bytes: usize,
    ) -> Result<Self> {
        ensure!(
            expected_data_part_bytes > 0,
            error::InvalidParameterSnafu {
                detail: "The data_size must be greater than 0.",
            }
        );

        let align_metadata_size = page_align_size(METADATA_LEN);
        let align_data_size = page_align_size(expected_data_part_bytes);
        let total_size = align_metadata_size + align_data_size * 2;

        file.set_len(total_size as u64).context(error::IoSnafu)?;

        info!(
            "actual metadata size: {}, actual data_part size: {}",
            align_metadata_size, align_data_size
        );

        let rw_prot = ProtFlags::PROT_READ | ProtFlags::PROT_WRITE;
        let none_prot = ProtFlags::PROT_NONE;

        let private_flags = MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS;
        let public_flags = MapFlags::MAP_SHARED | MapFlags::MAP_FIXED;

        // Unwrap is safe here because total_size is not zero.
        let total_size = NonZeroUsize::new(total_size).unwrap();

        // 1. Allocate the memory for the metadata and data part.
        let anchor_ptr_c_void = unsafe {
            mman::mmap_anonymous(None, total_size, none_prot, private_flags)
                .context(error::MmapAnonymousSnafu)?
        };

        let anchor_ptr = anchor_ptr_c_void.as_ptr() as *mut u8;

        // 2. Map the metadata part.
        // Unwrap is safe here because align_metadata_size + align_data_size is not zero.
        let mmap_len =
            NonZeroUsize::new(align_metadata_size + align_data_size).unwrap();
        // Unwrap is safe here because anchor_ptr is not null.
        let start_addr = NonZeroUsize::new(anchor_ptr as usize).unwrap();
        let offset = 0;
        let metadata_ptr = unsafe {
            mman::mmap(
                Some(start_addr),
                mmap_len,
                rw_prot,
                public_flags,
                file,
                offset,
            )
            .context(error::MmapSnafu)?
            .as_ptr() as *mut u8
        };

        // 3. Map the data part.
        // Unwrap is safe here because metadata_ptr is not null.
        let mmap_len = NonZeroUsize::new(align_data_size).unwrap();
        let start_addr = unsafe {
            let ptr = anchor_ptr.add(align_metadata_size + align_data_size);
            // Unwrap is safe here because ptr is not null.
            NonZeroUsize::new(ptr as usize).unwrap()
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

        // 4. Build the ring buffer.
        let data_part_ptr = unsafe { anchor_ptr.add(align_metadata_size) };
        let data_part_len = align_data_size;
        let metadata = unsafe { RingbufMetadata::new(metadata_ptr) };

        let anchor_ptr = NonNull::new(anchor_ptr as *mut c_void).unwrap();
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

    /// Reserve a data block with the expected bytes. This operation will advance the produce
    /// offset.
    ///
    /// Note: actual allocated bytes may be greater than the given bytes.
    pub fn reserve(&mut self, bytes: usize) -> Result<DataBlock<DropGuard>> {
        // 1. calculate the actual allocated bytes.
        let bytes = (bytes + 3) / 4 * 4;
        let actual_alloc_bytes = (bytes + HEADER_LEN) as u32;

        // 2. check if there is enough space.
        ensure!(
            actual_alloc_bytes <= self.remain_bytes(),
            error::NotEnoughSpaceSnafu {
                remaining: self.remain_bytes(),
                expected: actual_alloc_bytes,
            }
        );

        // 3. create the data block.
        let produce_offset = self.produce_offset();
        let start_ptr =
            unsafe { self.data_part_ptr.add(produce_offset as usize) };
        let drop_guard = self.drop_guard.clone();

        let data_block = unsafe {
            DataBlock::new(start_ptr, actual_alloc_bytes, drop_guard)?
        };

        // 4. advance the produce offset.
        unsafe {
            self.advance_produce_offset(actual_alloc_bytes);
        }

        Ok(data_block)
    }

    /// Peek a data block from the ring buffer. This operation will not update the consume
    /// offset and produce offset.
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

    /// Get the remaining bytes of the ring buffer.
    pub fn remain_bytes(&self) -> u32 {
        self.capacity() - self.written_bytes()
    }

    /// Get the capacity of the ring buffer.
    pub fn capacity(&self) -> u32 {
        self.data_part_len as u32 - 1
    }

    /// Get the written bytes of the ring buffer.
    pub fn written_bytes(&self) -> u32 {
        let produce_offset = self.produce_offset();
        let consumer_offset = self.atomic_consume_offset();
        if produce_offset >= consumer_offset {
            produce_offset - consumer_offset
        } else {
            self.data_part_len as u32 - (consumer_offset - produce_offset)
        }
    }

    /// Get the consume offset which is the next read position in ringbuf.
    pub fn consume_offset(&self) -> u32 {
        let ptr = self.metadata.consume_offset_ptr;

        unsafe { *ptr }
    }

    /// Get the produce offset which is the next write position in ringbuf.
    pub fn produce_offset(&self) -> u32 {
        let ptr = self.metadata.produce_offset_ptr;

        unsafe { *ptr }
    }

    /// Get the consume offset which is the next read position in ringbuf.
    ///
    /// Note: This function is atomic.
    pub fn atomic_consume_offset(&self) -> u32 {
        let ptr = self.metadata.consume_offset_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.load(Ordering::Relaxed)
    }

    /// Get the produce offset which is the next write position in ringbuf.
    ///
    /// Note: This function is atomic.
    pub fn atomic_produce_offset(&self) -> u32 {
        let ptr = self.metadata.produce_offset_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.load(Ordering::Relaxed)
    }

    /// Set the consume offset which is the next read position in ringbuf.
    ///
    /// Note: This function is atomic.
    pub fn atomic_set_consume_offset(&self, offset: u32) {
        let ptr = self.metadata.consume_offset_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.store(offset, Ordering::Relaxed);
    }

    /// Set the produce offset which is the next write position in ringbuf.
    ///
    /// Note: This function is atomic.
    pub fn atomic_set_produce_offset(&self, offset: u32) {
        let ptr = self.metadata.produce_offset_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.store(offset, Ordering::Relaxed);
    }

    /// Advance the consume offset.
    ///
    /// # Safety
    /// This function is unsafe because if used incorrectly it may cause data corruption.
    pub unsafe fn advance_consume_offset(&self, len: u32) {
        let ptr = self.metadata.consume_offset_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };

        let _ =
            atomic.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |pre| {
                Some((pre + len) % self.data_part_len as u32)
            });
    }

    /// Advance the produce offset.
    ///
    /// # Safety
    /// This function is unsafe because if used incorrectly it may cause data corruption.
    pub unsafe fn advance_produce_offset(&self, len: u32) {
        let ptr = self.metadata.produce_offset_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };

        let _ =
            atomic.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |pre| {
                Some((pre + len) % self.data_part_len as u32)
            });
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

#[cfg(test)]
mod tests {
    use super::Ringbuf;
    use crate::error;
    use crate::ringbuf::page_align_size;
    use crate::ringbuf::{self};

    #[test]
    fn test_ringbuf_metadata() {
        let file = tempfile::tempfile().unwrap();

        let ringbuf_producer = ringbuf::Ringbuf::new(&file, 1024).unwrap();
        ringbuf_producer.atomic_set_consume_offset(4);
        ringbuf_producer.atomic_set_produce_offset(8);

        unsafe {
            ringbuf_producer.advance_produce_offset(4);
        }

        let ringbuf_consumer = ringbuf::Ringbuf::from(&file, 1024).unwrap();
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

        let ringbuf_producer = ringbuf::Ringbuf::new(&file, 1024).unwrap();
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
    fn test_ringbuf_advance_produce_offset_with_multi_thread() {
        let file = tempfile::tempfile().unwrap();

        let expexted_ringbuf_size = 102400 + 1;
        let ringbuf =
            ringbuf::Ringbuf::new(&file, expexted_ringbuf_size).unwrap();
        let actual_alloc_bytes = page_align_size(expexted_ringbuf_size) as u32;

        let mut joins = Vec::with_capacity(10);
        for _ in 0..10 {
            let ringbuf_c = ringbuf.clone();
            let join = std::thread::spawn(move || {
                for _ in 0..10240 {
                    unsafe {
                        ringbuf_c.advance_produce_offset(1);
                    }
                }
            });
            joins.push(join);
        }

        for join in joins {
            join.join().unwrap();
        }

        assert_eq!(ringbuf.atomic_produce_offset(), 102400);

        unsafe {
            ringbuf.advance_produce_offset(actual_alloc_bytes - 102400);
        }
        assert_eq!(ringbuf.produce_offset(), 0);
    }

    #[test]
    fn test_ringbuf_advance_consume_offset_with_multi_thread() {
        let file = tempfile::tempfile().unwrap();

        let expexted_ringbuf_size = 102400 + 1;
        let ringbuf =
            ringbuf::Ringbuf::new(&file, expexted_ringbuf_size).unwrap();
        let actual_alloc_bytes = page_align_size(expexted_ringbuf_size) as u32;

        let mut joins = Vec::with_capacity(10);
        for _ in 0..10 {
            let ringbuf_c = ringbuf.clone();
            let join = std::thread::spawn(move || {
                for _ in 0..10240 {
                    unsafe {
                        ringbuf_c.advance_consume_offset(1);
                    }
                }
            });
            joins.push(join);
        }

        for join in joins {
            join.join().unwrap();
        }

        assert_eq!(ringbuf.atomic_consume_offset(), 102400);

        unsafe {
            ringbuf.advance_consume_offset(actual_alloc_bytes - 102400);
        }
        assert_eq!(ringbuf.consume_offset(), 0);
    }

    #[test]
    fn test_ringbuf_advance() {
        let file = tempfile::tempfile().unwrap();

        let ringbuf_producer = ringbuf::Ringbuf::new(&file, 1024).unwrap();
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

    #[test]
    fn test_ringbuf_write() {
        let data_size = 1024 * 32;

        let file = tempfile::tempfile().unwrap();
        let mut ringbuf = Ringbuf::new(&file, data_size).unwrap();

        for i in 1..10000 {
            let result = ringbuf.reserve(20);
            if matches!(result, Err(error::Error::NotEnoughSpace { .. })) {
                ringbuf.atomic_set_produce_offset(0);
                continue;
            }
            let mut pre_alloc = result.unwrap();
            let write_str = format!("hello, {}", i);
            // Unwrap is safe here because we have enough space.
            pre_alloc.write(write_str.as_bytes()).unwrap();
            pre_alloc.commit();
        }
    }
}

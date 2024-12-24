pub mod data_block;

use std::ffi::c_void;
use std::fs::File;
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use data_block::DataBlock;
use data_block::HEADER_LEN;
use nix::libc::_SC_PAGESIZE;
use nix::sys::mman;
use nix::sys::mman::MapFlags;
use nix::sys::mman::ProtFlags;
use once_cell::sync::OnceCell;
use snafu::ensure;
use snafu::OptionExt;
use snafu::ResultExt;
use tracing::error;

use crate::convert_num;
use crate::error;
use crate::error::Result;

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
    /// [Ringbuf] and related [DataBlock] is dropped.
    drop_guard: Arc<DropGuard>,
}

unsafe impl Send for Ringbuf {}
unsafe impl Sync for Ringbuf {}

// Unit is byte.
pub(crate) const METADATA_LEN: usize = 4 * 4;

// Unit is byte.
pub fn page_align_metadata_len() -> u64 {
    static LEN: OnceCell<u64> = OnceCell::new();
    *LEN.get_or_init(|| page_align_size(METADATA_LEN as u64))
}

// Unit is byte.
pub fn min_ringbuf_len() -> u64 {
    static LEN: OnceCell<u64> = OnceCell::new();
    *LEN.get_or_init(|| {
        page_align_size(HEADER_LEN as u64 + 1024) + page_align_metadata_len()
    })
}

impl Ringbuf {
    /// Creates a new ringbuf based on the given file.
    pub fn new(file: &File) -> Result<Self> {
        // 1. Calculate the file length and set file length.
        let mut file_len = file.metadata().context(error::IoSnafu)?.len();
        if file_len < min_ringbuf_len() {
            file_len = min_ringbuf_len();
        }
        let align_file_len = page_align_size(file_len);
        if file_len != align_file_len {
            file_len = align_file_len;
        }
        file.set_len(file_len).context(error::IoSnafu)?;

        // 2. Create the ring buffer.
        let ringbuf = unsafe { Self::from_unchecked(file)? };

        // 3. Reset the metadata part.
        ringbuf.set_consume_offset(0);
        ringbuf.set_produce_offset(0);

        Ok(ringbuf)
    }

    /// Recover a ring buffer from the given file.
    pub fn from(file: &File) -> Result<Self> {
        // 1. Check the file length is enough.
        let file_len = file.metadata().context(error::IoSnafu)?.len();
        ensure!(file_len >= min_ringbuf_len(), {
            let detail = format!(
                "The file length {} is too small, min size is {}.",
                file_len,
                min_ringbuf_len()
            );
            error::InvalidParameterSnafu { detail }
        });

        // 2. Check the file length is page aligned.
        ensure!(file_len == page_align_size(file_len), {
            let detail =
                format!("The file length {} is not page aligned.", file_len);

            error::InvalidParameterSnafu { detail }
        });

        unsafe { Self::from_unchecked(file) }
    }

    unsafe fn from_unchecked(file: &File) -> Result<Self> {
        let file_len = convert_num!(
            file.metadata().context(error::IoSnafu)?.len(),
            usize
        )?;
        let align_metadata_len =
            convert_num!(page_align_metadata_len(), usize)?;

        let align_datapart_len = file_len
            .checked_sub(align_metadata_len)
            .context(error::UsizeSubOverflowSnafu {
                a: file_len,
                b: align_metadata_len,
            })?;

        let rw_prot = ProtFlags::PROT_READ | ProtFlags::PROT_WRITE;
        let none_prot = ProtFlags::PROT_NONE;

        let private_flags = MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS;
        let public_flags = MapFlags::MAP_SHARED | MapFlags::MAP_FIXED;

        let total_size = align_metadata_len + align_datapart_len * 2;
        let total_size = NonZeroUsize::new(total_size).unwrap();

        // 1. Allocate the memory for the metadata and data part.
        let anchor_ptr_c_void = unsafe {
            mman::mmap_anonymous(None, total_size, none_prot, private_flags)
                .context(error::MmapAnonymousSnafu)?
        };

        let anchor_ptr = anchor_ptr_c_void.as_ptr() as *mut u8;

        // 2. Map the metadata part.
        let mmap_len = align_metadata_len + align_datapart_len;
        let mmap_len = NonZeroUsize::new(mmap_len).unwrap();
        let start_addr = Some(NonZeroUsize::new(anchor_ptr as usize).unwrap());
        let offset = 0;
        let metadata_ptr = unsafe {
            mman::mmap(
                start_addr,
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
        let mmap_len = NonZeroUsize::new(align_datapart_len).unwrap();
        let start_addr = unsafe {
            let ptr = anchor_ptr.add(align_metadata_len + align_datapart_len);
            Some(NonZeroUsize::new(ptr as usize).unwrap())
        };
        let offset = convert_num!(align_metadata_len, i64)?;
        let _ = unsafe {
            mman::mmap(
                start_addr,
                mmap_len,
                rw_prot,
                public_flags,
                file,
                offset,
            )
            .context(error::MmapSnafu)?
        };

        // 4. Build the ring buffer.
        let data_part_ptr = unsafe { anchor_ptr.add(align_metadata_len) };
        let data_part_len = align_datapart_len;
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

    /// Reserve a data block with the expected bytes. This operation will advance
    /// the produce offset.
    ///
    /// Note: actual allocated bytes may be greater than the given bytes.
    pub fn reserve(
        &mut self,
        bytes: usize,
        req_id: u32,
    ) -> Result<DataBlock<DropGuard>> {
        // 1. calculate the actual allocated bytes.
        let bytes = (bytes + 3) / 4 * 4;
        let actual_alloc_bytes = (bytes + HEADER_LEN) as u32;

        // 2. check if the actual allocate bytes exceeds the capacity.
        ensure!(
            actual_alloc_bytes <= self.capacity(),
            error::ExceedCapacitySnafu {
                capacity: self.capacity(),
                expected: actual_alloc_bytes,
            }
        );

        // 3. check if there is enough space.
        ensure!(
            actual_alloc_bytes <= self.remain_bytes(),
            error::NotEnoughSpaceSnafu {
                remaining: self.remain_bytes(),
                expected: actual_alloc_bytes,
            }
        );

        // 4. create the data block.
        let produce_offset = self.produce_offset();
        let start_ptr =
            unsafe { self.data_part_ptr.add(produce_offset as usize) };
        let drop_guard = self.drop_guard.clone();

        let data_block = unsafe {
            DataBlock::new(req_id, start_ptr, actual_alloc_bytes, drop_guard)?
        };

        // 5. advance the produce offset.
        unsafe {
            self.advance_produce_offset(actual_alloc_bytes);
        }

        Ok(data_block)
    }

    /// Peek a data block from the ring buffer. This operation will not update
    /// the consume offset and produce offset.
    pub fn peek(&self) -> Option<DataBlock<DropGuard>> {
        let consume_offset = self.consume_offset();
        let produce_offset = self.produce_offset();

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
        let consumer_offset = self.consume_offset();
        if produce_offset >= consumer_offset {
            produce_offset - consumer_offset
        } else {
            self.data_part_len as u32 - (consumer_offset - produce_offset)
        }
    }

    /// Get the consume offset which is the next read position in ringbuf.
    ///
    /// Note: This function is atomic.
    pub fn consume_offset(&self) -> u32 {
        let ptr = self.metadata.consume_offset_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.load(Ordering::Acquire)
    }

    /// Get the produce offset which is the next write position in ringbuf.
    ///
    /// Note: This function is atomic.
    pub fn produce_offset(&self) -> u32 {
        let ptr = self.metadata.produce_offset_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.load(Ordering::Acquire)
    }

    /// Set the consume offset which is the next read position in ringbuf.
    ///
    /// Note: This function is atomic.
    pub fn set_consume_offset(&self, offset: u32) {
        let ptr = self.metadata.consume_offset_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.store(offset, Ordering::Release);
    }

    /// Set the produce offset which is the next write position in ringbuf.
    ///
    /// Note: This function is atomic.
    pub fn set_produce_offset(&self, offset: u32) {
        let ptr = self.metadata.produce_offset_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };
        atomic.store(offset, Ordering::Release);
    }

    /// Advance the consume offset.
    ///
    /// # Safety
    /// This function is unsafe because if used incorrectly it may cause data corruption.
    pub unsafe fn advance_consume_offset(&self, len: u32) {
        let ptr = self.metadata.consume_offset_ptr;

        let atomic = unsafe { AtomicU32::from_ptr(ptr) };

        let _ =
            atomic.fetch_update(Ordering::Release, Ordering::Acquire, |pre| {
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
            atomic.fetch_update(Ordering::Release, Ordering::Acquire, |pre| {
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

pub(crate) fn page_align_size(size: u64) -> u64 {
    // Note: sys_page_size always power of 2.
    let mask = sys_page_size() - 1;
    (size + mask) & !mask
}

fn sys_page_size() -> u64 {
    unsafe { nix::libc::sysconf(_SC_PAGESIZE) as u64 }
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
    use crate::ringbuf;
    use crate::ringbuf::min_ringbuf_len;
    use crate::ringbuf::page_align_metadata_len;
    use crate::ringbuf::page_align_size;

    #[test]
    fn test_ringbuf_metadata() {
        let file = tempfile::tempfile().unwrap();

        let ringbuf_producer = ringbuf::Ringbuf::new(&file).unwrap();
        ringbuf_producer.set_consume_offset(4);
        ringbuf_producer.set_produce_offset(8);

        unsafe {
            ringbuf_producer.advance_produce_offset(4);
        }

        let ringbuf_consumer = ringbuf::Ringbuf::from(&file).unwrap();
        assert_eq!(ringbuf_consumer.consume_offset(), 4);
        assert_eq!(ringbuf_consumer.produce_offset(), 12);

        unsafe {
            ringbuf_consumer.advance_consume_offset(4);
        }

        assert_eq!(ringbuf_consumer.produce_offset(), 12);
        assert_eq!(ringbuf_producer.produce_offset(), 12);
    }

    #[test]
    fn test_ringbuf_remain_bytes() {
        let file = tempfile::tempfile().unwrap();
        file.set_len(min_ringbuf_len()).unwrap();

        let ringbuf_producer = ringbuf::Ringbuf::new(&file).unwrap();

        let actual_alloc_bytes = page_align_size(min_ringbuf_len()) as u32;
        let datapart_len =
            actual_alloc_bytes - page_align_metadata_len() as u32;

        ringbuf_producer.set_consume_offset(4);
        ringbuf_producer.set_produce_offset(8);

        assert_eq!(ringbuf_producer.remain_bytes(), datapart_len - 4 - 1);
        unsafe {
            ringbuf_producer.advance_produce_offset(4);
        }
        assert_eq!(ringbuf_producer.remain_bytes(), datapart_len - 8 - 1);

        ringbuf_producer.set_consume_offset(8);
        ringbuf_producer.set_produce_offset(4);
        assert_eq!(ringbuf_producer.remain_bytes(), 3);
    }

    #[test]
    fn test_ringbuf_advance_offset_with_multi_thread() {
        let file = tempfile::tempfile().unwrap();
        file.set_len(102400).unwrap();

        let ringbuf = ringbuf::Ringbuf::new(&file).unwrap();

        let mut joins = Vec::with_capacity(10);
        for _ in 0..10 {
            let ringbuf_c = ringbuf.clone();
            let join = std::thread::spawn(move || {
                for _ in 0..10240 {
                    unsafe {
                        ringbuf_c.advance_produce_offset(1);
                        ringbuf_c.advance_consume_offset(2);
                    }
                }
            });
            joins.push(join);
        }

        for join in joins {
            join.join().unwrap();
        }

        let produce_offset = ringbuf.produce_offset();
        let consume_offset = ringbuf.consume_offset();

        ringbuf.set_consume_offset(0);
        ringbuf.set_produce_offset(0);
        for _ in 0..102400 {
            unsafe {
                ringbuf.advance_produce_offset(1);
                ringbuf.advance_consume_offset(2);
            }
        }
        let expected_produce_offset = ringbuf.produce_offset();
        let expected_consume_offset = ringbuf.consume_offset();

        assert_eq!(expected_produce_offset, produce_offset);
        assert_eq!(expected_consume_offset, consume_offset);
    }

    #[test]
    fn test_ringbuf_advance() {
        let file = tempfile::tempfile().unwrap();

        let ringbuf_producer = ringbuf::Ringbuf::new(&file).unwrap();
        let datapart_len = ringbuf_producer.data_part_len as u32;

        ringbuf_producer.set_consume_offset(datapart_len - 8);
        unsafe {
            ringbuf_producer.advance_consume_offset(9);
        }
        assert_eq!(ringbuf_producer.consume_offset(), 1);

        ringbuf_producer.set_produce_offset(datapart_len - 1);
        unsafe {
            ringbuf_producer.advance_produce_offset(9);
        }
        assert_eq!(ringbuf_producer.produce_offset(), 8);
    }

    #[test]
    fn test_reserve_with_execced_capacity() {
        let file = tempfile::tempfile().unwrap();
        file.set_len(min_ringbuf_len()).unwrap();

        let mut ringbuf = ringbuf::Ringbuf::new(&file).unwrap();

        let result = ringbuf.reserve(ringbuf.capacity() as usize, 1);
        assert!(matches!(result, Err(error::Error::ExceedCapacity { .. })));
    }

    #[test]
    fn test_ringbuf_write() {
        let file = tempfile::tempfile().unwrap();
        let mut ringbuf = Ringbuf::new(&file).unwrap();

        for i in 1..100000 {
            let result = ringbuf.reserve(20, 1);
            if matches!(result, Err(error::Error::NotEnoughSpace { .. })) {
                ringbuf.set_produce_offset(0);
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

// TODO: remove the dead_code attribute after the implementation is completed.
#![allow(dead_code)]
use std::sync::Arc;

pub struct DataBlock<T> {
    data_ptr: *mut u8,
    data_len: usize,
    header: Header,
    object: Arc<T>,
}

unsafe impl<T> Send for DataBlock<T> {}
unsafe impl<T> Sync for DataBlock<T> {}

pub struct Header {
    // The capacity of the DataBlock.
    capacity: *mut u32,
    // If busy is 1, it means that the producer may be writing and the consumer cannot consume the Datablock.
    // If busy is 0, it means that the consumer can reading the DataBlock.
    busy: *mut u32,
}

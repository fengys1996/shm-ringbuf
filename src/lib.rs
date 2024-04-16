#![feature(pointer_is_aligned)]
#![feature(non_null_convenience)]
#![feature(strict_provenance)]
pub mod consumer;
pub mod error;
pub mod producer;

mod data_block;
mod fdpass;
mod grpc;
mod memfd;
mod ringbuf;

#[cfg(feature = "benchmark")]
pub use ringbuf::Ringbuf;

#![feature(pointer_is_aligned)]
#![feature(non_null_convenience)]
#![feature(strict_provenance)]
pub mod consumer;
pub mod data_block;
pub mod error;
pub mod producer;

mod fdpass;
mod memfd;
mod ringbuf;

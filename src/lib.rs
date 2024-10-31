pub mod consumer;
pub mod error;
pub mod producer;

mod fd_pass;
mod grpc;
mod macros;
mod memfd;
mod ringbuf;

pub use grpc::proto::shm_control_server::ShmControlServer;
pub use grpc::server::ShmCtlHandler;
#[cfg(feature = "benchmark")]
pub use ringbuf::Ringbuf;

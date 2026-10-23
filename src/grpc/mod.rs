pub mod client;
pub mod server;
pub mod status_code;

// Generated tonic APIs return tonic::Status by value.
#[allow(clippy::result_large_err)]
pub mod proto {
    tonic::include_proto!("shm");
}

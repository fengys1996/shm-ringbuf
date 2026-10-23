pub mod client;
pub mod server;
pub mod status_code;

pub mod proto {
    tonic::include_proto!("shm");
}

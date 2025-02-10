pub mod client;
pub mod server;

pub mod proto {
    tonic::include_proto!("shm");
}

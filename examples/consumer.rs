use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use shm_ringbuf::consumer::decode::ToStringDecoder;
use shm_ringbuf::consumer::settings::{ConsumerSettings, SettingsBuilder};
use shm_ringbuf::consumer::RingbufConsumer;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let settings = consumer_settings();
    let decoder = ToStringDecoder;
    let mut item_recv = RingbufConsumer::start_consume(settings, decoder).await;

    while let Some(item) = item_recv.recv().await {
        info!("{:?}", item);
    }
}

fn consumer_settings() -> ConsumerSettings {
    let grpc_sock_path = PathBuf::from_str("/tmp/ctl.sock").unwrap();
    let fdpass_sock_path = PathBuf::from_str("/tmp/fd.sock").unwrap();
    let process_interval = Duration::from_millis(10);

    SettingsBuilder::new()
        .grpc_sock_path(grpc_sock_path)
        .fdpass_sock_path(fdpass_sock_path)
        .process_interval(process_interval)
        .ringbuf_expire(Duration::from_secs(10))
        .ringbuf_expire_check_interval(Duration::from_secs(3))
        .build()
}

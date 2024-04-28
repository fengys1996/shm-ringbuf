use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use shm_ringbuf::consumer::decode::ToStringDecoder;
use shm_ringbuf::consumer::ConsumerSettings;
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
    let control_sock_path = PathBuf::from_str("/tmp/ctl.sock").unwrap();
    let sendfd_sock_path = PathBuf::from_str("/tmp/fd.sock").unwrap();
    let process_duration = Duration::from_millis(10);

    let _ = fs::remove_file(&control_sock_path);
    let _ = fs::remove_file(&sendfd_sock_path);

    ConsumerSettings {
        control_sock_path: control_sock_path.clone(),
        sendfd_sock_path: sendfd_sock_path.clone(),
        process_duration,
        ringbuf_expire: Duration::from_secs(10),
        ringbuf_check_interval: Duration::from_secs(3),
    }
}

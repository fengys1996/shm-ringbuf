use std::time::Duration;

use shm_ringbuf::consumer::decode::ToStringDecoder;
use shm_ringbuf::consumer::settings::ConsumerSettingsBuilder;
use shm_ringbuf::consumer::RingbufConsumer;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let settings = ConsumerSettingsBuilder::new()
        .grpc_sock_path("/tmp/ctl.sock")
        .fdpass_sock_path("/tmp/fd.sock")
        .process_interval(Duration::from_millis(10))
        .ringbuf_expire(Duration::from_secs(10))
        .ringbuf_expire_check_interval(Duration::from_secs(3))
        .build();

    let decoder = ToStringDecoder;
    let mut item_recv = RingbufConsumer::start_consume(settings, decoder).await;

    while let Some(item) = item_recv.recv().await {
        info!("{:?}", item);
    }
}

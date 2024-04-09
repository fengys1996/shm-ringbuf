use std::time::Duration;
use std::usize;

use shm_ringbuf::producer::prealloc::PreAlloc;
use shm_ringbuf::producer::ProducerSettings;
use shm_ringbuf::producer::RingbufProducer;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let control_sock_path = "/tmp/1.txt";
    let sendfd_sock_path = "/tmp/2.txt";

    let settings = ProducerSettings {
        control_sock_file_path: control_sock_path.to_string(),
        sendfd_sock_path: sendfd_sock_path.to_string(),
        size_of_ringbuf: 1024 * 20,
        heartbeat_interval_second: 1,
    };

    let producer = RingbufProducer::connect_lazy(settings).await.unwrap();

    for i in 1..10000 {
        let mut pre_alloc = producer.reserve(20).unwrap();

        let write_str = format!("hello, {}", i);
        info!("write: {}", write_str);

        wait_consumer_online(&pre_alloc, 20 * 5, Duration::from_secs(3))
            .await
            .unwrap();

        pre_alloc.write(write_str.as_bytes()).unwrap();

        pre_alloc.commit_and_notify(100).await;

        if i % 50 == 0 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn wait_consumer_online(
    pre_alloc: &PreAlloc,
    retry_num: usize,
    retry_interval: Duration,
) -> Result<(), String> {
    for _ in 0..retry_num {
        if pre_alloc.online() {
            return Ok(());
        }
        tokio::time::sleep(retry_interval).await;
    }

    Err("wait consumer online timeout".to_string())
}

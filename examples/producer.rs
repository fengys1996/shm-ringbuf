use std::time::Duration;

use shm_ringbuf::error;
use shm_ringbuf::producer::prealloc::PreAlloc;
use shm_ringbuf::producer::settings::SettingsBuilder;
use shm_ringbuf::producer::RingbufProducer;
use tokio::time::sleep;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let settings = SettingsBuilder::default()
        .fdpass_sock_path("/tmp/fd.sock")
        .ringbuf_len(32 * 1024 * 1024)
        .enable_notify(false)
        .build();

    let producer = RingbufProducer::connect(settings).await.unwrap();

    let start = std::time::Instant::now();
    for i in 0..10000 {
        let mut pre_alloc =
            reserve_with_retry(&producer, 20, 3, Duration::from_secs(1))
                .await
                .unwrap();

        let write_str = format!("hello, {}", i);

        pre_alloc.write(write_str.as_bytes()).unwrap();

        pre_alloc.commit();
    }
    info!("consume time: {:?}", start.elapsed());
}

async fn reserve_with_retry(
    producer: &RingbufProducer,
    size: usize,
    retry_num: usize,
    retry_interval: Duration,
) -> Result<PreAlloc, String> {
    for _ in 0..retry_num {
        let err = match producer.reserve(size) {
            Ok(pre) => return Ok(pre),
            Err(e) => e,
        };

        if !matches!(err, error::Error::NotEnoughSpace { .. }) {
            break;
        }

        info!("reserve failed, retry: {}, error: {:?}", size, err);
        sleep(retry_interval).await;
    }

    Err("reserve failed".to_string())
}

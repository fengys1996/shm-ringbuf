use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use shm_ringbuf::error;
use shm_ringbuf::producer::prealloc::PreAlloc;
use shm_ringbuf::producer::ProducerSettings;
use shm_ringbuf::producer::RingbufProducer;
use tokio::time::sleep;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let settings = producer_settings();
    let producer = RingbufProducer::connect(settings).await.unwrap();

    for i in 0..10000 {
        let mut pre_alloc =
            reserve_with_retry(&producer, 20, 3, Duration::from_secs(1))
                .await
                .unwrap();

        let write_str = format!("hello, {}", i);
        info!("write: {}", write_str);

        pre_alloc.write(write_str.as_bytes()).unwrap();

        pre_alloc.commit();

        if i % 100 == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    }
}

fn producer_settings() -> ProducerSettings {
    let sendfd_sock_path = PathBuf::from_str("/tmp/fd.sock").unwrap();
    let size_of_ringbuf = 1024 * 32;

    ProducerSettings {
        fdpass_sock_path: sendfd_sock_path,
        ringbuf_len: size_of_ringbuf,
        enable_notify: false,
    }
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

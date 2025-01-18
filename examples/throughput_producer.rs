use std::time::Duration;

use shm_ringbuf::error;
use shm_ringbuf::producer::prealloc::PreAlloc;
use shm_ringbuf::producer::settings::ProducerSettingsBuilder;
use shm_ringbuf::producer::RingbufProducer;
use tokio::time::sleep;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let settings = ProducerSettingsBuilder::new()
        .grpc_sock_path("/tmp/ctl.sock")
        .fdpass_sock_path("/tmp/fd.sock")
        .ringbuf_len(1024 * 1024)
        .enable_result_fetch(true)
        .heartbeat_interval(Duration::from_secs(1))
        .build();

    let producer = RingbufProducer::new(settings).await.unwrap();

    wait_consumer_online(&producer, 20 * 5, Duration::from_secs(3))
        .await
        .unwrap();

    let mut total_bytes = 0;
    let start = std::time::Instant::now();
    // mock 1KB write
    let write_bytes = vec![0u8; 1024];
    for _i in 0..10000 {
        total_bytes += write_bytes.len();

        let mut pre_alloc = reserve_with_retry(
            &producer,
            write_bytes.len(),
            3,
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        pre_alloc.write(&write_bytes).unwrap();

        pre_alloc.commit();

        //producer.notify_consumer(None).await;
        producer.notify_consumer(Some(1024 * 512)).await;
    }
    let elapsed = start.elapsed().as_millis();
    info!("elapsed: {} ms", elapsed);
    info!("total bytes: {}", total_bytes);
    info!(
        "throughput: {} MB/s",
        total_bytes as f64 / 1024.0 / 1024.0 / elapsed as f64 * 1000.0
    );
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

async fn wait_consumer_online(
    p: &RingbufProducer,
    retry_num: usize,
    retry_interval: Duration,
) -> Result<(), String> {
    for _ in 0..retry_num {
        if p.server_online() && p.result_fetch_normal() {
            return Ok(());
        }

        info!("wait consumer online or wait fetcher normal");
        sleep(retry_interval).await;
    }

    Err("wait consumer online timeout".to_string())
}

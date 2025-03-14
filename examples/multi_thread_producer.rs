use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use shm_ringbuf::error;
use shm_ringbuf::producer::prealloc::PreAlloc;
use shm_ringbuf::producer::settings::ProducerSettingsBuilder;
use shm_ringbuf::producer::RingbufProducer;
use tokio::time::sleep;
use tracing::info;

#[tokio::main]
async fn main() {
    // 1. Initialize log.
    tracing_subscriber::fmt::init();

    let settings = ProducerSettingsBuilder::new()
        .grpc_sock_path("/tmp/ctl.sock")
        .fdpass_sock_path("/tmp/fd.sock")
        .ringbuf_len(1024 * 1024)
        .heartbeat_interval(Duration::from_secs(1))
        .build();

    let producer = RingbufProducer::new(settings).await.unwrap();
    let producer = Arc::new(producer);

    let mut join_handles = Vec::with_capacity(4);

    for i in 0..4 {
        let producer = producer.clone();
        let join_handle = tokio::spawn(async move {
            write_msg(producer, i * 250..(i + 1) * 250).await;
        });
        join_handles.push(join_handle);
    }

    for join_handle in join_handles {
        join_handle.await.unwrap();
    }
}

async fn write_msg(producer: Arc<RingbufProducer>, range: Range<usize>) {
    for i in range {
        let mut pre_alloc =
            reserve_with_retry(&producer, 20, 3, Duration::from_secs(1))
                .await
                .unwrap();

        let write_str = format!("hello, {}", i);
        info!("write: {}", write_str);

        wait_consumer_online(&producer, 20 * 5, Duration::from_secs(3))
            .await
            .unwrap();

        pre_alloc.write(write_str.as_bytes()).unwrap();

        pre_alloc.commit();

        if i % 100 == 0 {
            sleep(Duration::from_millis(10)).await;
        }
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

async fn wait_consumer_online(
    p: &RingbufProducer,
    retry_num: usize,
    retry_interval: Duration,
) -> Result<(), String> {
    for _ in 0..retry_num {
        if p.server_online() && p.result_fetch_normal() {
            info!("consumer online and result fetcher normal");
            return Ok(());
        }

        info!("wait consumer online or wait result fetcher normal");
        sleep(retry_interval).await;
    }

    Err("wait consumer online timeout".to_string())
}

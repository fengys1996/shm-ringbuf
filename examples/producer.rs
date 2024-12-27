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
        .heartbeat_interval(Duration::from_secs(1))
        .build();

    let producer = RingbufProducer::new(settings).await.unwrap();

    let mut joins = Vec::with_capacity(100);
    for i in 0..10000 {
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

        let join = pre_alloc.wait_result();

        joins.push(join);

        if i % 100 == 0 {
            for j in joins.drain(..) {
                let _ = j.await;
            }
        }

        if i % 10 == 0 {
            sleep(Duration::from_millis(1000)).await;
        }
    }

    for j in joins {
        let _ = j.await;
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

        info!("wait consumer online or wait fetcher normal");
        sleep(retry_interval).await;
    }

    Err("wait consumer online timeout".to_string())
}

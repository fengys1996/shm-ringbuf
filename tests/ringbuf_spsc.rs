use std::fs;
use std::time::Duration;

use shm_ringbuf::consumer::decode::ToStringDecoder;
use shm_ringbuf::consumer::settings::SettingsBuilder;
use shm_ringbuf::consumer::RingbufConsumer;
use shm_ringbuf::error;
use shm_ringbuf::producer::prealloc::PreAlloc;
use shm_ringbuf::producer::ProducerSettings;
use shm_ringbuf::producer::RingbufProducer;
use tokio::time::sleep;

#[tokio::test]
async fn test_ringbuf_spsc() {
    tracing_subscriber::fmt::init();

    let dir = tempfile::tempdir().unwrap();
    let sendfd_sock_path = dir.path().join("sendfd.sock");

    let ringbuf_len = 1024 * 32;

    let settings = SettingsBuilder::default()
        .fdpass_sock_path(&sendfd_sock_path)
        .build();

    let mut recv_msgs =
        RingbufConsumer::start_consume(settings, ToStringDecoder).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let settings = ProducerSettings {
        fdpass_sock_path: sendfd_sock_path.clone(),
        ringbuf_len,
        enable_notify: false,
    };

    let producer = RingbufProducer::connect(settings).await.unwrap();

    let msg_num = 10000;
    tokio::spawn(async move {
        for i in 0..msg_num {
            let mut pre_alloc =
                reserve_with_retry(&producer, 20, 3, Duration::from_secs(1))
                    .await
                    .unwrap();

            let write_str = format!("hello, {}", i);

            pre_alloc.write(write_str.as_bytes()).unwrap();

            pre_alloc.commit_and_notify().await;
        }
    });

    for i in 0..msg_num {
        let item = recv_msgs.recv().await.unwrap();
        assert_eq!(item.unwrap(), format!("hello, {}", i));
    }

    let _ = fs::remove_file(sendfd_sock_path);
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
        sleep(retry_interval).await;
    }

    Err("reserve failed".to_string())
}

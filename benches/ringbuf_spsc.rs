use std::time::Duration;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use shm_ringbuf::consumer::decode::ToStringDecoder;
use shm_ringbuf::consumer::ConsumerSettings;
use shm_ringbuf::consumer::RingbufConsumer;
use shm_ringbuf::error;
use shm_ringbuf::producer::prealloc::PreAlloc;
use shm_ringbuf::producer::ProducerSettings;
use shm_ringbuf::producer::RingbufProducer;
use tokio::sync::mpsc::Receiver;
use tokio::time::sleep;

async fn prepare() -> (Receiver<String>, RingbufProducer) {
    let dir = tempfile::tempdir().unwrap();
    let control_sock_path = dir.path().join("control.sock");
    let sendfd_sock_path = dir.path().join("sendfd.sock");

    let size_of_ringbuf = 1024 * 20;

    let settings = ConsumerSettings {
        control_sock_path: control_sock_path.clone(),
        sendfd_sock_path: sendfd_sock_path.clone(),
        size_of_ringbuf,
        process_duration: Duration::from_millis(10),
    };

    let recv_msgs =
        RingbufConsumer::start_consume(settings, ToStringDecoder).await;

    let settings = ProducerSettings {
        control_sock_path: control_sock_path.clone(),
        sendfd_sock_path: sendfd_sock_path.clone(),
        size_of_ringbuf,
        heartbeat_interval_second: 1,
    };

    let producer = RingbufProducer::connect_lazy(settings).await.unwrap();

    // wait for consumer online
    sleep(Duration::from_secs(3)).await;

    (recv_msgs, producer)
}

async fn bench_ringbuf_spsc(
    recv_msgs: &mut Receiver<String>,
    producer: RingbufProducer,
) {
    tokio::spawn(async move {
        for i in 1..10000 {
            let mut pre_alloc =
                reserve_with_retry(&producer, 20, 3, Duration::from_secs(1))
                    .await
                    .unwrap();

            let write_str = format!("hello, {}", i);

            wait_consumer_online(&pre_alloc, 5, Duration::from_secs(3))
                .await
                .unwrap();

            pre_alloc.write(write_str.as_bytes()).unwrap();

            pre_alloc.commit_and_notify(100).await;
        }
    });

    for i in 1..10000 {
        let item = recv_msgs.recv().await.unwrap();
        assert_eq!(item, format!("hello, {}", i));
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let (mut recv_msgs, producer) = runtime.block_on(prepare());

    c.bench_function("spsc", |b| {
        b.iter(|| {
            runtime
                .block_on(bench_ringbuf_spsc(&mut recv_msgs, producer.clone()))
        })
    });
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

async fn wait_consumer_online(
    pre_alloc: &PreAlloc,
    retry_num: usize,
    retry_interval: Duration,
) -> Result<(), String> {
    for _ in 0..retry_num {
        if pre_alloc.online() {
            return Ok(());
        }
        sleep(retry_interval).await;
    }

    Err("wait consumer online timeout".to_string())
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{msg_num, reserve_with_retry, wait_consumer_online, MsgForward};
use shm_ringbuf::consumer::settings::ConsumerSettingsBuilder;
use shm_ringbuf::consumer::RingbufConsumer;
use shm_ringbuf::producer::settings::ProducerSettingsBuilder;
use shm_ringbuf::producer::RingbufProducer;

#[tokio::test]
async fn test_ringbuf_spsc_base() {
    tracing_subscriber::fmt::init();

    let (send_msgs, mut recv_msgs) = tokio::sync::mpsc::channel(100);

    let dir = tempfile::tempdir().unwrap();
    let grpc_sock_path = dir.path().join("control.sock");
    let fdpass_sock_path = dir.path().join("sendfd.sock");

    let settings = ConsumerSettingsBuilder::new()
        .grpc_sock_path(grpc_sock_path.clone())
        .fdpass_sock_path(fdpass_sock_path.clone())
        .process_interval(Duration::from_millis(10))
        .build();

    tokio::spawn(async move {
        let string_print = MsgForward { sender: send_msgs };
        RingbufConsumer::new(settings).run(string_print).await;
    });

    // wait for the consumer to start.
    tokio::time::sleep(Duration::from_millis(10)).await;

    let settings = ProducerSettingsBuilder::new()
        .grpc_sock_path(grpc_sock_path.clone())
        .fdpass_sock_path(fdpass_sock_path.clone())
        .build();

    let producer =
        Arc::new(RingbufProducer::connect_lazy(settings).await.unwrap());

    let msg_num = msg_num();

    tokio::spawn(async move {
        for i in 0..msg_num {
            let mut pre_alloc =
                reserve_with_retry(&producer, 20, 3, Duration::from_secs(1))
                    .await
                    .unwrap();

            let write_str = format!("hello, {}", i);

            wait_consumer_online(&producer, 5, Duration::from_secs(3))
                .await
                .unwrap();

            pre_alloc.write(write_str.as_bytes()).unwrap();

            pre_alloc.commit();
        }
    });

    for i in 0..msg_num {
        let msg = format!("hello, {}", i);
        assert_eq!(recv_msgs.recv().await.unwrap(), msg);
    }
}

#[tokio::test]
async fn test_ringbuf_spsc_with_notify() {
    tracing_subscriber::fmt::init();

    let (send_msgs, mut recv_msgs) = tokio::sync::mpsc::channel(100);

    let dir = tempfile::tempdir().unwrap();
    let grpc_sock_path = dir.path().join("control.sock");
    let fdpass_sock_path = dir.path().join("sendfd.sock");

    let settings = ConsumerSettingsBuilder::new()
        .grpc_sock_path(grpc_sock_path.clone())
        .fdpass_sock_path(fdpass_sock_path.clone())
        .process_interval(Duration::from_millis(10))
        // Set too long interval for testing notify.
        .process_interval(Duration::from_millis(1000))
        .build();

    tokio::spawn(async move {
        let string_print = MsgForward { sender: send_msgs };
        RingbufConsumer::new(settings).run(string_print).await;
    });

    // Wait for the consumer to start.
    tokio::time::sleep(Duration::from_millis(10)).await;

    let settings = ProducerSettingsBuilder::new()
        .grpc_sock_path(grpc_sock_path.clone())
        .fdpass_sock_path(fdpass_sock_path.clone())
        .build();

    let producer =
        Arc::new(RingbufProducer::connect_lazy(settings).await.unwrap());

    let msg_num = msg_num();

    tokio::spawn(async move {
        for i in 0..msg_num {
            let mut pre_alloc =
                reserve_with_retry(&producer, 20, 3, Duration::from_secs(1))
                    .await
                    .unwrap();

            let write_str = format!("hello, {}", i);

            wait_consumer_online(&producer, 5, Duration::from_secs(3))
                .await
                .unwrap();

            pre_alloc.write(write_str.as_bytes()).unwrap();

            pre_alloc.commit();

            producer.notify_consumer(Some(1000)).await;
        }
    });

    for i in 0..msg_num {
        let msg = format!("hello, {}", i);
        assert_eq!(recv_msgs.recv().await.unwrap(), msg);
    }
}

#[tokio::test]
async fn test_ringbuf_spsc_with_wait_result() {
    tracing_subscriber::fmt::init();

    let (send_msgs, mut recv_msgs) = tokio::sync::mpsc::channel(100);

    let dir = tempfile::tempdir().unwrap();
    let grpc_sock_path = dir.path().join("control.sock");
    let fdpass_sock_path = dir.path().join("sendfd.sock");

    let settings = ConsumerSettingsBuilder::new()
        .grpc_sock_path(grpc_sock_path.clone())
        .fdpass_sock_path(fdpass_sock_path.clone())
        .process_interval(Duration::from_millis(10))
        .build();

    tokio::spawn(async move {
        let string_print = MsgForward { sender: send_msgs };
        RingbufConsumer::new(settings).run(string_print).await;
    });

    // Wait for the consumer to start.
    tokio::time::sleep(Duration::from_millis(10)).await;

    let settings = ProducerSettingsBuilder::new()
        .grpc_sock_path(grpc_sock_path.clone())
        .fdpass_sock_path(fdpass_sock_path.clone())
        .build();

    let producer =
        Arc::new(RingbufProducer::connect_lazy(settings).await.unwrap());

    let msg_num = msg_num();

    tokio::spawn(async move {
        let mut joins = Vec::with_capacity(100);
        for i in 0..msg_num {
            let mut pre_alloc =
                reserve_with_retry(&producer, 20, 3, Duration::from_secs(1))
                    .await
                    .unwrap();

            let write_str = format!("hello, {}", i);

            wait_consumer_online(&producer, 5, Duration::from_secs(3))
                .await
                .unwrap();

            pre_alloc.write(write_str.as_bytes()).unwrap();

            pre_alloc.commit();

            let join = pre_alloc.wait_result();

            joins.push(join);

            // Wait the result every 1000 messages.
            if i % 1000 == 0 {
                for join in joins.drain(..) {
                    let result = join.await.unwrap();
                    assert_eq!(result.status_code, 0);
                }
            }
            if i == msg_num - 1 {
                for join in joins.drain(..) {
                    let result = join.await.unwrap();
                    assert_eq!(result.status_code, 0);
                }
            }
        }
    });

    for i in 0..msg_num {
        let msg = format!("hello, {}", i);
        assert_eq!(recv_msgs.recv().await.unwrap(), msg);
    }
}

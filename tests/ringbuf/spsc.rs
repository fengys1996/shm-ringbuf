use std::{sync::Arc, time::Duration};

use crate::common::{
    msg_num, start_producer, MsgForward, StartProducerOptions,
};
use shm_ringbuf::{
    consumer::{settings::ConsumerSettingsBuilder, RingbufConsumer},
    producer::{settings::ProducerSettingsBuilder, RingbufProducer},
};
use tokio::sync::mpsc;

#[tokio::test]
async fn test_ringbuf_spsc_base() {
    do_test_ringbuf_spsc(false, Duration::from_millis(10), None, 0, 1000).await;
}

#[tokio::test]
async fn test_ringbuf_spsc_with_notify() {
    // Set too long interval for testing notify.
    do_test_ringbuf_spsc(false, Duration::from_secs(100), Some(500), 501, 1000)
        .await;
}

#[tokio::test]
async fn test_ringbuf_spsc_with_wait_result() {
    do_test_ringbuf_spsc(true, Duration::from_millis(10), None, 0, 1000).await;
}

#[tokio::test]
async fn test_ringbuf_spsc_with_wait_result_and_notify() {
    // Set too long interval for testing notify.
    do_test_ringbuf_spsc(true, Duration::from_secs(100), Some(500), 501, 1000)
        .await;
}

async fn do_test_ringbuf_spsc(
    wait_result: bool,
    process_interval: Duration,
    notify_threshold: Option<u32>,
    min_msg_len: usize,
    max_msg_len: usize,
) {
    tracing_subscriber::fmt::init();

    let (send_msgs, mut recv_msgs) = mpsc::channel(100);

    let (expected_send, mut expected_recv) = mpsc::channel(100);

    let dir = tempfile::tempdir().unwrap();
    let grpc_sock_path = dir.path().join("control.sock");
    let fdpass_sock_path = dir.path().join("sendfd.sock");

    let settings = ConsumerSettingsBuilder::new()
        .grpc_sock_path(grpc_sock_path.clone())
        .fdpass_sock_path(fdpass_sock_path.clone())
        .process_interval(process_interval)
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

    let msg_num = msg_num();

    let producer = Arc::new(RingbufProducer::new(settings).await.unwrap());

    let options = StartProducerOptions {
        producer,
        msg_num,
        expected_send,
        wait_result,
        min_msg_len,
        max_msg_len,
        notify_threshold,
        msg_prefix: None,
    };

    start_producer(options).await;

    for _ in 0..msg_num {
        let expected = expected_recv.recv().await.unwrap();
        let actual = recv_msgs.recv().await.unwrap();

        assert_eq!(expected, actual);
    }
}

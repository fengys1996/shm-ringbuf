use std::{sync::Arc, time::Duration};

use crate::common::{
    msg_num, start_producer, MsgForward, StartProducerOptions,
};
use shm_ringbuf::{
    consumer::{settings::ConsumerSettingsBuilder, RingbufConsumer},
    error::SUCCESS,
    producer::{settings::ProducerSettingsBuilder, RingbufProducer},
};
use tokio::sync::mpsc;

#[tokio::test]
async fn test_ringbuf_mpsc_base() {
    do_test_ringbuf_mpsc(
        false,
        Duration::from_millis(10),
        None,
        0,
        1000,
        false,
    )
    .await;
}

#[tokio::test]
async fn test_ringbuf_mpsc_base_with_checksum() {
    do_test_ringbuf_mpsc(false, Duration::from_millis(10), None, 0, 1000, true)
        .await;
}

#[tokio::test]
async fn test_ringbuf_mpsc_with_notify() {
    // Set too long interval for testing notify.
    do_test_ringbuf_mpsc(
        false,
        Duration::from_secs(100),
        Some(500),
        501,
        1000,
        false,
    )
    .await;
}

#[tokio::test]
async fn test_ringbuf_mpsc_with_notify_with_checksum() {
    // Set too long interval for testing notify.
    do_test_ringbuf_mpsc(
        false,
        Duration::from_secs(100),
        Some(500),
        501,
        1000,
        true,
    )
    .await;
}

#[tokio::test]
async fn test_ringbuf_mpsc_with_wait_result() {
    do_test_ringbuf_mpsc(true, Duration::from_millis(10), None, 0, 1000, false)
        .await;
}

#[tokio::test]
async fn test_ringbuf_mpsc_with_wait_result_with_checksum() {
    do_test_ringbuf_mpsc(true, Duration::from_millis(10), None, 0, 1000, true)
        .await;
}

#[tokio::test]
async fn test_ringbuf_mpsc_with_wait_result_and_notify() {
    // Set too long interval for testing notify.
    do_test_ringbuf_mpsc(
        true,
        Duration::from_secs(100),
        Some(500),
        501,
        1000,
        false,
    )
    .await;
}

#[tokio::test]
async fn test_ringbuf_mpsc_with_wait_result_and_notify_with_checksum() {
    // Set too long interval for testing notify.
    do_test_ringbuf_mpsc(
        true,
        Duration::from_secs(100),
        Some(500),
        501,
        1000,
        true,
    )
    .await;
}

async fn do_test_ringbuf_mpsc(
    wait_result: bool,
    process_interval: Duration,
    notify_threshold: Option<u32>,
    min_msg_len: usize,
    max_msg_len: usize,
    enable_checksum: bool,
) {
    tracing_subscriber::fmt::init();

    let (send_msgs, mut recv_msgs) = mpsc::channel(100);

    let num_producers = 4;

    let mut expected_sends = Vec::with_capacity(4);
    let mut expected_recvs = Vec::with_capacity(4);

    for _ in 0..num_producers {
        let (send, recv) = mpsc::channel(100);
        expected_sends.push(send);
        expected_recvs.push(recv);
    }

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
        .enable_checksum(enable_checksum)
        .build();

    let msg_num = msg_num();

    let producer = Arc::new(RingbufProducer::new(settings).await.unwrap());

    for (i, expected_send) in expected_sends.into_iter().enumerate() {
        let options = StartProducerOptions {
            producer: producer.clone(),
            msg_num,
            expected_send,
            wait_result,
            min_msg_len,
            max_msg_len,
            notify_threshold,
            msg_prefix: Some(format!("{}-", i)),
            write_delay: None,
            expected_status_code: SUCCESS,
        };

        start_producer(options).await;
    }

    for _ in 0..num_producers * msg_num {
        let actual = recv_msgs.recv().await.unwrap();
        let index = actual.split('-').next().unwrap().parse::<usize>().unwrap();
        let expected = expected_recvs[index].recv().await.unwrap();

        assert_eq!(expected, actual);
    }
}

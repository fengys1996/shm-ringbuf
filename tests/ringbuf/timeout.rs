use std::{sync::Arc, time::Duration};

use shm_ringbuf::{
    consumer::{RingbufConsumer, settings::ConsumerSettingsBuilder},
    error::TIMEOUT,
    producer::{RingbufProducer, settings::ProducerSettingsBuilder},
};
use tokio::sync::mpsc;

use crate::common::{MsgForward, StartProducerOptions, start_producer};

#[tokio::test]
async fn test_subscription_ttl() {
    tracing_subscriber::fmt::init();

    let (send_msgs, mut recv_msgs) = mpsc::channel(100);

    let (expected_send, _recv) = mpsc::channel(100);

    let dir = tempfile::tempdir().unwrap();
    let grpc_sock_path = dir.path().join("control.sock");
    let fdpass_sock_path = dir.path().join("sendfd.sock");

    let settings = ConsumerSettingsBuilder::new()
        .grpc_sock_path(grpc_sock_path.clone())
        .fdpass_sock_path(fdpass_sock_path.clone())
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
        .expired_check_interval(Duration::from_millis(50))
        .subscription_ttl(Duration::from_millis(100))
        .build();

    let msg_num = 3;

    let producer = Arc::new(RingbufProducer::new(settings).await.unwrap());

    let options = StartProducerOptions {
        producer,
        msg_num,
        expected_send,
        wait_result: true,
        min_msg_len: 0,
        max_msg_len: 20,
        notify_threshold: None,
        msg_prefix: None,
        write_delay: Some(Duration::from_millis(500)),
        expected_status_code: TIMEOUT,
    };

    start_producer(options).await;

    for _ in 0..msg_num {
        let _ = recv_msgs.recv().await.unwrap();
    }
}

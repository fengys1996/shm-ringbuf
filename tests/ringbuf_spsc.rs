mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{
    gen_str, msg_num, reserve_with_retry, wait_consumer_online, MsgForward,
};
use shm_ringbuf::consumer::settings::ConsumerSettingsBuilder;
use shm_ringbuf::consumer::RingbufConsumer;
use shm_ringbuf::producer::settings::ProducerSettingsBuilder;
use shm_ringbuf::producer::RingbufProducer;

#[tokio::test]
async fn test_ringbuf_spsc_base() {
    do_test_ringbuf_spsc(false, Duration::from_millis(10), None, 0, 1000).await;
}

#[tokio::test]
async fn test_ringbuf_spsc_with_notify() {
    // Set too long interval for testing notify.
    do_test_ringbuf_spsc(false, Duration::from_secs(100), Some(500), 499, 1000)
        .await;
}

#[tokio::test]
async fn test_ringbuf_spsc_with_wait_result() {
    do_test_ringbuf_spsc(true, Duration::from_millis(10), None, 0, 1000).await;
}

#[tokio::test]
async fn test_ringbuf_spsc_with_wait_result_and_notify() {
    // Set too long interval for testing notify.
    do_test_ringbuf_spsc(true, Duration::from_secs(100), Some(500), 499, 1000)
        .await;
}

async fn do_test_ringbuf_spsc(
    wait_result: bool,
    process_interval: Duration,
    notify_limit: Option<u32>,
    min_msg_len: usize,
    max_msg_len: usize,
) {
    tracing_subscriber::fmt::init();

    let (send_msgs, mut recv_msgs) = tokio::sync::mpsc::channel(100);

    let (expected_send, mut expected_recv) = tokio::sync::mpsc::channel(100);

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

    let producer =
        Arc::new(RingbufProducer::connect_lazy(settings).await.unwrap());

    let msg_num = msg_num();

    tokio::spawn(async move {
        let mut joins = if wait_result {
            Some(Vec::with_capacity(1000))
        } else {
            None
        };

        for i in 0..msg_num {
            let write_str = gen_str(min_msg_len, max_msg_len);

            expected_send.send(write_str.clone()).await.unwrap();

            let mut pre_alloc = reserve_with_retry(
                &producer,
                write_str.len(),
                3,
                Duration::from_secs(1),
            )
            .await
            .unwrap();

            wait_consumer_online(&producer, 5, Duration::from_secs(3))
                .await
                .unwrap();

            pre_alloc.write(write_str.as_bytes()).unwrap();

            pre_alloc.commit();

            if let Some(limit) = notify_limit {
                // If we set a longer process interval, the last batch of messages
                // may not be processed quickly, because the data accumulated in
                // the ringbuf may be too small and does not exceed the notify limit,
                // so the notification will not be triggered. Therefore, we need
                // to trigger a notification at the end.
                if i == msg_num - 1 {
                    producer.notify_consumer(None).await;
                } else {
                    producer.notify_consumer(Some(limit)).await;
                }
            }

            if let Some(joins) = &mut joins {
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
        }
    });

    for _i in 0..msg_num {
        let expected = expected_recv.recv().await.unwrap();
        let actual = recv_msgs.recv().await.unwrap();

        assert_eq!(expected, actual);
    }
}

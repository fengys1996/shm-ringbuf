use std::str::from_utf8;
use std::sync::Arc;
use std::time::Duration;

use shm_ringbuf::consumer::process::DataProcess;
use shm_ringbuf::consumer::settings::ConsumerSettingsBuilder;
use shm_ringbuf::consumer::RingbufConsumer;
use shm_ringbuf::error::DataProcessResult;
use shm_ringbuf::error::{self};
use shm_ringbuf::producer::prealloc::PreAlloc;
use shm_ringbuf::producer::settings::ProducerSettingsBuilder;
use shm_ringbuf::producer::RingbufProducer;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;

#[tokio::test]
async fn test_ringbuf_spsc() {
    tracing_subscriber::fmt::init();

    let (send_msgs, mut recv_msgs) = tokio::sync::mpsc::channel(100);

    let dir = tempfile::tempdir().unwrap();
    let grpc_sock_path = dir.path().join("control.sock");
    let fdpass_sock_path = dir.path().join("sendfd.sock");

    let settings = ConsumerSettingsBuilder::new()
        .grpc_sock_path(grpc_sock_path.clone())
        .fdpass_sock_path(fdpass_sock_path.clone())
        .process_interval(Duration::from_millis(10))
        .ringbuf_expire(Duration::from_secs(10))
        .ringbuf_expire_check_interval(Duration::from_secs(3))
        .build();

    tokio::spawn(async move {
        let string_print = StringPrint { sender: send_msgs };
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

    let msg_num = 100;
    let mut joins = Vec::with_capacity(msg_num);

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

        joins.push(pre_alloc.wait_result());
    }
    for j in joins {
        let _ = j.await;
    }

    for i in 0..msg_num {
        let msg = format!("hello, {}", i);
        assert_eq!(recv_msgs.recv().await.unwrap(), msg);
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

        sleep(retry_interval).await;
    }

    Err("wait consumer online timeout".to_string())
}

pub struct StringPrint {
    sender: Sender<String>,
}

impl DataProcess for StringPrint {
    type Error = Error;

    async fn process(&self, data: &[u8]) -> Result<(), Self::Error> {
        let msg = from_utf8(data).map_err(|_| Error::DecodeError)?;

        let _ = self.sender.send(msg.to_string()).await;

        Ok(())
    }
}

#[derive(Debug)]
pub enum Error {
    DecodeError,
    ProcessError,
}

impl Error {
    pub fn status_code(&self) -> u32 {
        match self {
            Error::DecodeError => 1001,
            Error::ProcessError => 1002,
        }
    }

    pub fn message(&self) -> String {
        match self {
            Error::DecodeError => "decode error".to_string(),
            Error::ProcessError => "process error".to_string(),
        }
    }
}

impl From<Error> for DataProcessResult {
    fn from(err: Error) -> DataProcessResult {
        DataProcessResult {
            status_code: err.status_code(),
            message: err.message(),
        }
    }
}

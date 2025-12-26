use shm_ringbuf::{
    consumer::process::{DataProcess, ResultSender},
    error::DataProcessResult,
    producer::{RingbufProducer, prealloc::PreAlloc},
};
use std::fmt::{Display, Formatter};
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc::Sender, time::sleep};
use tracing::{error, warn};

pub struct MsgForward {
    pub sender: Sender<String>,
}

impl DataProcess for MsgForward {
    type Message = String;
    type Error = Error;

    fn decode(&self, data: &[u8]) -> Result<Self::Message, Self::Error> {
        String::from_utf8(data.to_vec()).map_err(|_| Error::DecodeError)
    }

    async fn process(&self, msg: Self::Message, result_sender: ResultSender) {
        if let Err(e) = self.do_process(&msg).await {
            result_sender.push_result(e).await;
        } else {
            result_sender.push_ok().await;
        }
    }
}

impl MsgForward {
    async fn do_process(&self, msg: &str) -> Result<(), Error> {
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

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl std::error::Error for Error {}

pub fn msg_num() -> usize {
    std::env::var("MSG_NUM")
        .unwrap_or_else(|_| "10000".to_string())
        .parse()
        .unwrap()
}

pub async fn reserve_with_retry(
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

        if !matches!(err, shm_ringbuf::error::Error::NotEnoughSpace { .. }) {
            error!("reserve failed, retry: {}, error: {:?}, break", size, err);
            break;
        }

        warn!("reserve failed, retry: {}, error: {:?}, retry", size, err);

        sleep(retry_interval).await;
    }

    Err("reserve failed".to_string())
}

pub async fn wait_consumer_online(
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

pub fn gen_str(min_len: usize, max_len: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789)(*&^%$#@!~";

    let len = rand::random::<u32>() as usize % (max_len - min_len) + min_len;

    let mut s = String::new();

    for _ in 0..len {
        s.push(CHARSET[rand::random::<u32>() as usize % CHARSET.len()] as char);
    }

    s
}

pub struct StartProducerOptions {
    pub producer: Arc<RingbufProducer>,
    pub msg_num: usize,
    pub expected_send: Sender<String>,
    pub wait_result: bool,
    pub min_msg_len: usize,
    pub max_msg_len: usize,
    pub notify_threshold: Option<u32>,
    pub msg_prefix: Option<String>,
    pub write_delay: Option<Duration>,
    pub expected_status_code: u32,
}

pub async fn start_producer(options: StartProducerOptions) {
    let StartProducerOptions {
        producer,
        msg_num,
        expected_send,
        wait_result,
        min_msg_len,
        max_msg_len,
        notify_threshold,
        msg_prefix,
        write_delay,
        expected_status_code,
    } = options;

    tokio::spawn(async move {
        let mut joins = if wait_result {
            Some(Vec::with_capacity(1000))
        } else {
            None
        };

        for i in 0..msg_num {
            wait_consumer_online(&producer, 5, Duration::from_secs(3))
                .await
                .unwrap();

            let write_str = gen_str(min_msg_len, max_msg_len);

            let write_str = if let Some(msg_prefix) = &msg_prefix {
                format!("{}{}", msg_prefix, write_str)
            } else {
                write_str
            };

            expected_send.send(write_str.clone()).await.unwrap();

            let mut pre_alloc = reserve_with_retry(
                &producer,
                write_str.len(),
                3,
                Duration::from_secs(1),
            )
            .await
            .unwrap();

            if let Some(write_delay) = write_delay {
                sleep(write_delay).await;
            }

            pre_alloc.write(write_str.as_bytes()).unwrap();

            pre_alloc.commit();

            if let Some(threshold) = notify_threshold {
                // If we set a longer process interval, the last batch of messages
                // may not be processed quickly, because the data accumulated in
                // the ringbuf may be too small and does not exceed the notify threshold,
                // so the notification will not be triggered. Therefore, we need
                // to trigger a notification at the end.
                if i == msg_num - 1 {
                    producer.notify_consumer(None).await;
                } else {
                    producer.notify_consumer(Some(threshold)).await;
                }
            }

            if let Some(joins) = &mut joins {
                let join = pre_alloc.wait_result();

                joins.push(join);

                // Wait the result every 1000 messages.
                if i % 1000 == 0 {
                    for join in joins.drain(..) {
                        let result = join.await.unwrap();
                        assert_eq!(result.status_code, expected_status_code);
                    }
                }
                if i == msg_num - 1 {
                    for join in joins.drain(..) {
                        let result = join.await.unwrap();
                        assert_eq!(result.status_code, expected_status_code);
                    }
                }
            }
        }
    });
}

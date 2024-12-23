use std::{str::from_utf8, time::Duration};

use shm_ringbuf::{
    consumer::process::DataProcess,
    error::DataProcessResult,
    producer::{prealloc::PreAlloc, RingbufProducer},
};
use tokio::{sync::mpsc::Sender, time::sleep};
use tracing::{error, warn};

pub struct MsgForward {
    pub sender: Sender<String>,
}

impl DataProcess for MsgForward {
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

pub fn msg_num() -> usize {
    std::env::var("MSG_NUM")
        .unwrap_or_else(|_| "100000".to_string())
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

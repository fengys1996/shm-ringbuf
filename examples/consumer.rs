use std::time::Duration;

use shm_ringbuf::consumer::process::DataProcess;
use shm_ringbuf::consumer::settings::ConsumerSettingsBuilder;
use shm_ringbuf::consumer::RingbufConsumer;
use shm_ringbuf::error::DataProcessResult;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let settings = ConsumerSettingsBuilder::new()
        .grpc_sock_path("/tmp/ctl.sock")
        .fdpass_sock_path("/tmp/fd.sock")
        .process_interval(Duration::from_millis(10))
        .ringbuf_expire(Duration::from_secs(10))
        .ringbuf_expire_check_interval(Duration::from_secs(3))
        .build();

    let ringbuf = RingbufConsumer::new(settings);
    ringbuf.run(PrintStringProcessor).await;
}

pub struct PrintStringProcessor;

impl DataProcess for PrintStringProcessor {
    type Error = Error;

    fn process(&self, data: &[u8]) -> Result<(), Self::Error> {
        let print_string =
            std::str::from_utf8(data).map_err(|_| Error::DecodeError)?;

        info!("receive: {}", print_string);

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

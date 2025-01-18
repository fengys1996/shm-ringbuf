use std::time::Duration;

use shm_ringbuf::consumer::process::{DataProcess, ResultSender};
use shm_ringbuf::consumer::settings::ConsumerSettingsBuilder;
use shm_ringbuf::consumer::RingbufConsumer;
use shm_ringbuf::error::DataProcessResult;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let settings = ConsumerSettingsBuilder::new()
        .grpc_sock_path("/tmp/ctl.sock")
        .fdpass_sock_path("/tmp/fd.sock")
        .process_interval(Duration::from_millis(10))
        .build();

    RingbufConsumer::new(settings).run(StringPrint).await;
}

pub struct StringPrint;

impl DataProcess for StringPrint {
    async fn process(&self, data: &[u8], result_sender: ResultSender) {
        if let Err(e) = self.do_process(data).await {
            result_sender.push_result(e).await;
        } else {
            result_sender.push_ok().await;
        }
    }
}

impl StringPrint {
    async fn do_process(&self, _data: &[u8]) -> Result<(), Error> {
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

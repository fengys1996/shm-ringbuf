use std::ffi::NulError;
use std::num::TryFromIntError;

use snafu::Location;
use snafu::Snafu;
use tokio::sync::oneshot::error::RecvError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("IO error"))]
    Io {
        source: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid parameter, detail: {}", detail))]
    InvalidParameter {
        detail: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to mmap anonymous"))]
    MmapAnonymous {
        source: nix::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to mmap"))]
    Mmap {
        source: nix::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Not enough space, remaining: {}, expected: {}",
        remaining,
        expected
    ))]
    NotEnoughSpace {
        remaining: u32,
        expected: u32,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Exceed capacity, expected: {}, capacity: {}",
        expected,
        capacity
    ))]
    ExceedCapacity {
        expected: u32,
        capacity: u32,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("From utf8"))]
    FromUtf8 {
        source: std::string::FromUtf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create memfd, name: {}", fd_name))]
    MemFd {
        fd_name: String,
        source: nix::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to fcntl"))]
    Fcntl {
        source: nix::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Tonic error"))]
    Tonic {
        source: Box<tonic::Status>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Generic error, status code: {}, msg: {}",
        status_code,
        status_msg
    ))]
    Generic {
        status_code: u32,
        status_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("The corresponding fd was not found"))]
    NotFoundRingbuf {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Server unavailable"))]
    ServerUnavailable {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serve with incoming"))]
    ServeWithIncoming {
        source: tonic::transport::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Contain an internal 0 byte"))]
    NulZero {
        source: NulError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert int, from {} to {}", from, to))]
    TryFromInt {
        from: String,
        to: String,
        source: TryFromIntError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to recv msg from oneshot channel"))]
    Recv {
        source: RecvError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Sub overflow, {} - {}", a, b))]
    UsizeSubOverflow {
        a: usize,
        b: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Result fetch is disabled"))]
    ResultFetchDisabled {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Result fetch is not ready"))]
    ResultFetchNotReady {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Failed to build ringbuf in consumer, detail: {}",
        detail
    ))]
    RingbufBuild {
        detail: String,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct DataProcessResult {
    pub status_code: u32,
    pub message: String,
}

impl DataProcessResult {
    pub fn ok() -> Self {
        Self {
            status_code: SUCCESS,
            message: String::new(),
        }
    }
}

/// 100000 - 200000 are error codes used internally by shm-ringbuf and should
/// not be used as business codes.
pub const SUCCESS: u32 = 0;
pub const CHECKSUM_MISMATCH: u32 = 100000;
pub const DECODE_ERROR: u32 = 100001;
pub const TIMEOUT: u32 = 100002;

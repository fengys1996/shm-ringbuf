use std::ffi::NulError;
use std::num::TryFromIntError;

use snafu::Location;
use snafu::Snafu;

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

    #[snafu(display("Tonic error"))]
    Tonic {
        source: tonic::Status,
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
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct DataProcessResult {
    pub status_code: u32,
    pub message: String,
}

impl DataProcessResult {
    pub fn ok() -> Self {
        Self {
            status_code: 0,
            message: String::new(),
        }
    }
}

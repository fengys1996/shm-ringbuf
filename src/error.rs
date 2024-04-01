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

    #[snafu(display("Failed to operate on memfd, name: {}", operate_name))]
    MemFd {
        operate_name: String,
        source: memfd::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

use std::ffi::CString;
use std::fs;
use std::os::fd::FromRawFd;
use std::os::fd::IntoRawFd;

use nix::sys::memfd;
use snafu::ResultExt;

use crate::error::Result;
use crate::error::{self};

/// Settings for creating a memfd.
#[derive(Debug, Clone)]
pub struct MemfdSettings {
    /// The name of the memfd. Only used for debugging.
    pub name: String,
    /// The size of the memfd.
    pub size: u64,
}

/// Create a memfd with the given settings.
pub fn memfd_create(settings: MemfdSettings) -> Result<fs::File> {
    let MemfdSettings { name, size } = settings;

    let c_name = CString::new(name.clone()).context(error::NulZeroSnafu)?;

    let flags = memfd::MemFdCreateFlag::MFD_CLOEXEC;

    let owned_fd = memfd::memfd_create(&c_name, flags)
        .context(error::MemFdSnafu { fd_name: name })?;

    let raw_fd = owned_fd.into_raw_fd();

    let file = unsafe { fs::File::from_raw_fd(raw_fd) };
    file.set_len(size).context(error::IoSnafu)?;

    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memfd_create() {
        let settings = MemfdSettings {
            name: "memfd".to_string(),
            size: 1024,
        };

        let file = memfd_create(settings).unwrap();

        let metadata = file.metadata().unwrap();
        assert_eq!(metadata.len(), 1024);
    }
}

use std::{
    ffi::CString,
    fs,
    os::fd::{FromRawFd, IntoRawFd},
};

use nix::sys::memfd;
use snafu::ResultExt;

use crate::error::{self, Result};

/// Settings for creating a memfd.
#[derive(Debug, Clone)]
pub struct MemfdSettings<'a> {
    /// The name of the memfd. Only used for debugging.
    pub name: &'a str,
    /// The size of the memfd.
    pub size: u64,
}

/// Create a memfd with the given settings.
pub fn memfd_create(settings: MemfdSettings) -> Result<fs::File> {
    let MemfdSettings { name, size } = settings;

    let c_name = CString::new(name.to_string()).context(error::NulZeroSnafu)?;

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
            name: "memfd",
            size: 1024,
        };

        let file = memfd_create(settings).unwrap();

        let metadata = file.metadata().unwrap();
        assert_eq!(metadata.len(), 1024);
    }
}

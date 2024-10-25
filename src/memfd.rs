use std::fs;
use std::os::fd::FromRawFd;
use std::os::fd::IntoRawFd;

#[cfg(not(target_os = "macos"))]
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
    #[cfg(not(target_os = "macos"))]
    {
        use std::ffi::CString;
        let MemfdSettings { name, size } = settings;

        let c_name = CString::new(name.clone()).context(error::NulZeroSnafu)?;
        // TODO: add sealing support. This can prevent some permission issues,
        // such as a user without write permissions writing to the ringbuf.
        let flags = memfd::MemFdCreateFlag::MFD_CLOEXEC;

        let owned_fd = memfd::memfd_create(&c_name, flags)
            .context(error::MemFdSnafu { fd_name: name })?;

        let raw_fd = owned_fd.into_raw_fd();

        let file = unsafe { fs::File::from_raw_fd(raw_fd) };
        file.set_len(size).context(error::IoSnafu)?;

        Ok(file)
    }
    #[cfg(target_os = "macos")]
    {
        let MemfdSettings { name, size } = settings;

        let base_path = String::from("/tmp/shm/");
        let path = base_path + &name;
        // Checking if the directory exists and creating if not
        if let Some(parent_dir) = std::path::Path::new(&path).parent() {
            if !parent_dir.exists() {
                fs::create_dir_all(parent_dir).context(error::IoSnafu)?;
            }
        };

        let owned_fd = fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .context(error::IoSnafu)?;

        // In order to reduce the writing to the disk, we can delete the file immediately, why?
        // Refer to: https://stackoverflow.com/questions/39779517/does-mac-os-have-a-way-to-create-an-anonymous-file-mapping
        // But we are only developing on macOS and will not use macOS as a production environment. So we will comment out the following.
        // fs::remove_file(path).context(error::IoSnafu)?;

        let raw_fd = owned_fd.into_raw_fd();

        let file = unsafe { fs::File::from_raw_fd(raw_fd) };
        file.set_len(size).context(error::IoSnafu)?;

        Ok(file)
    }
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

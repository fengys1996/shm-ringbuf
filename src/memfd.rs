use std::fs;

use snafu::ResultExt;

use crate::error::Result;
use crate::error::{self};

/// Settings for creating a memfd.
#[derive(Debug, Clone)]
pub struct Settings {
    /// The name of the memfd. Only used for debugging.
    pub name: String,
    /// The size of the memfd.
    pub size: u64,
    /// The path of the memfd.
    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd"
    )))]
    pub path: std::path::PathBuf,
}

/// Create a memfd with the given settings.
pub fn create_fd(settings: Settings) -> Result<fs::File> {
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd"
    ))]
    {
        use std::ffi::CString;

        use nix::sys::memfd;

        let Settings { name, size } = settings;

        let c_name = CString::new(name.clone()).context(error::NulZeroSnafu)?;

        let flags =
            memfd::MFdFlags::MFD_CLOEXEC | memfd::MFdFlags::MFD_ALLOW_SEALING;

        let owned_fd = memfd::memfd_create(c_name.as_c_str(), flags)
            .context(error::MemFdSnafu { fd_name: name })?;

        let file = fs::File::from(owned_fd);
        file.set_len(size).context(error::IoSnafu)?;

        disable_shrink_or_grow(&file)?;

        Ok(file)
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd"
    )))]
    {
        let Settings { name, size, path } = settings;
        let _ = name;

        // Checking if the directory exists and creating if not
        if let Some(parent_dir) = std::path::Path::new(&path).parent() {
            if !parent_dir.exists() {
                fs::create_dir_all(parent_dir).context(error::IoSnafu)?;
            }
        };

        let file = fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .context(error::IoSnafu)?;

        // In order to reduce the writing to the disk, we can delete the file immediately, why?
        // Refer to: https://stackoverflow.com/questions/39779517/does-mac-os-have-a-way-to-create-an-anonymous-file-mapping
        // But we are only developing on macOS and will not use macOS as a production environment. So we will comment out the following.
        fs::remove_file(path).context(error::IoSnafu)?;

        file.set_len(size).context(error::IoSnafu)?;

        Ok(file)
    }
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
fn disable_shrink_or_grow(file: &fs::File) -> Result<()> {
    use nix::fcntl::FcntlArg;
    use nix::fcntl::SealFlag;
    use nix::fcntl::fcntl;
    let seal_flag = SealFlag::F_SEAL_GROW | SealFlag::F_SEAL_SHRINK;
    let fcntl_arg = FcntlArg::F_ADD_SEALS(seal_flag);
    fcntl(file, fcntl_arg).context(error::FcntlSnafu)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd"
    ))]
    fn test_memfd_create() {
        use super::*;

        let settings = Settings {
            name: "memfd".to_string(),
            size: 1024,
        };

        let file = create_fd(settings).unwrap();

        let metadata = file.metadata().unwrap();
        assert_eq!(metadata.len(), 1024);
    }
}

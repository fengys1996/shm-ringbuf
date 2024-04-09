#![allow(dead_code)]
use std::fs;

use crate::error::Result;

#[derive(Debug, Clone)]
pub struct MmapSettings {
    pub name: String,
    pub size: u64,
}

/// Create a memfd with the given settings.
pub fn memfd_create(settings: MmapSettings) -> Result<fs::File> {
    do_memfd_create(settings)
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn do_memfd_create(_settings: MmapSettings) -> Result<fs::File> {
    unimplemented!("memfd_create is only supported on Linux and Android")
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn do_memfd_create(settings: MmapSettings) -> Result<fs::File> {
    use snafu::ResultExt;

    use crate::error;

    let MmapSettings { name, size } = settings;

    let opts = memfd::MemfdOptions::default().allow_sealing(false);
    let mfd = opts.create(name).context(error::MemFdSnafu {
        operate_name: "create",
    })?;

    mfd.as_file().set_len(size).context(error::IoSnafu)?;

    Ok(mfd.into_file())
}

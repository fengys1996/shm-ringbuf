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

    let opts = memfd::MemfdOptions::default().allow_sealing(true);
    let mfd = opts.create(name).context(error::MemFdSnafu {
        operate_name: "create",
    })?;

    mfd.as_file().set_len(size).context(error::IoSnafu)?;

    let mut seals = memfd::SealsHashSet::new();
    seals.insert(memfd::FileSeal::SealShrink);
    seals.insert(memfd::FileSeal::SealGrow);
    mfd.add_seals(&seals).context(error::MemFdSnafu {
        operate_name: "add_seals",
    })?;

    mfd.add_seal(memfd::FileSeal::SealSeal)
        .context(error::MemFdSnafu {
            operate_name: "add_seals",
        })?;

    Ok(mfd.into_file())
}

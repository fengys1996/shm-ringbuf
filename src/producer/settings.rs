use std::path::PathBuf;

const DEFAULT_FDPASS_SOCK_PATH: &str = "/tmp/fd.sock";
const DEFAULT_RINGBUF_LEN: usize = 1024 * 32;
const DEFAULT_ENABLE_NOTIFY: bool = false;

#[derive(Clone, Debug)]
pub struct ProducerSettings {
    /// The path of the unix socket for sending fd.
    pub(super) fdpass_sock_path: PathBuf,
    /// The len of the ringbuf, unit is byte.
    pub(super) ringbuf_len: usize,
    /// Whether to enable notification after commtting data.
    #[allow(dead_code)]
    pub(super) enable_notify: bool,
}

#[derive(Default)]
pub struct SettingsBuilder {
    fdpass_sock_path: Option<PathBuf>,
    ringbuf_len: Option<usize>,
    enable_notify: Option<bool>,
}

impl SettingsBuilder {
    pub fn new() -> Self {
        SettingsBuilder::default()
    }

    /// Set the path of the unix socket for passing file descriptor and other things.
    pub fn fdpass_sock_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.fdpass_sock_path = Some(path.into());
        self
    }

    /// Set the len of the ringbuf.
    pub fn ringbuf_len(mut self, len: usize) -> Self {
        self.ringbuf_len = Some(len);
        self
    }

    /// Set whether to enable notification after committing data.
    pub fn enable_notify(mut self, enable: bool) -> Self {
        self.enable_notify = Some(enable);
        self
    }

    pub fn build(self) -> ProducerSettings {
        let fdpass_sock_path = self
            .fdpass_sock_path
            .unwrap_or(DEFAULT_FDPASS_SOCK_PATH.into());

        let ringbuf_len = self.ringbuf_len.unwrap_or(DEFAULT_RINGBUF_LEN);

        let enable_notify = self.enable_notify.unwrap_or(DEFAULT_ENABLE_NOTIFY);

        ProducerSettings {
            fdpass_sock_path,
            ringbuf_len,
            enable_notify,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_settings_default() {
        let settings = SettingsBuilder::new().build();

        assert_eq!(
            settings.fdpass_sock_path.as_os_str(),
            DEFAULT_FDPASS_SOCK_PATH
        );

        assert_eq!(settings.ringbuf_len, DEFAULT_RINGBUF_LEN);

        assert_eq!(settings.enable_notify, DEFAULT_ENABLE_NOTIFY);
    }
}

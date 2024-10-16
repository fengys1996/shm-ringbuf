use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_GRPC_SOCK_PATH: &str = "/tmp/grpc.sock";
const DEFAULT_FDPASS_SOCK_PATH: &str = "/tmp/fdpass.sock";
const DEFAULT_RINGBUF_LEN: usize = 1024 * 1024;
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_RESULT_FETCH_RETRY_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Debug, Clone)]
pub struct ProducerSettings {
    pub(super) grpc_sock_path: PathBuf,
    pub(super) fdpass_sock_path: PathBuf,
    pub(super) ringbuf_len: usize,
    pub(super) heartbeat_interval: Duration,
    pub(super) result_fetch_retry_interval: Duration,
}

#[derive(Default)]
pub struct ProducerSettingsBuilder {
    grpc_sock_path: Option<PathBuf>,
    fdpass_sock_path: Option<PathBuf>,
    ringbuf_len: Option<usize>,
    heartbeat_interval: Option<Duration>,
    result_fetch_retry_interval: Option<Duration>,
}

impl ProducerSettingsBuilder {
    pub fn new() -> Self {
        ProducerSettingsBuilder::default()
    }

    /// Set the path of the unix socket for gRPC communication.
    pub fn grpc_sock_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.grpc_sock_path = Some(path.into());
        self
    }

    /// Set the path of the unix socket for passing file descriptor and other
    /// information.
    pub fn fdpass_sock_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.fdpass_sock_path = Some(path.into());
        self
    }

    /// Set the ringbuf length. The actual length of the ringbuf may be larger
    /// than the setting.
    pub fn ringbuf_len(mut self, len: usize) -> Self {
        self.ringbuf_len = Some(len);
        self
    }

    /// Set the heartbeat interval.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = Some(interval);
        self
    }

    /// Set the interval for retrying to fetch the result.
    pub fn result_fetch_retry_interval(mut self, interval: Duration) -> Self {
        self.result_fetch_retry_interval = Some(interval);
        self
    }

    pub fn build(self) -> ProducerSettings {
        let grpc_sock_path = self
            .grpc_sock_path
            .unwrap_or_else(|| PathBuf::from(DEFAULT_GRPC_SOCK_PATH));

        let fdpass_sock_path = self
            .fdpass_sock_path
            .unwrap_or_else(|| PathBuf::from(DEFAULT_FDPASS_SOCK_PATH));

        let ringbuf_len = self.ringbuf_len.unwrap_or(DEFAULT_RINGBUF_LEN);

        let heartbeat_interval = self
            .heartbeat_interval
            .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL);

        let result_fetch_retry_interval = self
            .result_fetch_retry_interval
            .unwrap_or(DEFAULT_RESULT_FETCH_RETRY_INTERVAL);

        ProducerSettings {
            grpc_sock_path,
            fdpass_sock_path,
            ringbuf_len,
            heartbeat_interval,
            result_fetch_retry_interval,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::producer::settings::DEFAULT_FDPASS_SOCK_PATH;
    use crate::producer::settings::DEFAULT_GRPC_SOCK_PATH;
    use crate::producer::settings::DEFAULT_HEARTBEAT_INTERVAL;
    use crate::producer::settings::DEFAULT_RINGBUF_LEN;

    #[test]
    fn test_default_settings() {
        let settings = super::ProducerSettingsBuilder::new().build();

        assert_eq!(
            settings.grpc_sock_path,
            PathBuf::from(DEFAULT_GRPC_SOCK_PATH)
        );
        assert_eq!(
            settings.fdpass_sock_path,
            PathBuf::from(DEFAULT_FDPASS_SOCK_PATH)
        );
        assert_eq!(settings.ringbuf_len, DEFAULT_RINGBUF_LEN);
        assert_eq!(settings.heartbeat_interval, DEFAULT_HEARTBEAT_INTERVAL);
    }

    #[test]
    fn test_settings() {
        let settings = super::ProducerSettingsBuilder::new()
            .grpc_sock_path("/tmp/grpc.sock")
            .fdpass_sock_path("/tmp/fdpass.sock")
            .ringbuf_len(1024 * 1024)
            .heartbeat_interval(std::time::Duration::from_secs(5))
            .build();

        assert_eq!(settings.grpc_sock_path, PathBuf::from("/tmp/grpc.sock"));
        assert_eq!(
            settings.fdpass_sock_path,
            PathBuf::from("/tmp/fdpass.sock")
        );
        assert_eq!(settings.ringbuf_len, 1024 * 1024);
        assert_eq!(
            settings.heartbeat_interval,
            std::time::Duration::from_secs(5)
        );
    }
}

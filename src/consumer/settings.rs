use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_GRPC_SOCK_PATH: &str = "/tmp/grpc.sock";
const DEFAULT_FDPASS_SOCK_PATH: &str = "/tmp/fdpass.sock";
const DEFAULT_PROCESS_DURATION: Duration = Duration::from_millis(100);

#[derive(Debug, Clone)]
pub struct ConsumerSettings {
    pub(super) grpc_sock_path: PathBuf,
    pub(super) fdpass_sock_path: PathBuf,
    pub(super) process_interval: Duration,
    pub(super) max_session_num: u64,
    pub(super) session_tti: Duration,
}

#[derive(Default)]
pub struct ConsumerSettingsBuilder {
    grpc_sock_path: Option<PathBuf>,
    fdpass_sock_path: Option<PathBuf>,
    process_duration: Option<Duration>,
    max_session_num: Option<u64>,
    session_tti: Option<Duration>,
}

impl ConsumerSettingsBuilder {
    pub fn new() -> Self {
        ConsumerSettingsBuilder::default()
    }

    /// Set the path of the unix socket for gRPC.
    pub fn grpc_sock_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.grpc_sock_path = Some(path.into());
        self
    }

    /// Set the path of the unix socket for passing file descriptor.
    pub fn fdpass_sock_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.fdpass_sock_path = Some(path.into());
        self
    }

    /// Set the process interval.
    pub fn process_interval(mut self, duration: Duration) -> Self {
        self.process_duration = Some(duration);
        self
    }

    /// Set the maximum number of sessions, which limits the number of producers
    /// at the same time.
    pub fn max_session_num(mut self, capacity: u64) -> Self {
        self.max_session_num = Some(capacity);
        self
    }

    /// Set the time-to-live of a session. If a session is not used for a long
    /// time, it will be purged.
    pub fn session_tti(mut self, ttl: Duration) -> Self {
        self.session_tti = Some(ttl);
        self
    }

    pub fn build(self) -> ConsumerSettings {
        let grpc_sock_path =
            self.grpc_sock_path.unwrap_or(DEFAULT_GRPC_SOCK_PATH.into());

        let fdpass_sock_path = self
            .fdpass_sock_path
            .unwrap_or(DEFAULT_FDPASS_SOCK_PATH.into());

        let process_duration =
            self.process_duration.unwrap_or(DEFAULT_PROCESS_DURATION);

        let max_session_capacity = self.max_session_num.unwrap_or(10);

        let session_ttl = self.session_tti.unwrap_or(Duration::from_secs(10));

        ConsumerSettings {
            grpc_sock_path,
            fdpass_sock_path,
            process_interval: process_duration,
            max_session_num: max_session_capacity,
            session_tti: session_ttl,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::ConsumerSettings;
    use super::ConsumerSettingsBuilder;
    use crate::consumer::settings::DEFAULT_FDPASS_SOCK_PATH;
    use crate::consumer::settings::DEFAULT_GRPC_SOCK_PATH;
    use crate::consumer::settings::DEFAULT_PROCESS_DURATION;

    #[test]
    fn test_settings_default() {
        let settings = ConsumerSettingsBuilder::new().build();

        let ConsumerSettings {
            grpc_sock_path,
            fdpass_sock_path,
            process_interval: process_duration,
            max_session_num: max_session_capacity,
            session_tti,
        } = settings;

        assert_eq!(grpc_sock_path.as_os_str(), DEFAULT_GRPC_SOCK_PATH);
        assert_eq!(fdpass_sock_path.as_os_str(), DEFAULT_FDPASS_SOCK_PATH);
        assert_eq!(process_duration, DEFAULT_PROCESS_DURATION);
        assert_eq!(max_session_capacity, 10);
        assert_eq!(session_tti, Duration::from_secs(10));
    }

    #[test]
    fn test_settings() {
        let settings = ConsumerSettingsBuilder::new()
            .grpc_sock_path("/tmp/grpc_test.sock")
            .fdpass_sock_path("/tmp/fd_test.sock")
            .process_interval(Duration::from_millis(100))
            .max_session_num(20)
            .session_tti(Duration::from_secs(30))
            .build();

        let ConsumerSettings {
            grpc_sock_path,
            fdpass_sock_path,
            process_interval: process_duration,
            max_session_num: max_session_capacity,
            session_tti,
        } = settings;

        assert_eq!(grpc_sock_path.as_os_str(), "/tmp/grpc_test.sock");
        assert_eq!(fdpass_sock_path.as_os_str(), "/tmp/fd_test.sock");
        assert_eq!(process_duration, Duration::from_millis(100));
        assert_eq!(max_session_capacity, 20);
        assert_eq!(session_tti, Duration::from_secs(30));
    }
}

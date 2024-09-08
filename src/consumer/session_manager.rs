#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use moka::sync::Cache;
use moka::sync::CacheBuilder;

use crate::ringbuf::Ringbuf;

/// When each client connects, the server will generate a session to save relevant
/// information.
pub struct Session {
    ringbuf: Ringbuf,
}
pub type SessionRef = Arc<Session>;

pub type ClientId = String;
pub type ClientIdRef = Arc<String>;

pub struct SessionManager<S> {
    sessions: Cache<ClientId, S>,
}
pub type SessionManagerRef = Arc<SessionManager<Session>>;

impl<S> SessionManager<S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Create a new session manager.
    pub fn new(max_capacity: u64, tti: Duration) -> Self {
        let cache = CacheBuilder::new(max_capacity).time_to_idle(tti).build();
        Self { sessions: cache }
    }

    /// Insert a session into the session manager.
    pub fn insert(&self, key: impl Into<ClientId>, session: S) {
        self.sessions.insert(key.into(), session);
    }

    /// Get a session from the session manager and refresh the ttl.
    pub fn get(&self, key: &ClientId) -> Option<S> {
        self.sessions.get(key)
    }

    /// Get the iterator of the session manager. It will not refresh the ttl.
    pub fn iter(&self) -> impl Iterator<Item = (ClientIdRef, S)> + '_ {
        self.sessions.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::SessionManager;

    #[test]
    fn test_session_manager() {
        // Read immediately after writing. All the sessions are valid.
        let ttl = Duration::from_millis(50);
        let session_manager = SessionManager::new(10, ttl);
        for i in 0..10 {
            session_manager.insert(i.to_string(), i);
        }
        for i in 0..10 {
            assert_eq!(session_manager.get(&(i.to_string())), Some(i));
        }

        // Read after the ttl expires. All the sessions are invalid.
        let ttl = Duration::from_millis(50);
        let session_manager = SessionManager::new(8, ttl);
        for i in 0..10 {
            session_manager.insert(i.to_string(), i);
        }
        std::thread::sleep(Duration::from_millis(100));
        for i in 0..10 {
            assert_eq!(session_manager.get(&(i.to_string())), None);
        }

        // Read after the ttl expires, but the session 0 is still valid, since
        // it is accessed before the ttl expires.
        let tti = Duration::from_millis(300);
        let session_manager = SessionManager::new(10, tti);
        for i in 0..10 {
            session_manager.insert(i.to_string(), i);
        }
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(session_manager.get(&"0".to_string()), Some(0));
        std::thread::sleep(Duration::from_millis(200));

        assert_eq!(session_manager.get(&"0".to_string()), Some(0));
        for i in 1..10 {
            assert_eq!(session_manager.get(&(i.to_string())), None);
        }
    }
}

use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use moka::sync::Cache;
use moka::sync::CacheBuilder;
use tokio::sync::mpsc::Sender;
use tonic::Status;
use tracing::info;
use tracing::warn;

use crate::error::DataProcessResult;
use crate::grpc::proto;
use crate::ringbuf::Ringbuf;

/// When each client connects, the server will generate a session to save relevant
/// information.
pub struct Session {
    client_id: ClientId,
    ringbuf: RwLock<Option<Arc<Ringbuf>>>,
    /// Send the results of data processing to the producer.
    result_sender: RwLock<Option<Sender<StdResult<proto::ResultSet, Status>>>>,
}
pub type SessionRef = Arc<Session>;

impl Session {
    /// Create a new session.
    pub fn new(client_id: ClientId) -> Self {
        Self {
            client_id,
            ringbuf: RwLock::new(None),
            result_sender: RwLock::new(None),
        }
    }

    /// Get the ringbuf of the session.
    pub fn ringbuf(&self) -> Option<Arc<Ringbuf>> {
        self.ringbuf.read().unwrap().clone()
    }

    /// Get the client id of the session.
    pub fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    /// Push an OK result to the producer.
    pub async fn push_ok(&self, request_id: u32) {
        self.push_result(request_id, DataProcessResult::ok()).await;
    }

    /// Push an result to the producer.
    pub async fn push_result(
        &self,
        request_id: u32,
        result: impl Into<DataProcessResult>,
    ) {
        let sender = (*self.result_sender.read().unwrap()).clone();

        if let Some(sender) = sender {
            let DataProcessResult {
                status_code,
                message,
            } = result.into();

            let result = proto::Result {
                id: request_id,
                status_code,
                message,
            };

            if let Err(e) = sender
                .send(Ok(proto::ResultSet {
                    results: vec![result],
                }))
                .await
            {
                warn!("failed to send result to producer, error: {}", e);
            }
        }
    }
}

pub type ClientId = String;
pub type ClientIdRef = Arc<String>;

pub struct SessionManager {
    sessions: Cache<ClientId, SessionRef>,
}
pub type SessionManagerRef = Arc<SessionManager>;

impl SessionManager {
    /// Create a new session manager.
    pub fn new(max_capacity: u64, tti: Duration) -> Self {
        let cache = CacheBuilder::new(max_capacity)
            .time_to_idle(tti)
            .eviction_listener(|k, _v, cause| {
                info!(
                    "A session was evicted, client id: {}, cause: {:?}",
                    k, cause
                );
            })
            .build();
        Self { sessions: cache }
    }

    /// Set the ringbuf of the session. If the session does not exist, it will
    /// be created first.
    pub fn set_ringbuf(&self, id: &ClientId, ringbuf: Arc<Ringbuf>) {
        let entry = self
            .sessions
            .entry(id.clone())
            .or_insert_with(|| Arc::new(Session::new(id.clone())));

        entry.value().ringbuf.write().unwrap().replace(ringbuf);
    }

    /// Set the result sender of the session. If the session does not exist, it
    /// will be created first.
    pub fn set_result_sender(
        &self,
        id: &ClientId,
        sender: Sender<std::result::Result<proto::ResultSet, Status>>,
    ) {
        let entry = self
            .sessions
            .entry(id.clone())
            .or_insert_with(|| Arc::new(Session::new(id.clone())));

        entry.value().result_sender.write().unwrap().replace(sender);
    }

    /// Get a session from the session manager and refresh the tti.
    pub fn get(&self, key: &ClientId) -> Option<SessionRef> {
        self.sessions.get(key)
    }

    /// Get the iterator of the session manager. It will not refresh the ttl.
    pub fn iter(&self) -> impl Iterator<Item = (ClientIdRef, SessionRef)> + '_ {
        self.sessions.into_iter()
    }
}

#![allow(dead_code)]
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

    pub fn is_ready(&self) -> bool {
        self.ringbuf.read().unwrap().is_some()
            && self.result_sender.read().unwrap().is_some()
    }

    /// Get the client id of the session.
    pub fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    /// Get the ringbuf of the session.
    pub fn ringbuf(&self) -> Option<Arc<Ringbuf>> {
        self.ringbuf.read().unwrap().clone()
    }

    /// Set the ringbuf.
    pub fn set_ringbuf(&self, ringbuf: Arc<Ringbuf>) {
        *self.ringbuf.write().unwrap() = Some(ringbuf);
    }

    /// Set the result sender.
    pub fn set_result_sender(
        &self,
        sender: Sender<StdResult<proto::ResultSet, Status>>,
    ) {
        *self.result_sender.write().unwrap() = Some(sender);
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
    pub(crate) sessions: Cache<ClientId, SessionRef>,
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

    /// Insert a session into the session manager.
    pub fn insert(&self, session: SessionRef) {
        let key = session.client_id().clone();
        self.sessions.insert(key, session);
    }

    /// Get a session from the session manager and refresh the tti.
    pub fn get(&self, key: &ClientId) -> Option<SessionRef> {
        self.sessions.get(key)
    }

    /// Get the iterator of the session manager. It will not refresh the ttl.
    pub fn iter(&self) -> impl Iterator<Item = (ClientIdRef, SessionRef)> + '_ {
        self.sessions.into_iter()
    }

    pub fn set_ringbuf(&self, id: &ClientId, ringbuf: Arc<Ringbuf>) {
        if let Some(session) = self.sessions.get(id) {
            session.set_ringbuf(ringbuf);
        }
    }

    pub fn set_result_sender(
        &self,
        id: &ClientId,
        sender: Sender<std::result::Result<proto::ResultSet, Status>>,
    ) {
        if let Some(session) = self.sessions.get(id) {
            session.set_result_sender(sender);
        }
    }
}

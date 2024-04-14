use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;

/// A shared ringbuf store with expire time.
pub struct ExpireDashMap<B> {
    map: Arc<DashMap<String, (B, Instant)>>,
    cancel: Sender<()>,
}

impl<B> Clone for ExpireDashMap<B> {
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
            cancel: self.cancel.clone(),
        }
    }
}

impl<B> ExpireDashMap<B>
where
    B: Send + Sync + 'static,
{
    pub async fn new(expire: Duration, check_interval: Duration) -> Self {
        let map = Arc::new(DashMap::new());

        let (sender, mut recv) = channel(1);

        let map_clone = map.clone();

        let periodic_task = async move {
            loop {
                tokio::select! {
                    _ = recv.recv() => break,
                    _ = sleep(check_interval) => {
                        map_clone.retain(|_, (_, instant)| {
                            Instant::now() - *instant < expire
                        });
                    }
                }
            }
        };
        // TODO: tokio spawn must be in tokio runtime. Have better way to do this?
        tokio::spawn(periodic_task);

        Self {
            map,
            cancel: sender,
        }
    }
}

impl<B> ExpireDashMap<B>
where
    B: Clone,
{
    pub fn get(&self, id: impl Into<String>) -> Option<B> {
        // Update the instant when get the value if exist.
        let entry = self.map.entry(id.into()).and_modify(|(_, instant)| {
            *instant = Instant::now();
        });

        // Return the value if exist.
        match entry {
            Entry::Occupied(e) => Some(e.get().0.clone()),
            Entry::Vacant(_) => None,
        }
    }

    pub fn set(&self, id: impl Into<String>, ringbuf: B) {
        self.map.insert(id.into(), (ringbuf, Instant::now()));
    }

    pub fn iter_mut(&self) -> impl Iterator<Item = (String, B)> + '_ {
        self.map
            .iter()
            .map(|item| (item.key().clone(), item.value().0.clone()))
    }

    pub fn map(&self) -> Arc<DashMap<String, (B, Instant)>> {
        self.map.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::ExpireDashMap;

    #[tokio::test]
    async fn test_ringbuf_store() {
        let expire = Duration::from_secs(1);
        let check_interval = Duration::from_secs(1);
        let ringbuf_store = ExpireDashMap::new(expire, check_interval).await;

        ringbuf_store.set("1", 1);
        ringbuf_store.set("2", 2);

        assert_eq!(ringbuf_store.get("1"), Some(1));
        assert_eq!(ringbuf_store.get("2"), Some(2));
        assert_eq!(ringbuf_store.get("3"), None);

        tokio::time::sleep(Duration::from_secs(2)).await;

        assert_eq!(ringbuf_store.get("1"), None);
        assert_eq!(ringbuf_store.get("2"), None);
        assert_eq!(ringbuf_store.get("3"), None);
    }
}

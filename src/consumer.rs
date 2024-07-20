pub mod decode;
pub(crate) mod rings_store;
pub mod settings;

use std::fmt::Debug;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;

use settings::ConsumerSettings;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tokio_util::sync::WaitForCancellationFuture;
use tracing::warn;

use self::decode::Context;
use self::decode::Decode;
use self::rings_store::ExpireDashMap;
use crate::fd_pass::FdRecvServer;
use crate::ringbuf::Ringbuf;

pub struct RingbufConsumer<D, Item, E> {
    ringbuf_store: RingbufStore,
    sender: Sender<StdResult<Item, E>>,
    decoder: D,
    notify: Arc<Notify>,
}

pub type RingbufStore = ExpireDashMap<Ringbuf>;

impl<D, Item, E> RingbufConsumer<D, Item, E>
where
    D: Decode<Item = Item, Error = E> + 'static,
    Item: Send + Sync + 'static,
    E: Debug + Send + 'static,
{
    pub async fn start_consume(
        settings: ConsumerSettings,
        decoder: D,
    ) -> Receiver<StdResult<Item, E>> {
        let ringbuf_store = ExpireDashMap::new(
            settings.ringbuf_expire,
            settings.ringbuf_check_interval,
        )
        .await;

        let (send, recv) = channel(1024);

        let notify = Arc::new(Notify::new());

        let consumer = RingbufConsumer {
            ringbuf_store,
            decoder,
            sender: send,
            notify: notify.clone(),
        };

        let mut server: FdRecvServer<WaitForCancellationFuture> =
            FdRecvServer::with_shutdown(
                settings.fdpass_sock_path,
                consumer.ringbuf_store.clone(),
                None,
            );
        // 1. start the server to receive the fd.
        tokio::spawn(async move {
            server.run().await.unwrap();
        });

        let process_duration = settings.process_interval;

        // 3. start the consumer loop.
        tokio::spawn(async move {
            consumer.process_loop(process_duration, None).await;
        });

        recv
    }

    pub async fn process_loop(
        mut self,
        interval: Duration,
        cancel: Option<CancellationToken>,
    ) {
        loop {
            self.process_ringbufs().await;
            if let Some(cancel) = &cancel {
                tokio::select! {
                    _ = self.notify.notified() => {}
                    _ = sleep(interval) => {}
                    _ = cancel.cancelled() => {
                        warn!("receive cancel signal, stop processing ringbufs.");
                        break
                    },
                }
            } else {
                tokio::select! {
                    _ = self.notify.notified() => {}
                    _ = sleep(interval) => {}
                }
            }
        }
    }

    async fn process_ringbufs(&mut self) {
        for (_, mut ring) in self.ringbuf_store.iter_mut() {
            self.process_ringbuf(&mut ring).await;
        }
    }

    async fn process_ringbuf(&self, ringbuf: &mut Ringbuf) {
        while let Some(data_block) = ringbuf.peek() {
            if data_block.atomic_is_busy() {
                break;
            }

            let item = self.decoder.decode(data_block.slice(), Context {});
            if let Err(e) = self.sender.send(item).await {
                warn!("item recv is dropped, trigger server shutdown, detail: {:?}", e);
                break;
            }
            unsafe { ringbuf.advance_consume_offset(data_block.total_len()) }
        }
    }
}

impl<D, Item, E> Drop for RingbufConsumer<D, Item, E> {
    fn drop(&mut self) {}
}

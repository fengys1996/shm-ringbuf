pub mod decode;
pub(crate) mod rings_store;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use self::decode::Context;
use self::decode::Decode;
use self::rings_store::ExpireDashMap;
use crate::error::Result;
use crate::fdpass::FdRecvServer;
use crate::grpc::ShmCtlServer;
use crate::ringbuf::Ringbuf;

pub struct RingbufConsumer<D, Item> {
    ringbuf_store: RingbufStore,
    sender: Sender<Item>,
    decoder: D,
    notify: Arc<Notify>,
    cancel: CancellationToken,
}

pub type RingbufStore = ExpireDashMap<Ringbuf>;

pub struct ConsumerSettings {
    pub control_sock_path: PathBuf,
    pub sendfd_sock_path: PathBuf,
    pub process_duration: Duration,
    pub ringbuf_expire: Duration,
    pub ringbuf_check_interval: Duration,
}

impl<D, Item> RingbufConsumer<D, Item>
where
    D: Decode<Item = Item> + 'static,
    Item: Send + Sync + 'static,
{
    pub async fn start_consume(
        settings: ConsumerSettings,
        decoder: D,
    ) -> Receiver<Item> {
        let ringbuf_store = ExpireDashMap::new(
            settings.ringbuf_expire,
            settings.ringbuf_check_interval,
        )
        .await;

        let (send, recv) = channel(1024);

        let notify = Arc::new(Notify::new());

        let cancel_token = CancellationToken::new();

        let consumer = RingbufConsumer {
            ringbuf_store,
            decoder,
            sender: send,
            notify: notify.clone(),
            cancel: cancel_token.clone(),
        };

        let ringbuf_store = consumer.ringbuf_store.clone();
        let cancel_token_clone = cancel_token.clone();

        // 1. start the server to receive the fd.
        tokio::spawn(async move {
            let mut server = FdRecvServer::with_shutdown(
                settings.sendfd_sock_path,
                ringbuf_store,
                Some(cancel_token_clone.cancelled()),
            );
            server.run().await.unwrap();
        });

        let ringbuf_store = consumer.ringbuf_store.clone();
        let cancel_token_clone = cancel_token.clone();

        // 2. start the server to receive the control message.
        tokio::spawn(async move {
            let mut server = ShmCtlServer::with_shutdown(
                settings.control_sock_path,
                notify,
                ringbuf_store,
                Some(cancel_token_clone.cancelled()),
            );
            server.run().await.unwrap();
        });

        let process_duration = settings.process_duration;
        let cancel_token_clone = cancel_token.clone();

        // 3. start the consumer loop.
        tokio::spawn(async move {
            consumer
                .process_loop(process_duration, Some(cancel_token_clone))
                .await;
        });

        recv
    }

    pub async fn process_loop(
        mut self,
        interval: Duration,
        cancel: Option<CancellationToken>,
    ) {
        loop {
            self.process_ringbufs().await.unwrap();
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

    async fn process_ringbufs(&mut self) -> Result<()> {
        for (_, mut ring) in self.ringbuf_store.iter_mut() {
            self.process_ringbuf(&mut ring).await?;
        }
        Ok(())
    }

    async fn process_ringbuf(&self, ringbuf: &mut Ringbuf) -> Result<()> {
        while let Some(data_block) = ringbuf.peek() {
            if data_block.atomic_is_busy() {
                break;
            }

            let item = self.decoder.decode(data_block.slice(), Context {})?;
            if let Err(e) = self.sender.send(item).await {
                warn!("item recv is dropped, trigger server shutdown, detail: {:?}", e);
                self.cancel.cancel();
                break;
            }
            unsafe { ringbuf.advance_consume_offset(data_block.total_len()) }
        }
        Ok(())
    }
}

impl<D, Item> Drop for RingbufConsumer<D, Item> {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

pub mod decode;
pub(crate) mod session_manager;
pub mod settings;

use std::fmt::Debug;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;

use session_manager::SessionManager;
use session_manager::SessionManagerRef;
use settings::ConsumerSettings;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use self::decode::Context;
use self::decode::Decode;
use crate::fd_pass::FdRecvServer;
use crate::grpc::ShmCtlServer;
use crate::ringbuf::Ringbuf;

pub struct RingbufConsumer<D, Item, E> {
    session_manager: SessionManagerRef,
    sender: Sender<StdResult<Item, E>>,
    decoder: D,
    notify: Arc<Notify>,
    cancel: CancellationToken,
}

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
        let session_manager = Arc::new(SessionManager::new(
            settings.max_session_capacity,
            settings.session_tti,
        ));

        let (send, recv) = channel(1024);

        let notify = Arc::new(Notify::new());

        let cancel_token = CancellationToken::new();

        let consumer = RingbufConsumer {
            session_manager,
            decoder,
            sender: send,
            notify: notify.clone(),
            cancel: cancel_token.clone(),
        };

        let session_manager = consumer.session_manager.clone();
        let cancel_token_clone = cancel_token.clone();

        // 1. start the server to receive the fd.
        tokio::spawn(async move {
            let mut server = FdRecvServer::with_shutdown(
                settings.fdpass_sock_path,
                session_manager,
                Some(cancel_token_clone.cancelled()),
            );
            server.run().await.unwrap();
        });

        let session_manager = consumer.session_manager.clone();
        let cancel_token_clone = cancel_token.clone();

        // 2. start the server to receive the control message.
        tokio::spawn(async move {
            let mut server = ShmCtlServer::with_shutdown(
                settings.grpc_sock_path,
                notify,
                session_manager,
                Some(cancel_token_clone.cancelled()),
            );
            server.run().await.unwrap();
        });

        let process_duration = settings.process_interval;
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
        for (_, session) in self.session_manager.iter() {
            self.process_ringbuf(session.ringbuf()).await;
        }
    }

    async fn process_ringbuf(&self, ringbuf: &Ringbuf) {
        while let Some(data_block) = ringbuf.peek() {
            if data_block.is_busy() {
                break;
            }

            let item =
                self.decoder.decode(data_block.slice().unwrap(), Context {});
            if let Err(e) = self.sender.send(item).await {
                warn!("item recv is dropped, trigger server shutdown, detail: {:?}", e);
                self.cancel.cancel();
                break;
            }
            unsafe { ringbuf.advance_consume_offset(data_block.total_len()) }
        }
    }
}

impl<D, Item, E> Drop for RingbufConsumer<D, Item, E> {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

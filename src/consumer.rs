pub mod process;
pub mod settings;

pub(crate) mod session_manager;

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use process::DataProcess;
use session_manager::SessionManager;
use session_manager::SessionManagerRef;
use session_manager::SessionRef;
use settings::ConsumerSettings;
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::error::DataProcessResult;
use crate::fd_pass::FdRecvServer;
use crate::grpc::server::ShmCtlServer;

pub struct RingbufConsumer {
    session_manager: SessionManagerRef,
    notify: Arc<Notify>,
    settings: ConsumerSettings,
    cancel: CancellationToken,
}

impl RingbufConsumer {
    pub fn new(settings: ConsumerSettings) -> Self {
        let session_manager = Arc::new(SessionManager::new(
            settings.max_session_capacity,
            settings.session_tti,
        ));

        let notify = Arc::new(Notify::new());

        let cancel = CancellationToken::new();

        RingbufConsumer {
            session_manager,
            notify,
            cancel,
            settings,
        }
    }

    pub async fn run<P, E>(&self, processor: P)
    where
        P: DataProcess<Error = E>,
        E: Into<DataProcessResult> + Debug + Send,
    {
        self.start_grpc_server().await;

        self.start_fdrecv_server().await;

        let interval = self.settings.process_interval;
        let cancel = self.cancel.clone();
        self.process_loop(&processor, interval, Some(cancel)).await;
    }

    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    async fn start_grpc_server(&self) {
        let cancel = self.cancel.clone();
        let notify = self.notify.clone();
        let session_manager = self.session_manager.clone();
        let grpc_sock_path = self.settings.grpc_sock_path.clone();

        tokio::spawn(async move {
            let mut server = ShmCtlServer::with_shutdown(
                grpc_sock_path,
                notify,
                session_manager,
                Some(cancel.cancelled()),
            );
            server.run().await.unwrap();
        });
    }

    async fn start_fdrecv_server(&self) {
        let cancel = self.cancel.clone();
        let session_manager = self.session_manager.clone();
        let fdpass_sock_path = self.settings.fdpass_sock_path.clone();

        tokio::spawn(async move {
            let mut server = FdRecvServer::with_shutdown(
                fdpass_sock_path,
                session_manager,
                Some(cancel.cancelled()),
            );
            server.run().await.unwrap();
        });
    }

    pub async fn process_loop<P, E>(
        &self,
        processor: &P,
        interval: Duration,
        cancel: Option<CancellationToken>,
    ) where
        P: DataProcess<Error = E>,
        E: Into<DataProcessResult> + Debug + Send,
    {
        loop {
            process_all_sessions(&self.session_manager, processor).await;
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
}

async fn process_all_sessions<P, E>(
    session_manager: &SessionManagerRef,
    processor: &P,
) where
    P: DataProcess<Error = E>,
    E: Into<DataProcessResult>,
{
    for (_, session) in session_manager.iter() {
        process_session(&session, processor).await;
    }
}

async fn process_session<P, E>(session: &SessionRef, processor: &P)
where
    P: DataProcess<Error = E>,
    E: Into<DataProcessResult>,
{
    let ringbuf = session.ringbuf();

    while let Some(data_block) = ringbuf.peek() {
        if data_block.is_busy() {
            break;
        }

        let data_slice = data_block.slice().unwrap();
        let business_id = data_block.business_id();

        if let Err(e) = processor.process(data_slice).await {
            session.push_result(business_id, e).await;
        } else {
            session.push_ok(business_id).await;
        }

        unsafe { ringbuf.advance_consume_offset(data_block.total_len()) }
    }
}

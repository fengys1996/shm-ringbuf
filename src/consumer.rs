pub mod process;
pub mod settings;

pub(crate) mod session_manager;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use process::DataProcess;
use process::ResultSender;
use session_manager::SessionManager;
use session_manager::SessionManagerRef;
use session_manager::SessionRef;
use settings::ConsumerSettings;
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::error::DataProcessResult;
use crate::error::CHECKSUM_MISMATCH;
use crate::fd_pass::FdRecvServer;
use crate::grpc::proto::shm_control_server::ShmControlServer;
use crate::grpc::server::ShmCtlHandler;
use crate::grpc::server::ShmCtlServer;

/// The consumer of the ringbuf based on shared memory.
pub struct RingbufConsumer {
    session_manager: SessionManagerRef,
    notify: Arc<Notify>,
    settings: ConsumerSettings,
    cancel: CancellationToken,
    grpc_detached: AtomicBool,
    started: AtomicBool,
}

impl RingbufConsumer {
    /// Create a [`RingbufConsumer`] by the given settings.
    pub fn new(settings: ConsumerSettings) -> Self {
        let (consumer, _) = Self::new_with_detach_grpc(settings, false);
        consumer
    }

    /// Create a [`RingbufConsumer`] by the given settings and detach the gRPC
    /// server according to the `detached` flag.
    ///
    /// Since some users have their own grpc services and do not want to occupy
    /// an additional uds.
    ///
    /// Note: If detached, the `grpc_sock_path` in the settings will be ignored.
    pub fn new_with_detach_grpc(
        settings: ConsumerSettings,
        detached: bool,
    ) -> (RingbufConsumer, Option<ShmControlServer<ShmCtlHandler>>) {
        let session_manager = Arc::new(SessionManager::new(
            settings.max_session_num,
            settings.session_tti,
        ));
        let notify = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let started = AtomicBool::new(false);
        let grpc_detached = AtomicBool::new(detached);

        let grpc_server = if detached {
            let handler = ShmCtlHandler {
                notify: notify.clone(),
                session_manager: session_manager.clone(),
            };
            Some(ShmControlServer::new(handler))
        } else {
            None
        };

        let consumer = RingbufConsumer {
            session_manager,
            notify,
            cancel,
            settings,
            grpc_detached,
            started,
        };

        (consumer, grpc_server)
    }

    /// Run the consumer, which will block the current thread.
    pub async fn run<P>(&self, processor: P)
    where
        P: DataProcess,
    {
        if self
            .started
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            warn!("the consumer has already started.");
            return;
        }

        if !self.grpc_detached.load(Ordering::Relaxed) {
            self.start_grpc_server().await;
        }

        self.start_fdrecv_server().await;

        let interval = self.settings.process_interval;
        let cancel = self.cancel.clone();
        self.process_loop(&processor, interval, Some(cancel)).await;
    }

    /// Cancel the consumer.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Start the gRPC server.
    /// 1. receive the notification from the producer.
    /// 2. send the execution results to the producer via gRPC stream.
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

    /// Start the server to receive the file descriptors from the producer.
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

    /// The main loop to process the ringbufs.
    async fn process_loop<P>(
        &self,
        processor: &P,
        interval: Duration,
        cancel: Option<CancellationToken>,
    ) where
        P: DataProcess,
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

async fn process_all_sessions<P>(
    session_manager: &SessionManagerRef,
    processor: &P,
) where
    P: DataProcess,
{
    for (_, session) in session_manager.iter() {
        process_session(&session, processor).await;
    }
}

async fn process_session<P>(session: &SessionRef, processor: &P)
where
    P: DataProcess,
{
    let ringbuf = session.ringbuf();
    let enable_checksum = session.enable_checksum();

    while let Some(data_block) = ringbuf.peek() {
        if data_block.is_busy() {
            break;
        }

        let data_slice = data_block.slice().unwrap();
        let req_id = data_block.req_id();

        if enable_checksum
            && crc32fast::hash(data_slice) != data_block.checksum()
        {
            let ret = DataProcessResult {
                status_code: CHECKSUM_MISMATCH,
                message: format!(
                    "checksum mismatch, client id: {}, req id: {}",
                    session.client_id(),
                    req_id
                ),
            };
            session.push_result(req_id, ret).await;
            unsafe { ringbuf.advance_consume_offset(data_block.total_len()) }
            continue;
        }

        let result_sender = ResultSender {
            request_id: req_id,
            session: session.clone(),
        };

        processor.process(data_slice, result_sender).await;

        unsafe { ringbuf.advance_consume_offset(data_block.total_len()) }
    }
}

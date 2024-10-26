use std::fs::File;
use std::future::Future;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::path::Path;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use passfd::tokio::FdPassingExt;
use snafu::location;
use snafu::ResultExt;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio::time::sleep;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::consumer::session_manager::Session;
use crate::consumer::session_manager::SessionManagerRef;
use crate::error;
use crate::error::Result;
use crate::ringbuf::Ringbuf;

/// The server for receiving fd from producer, create Ringbuf, and add the Ringbuf
/// to RingbufStore.
pub struct FdRecvServer<F> {
    /// The unix sock path for receiving fd.
    sock_path: PathBuf,

    session_manager: SessionManagerRef,

    /// The future for shutdown the server gracefully.
    shutdown: Option<F>,
}

impl<F> Drop for FdRecvServer<F> {
    fn drop(&mut self) {
        if self.sock_path.metadata().is_err() {
            return;
        }

        if std::fs::remove_file(&self.sock_path).is_err() {
            warn!(
                "failed to remove the unix socket file: {:?}",
                self.sock_path
            );
            return;
        }

        info!("remove the unix socket file: {:?}", self.sock_path);
    }
}

impl<F> FdRecvServer<F>
where
    F: Future<Output = ()>,
{
    /// Create a new FdRecvServer.
    pub fn with_shutdown(
        sock_path: impl Into<PathBuf>,
        session_manager: SessionManagerRef,
        shutdown: Option<F>,
    ) -> Self {
        FdRecvServer {
            sock_path: sock_path.into(),
            session_manager,
            shutdown,
        }
    }

    /// Run the FdRecvServer.
    pub async fn run(&mut self) -> Result<()> {
        if let Some(parent) = self.sock_path.parent() {
            fs::create_dir_all(parent).await.context(error::IoSnafu)?;
        }

        if self.sock_path.metadata().is_ok() {
            fs::remove_file(&self.sock_path)
                .await
                .context(error::IoSnafu)?;

            info!("remove the unix socket file: {:?}", self.sock_path);
        }

        let listener =
            UnixListener::bind(&self.sock_path).context(error::IoSnafu)?;

        if let Some(shutdown) = self.shutdown.take() {
            let mut pin_shutdown = pin!(shutdown);

            loop {
                tokio::select! {
                    ret = self.run_once(&listener) => ret?,
                    _ = &mut pin_shutdown => {
                        info!("receive cancel signal, exit the recvfd server.");
                        return Ok(());
                    }
                }
            }
        }

        loop {
            self.run_once(&listener).await?;
        }
    }

    /// Run the server once.
    ///
    /// Note: This function tries its best not to fail.
    async fn run_once(&self, listener: &UnixListener) -> Result<()> {
        let stream = accept(listener).await?;
        info!("accept a new unix stream for sending fd.");

        let mut handler = Handler {
            stream,
            session_manager: self.session_manager.clone(),
        };

        tokio::spawn(async move {
            if let Err(e) = handler.handle().await {
                error!("handle error: {}", e);
            }
        });

        Ok(())
    }
}

struct Handler {
    stream: UnixStream,
    session_manager: SessionManagerRef,
}

impl Handler {
    async fn handle(&mut self) -> Result<()> {
        let stream = &mut self.stream;

        // 1. Read the length of client id.
        let len_of_id = stream.read_u32().await.context(error::IoSnafu)?;

        // 2. Read the client id (UTF-8 string).
        let mut buf = vec![0; len_of_id as usize];

        stream.read_exact(&mut buf).await.context(error::IoSnafu)?;

        let client_id = String::from_utf8(buf).context(error::FromUtf8Snafu)?;

        // 3. Recv the fd.
        let fd = stream.recv_fd().await.context(error::IoSnafu)?;
        let file = unsafe { File::from_raw_fd(fd) };

        // 4. Log the client id and the length of client id.
        info!(
            "recv a client id: {}, the len of client id: {}",
            client_id, len_of_id
        );

        // 5. Create the ringbuf.
        let ringbuf = Ringbuf::from(&file)?;

        // 6. Store the ringbuf to ringbuf store.
        let session = Arc::new(Session::new(ringbuf));
        self.session_manager.insert(client_id, session);

        Ok(())
    }
}

/// Accept an unix stream.
///
/// Errors are handled by backing off and retrying. An exponential backoff
/// strategy is used.
async fn accept(listener: &UnixListener) -> Result<UnixStream> {
    let mut backoff = 1;

    loop {
        match listener.accept().await {
            Ok((socket, _)) => return Ok(socket),
            Err(err) => {
                warn!(
                    "accept error: {}, try again after {} seconds",
                    err, backoff
                );
                if backoff > 64 {
                    error!("accept error: {}", err);
                    return Err(error::Error::Io {
                        source: err,
                        location: location!(),
                    });
                }
            }
        }

        sleep(Duration::from_secs(backoff)).await;
        backoff *= 2;
    }
}

pub async fn send_fd(
    sock_path: impl AsRef<Path>,
    file: &File,
    name: impl Into<String>,
) -> Result<()> {
    let mut stream = UnixStream::connect(sock_path.as_ref())
        .await
        .context(error::IoSnafu)?;

    let name = name.into();
    let name = name.as_bytes();

    stream
        .write_u32(name.len() as u32)
        .await
        .context(error::IoSnafu)?;
    stream.write_all(name).await.context(error::IoSnafu)?;

    stream
        .send_fd(file.as_raw_fd())
        .await
        .context(error::IoSnafu)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time::sleep;
    use tokio_util::sync::CancellationToken;

    use super::send_fd;
    use crate::consumer::session_manager::SessionManager;
    use crate::fd_pass::FdRecvServer;
    use crate::ringbuf::page_align_size;

    #[tokio::test]
    async fn test_fd_pass() {
        let session_manager =
            Arc::new(SessionManager::new(200, Duration::from_secs(10)));

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("fd.sock");

        let cancel = CancellationToken::new();

        let cancel_c = cancel.clone();
        let path_c = path.clone();
        let session_manager_c = session_manager.clone();

        tokio::spawn(async move {
            let mut server = FdRecvServer::with_shutdown(
                path_c,
                session_manager_c,
                Some(cancel_c.cancelled()),
            );
            server.run().await.unwrap();
        });

        // wait for the server to start.
        sleep(Duration::from_millis(100)).await;

        // mock concurrent sending
        let mut joins = Vec::with_capacity(100);
        for i in 0..100 {
            let path_c = path.clone();
            let join = tokio::spawn(async move {
                let file = tempfile::tempfile().unwrap();
                file.set_len(page_align_size(10240)).unwrap();
                let client_id = format!("client_id_{}", i);
                send_fd(path_c, &file, client_id).await.unwrap();
            });
            joins.push(join);
        }

        for join in joins {
            join.await.unwrap();
        }

        // wait for the server to handle the fd.
        sleep(Duration::from_millis(100)).await;

        for i in 0..100 {
            let client_id = format!("client_id_{}", i);
            assert!(session_manager.get(&client_id).is_some());
        }

        cancel.cancel();
    }
}

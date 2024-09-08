use std::fs::File;
use std::future::Future;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::path::Path;
use std::path::PathBuf;
use std::pin::pin;
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

use crate::consumer::RingbufStore;
use crate::error;
use crate::error::Result;
use crate::ringbuf::Ringbuf;

/// The server for receiving fd from producer, create Ringbuf, and add the Ringbuf
/// to RingbufStore.
pub struct FdRecvServer<F> {
    /// The unix sock path for receiving fd.
    sock_path: PathBuf,

    /// The store is used to manage the Ringbuf.
    ringbuf_store: RingbufStore,

    /// The future for shutdown the server gracefully.
    shutdown: Option<F>,
}

impl<F> Drop for FdRecvServer<F> {
    fn drop(&mut self) {
        if self.sock_path.metadata().is_err() {
            return;
        }

        if std::fs::remove_file(&self.sock_path).is_err() {
            error!(
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
        ringbuf_store: RingbufStore,
        shutdown: Option<F>,
    ) -> Self {
        FdRecvServer {
            sock_path: sock_path.into(),
            ringbuf_store,
            shutdown,
        }
    }

    /// Run the FdRecvServer.
    pub async fn run(&mut self) -> Result<()> {
        if let Some(parent) = self.sock_path.parent() {
            fs::create_dir_all(parent).await.context(error::IoSnafu)?;

            warn!(
                "create the parent directory for the unix socket file: {:?}",
                parent
            );
        }

        if self.sock_path.metadata().is_ok() {
            fs::remove_file(&self.sock_path)
                .await
                .context(error::IoSnafu)?;

            warn!("remove the unix socket file: {:?}", self.sock_path);
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
            ringbuf_store: self.ringbuf_store.clone(),
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
    ringbuf_store: RingbufStore,
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

        // 3. Read the length of ringbuf.
        let len_of_ringbuf = stream.read_u32().await.context(error::IoSnafu)?;

        // 4. Log the client id and the length of client id.
        info!(
            "recv a client id: {}, the len of client id: {}, the len of ringbuf: {}",
            client_id, len_of_id, len_of_ringbuf
        );

        // 5. Recv the fd.
        let fd = stream.recv_fd().await.context(error::IoSnafu)?;
        let file = unsafe { File::from_raw_fd(fd) };

        // 6. Create the ringbuf.
        let ringbuf = Ringbuf::from(&file, len_of_ringbuf as usize)?;

        // 7. Store the ringbuf to ringbuf store.
        self.ringbuf_store.set(client_id, ringbuf);

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
    len_of_ringbuf: u32,
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
        .write_u32(len_of_ringbuf)
        .await
        .context(error::IoSnafu)?;

    stream
        .send_fd(file.as_raw_fd())
        .await
        .context(error::IoSnafu)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::sleep;
    use tokio_util::sync::CancellationToken;

    use crate::{consumer::RingbufStore, fd_pass::FdRecvServer};

    use super::send_fd;

    #[tokio::test]
    async fn test_fd_pass() {
        let rb_store =
            RingbufStore::new(Duration::from_secs(10), Duration::from_secs(5))
                .await;

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("fd.sock");

        let cancel = CancellationToken::new();

        let cancel_c = cancel.clone();
        let path_c = path.clone();
        let rb_store_c = rb_store.clone();

        tokio::spawn(async move {
            let mut server = FdRecvServer::with_shutdown(
                path_c,
                rb_store_c,
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
                let client_id = format!("client_id_{}", i);

                send_fd(path_c, &file, client_id, 1024).await.unwrap();
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
            assert!(rb_store.get(&client_id).is_some());
        }

        cancel.cancel();
    }
}

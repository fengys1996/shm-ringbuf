use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::path::Path;
use std::path::PathBuf;
use std::pin::pin;
use std::time::Duration;

use passfd::tokio::FdPassingExt;
use snafu::location;
use snafu::Location;
use snafu::ResultExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio::time;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::consumer::RingbufStore;
use crate::error;
use crate::error::Result;
use crate::ringbuf::Ringbuf;

/// The server for receiving fd from producer, create Ringbuf, and add the Ringbuf
/// to ShareRingbufSet.
pub struct FdRecvServer<F> {
    /// The unix sock path for receiving fd.
    sock_path: PathBuf,
    ringbuf_store: RingbufStore,
    shutdown: Option<F>,
}

impl<F> FdRecvServer<F>
where
    F: std::future::Future<Output = ()>,
{
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

    pub async fn run(&mut self) -> Result<()> {
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

    async fn run_once(&self, listener: &UnixListener) -> Result<()> {
        let stream = accept(listener).await?;
        info!("accept a new unix stream for sending fd.");

        let mut handler = Handler {
            stream,
            ringbuf_store: self.ringbuf_store.clone(),
        };

        if let Err(e) = handler.handle().await {
            error!("handle error: {}", e);
        }

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

        // 1. Read the length of fd name.
        let len = stream.read_u32().await.context(error::IoSnafu)?;

        // 2. Read the fd name.
        let mut buf = vec![0; len as usize];
        stream.read_exact(&mut buf).await.context(error::IoSnafu)?;
        let id = String::from_utf8(buf).context(error::FromUtf8Snafu)?;
        info!("the len of fd name: {}, fd name: {}", len, id);

        // 3. Read the length of ringbuf.
        let len_of_ringbuf = stream.read_u32().await.context(error::IoSnafu)?;

        // 3. Recv the fd.
        let fd = stream.recv_fd().await.context(error::IoSnafu)?;
        let file = unsafe { File::from_raw_fd(fd) };

        // 4. create the ringbuf.
        let ringbuf = Ringbuf::from_raw(&file, len_of_ringbuf as usize)?;

        // 5. store the ringbuf to ring_set.
        self.ringbuf_store.set(id, ringbuf);

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

        time::sleep(Duration::from_secs(backoff)).await;
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

use std::fs;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::time::Sleep;

use crate::error::Result;
use crate::fdpass::FdRecvServer;
use crate::ringbuf::Ringbuf;

pub struct RingbufConsumer<D, Item> {
    rings: ShareRingbufSet,
    sender: Sender<Item>,
    decoder: D,
}

pub struct ConsumerSettings {
    pub control_file_path: String,
    pub sendfd_file_path: String,
    pub size_of_ringbuf: usize,
}

pub type ShareRingbufSet = Arc<DashMap<String, Ringbuf>>;

impl<D, Item> RingbufConsumer<D, Item>
where
    D: Decode<Item = Item> + 'static,
    Item: Send + Sync + 'static,
{
    pub async fn new(
        settings: ConsumerSettings,
        decoder: D,
    ) -> (Self, Receiver<Item>) {
        let rings = DashMap::new();
        let (send, recv) = channel(1024);

        let consumer = RingbufConsumer {
            rings: Arc::new(rings),
            decoder,
            sender: send,
        };

        let rings = consumer.rings.clone();
        tokio::spawn(async move {
            let mut server: FdRecvServer<Sleep> =
                FdRecvServer::new(settings.control_file_path, rings);
            server.run().await.unwrap();
        });

        (consumer, recv)
    }

    // TODO: Only for test, remove it later.
    pub fn from_fs(
        file: fs::File,
        data_size: usize,
        decoder: D,
    ) -> (Self, Receiver<Item>) {
        let ring = Ringbuf::from_raw(file, data_size).unwrap();
        let rings = DashMap::new();
        rings.insert("1".to_string(), ring);
        let (send, recv) = channel(1024);

        let consumer = RingbufConsumer {
            rings: Arc::new(rings),
            decoder,
            sender: send,
        };

        (consumer, recv)
    }

    pub async fn start_process_loop(&mut self) {
        loop {
            self.process_ringbufs().await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn process_ringbufs(&mut self) -> Result<()> {
        for mut ring in self.rings.iter_mut() {
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
            // TODO: handle the error
            self.sender.send(item).await.unwrap();
            ringbuf.advance_consume_offset(data_block.total_len())
        }
        Ok(())
    }
}

pub trait Decode: Send + Sync {
    type Item;

    fn decode(&self, data: &[u8], ctx: Context) -> Result<Self::Item>;
}

pub struct Context {}

pub struct ToStringDecoder;

impl Decode for ToStringDecoder {
    type Item = String;

    fn decode(&self, data: &[u8], _ctx: Context) -> Result<Self::Item> {
        Ok(String::from_utf8_lossy(data).to_string())
    }
}

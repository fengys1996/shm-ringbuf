use shm_ringbuf::consumer::ConsumerSettings;
use shm_ringbuf::consumer::RingbufConsumer;
use shm_ringbuf::consumer::ToStringDecoder;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let settings = ConsumerSettings {
        control_file_path: "/tmp/1.txt".to_string(),
        sendfd_file_path: "/tmp/1.txt".to_string(),
        size_of_ringbuf: 1024 * 20,
    };
    let (mut consumer, mut recv) =
        RingbufConsumer::new(settings, ToStringDecoder).await;

    tokio::spawn(async move {
        consumer.start_process_loop().await;
    });

    while let Some(item) = recv.recv().await {
        info!("{:?}", item);
    }
}

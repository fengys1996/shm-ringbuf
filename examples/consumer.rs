use std::fs;
use std::time::Duration;

use shm_ringbuf::consumer::decode::ToStringDecoder;
use shm_ringbuf::consumer::ConsumerSettings;
use shm_ringbuf::consumer::RingbufConsumer;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let control_sock_path = "/tmp/1.txt";
    let sendfd_sock_path = "/tmp/2.txt";

    let _ = fs::remove_file(control_sock_path);
    let _ = fs::remove_file(sendfd_sock_path);

    let settings = ConsumerSettings {
        control_sock_path: control_sock_path.to_string(),
        sendfd_sock_path: sendfd_sock_path.to_string(),
        size_of_ringbuf: 1024 * 20,
        process_duration: Duration::from_secs(1),
    };

    let mut recv =
        RingbufConsumer::start_consume(settings, ToStringDecoder).await;

    tokio::spawn(async move {
        while let Some(item) = recv.recv().await {
            info!("{:?}", item);
        }
    });

    tokio::time::sleep(Duration::from_secs(100)).await;
}

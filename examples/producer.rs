use shm_ringbuf::producer::ProducerSettings;
use shm_ringbuf::producer::RingbufProducer;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let settings = ProducerSettings {
        control_file_path: "/tmp/1.txt".to_string(),
        sendfd_file_path: "/tmp/1.txt".to_string(),
        size_of_ringbuf: 1024 * 20,
    };

    let mut producer = RingbufProducer::new(settings).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    for i in 0..10000 {
        let mut pre_alloc = producer.reserve(20).unwrap();
        let write_str = format!("hello{}", i);
        pre_alloc.write(write_str.as_bytes()).unwrap();
        // info!("write: {}", write_str);
        pre_alloc.commit().await;
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
    }
}

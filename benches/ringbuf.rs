#[cfg(feature = "benchmark")]
criterion::criterion_group!(
    benches,
    benches::criterion_bench_ringbuf_write,
    benches::criterion_bench_ringbuf_write_and_read
);
#[cfg(feature = "benchmark")]
criterion::criterion_main!(benches);

#[cfg(feature = "benchmark")]
pub mod benches {
    use criterion::Criterion;
    use shm_ringbuf::error;
    use shm_ringbuf::Ringbuf;

    fn bench_ringbuf_write() {
        write_ringbuf_until_full();
    }

    fn bench_ringbuf_write_and_read() {
        let mut ringbuf = write_ringbuf_until_full();
        peek_ringbuf_until_empty(&mut ringbuf);
    }

    fn write_ringbuf_until_full() -> Ringbuf {
        let data_size = 1024 * 32_u64;

        let file = tempfile::tempfile().unwrap();
        file.set_len(data_size).unwrap();

        let mut ringbuf = Ringbuf::new(&file).unwrap();
        let mut i = 1;
        loop {
            let result = ringbuf.reserve(20, i);
            if matches!(result, Err(error::Error::NotEnoughSpace { .. })) {
                break;
            }
            let mut pre_alloc = result.unwrap();
            let write_str = format!("hello, {}", i);
            pre_alloc.write(write_str.as_bytes()).unwrap();
            pre_alloc.commit();
            i += 1;
        }
        ringbuf
    }

    fn peek_ringbuf_until_empty(ringbuf: &mut Ringbuf) {
        loop {
            let Some(data_block) = ringbuf.peek() else {
                break;
            };

            if ringbuf.peek().is_none() {
                break;
            }

            unsafe {
                ringbuf.advance_consume_offset(data_block.total_len());
            }
        }
    }

    pub fn criterion_bench_ringbuf_write(c: &mut Criterion) {
        c.bench_function("ringbuf_write", |b| b.iter(bench_ringbuf_write));
    }

    pub fn criterion_bench_ringbuf_write_and_read(c: &mut Criterion) {
        c.bench_function("ringbuf_write_and_read", |b| {
            b.iter(bench_ringbuf_write_and_read)
        });
    }
}

#[cfg(not(feature = "benchmark"))]
fn main() {
    // Benchmarks are disabled. Enable the 'benchmarks' feature to run benchmarks.
}

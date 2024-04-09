fn main() {
    tonic_build::configure()
        .compile(&["proto/shm.proto"], &["proto"])
        .expect("compile proto");
}

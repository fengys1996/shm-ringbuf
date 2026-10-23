fn main() {
    tonic_prost_build::compile_protos("proto/shm.proto")
        .expect("compile proto");
}

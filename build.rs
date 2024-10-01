fn main() {
    tonic_build::compile_protos("proto/shm.proto").expect("compile proto");
}

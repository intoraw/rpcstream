use std::path::PathBuf;
use tonic_build::{self};

fn main() {
    let out_dir = PathBuf::from("./src/generated");
    tonic_build::configure()
        .out_dir(out_dir)
        .compile(&["proto/rpcstream.proto"], &["proto"])
        .unwrap();
}

extern crate prost_build;

fn main() {
    println!("cargo:rerun-if-changed=src/entry.proto");
    prost_build::compile_protos(&["src/entry.proto"], &["src/"]).unwrap();
}

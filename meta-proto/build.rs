//! Compile the BrewFS metadata service protobuf contract.
//!
//! Requires a system protoc (CI installs protobuf-compiler; macOS uses brew).
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let proto_dir = std::path::Path::new(&manifest_dir).join("../proto");
    let proto_file = proto_dir.join("brewfs/meta/v1/meta_service.proto");

    println!("cargo:rerun-if-changed={}", proto_file.display());

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto_file], &[proto_dir])?;
    Ok(())
}

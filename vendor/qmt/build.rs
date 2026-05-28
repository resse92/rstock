fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    let proto_dir = std::path::Path::new("proto");
    let protos = [
        proto_dir.join("common.proto"),
        proto_dir.join("health.proto"),
        proto_dir.join("trading.proto"),
        proto_dir.join("data.proto"),
    ];

    tonic_build::configure()
        .build_server(false)
        .compile_protos(&protos, &[proto_dir])?;

    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    Ok(())
}

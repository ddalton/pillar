fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile CSI protobuf
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile(
            &["proto/csi.proto"],
            &["proto/"],
        )?;

    println!("cargo:rerun-if-changed=proto/csi.proto");

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // List of protobuf files to compile
    let proto_files = vec![
        // HDFS protos
        "proto/hdfs.proto",
        "proto/ClientNamenodeProtocol.proto",
        "proto/ClientDatanodeProtocol.proto",
        "proto/DatanodeProtocol.proto",
        "proto/HdfsServer.proto",
        "proto/acl.proto",
        "proto/encryption.proto",
        "proto/erasurecoding.proto",
        "proto/xattr.proto",
        "proto/datatransfer.proto",
        "proto/inotify.proto",
        "proto/ReconfigurationProtocol.proto",
        // Common protos (required by HDFS protos)
        "proto/Security.proto",
        "proto/HAServiceProtocol.proto",
        "proto/RpcHeader.proto",
        "proto/IpcConnectionContext.proto",
        "proto/ProtobufRpcEngine.proto",
    ];

    // Configure tonic-build
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&proto_files, &["proto/"])?;

    // Tell Cargo to rerun this build script if proto files change
    for file in &proto_files {
        println!("cargo:rerun-if-changed={}", file);
    }

    Ok(())
}

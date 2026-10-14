//! Hadoop IPC server for `ClientProtocol`, so stock HDFS clients
//! (`hadoop fs`, Spark, hdfs-native, ...) can talk to the NameNode.
//!
//! Hadoop clients do not speak gRPC: they open a TCP connection, send the
//! `hrpc` preamble and then length-framed protobuf calls. Pillar serves
//! both on the client RPC port: [`accept_loop`] peeks at each new
//! connection and hands `hrpc` ones to the IPC engine
//! ([`pillar_common::ipc`]) and everything else (HTTP/2, i.e. gRPC) to tonic.

pub mod dispatch;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use pillar_proto::hadoop::hdfs::client_namenode_protocol_server::ClientNamenodeProtocol;

pub use pillar_common::ipc::{is_hadoop_ipc, Authentication, KerberosServer, RpcError, RpcHandler};

/// The protocol Hadoop clients name in `RequestHeaderProto`.
pub const CLIENT_PROTOCOL: &str = "org.apache.hadoop.hdfs.protocol.ClientProtocol";

/// Metadata key naming the Java exception a Status stands for, when no
/// status code maps to it (see [`hadoop_status`]).
const EXCEPTION_KEY: &str = "x-hadoop-exception";

/// A Status that Hadoop IPC clients receive as the Java exception `class`
/// (e.g. `org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException`).
pub fn hadoop_status(code: tonic::Code, class: &'static str, message: impl Into<String>) -> tonic::Status {
    let mut s = tonic::Status::new(code, message);
    s.metadata_mut().insert(EXCEPTION_KEY, tonic::metadata::MetadataValue::from_static(class));
    s
}

/// The effective user of the Hadoop IPC connection a request came in on
/// (from its connection context; SIMPLE auth), attached to every request's
/// extensions.
#[derive(Debug, Clone)]
pub struct CallerUser(pub String);

/// Map a service error onto the exception a Hadoop client unwraps.
pub fn rpc_error(s: tonic::Status) -> RpcError {
    use tonic::Code;
    let msg = s.message();
    if let Some(class) = s.metadata().get(EXCEPTION_KEY).and_then(|v| v.to_str().ok()) {
        return RpcError::application(class, msg);
    }
    let lower = msg.to_ascii_lowercase();
    let class = match s.code() {
        Code::NotFound => "java.io.FileNotFoundException",
        Code::AlreadyExists => "org.apache.hadoop.fs.FileAlreadyExistsException",
        Code::PermissionDenied | Code::Unauthenticated => "org.apache.hadoop.security.AccessControlException",
        Code::Unimplemented => "java.lang.UnsupportedOperationException",
        Code::InvalidArgument | Code::FailedPrecondition if lower.contains("not empty") => {
            "org.apache.hadoop.fs.PathIsNotEmptyDirectoryException"
        }
        Code::InvalidArgument | Code::FailedPrecondition if lower.contains("not a directory") => {
            "org.apache.hadoop.fs.ParentNotDirectoryException"
        }
        Code::InvalidArgument => "org.apache.hadoop.HadoopIllegalArgumentException",
        _ => "java.io.IOException",
    };
    RpcError::application(class, msg)
}

/// `ClientProtocol` over the tonic service, for the IPC engine.
pub struct ClientProtocol<T> {
    svc: Arc<T>,
    auth: Arc<Authentication>,
}

#[async_trait]
impl<T: ClientNamenodeProtocol> RpcHandler for ClientProtocol<T> {
    fn protocol(&self) -> &'static str {
        CLIENT_PROTOCOL
    }

    fn authentication(&self) -> &Authentication {
        &self.auth
    }

    async fn call(&self, method: &str, body: &[u8], user: &str) -> Result<Vec<u8>, RpcError> {
        dispatch::dispatch(&*self.svc, method, body, user).await
    }
}

// ------------------------------------------------------------- listener

/// Accept connections on the client RPC port: Hadoop IPC (`hrpc`) is served
/// here, anything else is passed to `grpc` (tonic's incoming stream).
pub async fn accept_loop<T>(
    listener: TcpListener,
    svc: Arc<T>,
    auth: Arc<Authentication>,
    grpc: mpsc::Sender<std::io::Result<TcpStream>>,
) where
    T: ClientNamenodeProtocol,
{
    let handler = Arc::new(ClientProtocol { svc, auth });
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                warn!("accept failed: {}", e);
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let handler = handler.clone();
        let grpc = grpc.clone();
        tokio::spawn(async move {
            match is_hadoop_ipc(&stream).await {
                Ok(true) => {
                    debug!("Hadoop IPC connection from {}", peer);
                    if let Err(e) = pillar_common::ipc::serve_connection(stream, handler, peer).await {
                        debug!("Hadoop IPC connection from {} ended: {}", peer, e);
                    }
                }
                Ok(false) => {
                    let _ = grpc.send(Ok(stream)).await;
                }
                Err(e) => debug!("connection from {} closed before speaking: {}", peer, e),
            }
        });
    }
}

/// Bind `addr` and serve Hadoop IPC and gRPC on it. Returns the stream of
/// connections tonic should serve.
pub async fn bind_shared<T: ClientNamenodeProtocol>(
    addr: SocketAddr,
    svc: Arc<T>,
) -> std::io::Result<tokio_stream::wrappers::ReceiverStream<std::io::Result<TcpStream>>> {
    bind_shared_with(addr, svc, Arc::new(Authentication::Simple)).await
}

/// [`bind_shared`], with Hadoop IPC callers authenticated by `auth`.
pub async fn bind_shared_with<T: ClientNamenodeProtocol>(
    addr: SocketAddr,
    svc: Arc<T>,
    auth: Arc<Authentication>,
) -> std::io::Result<tokio_stream::wrappers::ReceiverStream<std::io::Result<TcpStream>>> {
    let listener = TcpListener::bind(addr).await?;
    let mode = match &*auth {
        Authentication::Simple => "SIMPLE".to_string(),
        Authentication::Kerberos(k) => format!("Kerberos as {}", k.principal()),
    };
    info!("Client RPC port {}: Hadoop IPC ({}) and gRPC", listener.local_addr()?, mode);
    let (tx, rx) = mpsc::channel(256);
    tokio::spawn(accept_loop(listener, svc, auth, tx));
    Ok(tokio_stream::wrappers::ReceiverStream::new(rx))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_client_protocol_method_is_routable() {
        assert_eq!(dispatch::METHODS.len(), 108);
        for m in ["getFileInfo", "mkdirs", "create", "addBlock", "complete", "getBlockLocations", "getListing",
            "delete", "rename", "rename2", "getServerDefaults", "renewLease", "setPermission", "getFsStats"]
        {
            assert!(dispatch::METHODS.contains(&m), "{m} missing");
        }
    }

    #[test]
    fn status_codes_map_to_the_exceptions_hadoop_unwraps() {
        let c = |code, msg: &str| rpc_error(tonic::Status::new(code, msg)).exception_class;
        assert_eq!(c(tonic::Code::NotFound, "x"), "java.io.FileNotFoundException");
        assert_eq!(c(tonic::Code::AlreadyExists, "x"), "org.apache.hadoop.fs.FileAlreadyExistsException");
        assert_eq!(c(tonic::Code::InvalidArgument, "Directory not empty"), "org.apache.hadoop.fs.PathIsNotEmptyDirectoryException");
        assert_eq!(c(tonic::Code::Internal, "x"), "java.io.IOException");
    }
}

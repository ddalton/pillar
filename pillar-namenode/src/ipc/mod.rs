//! Hadoop IPC server for `ClientProtocol`, so stock HDFS clients
//! (`hadoop fs`, Spark, hdfs-native, ...) can talk to the NameNode.
//!
//! Hadoop clients do not speak gRPC: they open a TCP connection, send the
//! `hrpc` preamble and then length-framed protobuf calls. Pillar serves
//! both on the client RPC port: [`accept_loop`] peeks at each new
//! connection and hands `hrpc` ones to [`serve_connection`] and everything
//! else (HTTP/2, i.e. gRPC) to tonic.
//!
//! ```text
//! preamble  "hrpc" version(u8=9) serviceClass(u8) authProtocol(u8: 0 NONE | 0xDF SASL)
//! call      len(u32) <delimited RpcRequestHeaderProto> <delimited RequestHeaderProto>
//!           <delimited request>
//! reply     len(u32) <delimited RpcResponseHeaderProto> [<delimited response>]
//! ```
//!
//! Special call ids: -33 SASL, -3 connection context, -4 ping. Only SIMPLE
//! authentication is supported: a SASL `NEGOTIATE` is answered with
//! `SUCCESS`, exactly as a Hadoop server with only SIMPLE enabled does.
//! Calls are dispatched concurrently and answered as they complete.

pub mod dispatch;

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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use pillar_proto::hadoop::common::{
    rpc_response_header_proto::{RpcErrorCodeProto, RpcStatusProto},
    rpc_sasl_proto::SaslState,
    IpcConnectionContextProto, RequestHeaderProto, RpcRequestHeaderProto, RpcResponseHeaderProto, RpcSaslProto,
};
use pillar_proto::hadoop::hdfs::client_namenode_protocol_server::ClientNamenodeProtocol;

/// `RpcConstants.CURRENT_VERSION`.
pub const IPC_VERSION: u8 = 9;
const AUTH_PROTOCOL_NONE: u8 = 0;
const AUTH_PROTOCOL_SASL: u8 = 0xDF; // -33

const CALL_ID_SASL: i32 = -33;
const CALL_ID_CONNECTION_CONTEXT: i32 = -3;
const CALL_ID_PING: i32 = -4;
/// `RpcRequestHeaderProto.OperationProto.RPC_CLOSE_CONNECTION`.
const RPC_OP_CLOSE_CONNECTION: i32 = 2;

/// The protocol Hadoop clients name in `RequestHeaderProto`.
pub const CLIENT_PROTOCOL: &str = "org.apache.hadoop.hdfs.protocol.ClientProtocol";

/// `ipc.maximum.data.length` default.
const MAX_FRAME: usize = 128 * 1024 * 1024;

/// A failed call, as Hadoop reports it: a Java exception class and message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcError {
    pub exception_class: String,
    pub message: String,
    pub code: RpcErrorCodeProto,
}

impl RpcError {
    fn new(class: &str, message: impl Into<String>, code: RpcErrorCodeProto) -> Self {
        RpcError { exception_class: class.to_string(), message: message.into(), code }
    }

    pub fn no_such_method(method: &str) -> Self {
        Self::new(
            "org.apache.hadoop.ipc.RpcNoSuchMethodException",
            format!("Unknown method {} called on {}", method, CLIENT_PROTOCOL),
            RpcErrorCodeProto::ErrorNoSuchMethod,
        )
    }

    pub fn bad_request(method: &str, e: prost::DecodeError) -> Self {
        Self::new("java.io.IOException", format!("cannot decode {} request: {}", method, e), RpcErrorCodeProto::ErrorApplication)
    }

    /// Map a service error onto the exception a Hadoop client unwraps.
    pub fn from_status(s: tonic::Status) -> Self {
        use tonic::Code;
        let msg = s.message();
        if let Some(class) = s.metadata().get(EXCEPTION_KEY).and_then(|v| v.to_str().ok()) {
            return Self::new(class, msg, RpcErrorCodeProto::ErrorApplication);
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
        Self::new(class, msg, RpcErrorCodeProto::ErrorApplication)
    }
}

// ------------------------------------------------------------- listener

/// Accept connections on the client RPC port: Hadoop IPC (`hrpc`) is served
/// here, anything else is passed to `grpc` (tonic's incoming stream).
pub async fn accept_loop<T>(listener: TcpListener, svc: Arc<T>, grpc: mpsc::Sender<std::io::Result<TcpStream>>)
where
    T: ClientNamenodeProtocol,
{
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
        let svc = svc.clone();
        let grpc = grpc.clone();
        tokio::spawn(async move {
            match is_hadoop_ipc(&stream).await {
                Ok(true) => {
                    debug!("Hadoop IPC connection from {}", peer);
                    if let Err(e) = serve_connection(stream, svc, peer).await {
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

/// Peek (without consuming) for the 4-byte `hrpc` magic. gRPC clients open
/// with the HTTP/2 preface `PRI `, so the first four octets decide.
async fn is_hadoop_ipc(stream: &TcpStream) -> std::io::Result<bool> {
    let mut buf = [0u8; 4];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let n = tokio::time::timeout_at(deadline, stream.peek(&mut buf))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "no preamble"))??;
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        if n >= 4 || buf[..n] != b"hrpc"[..n] {
            return Ok(n >= 4 && &buf == b"hrpc");
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

// ------------------------------------------------------------ connection

fn take_delimited<M: Message + Default>(buf: &mut Bytes) -> Result<M, String> {
    M::decode_length_delimited(&mut *buf).map_err(|e| e.to_string())
}

/// A length-delimited message's raw bytes (the request body is decoded
/// later, by the method it is addressed to).
fn take_delimited_bytes(buf: &mut Bytes) -> Result<Bytes, String> {
    let len = prost::encoding::decode_varint(&mut *buf).map_err(|e| e.to_string())? as usize;
    if len > buf.len() {
        return Err(format!("message of {} bytes in a {} byte frame", len, buf.len()));
    }
    Ok(buf.split_to(len))
}

fn frame(header: &RpcResponseHeaderProto, body: Option<&[u8]>) -> Bytes {
    let mut payload = header.encode_length_delimited_to_vec();
    if let Some(b) = body {
        prost::encoding::encode_varint(b.len() as u64, &mut payload);
        payload.extend_from_slice(b);
    }
    let mut out = BytesMut::with_capacity(4 + payload.len());
    out.put_u32(payload.len() as u32);
    out.put_slice(&payload);
    out.freeze()
}

fn response_header(req: &RpcRequestHeaderProto, status: RpcStatusProto) -> RpcResponseHeaderProto {
    RpcResponseHeaderProto {
        // uint32 on the wire: special (negative) ids keep their bit pattern.
        call_id: req.call_id as u32,
        status: status as i32,
        server_ipc_version_num: Some(IPC_VERSION as u32),
        client_id: Some(req.client_id.clone()),
        retry_count: Some(req.retry_count.unwrap_or(-1)),
        ..Default::default()
    }
}

fn error_frame(req: &RpcRequestHeaderProto, e: &RpcError) -> Bytes {
    let mut h = response_header(req, RpcStatusProto::Error);
    h.exception_class_name = Some(e.exception_class.clone());
    h.error_msg = Some(e.message.clone());
    h.error_detail = Some(e.code as i32);
    frame(&h, None)
}

/// Serve one Hadoop IPC connection until the client closes it.
pub async fn serve_connection<S, T>(stream: S, svc: Arc<T>, peer: SocketAddr) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
    T: ClientNamenodeProtocol,
{
    let (mut rd, mut wr) = tokio::io::split(stream);

    let mut preamble = [0u8; 7];
    rd.read_exact(&mut preamble).await?;
    if &preamble[..4] != b"hrpc" || preamble[4] != IPC_VERSION {
        warn!("{}: unsupported IPC preamble {:02x?}", peer, preamble);
        return Ok(());
    }
    let auth = preamble[6];
    if auth != AUTH_PROTOCOL_NONE && auth != AUTH_PROTOCOL_SASL {
        warn!("{}: unsupported auth protocol {:#x}", peer, auth);
        return Ok(());
    }

    // Replies may complete out of order; one writer task owns the socket.
    let (out_tx, mut out_rx) = mpsc::channel::<Bytes>(64);
    let writer = tokio::spawn(async move {
        while let Some(frame) = out_rx.recv().await {
            wr.write_all(&frame).await?;
            wr.flush().await?;
        }
        Ok::<_, std::io::Error>(())
    });

    let mut user = String::new();
    let result: std::io::Result<()> = async {
        loop {
            let len = match rd.read_u32().await {
                Ok(n) => n as usize,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e),
            };
            if len > MAX_FRAME {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{} byte RPC frame", len)));
            }
            let mut buf = BytesMut::zeroed(len);
            rd.read_exact(&mut buf).await?;
            let mut buf = buf.freeze();

            let header: RpcRequestHeaderProto = take_delimited(&mut buf)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("RPC header: {}", e)))?;
            if header.rpc_op == Some(RPC_OP_CLOSE_CONNECTION) {
                return Ok(());
            }

            match header.call_id {
                CALL_ID_SASL => {
                    let sasl: RpcSaslProto = take_delimited(&mut buf)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                    // SIMPLE-only: NEGOTIATE succeeds at once; anything else
                    // (a client insisting on Kerberos) is refused.
                    if sasl.state == SaslState::Negotiate as i32 {
                        let reply = RpcSaslProto { state: SaslState::Success as i32, ..Default::default() };
                        let _ = out_tx.send(frame(&response_header(&header, RpcStatusProto::Success), Some(&reply.encode_to_vec()))).await;
                    } else {
                        let e = RpcError::new(
                            "org.apache.hadoop.security.AccessControlException",
                            "Pillar supports only SIMPLE authentication",
                            RpcErrorCodeProto::FatalUnauthorized,
                        );
                        let mut h = response_header(&header, RpcStatusProto::Fatal);
                        h.exception_class_name = Some(e.exception_class);
                        h.error_msg = Some(e.message);
                        h.error_detail = Some(e.code as i32);
                        let _ = out_tx.send(frame(&h, None)).await;
                        return Ok(());
                    }
                }
                CALL_ID_CONNECTION_CONTEXT => {
                    let ctx: IpcConnectionContextProto = take_delimited(&mut buf)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                    user = ctx.user_info.and_then(|u| u.effective_user).unwrap_or_default();
                    debug!("{}: connection context user={:?} protocol={:?}", peer, user, ctx.protocol);
                }
                CALL_ID_PING => {}
                id if id >= 0 => {
                    let parsed = take_delimited::<RequestHeaderProto>(&mut buf)
                        .and_then(|rh| take_delimited_bytes(&mut buf).map(|body| (rh, body)));
                    let (rh, body) = match parsed {
                        Ok(v) => v,
                        Err(e) => {
                            let err = RpcError::new("java.io.IOException", format!("bad request: {}", e), RpcErrorCodeProto::ErrorApplication);
                            let _ = out_tx.send(error_frame(&header, &err)).await;
                            continue;
                        }
                    };
                    if rh.declaring_class_protocol_name != CLIENT_PROTOCOL {
                        let err = RpcError::new(
                            "org.apache.hadoop.ipc.RpcNoSuchProtocolException",
                            format!("Unknown protocol: {}", rh.declaring_class_protocol_name),
                            RpcErrorCodeProto::ErrorNoSuchProtocol,
                        );
                        let _ = out_tx.send(error_frame(&header, &err)).await;
                        continue;
                    }
                    let svc = svc.clone();
                    let out_tx = out_tx.clone();
                    let user = user.clone();
                    tokio::spawn(async move {
                        let reply = match dispatch::dispatch(&*svc, &rh.method_name, &body, &user).await {
                            Ok(resp) => frame(&response_header(&header, RpcStatusProto::Success), Some(&resp)),
                            Err(e) => {
                                debug!("{} -> {}: {}", rh.method_name, e.exception_class, e.message);
                                error_frame(&header, &e)
                            }
                        };
                        let _ = out_tx.send(reply).await;
                    });
                }
                id => debug!("{}: ignoring call id {}", peer, id),
            }
        }
    }
    .await;

    drop(out_tx);
    let _ = writer.await;
    if !user.is_empty() {
        debug!("Hadoop IPC connection from {} ({}) closed", peer, user);
    }
    result
}

/// Bind `addr` and serve Hadoop IPC and gRPC on it. Returns the stream of
/// connections tonic should serve.
pub async fn bind_shared<T: ClientNamenodeProtocol>(
    addr: SocketAddr,
    svc: Arc<T>,
) -> std::io::Result<tokio_stream::wrappers::ReceiverStream<std::io::Result<TcpStream>>> {
    let listener = TcpListener::bind(addr).await?;
    info!("Client RPC port {}: Hadoop IPC and gRPC", listener.local_addr()?);
    let (tx, rx) = mpsc::channel(256);
    tokio::spawn(accept_loop(listener, svc, tx));
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
        let c = |code, msg: &str| RpcError::from_status(tonic::Status::new(code, msg)).exception_class;
        assert_eq!(c(tonic::Code::NotFound, "x"), "java.io.FileNotFoundException");
        assert_eq!(c(tonic::Code::AlreadyExists, "x"), "org.apache.hadoop.fs.FileAlreadyExistsException");
        assert_eq!(c(tonic::Code::InvalidArgument, "Directory not empty"), "org.apache.hadoop.fs.PathIsNotEmptyDirectoryException");
        assert_eq!(c(tonic::Code::Internal, "x"), "java.io.IOException");
    }

    #[test]
    fn negative_call_ids_keep_their_bits_in_the_uint32_reply() {
        let req = RpcRequestHeaderProto { call_id: -33, ..Default::default() };
        assert_eq!(response_header(&req, RpcStatusProto::Success).call_id, 0xFFFF_FFDF);
    }

    #[test]
    fn frames_are_length_prefixed_delimited_messages() {
        let req = RpcRequestHeaderProto { call_id: 7, client_id: vec![1; 16], ..Default::default() };
        let f = frame(&response_header(&req, RpcStatusProto::Success), Some(b"abc"));
        let len = u32::from_be_bytes(f[..4].try_into().unwrap()) as usize;
        assert_eq!(len, f.len() - 4);
        let mut rest = f.slice(4..);
        let h: RpcResponseHeaderProto = take_delimited(&mut rest).unwrap();
        assert_eq!(h.call_id, 7);
        assert_eq!(&take_delimited_bytes(&mut rest).unwrap()[..], b"abc");
        assert!(!rest.has_remaining());
    }
}

//! Hadoop IPC server core (the RPC engine of `hadoop fs`, DFSClient,
//! hdfs-native, ...), shared by the NameNode (`ClientProtocol`) and the
//! DataNode (`ClientDatanodeProtocol`). A protocol is an [`RpcHandler`]:
//! the name clients declare, and a call by method name.
//!
//! ```text
//! preamble  "hrpc" version(u8=9) serviceClass(u8) authProtocol(u8: 0 NONE | 0xDF SASL)
//! call      len(u32) <delimited RpcRequestHeaderProto> <delimited RequestHeaderProto>
//!           <delimited request>
//! reply     len(u32) <delimited RpcResponseHeaderProto> [<delimited response>]
//! ```
//!
//! Special call ids: -33 SASL, -3 connection context, -4 ping. Calls are
//! dispatched concurrently and answered as they complete.
//!
//! Authentication ([`Authentication`]) is SIMPLE (the client says who it
//! is; a SASL `NEGOTIATE` is answered with `SUCCESS`, as a Hadoop server
//! with only SIMPLE enabled does) or Kerberos: SASL GSSAPI, as Hadoop's
//! `SaslRpcServer` (`hadoop.security.authentication=kerberos`, QOP
//! `authentication`):
//!
//! ```text
//! C NEGOTIATE                  S NEGOTIATE auths=[KERBEROS GSSAPI <protocol>/<server id>]
//! C INITIATE  AP-REQ           S CHALLENGE AP-REP           (mutual authentication)
//! C RESPONSE  (empty)          S CHALLENGE wrap(offer)      (security layer: auth only)
//! C RESPONSE  wrap(choice)     S SUCCESS
//! ```
//!
//! The caller is then the authenticated principal, mapped to a short name
//! by Hadoop's DEFAULT `auth_to_local` rule; a connection context naming
//! anyone else (impersonation, which needs proxy-user rules Pillar does not
//! have) is refused, as is any call before authentication.
//!
//! A protocol may also accept tokens ([`RpcHandler::tokens`]; the DataNode
//! takes block access tokens): NEGOTIATE then offers `TOKEN DIGEST-MD5`
//! first, with its challenge, and the caller is whoever the token names.
//!
//! ```text
//! C NEGOTIATE                  S NEGOTIATE auths=[TOKEN DIGEST-MD5 challenge, ...]
//! C INITIATE  digest-response  S SUCCESS rspauth
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::digest_md5::DigestServer;
use pillar_proto::hadoop::common::{
    rpc_response_header_proto::{RpcErrorCodeProto, RpcStatusProto},
    rpc_sasl_proto::{self, SaslState},
    IpcConnectionContextProto, RequestHeaderProto, RpcRequestHeaderProto, RpcResponseHeaderProto, RpcSaslProto,
};

/// `RpcConstants.CURRENT_VERSION`.
pub const IPC_VERSION: u8 = 9;
const AUTH_PROTOCOL_NONE: u8 = 0;
const AUTH_PROTOCOL_SASL: u8 = 0xDF; // -33

const CALL_ID_SASL: i32 = -33;
const CALL_ID_CONNECTION_CONTEXT: i32 = -3;
const CALL_ID_PING: i32 = -4;
/// `RpcRequestHeaderProto.OperationProto.RPC_CLOSE_CONNECTION`.
const RPC_OP_CLOSE_CONNECTION: i32 = 2;

/// `ipc.maximum.data.length` default.
const MAX_FRAME: usize = 128 * 1024 * 1024;

/// How a server authenticates its callers.
pub enum Authentication {
    /// SIMPLE: the caller is whoever its connection context says.
    Simple,
    /// Kerberos (SASL GSSAPI).
    Kerberos(KerberosServer),
}

static SIMPLE: Authentication = Authentication::Simple;

/// Checks tokens presented with SASL DIGEST-MD5 (Hadoop's TOKEN method).
pub trait TokenAuthority: Send + Sync {
    /// Whether tokens are accepted at all (offered in NEGOTIATE).
    fn enabled(&self) -> bool;

    /// For a token identifier: the caller it authenticates and the token's
    /// password, if the token is valid.
    fn password(&self, identifier: &[u8]) -> Option<(String, Vec<u8>)>;
}

/// Hadoop's SASL realm and server id for DIGEST-MD5 (`SASL_DEFAULT_REALM`).
const SASL_DEFAULT_REALM: &str = "default";

/// A Kerberos service: its principal (`<service>/<host>@<REALM>`) and the
/// keytab holding its keys.
pub struct KerberosServer {
    keytab: krb5::Keytab,
    principal: String,
    /// `<service>`: what Hadoop calls the SASL protocol (`nn`, `dn`).
    protocol: String,
    /// `<host>`: the SASL server id.
    server_id: String,
    /// Principals of this realm map to short names (DEFAULT rule).
    realm: String,
}

impl KerberosServer {
    /// `principal` must be `<service>/<host>@<REALM>` with a key in the keytab.
    pub fn new(keytab: &std::path::Path, principal: &str) -> std::io::Result<Self> {
        let invalid = |m: String| std::io::Error::new(std::io::ErrorKind::InvalidInput, m);
        let (name, realm) = principal
            .split_once('@')
            .ok_or_else(|| invalid(format!("principal {} has no realm", principal)))?;
        let (protocol, server_id) = name
            .split_once('/')
            .ok_or_else(|| invalid(format!("principal {} is not <service>/<host>@<REALM>", principal)))?;
        let keytab = krb5::Keytab::load(keytab)
            .map_err(|e| invalid(format!("keytab {}: {}", keytab.display(), e)))?;
        if keytab.find_key(principal).is_none() {
            return Err(invalid(format!("the keytab has no key for {}", principal)));
        }
        Ok(Self {
            keytab,
            principal: principal.to_string(),
            protocol: protocol.to_string(),
            server_id: server_id.to_string(),
            realm: realm.to_string(),
        })
    }

    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Hadoop's DEFAULT `auth_to_local` rule: a principal of this realm is
    /// its first component (`alice@R`, `alice/host@R` -> `alice`); others
    /// have no local name.
    pub fn short_name(&self, principal: &str) -> Option<String> {
        let (name, realm) = principal.split_once('@')?;
        (realm == self.realm).then(|| name.split('/').next().unwrap_or(name).to_string()).filter(|n| !n.is_empty())
    }
}

/// A protocol served over Hadoop IPC.
#[async_trait]
pub trait RpcHandler: Send + Sync + 'static {
    /// The protocol name clients put in `RequestHeaderProto`.
    fn protocol(&self) -> &'static str;

    /// How callers authenticate (default SIMPLE).
    fn authentication(&self) -> &Authentication {
        &SIMPLE
    }

    /// Tokens the protocol accepts as credentials, besides `authentication`.
    fn tokens(&self) -> Option<&dyn TokenAuthority> {
        None
    }

    /// Run `method` on the encoded request `body` for `user` (the
    /// connection's effective user); the encoded response.
    async fn call(&self, method: &str, body: &[u8], user: &str) -> Result<Vec<u8>, RpcError>;
}

/// A failed call, as Hadoop reports it: a Java exception class and message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcError {
    pub exception_class: String,
    pub message: String,
    pub code: RpcErrorCodeProto,
}

impl RpcError {
    pub fn new(class: &str, message: impl Into<String>, code: RpcErrorCodeProto) -> Self {
        RpcError { exception_class: class.to_string(), message: message.into(), code }
    }

    /// An application error: the Java exception `class` with `message`.
    pub fn application(class: &str, message: impl Into<String>) -> Self {
        Self::new(class, message, RpcErrorCodeProto::ErrorApplication)
    }

    pub fn no_such_method(method: &str, protocol: &str) -> Self {
        Self::new(
            "org.apache.hadoop.ipc.RpcNoSuchMethodException",
            format!("Unknown method {} called on {}", method, protocol),
            RpcErrorCodeProto::ErrorNoSuchMethod,
        )
    }

    pub fn bad_request(method: &str, e: prost::DecodeError) -> Self {
        Self::application("java.io.IOException", format!("cannot decode {} request: {}", method, e))
    }
}

/// Peek (without consuming) for the 4-byte `hrpc` magic. gRPC clients open
/// with the HTTP/2 preface `PRI `, so the first four octets decide.
pub async fn is_hadoop_ipc(stream: &TcpStream) -> std::io::Result<bool> {
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

/// Serve Hadoop IPC (only) on `listener` with `handler`.
pub async fn serve<H: RpcHandler>(listener: TcpListener, handler: Arc<H>) {
    if let Ok(addr) = listener.local_addr() {
        info!("Hadoop IPC ({}) on {}", handler.protocol(), addr);
    }
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
        tokio::spawn(async move {
            if let Err(e) = serve_connection(stream, handler, peer).await {
                debug!("Hadoop IPC connection from {} ended: {}", peer, e);
            }
        });
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

const ACCESS_CONTROL: &str = "org.apache.hadoop.security.AccessControlException";
const SIMPLE_NOT_ENABLED: &str = "SIMPLE authentication is not enabled.  Available:[KERBEROS]";

/// A fatal reply: the client gives up on the connection.
fn fatal_frame(req: &RpcRequestHeaderProto, class: &str, msg: &str) -> Bytes {
    let mut h = response_header(req, RpcStatusProto::Fatal);
    h.exception_class_name = Some(class.to_string());
    h.error_msg = Some(msg.to_string());
    h.error_detail = Some(RpcErrorCodeProto::FatalUnauthorized as i32);
    frame(&h, None)
}

/// SASL GSSAPI (RFC 4752) on the server side, step by step.
#[derive(Default)]
struct Gssapi {
    context: Option<krb5::KerberosContext>,
    /// The security-layer offer was sent; the next token is the choice.
    offered: bool,
    /// Set once the exchange completed.
    principal: Option<String>,
}

/// RFC 4752 security layer bit: no protection beyond authentication
/// (Hadoop's `hadoop.rpc.protection=authentication`).
const SASL_LAYER_NONE: u8 = 1;

impl Gssapi {
    fn step(&mut self, k: &KerberosServer, msg: &RpcSaslProto) -> Result<RpcSaslProto, String> {
        let challenge = |token: Vec<u8>| RpcSaslProto { state: SaslState::Challenge as i32, token: Some(token), ..Default::default() };
        match SaslState::try_from(msg.state) {
            Ok(SaslState::Negotiate) => Ok(RpcSaslProto {
                state: SaslState::Negotiate as i32,
                auths: vec![rpc_sasl_proto::SaslAuth {
                    method: "KERBEROS".into(),
                    mechanism: "GSSAPI".into(),
                    protocol: Some(k.protocol.clone()),
                    server_id: Some(k.server_id.clone()),
                    challenge: None,
                }],
                ..Default::default()
            }),
            Ok(SaslState::Initiate) => {
                if !msg.auths.iter().any(|a| a.mechanism == "GSSAPI") {
                    return Err("only the GSSAPI mechanism (Kerberos) is supported".into());
                }
                let token = msg.token.as_deref().unwrap_or_default();
                let (context, ap_rep) = krb5::KerberosContext::accept_token(&k.keytab, token)
                    .map_err(|e| format!("GSS initiate failed: {}", e))?;
                if context.service_principal != k.principal {
                    return Err(format!("ticket for {}, not {}", context.service_principal, k.principal));
                }
                self.context = Some(context);
                Ok(challenge(ap_rep))
            }
            Ok(SaslState::Response) if !self.offered => {
                // The initiator has the AP-REP; offer the security layers
                // (only "none"), wrapped (integrity, no confidentiality).
                let context = self.context.as_ref().ok_or("RESPONSE before INITIATE")?;
                let tokens = context.per_message_tokens().map_err(|e| e.to_string())?;
                let offer = tokens
                    .emit_wrap_unsealed(0, &[SASL_LAYER_NONE, 0, 0, 0], 0)
                    .map_err(|e| e.to_string())?;
                self.offered = true;
                Ok(challenge(offer))
            }
            Ok(SaslState::Response) => {
                let context = self.context.as_ref().ok_or("RESPONSE before INITIATE")?;
                let tokens = context.per_message_tokens().map_err(|e| e.to_string())?;
                let token = msg.token.as_deref().unwrap_or_default();
                let choice = tokens
                    .verify_wrap_unsealed(token)
                    .or_else(|_| tokens.unwrap(token))
                    .map_err(|e| format!("security layer choice: {}", e))?
                    .message;
                if choice.len() < 4 || choice[0] & SASL_LAYER_NONE == 0 {
                    return Err("the client chose a security layer this server does not offer".into());
                }
                // An authorization id, if any, must be the authenticated one.
                let authz = String::from_utf8_lossy(&choice[4..]).into_owned();
                if !authz.is_empty() && authz != context.client_principal {
                    return Err(format!("{} may not act as {}", context.client_principal, authz));
                }
                self.principal = Some(context.client_principal.clone());
                Ok(RpcSaslProto { state: SaslState::Success as i32, ..Default::default() })
            }
            _ => Err(format!("unexpected SASL state {}", msg.state)),
        }
    }
}

/// SASL DIGEST-MD5 with a token: the client's INITIATE answers the
/// challenge sent with NEGOTIATE. The username is the base64 token
/// identifier, the password the base64 token password.
fn token_step(d: &DigestServer, tokens: &dyn TokenAuthority, msg: &RpcSaslProto) -> Result<(String, RpcSaslProto), String> {
    use base64::Engine;
    let b64 = &base64::engine::general_purpose::STANDARD;
    let mut caller = None;
    let (_, rspauth) = d.respond(msg.token.as_deref().unwrap_or_default(), |username| {
        let identifier = b64.decode(username).ok()?;
        let (who, password) = tokens.password(&identifier)?;
        caller = Some(who);
        Some(b64.encode(password))
    })?;
    let caller = caller.ok_or("no caller")?;
    Ok((caller, RpcSaslProto { state: SaslState::Success as i32, token: Some(rspauth), ..Default::default() }))
}

/// Serve one Hadoop IPC connection until the client closes it.
pub async fn serve_connection<S, H>(stream: S, handler: Arc<H>, peer: SocketAddr) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
    H: RpcHandler,
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
    // Kerberos: the SASL exchange's state, and once it succeeded the
    // authenticated principal and its local name.
    let mut gssapi = Gssapi::default();
    let mut authenticated: Option<(String, Option<String>)> = None;
    // Token authentication: the exchange, and the caller once it succeeded.
    let mut digest: Option<DigestServer> = None;
    let mut token_caller: Option<String> = None;
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
                    let tokens = handler.tokens().filter(|t| t.enabled());
                    let negotiate = sasl.state == SaslState::Negotiate as i32;
                    let wants_digest = sasl.state == SaslState::Initiate as i32
                        && sasl.auths.first().map_or(false, |a| a.mechanism == "DIGEST-MD5");
                    let reply = match handler.authentication() {
                        _ if wants_digest => match (&digest, tokens) {
                            (Some(d), Some(t)) => token_step(d, t, &sasl).map(|(caller, step)| {
                                token_caller = Some(caller);
                                step
                            }),
                            _ => Err("token authentication is not enabled".to_string()),
                        },
                        // SIMPLE-only: NEGOTIATE succeeds at once; anything
                        // else (a client insisting on Kerberos) is refused.
                        // With tokens, SIMPLE is offered after TOKEN.
                        Authentication::Simple if negotiate => Ok(match tokens {
                            None => RpcSaslProto { state: SaslState::Success as i32, ..Default::default() },
                            Some(_) => RpcSaslProto {
                                state: SaslState::Negotiate as i32,
                                auths: vec![rpc_sasl_proto::SaslAuth { method: "SIMPLE".into(), mechanism: String::new(), ..Default::default() }],
                                ..Default::default()
                            },
                        }),
                        Authentication::Simple => Err("Pillar has only SIMPLE authentication enabled".to_string()),
                        Authentication::Kerberos(k) => match gssapi.step(k, &sasl) {
                            Ok(step) => {
                                if let Some(principal) = &gssapi.principal {
                                    authenticated = Some((principal.clone(), k.short_name(principal)));
                                }
                                Ok(step)
                            }
                            Err(e) => Err(e),
                        },
                    };
                    // TOKEN goes first in the offer, as in Hadoop.
                    let reply = match reply {
                        Ok(mut offer) if negotiate && tokens.is_some() => {
                            let d = digest.insert(DigestServer::new(SASL_DEFAULT_REALM, "", SASL_DEFAULT_REALM));
                            offer.auths.insert(0, rpc_sasl_proto::SaslAuth {
                                method: "TOKEN".into(),
                                mechanism: "DIGEST-MD5".into(),
                                protocol: Some(String::new()),
                                server_id: Some(SASL_DEFAULT_REALM.into()),
                                challenge: Some(d.challenge()),
                            });
                            Ok(offer)
                        }
                        r => r,
                    };
                    match reply {
                        Ok(reply) => {
                            let _ = out_tx.send(frame(&response_header(&header, RpcStatusProto::Success), Some(&reply.encode_to_vec()))).await;
                        }
                        Err(msg) => {
                            warn!("{}: authentication failed: {}", peer, msg);
                            let _ = out_tx.send(fatal_frame(&header, "javax.security.sasl.SaslException", &msg)).await;
                            return Ok(());
                        }
                    }
                }
                CALL_ID_CONNECTION_CONTEXT => {
                    let ctx: IpcConnectionContextProto = take_delimited(&mut buf)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                    let claimed = ctx.user_info.and_then(|u| u.effective_user).unwrap_or_default();
                    debug!("{}: connection context user={:?} protocol={:?}", peer, claimed, ctx.protocol);
                    match handler.authentication() {
                        // A token names the caller; Hadoop clients send no
                        // user in this case.
                        _ if token_caller.is_some() => {
                            let caller = token_caller.clone().unwrap();
                            if !claimed.is_empty() && claimed != caller {
                                let msg = format!("Authenticated user ({}) doesn't match what the client claims to be ({})", caller, claimed);
                                let _ = out_tx.send(fatal_frame(&header, ACCESS_CONTROL, &msg)).await;
                                return Ok(());
                            }
                            user = caller;
                        }
                        Authentication::Simple => user = claimed,
                        Authentication::Kerberos(_) => {
                            let Some((principal, short)) = &authenticated else {
                                let _ = out_tx.send(fatal_frame(&header, ACCESS_CONTROL, SIMPLE_NOT_ENABLED)).await;
                                return Ok(());
                            };
                            let Some(short) = short else {
                                let msg = format!("No auth_to_local rule maps {} to a user", principal);
                                let _ = out_tx.send(fatal_frame(&header, ACCESS_CONTROL, &msg)).await;
                                return Ok(());
                            };
                            // The client names itself (full principal or short
                            // name); anyone else would be impersonation.
                            if !claimed.is_empty() && claimed != *principal && claimed != *short {
                                let msg = format!("User {} (auth:KERBEROS) is not authorized to impersonate {}", principal, claimed);
                                warn!("{}: {}", peer, msg);
                                let _ = out_tx.send(fatal_frame(&header, ACCESS_CONTROL, &msg)).await;
                                return Ok(());
                            }
                            user = short.clone();
                        }
                    }
                }
                CALL_ID_PING => {}
                id if id >= 0 => {
                    if matches!(handler.authentication(), Authentication::Kerberos(_)) && user.is_empty() {
                        let _ = out_tx.send(fatal_frame(&header, ACCESS_CONTROL, SIMPLE_NOT_ENABLED)).await;
                        return Ok(());
                    }
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
                    if rh.declaring_class_protocol_name != handler.protocol() {
                        let err = RpcError::new(
                            "org.apache.hadoop.ipc.RpcNoSuchProtocolException",
                            format!("Unknown protocol: {}", rh.declaring_class_protocol_name),
                            RpcErrorCodeProto::ErrorNoSuchProtocol,
                        );
                        let _ = out_tx.send(error_frame(&header, &err)).await;
                        continue;
                    }
                    let handler = handler.clone();
                    let out_tx = out_tx.clone();
                    let user = user.clone();
                    tokio::spawn(async move {
                        let reply = match handler.call(&rh.method_name, &body, &user).await {
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


#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;

    fn server(principal: &str) -> KerberosServer {
        let (name, realm) = principal.split_once('@').unwrap();
        let (protocol, server_id) = name.split_once('/').unwrap();
        KerberosServer {
            keytab: krb5::Keytab::from_keys(Vec::new()),
            principal: principal.into(),
            protocol: protocol.into(),
            server_id: server_id.into(),
            realm: realm.into(),
        }
    }

    #[test]
    fn principals_map_to_short_names_by_the_default_rule() {
        let k = server("nn/nn.example.com@EXAMPLE.COM");
        assert_eq!(k.short_name("alice@EXAMPLE.COM").as_deref(), Some("alice"));
        assert_eq!(k.short_name("hdfs/dn1.example.com@EXAMPLE.COM").as_deref(), Some("hdfs"));
        // Another realm has no local name; nor does a malformed principal.
        assert_eq!(k.short_name("alice@OTHER.COM"), None);
        assert_eq!(k.short_name("alice"), None);
        assert_eq!(k.short_name("@EXAMPLE.COM"), None);
    }

    #[test]
    fn a_kerberos_server_needs_service_host_realm_and_a_key() {
        let dir = std::env::temp_dir().join(format!("pillar-kt-{}", std::process::id()));
        let missing = dir.join("none.keytab");
        for bad in ["nn@EXAMPLE.COM", "nn/host", "alice"] {
            let e = KerberosServer::new(&missing, bad).err().expect(bad);
            assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput, "{bad}");
        }
        let e = KerberosServer::new(&missing, "nn/host@EXAMPLE.COM").err().expect("no keytab");
        assert!(e.to_string().contains("keytab"), "{e}");
    }

    #[test]
    fn negotiation_offers_kerberos_and_steps_are_ordered() {
        let k = server("nn/localhost@PILLAR.TEST");
        let mut g = Gssapi::default();
        let offer = g.step(&k, &RpcSaslProto { state: SaslState::Negotiate as i32, ..Default::default() }).unwrap();
        assert_eq!(offer.state, SaslState::Negotiate as i32);
        let a = &offer.auths[0];
        assert_eq!((a.method.as_str(), a.mechanism.as_str()), ("KERBEROS", "GSSAPI"));
        assert_eq!((a.protocol.as_deref(), a.server_id.as_deref()), (Some("nn"), Some("localhost")));
        // A RESPONSE before INITIATE, or a non-GSSAPI mechanism, is refused.
        let resp = RpcSaslProto { state: SaslState::Response as i32, ..Default::default() };
        assert!(g.step(&k, &resp).is_err());
        let digest = RpcSaslProto {
            state: SaslState::Initiate as i32,
            auths: vec![rpc_sasl_proto::SaslAuth { method: "TOKEN".into(), mechanism: "DIGEST-MD5".into(), ..Default::default() }],
            ..Default::default()
        };
        assert!(g.step(&k, &digest).is_err());
        assert!(g.principal.is_none());
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

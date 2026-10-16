//! Kerberos for the DataNode-to-NameNode channel (gRPC `DatanodeProtocol`).
//!
//! HDFS runs DatanodeProtocol over Hadoop IPC with SASL GSSAPI; Pillar's
//! runs over gRPC, so each call carries its own Kerberos AP-REQ instead.
//! With `authentication` the reply carries the NameNode's AP-REP:
//!
//! ```text
//! DN  request   x-kerberos-ap-req: base64(GSS initial token, mutual auth)
//! NN  response  x-kerberos-ap-rep: base64(AP-REP)
//! ```
//!
//! With `integrity` or `privacy` (`--rpc-protection`, as for Hadoop IPC)
//! the call's own subkey (from the AP-REQ's authenticator, which only the
//! NameNode can read) protects both bodies with RFC 4121 Wrap tokens,
//! sealed for privacy:
//!
//! ```text
//! DN  request   x-kerberos-ap-req, x-kerberos-protection: integrity|privacy
//!               body: Wrap(request body)
//! NN  response  x-kerberos-protection, body: Wrap(response body + gRPC status)
//! ```
//!
//! A reply that unwraps under the call's subkey can only come from the
//! NameNode, so it authenticates the NameNode as the AP-REP does; the gRPC
//! status travels inside it, so it cannot be altered either.
//!
//! The NameNode accepts the AP-REQ with its keytab, refuses a token it has
//! seen before (the krb5 crate's replay cache: process-wide, covering the
//! clock-skew window, as MIT's acceptors do), lets only
//! DataNode principals (`dn/<any host>@<its realm>` by default) call, and
//! only at a protection level it allows. The DataNode checks every reply,
//! so it only takes commands from the real NameNode.
//!
//! The DataNode logs in from its keytab and gets a ticket for the NameNode
//! in the background ([`KerberosClient::spawn_refresher`]), renewing it
//! before it expires, so no call waits on the KDC.

use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use base64::Engine;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use http_body_util::BodyExt;
use krb5::token::{ContextKey, PerMessageTokens};
use tonic::body::BoxBody;
use tonic::server::NamedService;
use tracing::{info, warn};

use crate::ipc::KerberosServer;
use crate::sasl::{Protection, Qop};

pub const AP_REQ_HEADER: &str = "x-kerberos-ap-req";
pub const AP_REP_HEADER: &str = "x-kerberos-ap-rep";
pub const PROTECTION_HEADER: &str = "x-kerberos-protection";

/// The gRPC status fields a protected reply carries inside its Wrap token.
const STATUS_FIELDS: [&str; 3] = ["grpc-status", "grpc-message", "grpc-status-details-bin"];

/// RFC 4120's recommended clock-skew tolerance.
const MAX_SKEW_SECS: i64 = 300;

/// Largest body either side protects (block reports of large DataNodes).
const MAX_PROTECTED_BODY: usize = 256 * 1024 * 1024;

const GSS_C_MUTUAL_FLAG: u32 = 2;

type BoxFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;

fn b64() -> &'static base64::engine::GeneralPurpose {
    &base64::engine::general_purpose::STANDARD
}

fn now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}

fn header_qop(headers: &http::HeaderMap) -> Result<Qop, String> {
    match headers.get(PROTECTION_HEADER).map(|v| v.to_str().unwrap_or("?")) {
        None => Ok(Qop::Authentication),
        Some("integrity") => Ok(Qop::Integrity),
        Some("privacy") => Ok(Qop::Privacy),
        Some(other) => Err(format!("unknown RPC protection {:?}", other)),
    }
}

/// Wrap `message` (sealed for privacy). One token per direction per call,
/// under a key used for nothing else, so the sequence number is 0.
fn wrap(tokens: &PerMessageTokens<ContextKey>, qop: Qop, message: &[u8]) -> Result<Vec<u8>, String> {
    match qop {
        Qop::Privacy => tokens.emit_wrap(0, message),
        _ => tokens.emit_wrap_unsealed(0, message, 0),
    }
    .map_err(|e| e.to_string())
}

/// Verify (and decrypt) a token from the peer; on a privacy call it must
/// be sealed.
fn unwrap(tokens: &PerMessageTokens<ContextKey>, qop: Qop, token: &[u8]) -> Result<Vec<u8>, String> {
    let v = tokens
        .verify_wrap_unsealed(token)
        .or_else(|_| tokens.unwrap(token))
        .map_err(|e| format!("protected body does not verify: {}", e))?;
    if qop == Qop::Privacy && v.flags & krb5::token::FLAG_SEALED == 0 {
        return Err("unencrypted body on a privacy call".into());
    }
    Ok(v.message)
}

/// A whole body, at most [`MAX_PROTECTED_BODY`] bytes, with its trailers.
async fn collect<B>(body: B) -> Result<(Bytes, Option<http::HeaderMap>), String>
where
    B: http_body::Body<Data = Bytes>,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let limited = http_body_util::Limited::new(body, MAX_PROTECTED_BODY);
    let collected = limited.collect().await.map_err(|e| format!("reading the body: {}", e))?;
    let trailers = collected.trailers().cloned();
    Ok((collected.to_bytes(), trailers))
}

/// A body of `data`, then `trailers` if any.
fn body_of(data: Bytes, trailers: Option<http::HeaderMap>) -> BoxBody {
    let mut frames = vec![Ok::<_, std::convert::Infallible>(http_body::Frame::data(data))];
    if let Some(t) = trailers.filter(|t| !t.is_empty()) {
        frames.push(Ok(http_body::Frame::trailers(t)));
    }
    tonic::body::boxed(http_body_util::StreamBody::new(tokio_stream::iter(frames)))
}

/// A protected reply's plaintext: the body (length prefixed), then the gRPC
/// status fields, each a length-prefixed name and value.
fn encode_reply(body: &[u8], status: &[(String, Vec<u8>)]) -> Vec<u8> {
    let mut out = BytesMut::with_capacity(body.len() + 64);
    out.put_u32(body.len() as u32);
    out.put_slice(body);
    for (name, value) in status {
        out.put_u16(name.len() as u16);
        out.put_slice(name.as_bytes());
        out.put_u32(value.len() as u32);
        out.put_slice(value);
    }
    out.to_vec()
}

fn decode_reply(mut p: &[u8]) -> Result<(Bytes, http::HeaderMap), String> {
    let short = || "truncated protected reply".to_string();
    if p.remaining() < 4 {
        return Err(short());
    }
    let n = p.get_u32() as usize;
    if p.remaining() < n {
        return Err(short());
    }
    let body = Bytes::copy_from_slice(&p[..n]);
    p.advance(n);
    let mut trailers = http::HeaderMap::new();
    while p.has_remaining() {
        if p.remaining() < 2 {
            return Err(short());
        }
        let n = p.get_u16() as usize;
        if p.remaining() < n + 4 {
            return Err(short());
        }
        let name = std::str::from_utf8(&p[..n]).map_err(|_| short())?.to_string();
        p.advance(n);
        let m = p.get_u32() as usize;
        if p.remaining() < m {
            return Err(short());
        }
        let value = http::HeaderValue::from_bytes(&p[..m]).map_err(|e| e.to_string())?;
        p.advance(m);
        if STATUS_FIELDS.contains(&name.as_str()) {
            trailers.insert(http::HeaderName::from_bytes(name.as_bytes()).map_err(|e| e.to_string())?, value);
        }
    }
    Ok((body, trailers))
}

// ---------------------------------------------------------------- server

/// The caller a request was authenticated as (a request extension).
#[derive(Debug, Clone)]
pub struct KerberosCaller(pub String);

/// The NameNode side: who may call, and at which protection.
pub struct KerberosAcceptor {
    server: KerberosServer,
    /// The service a caller's principal must have (`dn`), in our realm.
    caller_service: String,
    protection: Protection,
}

/// An accepted AP-REQ.
pub struct Accepted {
    pub principal: String,
    /// Base64 AP-REP, for a call that is only authenticated.
    pub ap_rep: String,
    context: krb5::KerberosContext,
}

impl KerberosAcceptor {
    /// `callers` is a principal pattern, `<service>/_HOST@<REALM>`: any host
    /// of that service in the server's realm (Hadoop's
    /// `dfs.datanode.kerberos.principal`). Calls may use any level of
    /// `protection`.
    pub fn new(server: KerberosServer, callers: &str, protection: Protection) -> std::io::Result<Self> {
        let invalid = |m: String| std::io::Error::new(std::io::ErrorKind::InvalidInput, m);
        let (name, realm) = callers.split_once('@').ok_or_else(|| invalid(format!("{} has no realm", callers)))?;
        let service = name.split('/').next().unwrap_or_default();
        if service.is_empty() || realm != server.realm {
            return Err(invalid(format!("callers {} must be <service>/_HOST@{}", callers, server.realm)));
        }
        Ok(Self { caller_service: service.to_string(), server, protection })
    }

    /// Authenticate one request's AP-REQ.
    pub fn accept(&self, ap_req_b64: Option<&str>) -> Result<Accepted, String> {
        let token = b64()
            .decode(ap_req_b64.ok_or("no Kerberos credentials (x-kerberos-ap-req)")?)
            .map_err(|_| "malformed x-kerberos-ap-req")?;
        let (context, ap_rep) = krb5::KerberosContext::accept_token_with_skew(&self.server.keytab, &token, MAX_SKEW_SECS)
            .map_err(|e| format!("Kerberos authentication failed: {}", e))?;
        if context.service_principal != self.server.principal {
            return Err(format!("ticket for {}, not {}", context.service_principal, self.server.principal));
        }
        let principal = context.client_principal.clone();
        let allowed = principal
            .split_once('@')
            .map_or(false, |(name, realm)| realm == self.server.realm && name.split('/').next() == Some(&self.caller_service));
        if !allowed {
            return Err(format!("{} is not a {} principal of {}", principal, self.caller_service, self.server.realm));
        }
        // A replayed AP-REQ was refused above, by the crate's replay cache.
        Ok(Accepted { principal, ap_rep: b64().encode(ap_rep), context })
    }
}

/// A gRPC service that only Kerberos-authenticated callers reach.
#[derive(Clone)]
pub struct Authenticated<S> {
    inner: S,
    acceptor: Arc<KerberosAcceptor>,
}

impl<S> Authenticated<S> {
    pub fn new(inner: S, acceptor: Arc<KerberosAcceptor>) -> Self {
        Self { inner, acceptor }
    }
}

impl<S: NamedService> NamedService for Authenticated<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> tower::Service<http::Request<BoxBody>> for Authenticated<S>
where
    S: tower::Service<http::Request<BoxBody>, Response = http::Response<BoxBody>, Error = std::convert::Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<BoxBody>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<BoxBody>) -> Self::Future {
        let path = req.uri().path().to_string();
        let refuse = move |msg: String| -> Self::Future {
            warn!("Refusing {}: {}", path, msg);
            Box::pin(async move { Ok(tonic::Status::unauthenticated(msg).into_http()) })
        };
        // Who is calling first, then whether its protection will do.
        let accepted = match self.acceptor.accept(req.headers().get(AP_REQ_HEADER).and_then(|v| v.to_str().ok())) {
            Ok(a) => a,
            Err(msg) => return refuse(msg),
        };
        let qop = match header_qop(req.headers()) {
            Ok(q) if self.acceptor.protection.allows(q) => q,
            Ok(q) => {
                return refuse(format!(
                    "{} asks for RPC protection {}; this NameNode allows {}",
                    accepted.principal, q, self.acceptor.protection
                ))
            }
            Err(e) => return refuse(e),
        };
        req.extensions_mut().insert(KerberosCaller(accepted.principal.clone()));
        // The service was polled ready; the clone takes its place.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        if qop == Qop::Authentication {
            let ap_rep = accepted.ap_rep;
            return Box::pin(async move {
                let mut resp = inner.call(req).await?;
                if let Ok(v) = http::HeaderValue::from_str(&ap_rep) {
                    resp.headers_mut().insert(AP_REP_HEADER, v);
                }
                Ok(resp)
            });
        }
        Box::pin(async move {
            let unauthenticated = |m: String| Ok(tonic::Status::unauthenticated(m).into_http());
            let tokens = match accepted.context.per_message_tokens() {
                Ok(t) => t,
                Err(e) => return unauthenticated(e.to_string()),
            };
            let (parts, body) = req.into_parts();
            let plain = match collect(body).await.and_then(|(b, _)| unwrap(&tokens, qop, &b)) {
                Ok(p) => p,
                Err(e) => {
                    warn!("Refusing a protected call from {}: {}", accepted.principal, e);
                    return unauthenticated(e);
                }
            };
            let resp = inner.call(http::Request::from_parts(parts, body_of(plain.into(), None))).await?;
            // The reply, gRPC status included, goes back inside a Wrap token.
            let (mut parts, body) = resp.into_parts();
            let (data, trailers) = match collect(body).await {
                Ok(c) => c,
                Err(e) => return Ok(tonic::Status::internal(e).into_http()),
            };
            let mut status = Vec::new();
            for name in STATUS_FIELDS {
                let v = trailers.as_ref().and_then(|t| t.get(name)).or_else(|| parts.headers.get(name));
                if let Some(v) = v {
                    status.push((name.to_string(), v.as_bytes().to_vec()));
                }
                parts.headers.remove(name);
            }
            let token = match wrap(&tokens, qop, &encode_reply(&data, &status)) {
                Ok(t) => t,
                Err(e) => return Ok(tonic::Status::internal(e).into_http()),
            };
            parts.headers.insert(PROTECTION_HEADER, http::HeaderValue::from_static(if qop == Qop::Privacy { "privacy" } else { "integrity" }));
            parts.headers.remove(http::header::CONTENT_LENGTH);
            let mut done = http::HeaderMap::new();
            done.insert("grpc-status", http::HeaderValue::from_static("0"));
            Ok(http::Response::from_parts(parts, body_of(token.into(), Some(done))))
        })
    }
}

// ---------------------------------------------------------------- client

/// The DataNode side: its keytab login, its ticket for the NameNode, and
/// the protection its calls ask for.
pub struct KerberosClient {
    client: krb5::Principal,
    service: krb5::Principal,
    keytab: krb5::Keytab,
    kdc: krb5::AsyncKdcClient,
    ticket: RwLock<Option<krb5::Credentials>>,
    qop: Qop,
}

impl KerberosClient {
    /// Log in as `principal` from `keytab` to call `service` (the
    /// NameNode's principal); KDCs come from krb5.conf (`KRB5_CONFIG`).
    /// Calls ask for the strongest level of `protection`.
    pub fn new(keytab: &std::path::Path, principal: &str, service: &str, protection: &Protection) -> std::io::Result<Self> {
        let invalid = |m: String| std::io::Error::new(std::io::ErrorKind::InvalidInput, m);
        let client = krb5::Principal::parse(principal).map_err(|e| invalid(format!("{}: {}", principal, e)))?;
        let service = krb5::Principal::parse(service).map_err(|e| invalid(format!("{}: {}", service, e)))?;
        let keytab = krb5::Keytab::load(keytab).map_err(|e| invalid(format!("keytab {}: {}", keytab.display(), e)))?;
        if keytab.find_key(principal).is_none() {
            return Err(invalid(format!("the keytab has no key for {}", principal)));
        }
        let conf = krb5::Krb5Conf::load_default().map_err(|e| invalid(format!("krb5.conf: {}", e)))?;
        let kdc = krb5::KdcClient::from_config(&conf, &client.realm)
            .map_err(|e| invalid(format!("KDCs of {}: {}", client.realm, e)))?;
        let kdc = krb5::AsyncKdcClient::new(kdc);
        let qop = protection.levels().iter().copied().max().unwrap_or(Qop::Authentication);
        Ok(Self { client, service, keytab, kdc, ticket: RwLock::new(None), qop })
    }

    /// Log in and get a fresh ticket for the service from the KDC.
    /// Returns the ticket's end time.
    pub async fn refresh(&self) -> Result<i64, String> {
        let tgt = self.kdc.login_with_keytab(&self.client, &self.keytab).await.map_err(|e| format!("login as {}: {}", self.client, e))?;
        let ticket = self.kdc.service_ticket(&tgt, &self.service).await.map_err(|e| format!("ticket for {}: {}", self.service, e))?;
        let end = ticket.endtime;
        *self.ticket.write().unwrap() = Some(ticket);
        Ok(end)
    }

    /// Keep a valid ticket: get one now, and a new one ten minutes before
    /// each expires (retrying every 30 s while the KDC cannot be reached).
    pub fn spawn_refresher(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let wait = match self.refresh().await {
                    Ok(end) => {
                        info!("Kerberos: {} has a ticket for {} until {}", self.client, self.service, end);
                        Duration::from_secs((end - now() - 600).max(30) as u64)
                    }
                    Err(e) => {
                        warn!("Kerberos: {}", e);
                        Duration::from_secs(30)
                    }
                };
                tokio::time::sleep(wait).await;
            }
        })
    }

    /// A new AP-REQ for one call, and the context that checks the reply.
    /// Mutual authentication for an authenticated-only call (the AP-REP);
    /// a protected call is answered under its own subkey instead.
    pub(crate) fn initiate(&self, mutual: bool) -> Result<(krb5::InitiatorContext, String), String> {
        let ticket = self.ticket.read().unwrap();
        let ticket = ticket.as_ref().filter(|t| !t.is_expired_at(now())).ok_or("no Kerberos ticket for the NameNode yet")?;
        let flags = if mutual { GSS_C_MUTUAL_FLAG } else { 0 };
        let (context, token) = krb5::InitiatorContext::new(ticket, flags).map_err(|e| e.to_string())?;
        Ok((context, b64().encode(token)))
    }
}

/// A gRPC channel whose calls carry Kerberos credentials when `kerberos`
/// is set, and whose replies must then prove they come from the service.
#[derive(Clone)]
pub struct AuthChannel {
    inner: tonic::transport::Channel,
    kerberos: Option<Arc<KerberosClient>>,
}

impl AuthChannel {
    pub fn new(inner: tonic::transport::Channel, kerberos: Option<Arc<KerberosClient>>) -> Self {
        Self { inner, kerberos }
    }
}

pub type AuthError = Box<dyn std::error::Error + Send + Sync>;

/// A refusal (a gRPC error status) needs no proof: it is an error either way.
fn is_refusal(resp: &http::Response<BoxBody>) -> bool {
    resp.headers().get("grpc-status").map_or(false, |s| s != "0")
}

impl tower::Service<http::Request<BoxBody>> for AuthChannel {
    type Response = http::Response<BoxBody>;
    type Error = AuthError;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        tower::Service::poll_ready(&mut self.inner, cx).map_err(Into::into)
    }

    fn call(&mut self, mut req: http::Request<BoxBody>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let Some(kerberos) = self.kerberos.clone() else {
            return Box::pin(async move { inner.call(req).await.map_err(Into::into) });
        };
        let qop = kerberos.qop;
        if qop == Qop::Authentication {
            return Box::pin(async move {
                let (mut context, token) = kerberos.initiate(true)?;
                req.headers_mut().insert(AP_REQ_HEADER, http::HeaderValue::from_str(&token)?);
                let resp = inner.call(req).await?;
                match resp.headers().get(AP_REP_HEADER) {
                    Some(v) => {
                        let ap_rep = b64().decode(v.as_bytes()).map_err(|_| "malformed x-kerberos-ap-rep")?;
                        context.step(&ap_rep).map_err(|e| format!("the NameNode's AP-REP does not verify: {}", e))?;
                        Ok(resp)
                    }
                    None if is_refusal(&resp) => Ok(resp),
                    None => Err("the NameNode did not authenticate itself (no AP-REP)".into()),
                }
            });
        }
        Box::pin(async move {
            let (context, token) = kerberos.initiate(false)?;
            let tokens = context.per_message_tokens().map_err(|e| e.to_string())?;
            let (mut parts, body) = req.into_parts();
            let (plain, _) = collect(body).await?;
            parts.headers.insert(AP_REQ_HEADER, http::HeaderValue::from_str(&token)?);
            parts.headers.insert(PROTECTION_HEADER, http::HeaderValue::from_static(if qop == Qop::Privacy { "privacy" } else { "integrity" }));
            parts.headers.remove(http::header::CONTENT_LENGTH);
            let sealed = wrap(&tokens, qop, &plain)?;
            let resp = inner.call(http::Request::from_parts(parts, body_of(sealed.into(), None))).await?;
            match header_qop(resp.headers()) {
                Ok(q) if q == qop => {}
                _ if is_refusal(&resp) => return Ok(resp),
                _ => return Err(format!("the NameNode's reply is not protected ({} was asked for)", qop).into()),
            }
            let (mut parts, body) = resp.into_parts();
            let (token, _) = collect(body).await?;
            let (data, trailers) = decode_reply(&unwrap(&tokens, qop, &token).map_err(|e| format!("the NameNode's reply: {}", e))?)?;
            parts.headers.remove(PROTECTION_HEADER);
            parts.headers.remove(http::header::CONTENT_LENGTH);
            Ok(http::Response::from_parts(parts, body_of(data, Some(trailers))))
        })
    }
}

/// Against a live KDC: `PILLAR_KDC_DIR` is a `tests/kerberos/kdc.sh`
/// directory (realm PILLAR.TEST), with `KRB5_CONFIG` pointing into it.
/// `tests/interop/run-java-kerberos.sh` runs these; without a KDC they skip.
#[cfg(test)]
mod tests {
    use super::*;

    fn kdc() -> Option<std::path::PathBuf> {
        let dir = std::env::var_os("PILLAR_KDC_DIR").map(std::path::PathBuf::from);
        if dir.is_none() {
            eprintln!("PILLAR_KDC_DIR unset: skipping");
        }
        dir
    }

    fn acceptor(dir: &std::path::Path) -> KerberosAcceptor {
        let server = KerberosServer::new(&dir.join("nn.keytab"), "nn/localhost@PILLAR.TEST").unwrap();
        KerberosAcceptor::new(server, "dn/_HOST@PILLAR.TEST", "authentication,integrity,privacy".parse().unwrap()).unwrap()
    }

    fn client(dir: &std::path::Path, keytab: &str, principal: &str) -> KerberosClient {
        let c = KerberosClient::new(&dir.join(keytab), principal, "nn/localhost@PILLAR.TEST", &Protection::default()).unwrap();
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(c.refresh()).unwrap();
        c
    }

    #[test]
    fn a_datanode_authenticates_once_per_call_and_replays_are_refused() {
        let Some(dir) = kdc() else { return };
        let nn = acceptor(&dir);
        let dn = client(&dir, "dn.keytab", "dn/localhost@PILLAR.TEST");
        for _ in 0..3 {
            let (mut context, token) = dn.initiate(true).unwrap();
            let accepted = nn.accept(Some(&token)).unwrap();
            assert_eq!(accepted.principal, "dn/localhost@PILLAR.TEST");
            // The DataNode checks the NameNode's reply.
            context.step(&b64().decode(accepted.ap_rep).unwrap()).unwrap();
            // The same AP-REQ a second time (captured and replayed).
            let e = nn.accept(Some(&token)).err().unwrap();
            assert!(e.contains("replay"), "{e}");
        }
    }

    #[test]
    fn only_datanode_principals_may_call() {
        let Some(dir) = kdc() else { return };
        let nn = acceptor(&dir);
        let alice = client(&dir, "alice.keytab", "alice@PILLAR.TEST");
        let (_, token) = alice.initiate(true).unwrap();
        let e = nn.accept(Some(&token)).err().unwrap();
        assert!(e.contains("alice@PILLAR.TEST is not a dn principal"), "{e}");
        assert!(nn.accept(None).err().unwrap().contains("no Kerberos credentials"));
        assert!(nn.accept(Some("bm90IGEgdG9rZW4=")).is_err());
    }

    #[test]
    fn a_datanode_refuses_a_reply_not_from_the_namenode() {
        let Some(dir) = kdc() else { return };
        let nn = acceptor(&dir);
        let dn = client(&dir, "dn.keytab", "dn/localhost@PILLAR.TEST");
        // An AP-REP for another call does not prove this one was answered
        // by the NameNode.
        let (mut first, _) = dn.initiate(true).unwrap();
        let (_, second) = dn.initiate(true).unwrap();
        let other_rep = nn.accept(Some(&second)).unwrap().ap_rep;
        assert!(first.step(&b64().decode(other_rep).unwrap()).is_err());
    }

    #[test]
    fn protected_bodies_are_bound_to_their_call() {
        let Some(dir) = kdc() else { return };
        let nn = acceptor(&dir);
        let dn = client(&dir, "dn.keytab", "dn/localhost@PILLAR.TEST");
        for qop in [Qop::Integrity, Qop::Privacy] {
            let (context, token) = dn.initiate(false).unwrap();
            let dn_tokens = context.per_message_tokens().unwrap();
            let nn_tokens = nn.accept(Some(&token)).unwrap().context.per_message_tokens().unwrap();
            // Request: the NameNode reads what the DataNode wrapped.
            let req = wrap(&dn_tokens, qop, b"blockReport for dn0").unwrap();
            assert_eq!(req.windows(11).any(|w| w == b"blockReport"), qop == Qop::Integrity);
            assert_eq!(unwrap(&nn_tokens, qop, &req).unwrap(), b"blockReport for dn0");
            // Reply, with its gRPC status inside.
            let status = vec![("grpc-status".to_string(), b"0".to_vec())];
            let rep = wrap(&nn_tokens, qop, &encode_reply(b"commands", &status)).unwrap();
            let (body, trailers) = decode_reply(&unwrap(&dn_tokens, qop, &rep).unwrap()).unwrap();
            assert_eq!((&body[..], trailers["grpc-status"].as_bytes()), (&b"commands"[..], &b"0"[..]));
            // Tampered: refused. A reply from another call: refused.
            let mut bad = rep.clone();
            let last = bad.len() - 1;
            bad[last] ^= 1;
            assert!(unwrap(&dn_tokens, qop, &bad).is_err());
            let (other, other_token) = dn.initiate(false).unwrap();
            let other_nn = nn.accept(Some(&other_token)).unwrap().context.per_message_tokens().unwrap();
            let foreign = wrap(&other_nn, qop, b"forged").unwrap();
            assert!(unwrap(&dn_tokens, qop, &foreign).is_err());
            drop(other);
            // Privacy refuses an unsealed body.
            if qop == Qop::Privacy {
                let plain = wrap(&dn_tokens, Qop::Integrity, b"x").unwrap();
                assert!(unwrap(&nn_tokens, Qop::Privacy, &plain).is_err());
            }
        }
    }

    #[test]
    fn replies_round_trip_their_status() {
        let status = vec![("grpc-status".to_string(), b"5".to_vec()), ("grpc-message".to_string(), b"not%20found".to_vec())];
        let (body, t) = decode_reply(&encode_reply(b"", &status)).unwrap();
        assert!(body.is_empty());
        assert_eq!(t["grpc-status"], "5");
        assert_eq!(t["grpc-message"], "not%20found");
        assert!(decode_reply(&[0, 0, 0, 9, 1]).is_err());
    }
}

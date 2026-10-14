//! Kerberos for the DataNode-to-NameNode channel (gRPC `DatanodeProtocol`).
//!
//! HDFS runs DatanodeProtocol over Hadoop IPC with SASL GSSAPI; Pillar's
//! runs over gRPC, so each call carries its own Kerberos AP-REQ instead:
//!
//! ```text
//! DN  request   x-kerberos-ap-req: base64(GSS initial token, mutual auth)
//! NN  response  x-kerberos-ap-rep: base64(AP-REP)
//! ```
//!
//! The NameNode accepts the AP-REQ with its keytab, refuses a token it has
//! seen before (a replay cache covering the clock-skew window), and lets
//! only DataNode principals (`dn/<any host>@<its realm>` by default) call.
//! The DataNode checks the AP-REP on every response, so it only takes
//! commands from the real NameNode. Calls are authenticated, not protected:
//! as with `hadoop.rpc.protection=authentication`, their content has no
//! integrity or privacy of its own.
//!
//! The DataNode logs in from its keytab and gets a ticket for the NameNode
//! in the background ([`KerberosClient::spawn_refresher`]), renewing it
//! before it expires, so no call waits on the KDC.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use base64::Engine;
use sha1::{Digest, Sha1};
use tonic::body::BoxBody;
use tonic::server::NamedService;
use tracing::{info, warn};

use crate::ipc::KerberosServer;

pub const AP_REQ_HEADER: &str = "x-kerberos-ap-req";
pub const AP_REP_HEADER: &str = "x-kerberos-ap-rep";

/// RFC 4120's recommended clock-skew tolerance.
const MAX_SKEW_SECS: i64 = 300;

const GSS_C_MUTUAL_FLAG: u32 = 2;

type BoxFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;

fn b64() -> &'static base64::engine::GeneralPurpose {
    &base64::engine::general_purpose::STANDARD
}

fn now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}

// ---------------------------------------------------------------- server

/// The caller a request was authenticated as (a request extension).
#[derive(Debug, Clone)]
pub struct KerberosCaller(pub String);

/// The NameNode side: who may call, and the tokens already used.
pub struct KerberosAcceptor {
    server: KerberosServer,
    /// The service a caller's principal must have (`dn`), in our realm.
    caller_service: String,
    /// AP-REQ digests seen, with when they may be forgotten.
    seen: Mutex<HashMap<[u8; 20], i64>>,
}

impl KerberosAcceptor {
    /// `callers` is a principal pattern, `<service>/_HOST@<REALM>`: any host
    /// of that service in the server's realm (Hadoop's
    /// `dfs.datanode.kerberos.principal`).
    pub fn new(server: KerberosServer, callers: &str) -> std::io::Result<Self> {
        let invalid = |m: String| std::io::Error::new(std::io::ErrorKind::InvalidInput, m);
        let (name, realm) = callers.split_once('@').ok_or_else(|| invalid(format!("{} has no realm", callers)))?;
        let service = name.split('/').next().unwrap_or_default();
        if service.is_empty() || realm != server.realm {
            return Err(invalid(format!("callers {} must be <service>/_HOST@{}", callers, server.realm)));
        }
        Ok(Self { caller_service: service.to_string(), server, seen: Mutex::new(HashMap::new()) })
    }

    /// Authenticate one request's AP-REQ: the caller's principal and the
    /// AP-REP to send back.
    pub fn accept(&self, ap_req_b64: Option<&str>) -> Result<(String, String), String> {
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
        // An authenticator is good for the skew window either side of now;
        // remember each token that long, so none is accepted twice.
        let digest: [u8; 20] = Sha1::digest(&token).into();
        let t = now();
        let mut seen = self.seen.lock().unwrap();
        seen.retain(|_, until| *until > t);
        if seen.insert(digest, t + 2 * MAX_SKEW_SECS + 1).is_some() {
            return Err(format!("replayed Kerberos authenticator from {}", principal));
        }
        Ok((principal, b64().encode(ap_rep)))
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
        let header = req.headers().get(AP_REQ_HEADER).and_then(|v| v.to_str().ok());
        match self.acceptor.accept(header) {
            Err(msg) => {
                warn!("Refusing {}: {}", req.uri().path(), msg);
                Box::pin(async move { Ok(tonic::Status::unauthenticated(msg).into_http()) })
            }
            Ok((principal, ap_rep)) => {
                req.extensions_mut().insert(KerberosCaller(principal));
                // The service was polled ready; the clone takes its place.
                let clone = self.inner.clone();
                let mut inner = std::mem::replace(&mut self.inner, clone);
                Box::pin(async move {
                    let mut resp = inner.call(req).await?;
                    if let Ok(v) = http::HeaderValue::from_str(&ap_rep) {
                        resp.headers_mut().insert(AP_REP_HEADER, v);
                    }
                    Ok(resp)
                })
            }
        }
    }
}

// ---------------------------------------------------------------- client

/// The DataNode side: its keytab login and its ticket for the NameNode.
pub struct KerberosClient {
    client: krb5::Principal,
    service: krb5::Principal,
    keytab: krb5::Keytab,
    kdc: krb5::KdcClient,
    ticket: RwLock<Option<krb5::Credentials>>,
}

impl KerberosClient {
    /// Log in as `principal` from `keytab` to call `service` (the
    /// NameNode's principal); KDCs come from krb5.conf (`KRB5_CONFIG`).
    pub fn new(keytab: &std::path::Path, principal: &str, service: &str) -> std::io::Result<Self> {
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
        Ok(Self { client, service, keytab, kdc, ticket: RwLock::new(None) })
    }

    /// Log in and get a fresh ticket for the service (talks to the KDC:
    /// blocking). Returns the ticket's end time.
    pub fn refresh(&self) -> Result<i64, String> {
        let tgt = self.kdc.login_with_keytab(&self.client, &self.keytab).map_err(|e| format!("login as {}: {}", self.client, e))?;
        let ticket = self.kdc.service_ticket(&tgt, &self.service).map_err(|e| format!("ticket for {}: {}", self.service, e))?;
        let end = ticket.endtime;
        *self.ticket.write().unwrap() = Some(ticket);
        Ok(end)
    }

    /// Keep a valid ticket: get one now, and a new one ten minutes before
    /// each expires (retrying every 30 s while the KDC cannot be reached).
    pub fn spawn_refresher(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let this = self.clone();
                let wait = match tokio::task::spawn_blocking(move || this.refresh()).await {
                    Ok(Ok(end)) => {
                        info!("Kerberos: {} has a ticket for {} until {}", self.client, self.service, end);
                        Duration::from_secs((end - now() - 600).max(30) as u64)
                    }
                    Ok(Err(e)) => {
                        warn!("Kerberos: {}", e);
                        Duration::from_secs(30)
                    }
                    Err(e) => {
                        warn!("Kerberos ticket refresh panicked: {}", e);
                        Duration::from_secs(30)
                    }
                };
                tokio::time::sleep(wait).await;
            }
        })
    }

    /// A new AP-REQ for one call, and the context that checks the reply.
    pub(crate) fn initiate(&self) -> Result<(krb5::InitiatorContext, String), String> {
        let ticket = self.ticket.read().unwrap();
        let ticket = ticket.as_ref().filter(|t| !t.is_expired_at(now())).ok_or("no Kerberos ticket for the NameNode yet")?;
        let (context, token) = krb5::InitiatorContext::new(ticket, GSS_C_MUTUAL_FLAG).map_err(|e| e.to_string())?;
        Ok((context, b64().encode(token)))
    }
}

/// A gRPC channel whose calls carry Kerberos credentials when `kerberos`
/// is set, and whose responses must then prove they come from the
/// service (the AP-REP).
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
        Box::pin(async move {
            let (mut context, token) = kerberos.initiate()?;
            req.headers_mut().insert(AP_REQ_HEADER, http::HeaderValue::from_str(&token)?);
            let resp = inner.call(req).await?;
            match resp.headers().get(AP_REP_HEADER) {
                Some(v) => {
                    let ap_rep = b64().decode(v.as_bytes()).map_err(|_| "malformed x-kerberos-ap-rep")?;
                    context.step(&ap_rep).map_err(|e| format!("the NameNode's AP-REP does not verify: {}", e))?;
                    Ok(resp)
                }
                // A refusal carries no AP-REP; it is an error either way.
                None if resp.headers().get("grpc-status").map_or(false, |s| s != "0") => Ok(resp),
                None => Err("the NameNode did not authenticate itself (no AP-REP)".into()),
            }
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
        KerberosAcceptor::new(server, "dn/_HOST@PILLAR.TEST").unwrap()
    }

    fn client(dir: &std::path::Path, keytab: &str, principal: &str) -> KerberosClient {
        let c = KerberosClient::new(&dir.join(keytab), principal, "nn/localhost@PILLAR.TEST").unwrap();
        c.refresh().unwrap();
        c
    }

    #[test]
    fn a_datanode_authenticates_once_per_call_and_replays_are_refused() {
        let Some(dir) = kdc() else { return };
        let nn = acceptor(&dir);
        let dn = client(&dir, "dn.keytab", "dn/localhost@PILLAR.TEST");
        for _ in 0..3 {
            let (mut context, token) = dn.initiate().unwrap();
            let (who, ap_rep) = nn.accept(Some(&token)).unwrap();
            assert_eq!(who, "dn/localhost@PILLAR.TEST");
            // The DataNode checks the NameNode's reply.
            context.step(&b64().decode(ap_rep).unwrap()).unwrap();
            // The same AP-REQ a second time (captured and replayed).
            let e = nn.accept(Some(&token)).unwrap_err();
            assert!(e.contains("replayed"), "{e}");
        }
    }

    #[test]
    fn only_datanode_principals_may_call() {
        let Some(dir) = kdc() else { return };
        let nn = acceptor(&dir);
        let alice = client(&dir, "alice.keytab", "alice@PILLAR.TEST");
        let (_, token) = alice.initiate().unwrap();
        let e = nn.accept(Some(&token)).unwrap_err();
        assert!(e.contains("alice@PILLAR.TEST is not a dn principal"), "{e}");
        assert!(nn.accept(None).unwrap_err().contains("no Kerberos credentials"));
        assert!(nn.accept(Some("bm90IGEgdG9rZW4=")).is_err());
    }

    #[test]
    fn a_datanode_refuses_a_reply_not_from_the_namenode() {
        let Some(dir) = kdc() else { return };
        let nn = acceptor(&dir);
        let dn = client(&dir, "dn.keytab", "dn/localhost@PILLAR.TEST");
        // An AP-REP for another call does not prove this one was answered
        // by the NameNode.
        let (mut first, _) = dn.initiate().unwrap();
        let (_, second) = dn.initiate().unwrap();
        let (_, other_rep) = nn.accept(Some(&second)).unwrap();
        assert!(first.step(&b64().decode(other_rep).unwrap()).is_err());
    }
}

//! Data transfer encryption and protection on the data port (HDFS
//! `SaslDataTransferServer` / `SaslDataTransferClient`):
//! `dfs.encrypt.data.transfer` and `dfs.data.transfer.protection`.
//!
//! A client opens with the magic number `0xDEADBEEF`, then a SASL
//! DIGEST-MD5 exchange in varint-delimited `DataTransferEncryptorMessageProto`s:
//!
//! ```text
//! C  magic, {payload: ""}
//! S  {payload: challenge}
//! C  {payload: digest-response, cipher_option: [AES/CTR/NoPadding]}
//! S  {payload: rspauth, cipher_option: [AES/CTR, keys wrapped by SASL]}
//! ```
//!
//! The credential is a data encryption key from the NameNode
//! (`getDataEncryptionKey`: username `<key id> <block pool> <nonce>`,
//! password an HMAC of the block key) or a block access token (username its
//! identifier). With privacy and a client that offers AES/CTR, the server
//! picks fresh AES keys, sends them wrapped by the SASL layer, and the rest
//! of the connection is AES-CTR ([`AesCtrStream`]): the fast path, at AES-NI
//! speed with no framing. Otherwise every write is a SASL token (4-byte
//! length, then DIGEST-MD5 `auth-int` or `auth-conf` with RC4), or, with
//! `auth`, the stream is left as it is.
//!
//! DataNodes do the client side for their own transfers (pipeline mirrors,
//! replica copies, Pillar's protocol), with a token they mint.

use std::sync::OnceLock;

use base64::Engine;
use rand::RngCore;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, warn};

use pillar_common::crypto_stream::{AesCtr, AesCtrStream};
use pillar_common::digest_md5::{DigestClient, DigestServer};
use pillar_common::hdfs::block_token::mode;
use pillar_common::sasl::{Protection, Qop, SecurityLayer};
use pillar_proto::hdfs::{
    data_transfer_encryptor_message_proto::DataTransferEncryptorStatus, CipherOptionProto, CipherSuiteProto,
    DataTransferEncryptorMessageProto,
};

use super::hdfs_dtp::{read_delimited, write_delimited};
use super::{Result, TransportError};
use crate::storage::block_storage::BlockStorage;

/// `DataTransferSaslUtil.SASL_TRANSFER_MAGIC_NUMBER`.
pub const MAGIC: u32 = 0xDEAD_BEEF;

/// Hadoop's SASL protocol and server name for data transfer.
const DIGEST_URI: &str = "hdfs/0";

/// AES key size the server picks (Hadoop's default 128 bits).
const AES_KEY_LEN: usize = 16;

/// How a connection was secured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// No handshake, or SASL `auth` only.
    Plain,
    /// Every write a SASL token at this level (DIGEST-MD5).
    Sasl(Qop),
    /// AES/CTR after the handshake: the fast path.
    Aes,
}

/// A data connection, secured or not.
pub trait DataIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> DataIo for T {}
pub type DataStream = Box<dyn DataIo>;

/// What a DataNode requires of data connections.
#[derive(Debug, Clone, Default)]
pub struct DataSecurity {
    /// `dfs.encrypt.data.transfer`: every connection encrypted.
    pub encrypt: bool,
    /// `dfs.data.transfer.protection`: SASL with block tokens at one of
    /// these levels.
    pub protection: Option<Protection>,
}

impl DataSecurity {
    /// Whether connections must open with the SASL handshake.
    pub fn required(&self) -> bool {
        self.encrypt || self.protection.is_some()
    }

    /// The levels a connection may negotiate.
    fn levels(&self) -> Protection {
        if self.encrypt {
            "privacy".parse().expect("valid")
        } else {
            self.protection.clone().unwrap_or_default()
        }
    }
}

static NO_SECURITY: OnceLock<DataSecurity> = OnceLock::new();

/// The storage's data security, none until configured.
pub fn security_of(storage: &BlockStorage) -> &DataSecurity {
    storage.data_security.get().unwrap_or_else(|| NO_SECURITY.get_or_init(DataSecurity::default))
}

fn protocol_error(m: impl Into<String>) -> TransportError {
    TransportError::ProtocolError(m.into())
}

fn message(status: DataTransferEncryptorStatus, payload: Option<Vec<u8>>, text: Option<String>) -> DataTransferEncryptorMessageProto {
    DataTransferEncryptorMessageProto { status: status as i32, payload, message: text, ..Default::default() }
}

async fn read_message<S: AsyncRead + Unpin>(s: &mut S) -> Result<DataTransferEncryptorMessageProto> {
    let m: DataTransferEncryptorMessageProto = read_delimited(s).await?;
    match DataTransferEncryptorStatus::try_from(m.status) {
        Ok(DataTransferEncryptorStatus::Success) => Ok(m),
        _ => Err(protocol_error(format!("data transfer SASL refused: {}", m.message.unwrap_or_default()))),
    }
}

// ---------------------------------------------------------------- server

/// The server side, after the magic number: authenticate the client and
/// return the connection as it continues.
pub async fn accept<S>(mut s: S, storage: &BlockStorage) -> Result<(DataStream, Mode)>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let security = security_of(storage);
    let _initial = read_message(&mut s).await?;
    let digest = DigestServer::new("0", "hdfs", "0", &security.levels());
    write_delimited(&mut s, &message(DataTransferEncryptorStatus::Success, Some(digest.challenge()), None)).await?;
    s.flush().await?;

    let response = read_message(&mut s).await?;
    let wants_aes = response.cipher_option.iter().any(|o| o.suite == CipherSuiteProto::AesCtrNopadding as i32);
    let mut unknown_key = false;
    let outcome = digest.respond(response.payload.as_deref().unwrap_or_default(), |username| {
        let password = password_for(storage, username);
        unknown_key = password.is_none() && username.split(' ').count() == 3;
        password
    });
    let outcome = match outcome {
        Ok(o) => o,
        Err(e) => {
            let status = if unknown_key { DataTransferEncryptorStatus::ErrorUnknownKey } else { DataTransferEncryptorStatus::Error };
            let _ = write_delimited(&mut s, &message(status, None, Some(e.clone()))).await;
            let _ = s.flush().await;
            return Err(protocol_error(e));
        }
    };

    let mut reply = message(DataTransferEncryptorStatus::Success, Some(outcome.rspauth.clone()), None);
    let secured: (DataStream, Mode) = match outcome.layer {
        Some(mut layer) if layer.qop == Qop::Privacy && wants_aes => {
            // The fast path: fresh AES keys, sent under the SASL layer.
            let key = || {
                let mut k = [0u8; AES_KEY_LEN];
                rand::thread_rng().fill_bytes(&mut k);
                k
            };
            let iv = || {
                let mut v = [0u8; 16];
                rand::thread_rng().fill_bytes(&mut v);
                v
            };
            let (in_key, in_iv, out_key, out_iv) = (key(), iv(), key(), iv());
            reply.cipher_option = vec![CipherOptionProto {
                suite: CipherSuiteProto::AesCtrNopadding as i32,
                in_key: Some(layer.seal.seal(&in_key).map_err(protocol_error)?),
                in_iv: Some(in_iv.to_vec()),
                out_key: Some(layer.seal.seal(&out_key).map_err(protocol_error)?),
                out_iv: Some(out_iv.to_vec()),
            }];
            write_delimited(&mut s, &reply).await?;
            s.flush().await?;
            debug!("Data transfer: AES/CTR for {}", outcome.username);
            (Box::new(AesCtrStream::new(s, AesCtr::new(&in_key, &in_iv)?, AesCtr::new(&out_key, &out_iv)?)), Mode::Aes)
        }
        Some(layer) => {
            write_delimited(&mut s, &reply).await?;
            s.flush().await?;
            debug!("Data transfer: SASL {} for {}", layer.qop, outcome.username);
            let qop = layer.qop;
            (sasl_stream(s, layer, outcome.peer_maxbuf), Mode::Sasl(qop))
        }
        None => {
            write_delimited(&mut s, &reply).await?;
            s.flush().await?;
            (Box::new(s), Mode::Plain)
        }
    };
    Ok(secured)
}

/// The SASL password for a client's username: a data encryption key
/// (`<key id> <block pool> <nonce>`) or a block token identifier.
fn password_for(storage: &BlockStorage, username: &str) -> Option<String> {
    let b64 = &base64::engine::general_purpose::STANDARD;
    let parts: Vec<&str> = username.split(' ').collect();
    let secret = if let [key_id, _pool, nonce] = parts[..] {
        storage.tokens.encryption_key(key_id.parse().ok()?, &b64.decode(nonce).ok()?)?
    } else {
        storage.tokens.password(&b64.decode(username).ok()?)?.1
    };
    Some(b64.encode(secret))
}

// ---------------------------------------------------------------- client

/// The client side on a new connection to another DataNode: nothing when
/// this DataNode requires no security, else the handshake with a token for
/// `block_id` this DataNode mints (or a data encryption key).
pub async fn connect<S>(s: S, storage: &BlockStorage, block_id: u64) -> Result<(DataStream, Mode)>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    connect_with(s, storage, block_id, true).await
}

/// [`connect`], offering AES/CTR or not (Hadoop clients offer it only with
/// `dfs.encrypt.data.transfer.cipher.suites` set).
pub async fn connect_with<S>(mut s: S, storage: &BlockStorage, block_id: u64, offer_aes: bool) -> Result<(DataStream, Mode)>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let security = security_of(storage);
    if !security.required() {
        return Ok((Box::new(s), Mode::Plain));
    }
    let b64 = &base64::engine::general_purpose::STANDARD;
    let (username, password) = if security.encrypt {
        let (key_id, nonce, key) = storage.tokens.new_encryption_key().ok_or_else(|| protocol_error("no block keys from the NameNode yet"))?;
        (format!("{} pillar {}", key_id, b64.encode(nonce)), b64.encode(key))
    } else {
        let t = storage
            .tokens
            .mint(block_id, mode::READ | mode::WRITE | mode::COPY)
            .ok_or_else(|| protocol_error("data transfer protection needs block access tokens"))?;
        (b64.encode(&t.identifier), b64.encode(&t.password))
    };
    s.write_u32(MAGIC).await?;
    write_delimited(&mut s, &message(DataTransferEncryptorStatus::Success, Some(Vec::new()), None)).await?;
    s.flush().await?;
    let challenge = read_message(&mut s).await?;
    let mut client = DigestClient::new(&username, &password, DIGEST_URI);
    let response = client.respond(challenge.payload.as_deref().unwrap_or_default(), &security.levels()).map_err(protocol_error)?;
    let mut m = message(DataTransferEncryptorStatus::Success, Some(response), None);
    if offer_aes {
        m.cipher_option = vec![CipherOptionProto { suite: CipherSuiteProto::AesCtrNopadding as i32, ..Default::default() }];
    }
    write_delimited(&mut s, &m).await?;
    s.flush().await?;
    let done = read_message(&mut s).await?;
    let layer = client.finish(done.payload.as_deref().unwrap_or_default()).map_err(protocol_error)?;
    Ok(match (layer, done.cipher_option.first()) {
        (Some(mut layer), Some(o)) if o.suite == CipherSuiteProto::AesCtrNopadding as i32 => {
            let get = |v: &Option<Vec<u8>>, what: &str| v.clone().ok_or_else(|| protocol_error(format!("cipher option without {}", what)));
            let in_key = layer.open.open(&get(&o.in_key, "in key")?).map_err(protocol_error)?;
            let out_key = layer.open.open(&get(&o.out_key, "out key")?).map_err(protocol_error)?;
            // The server reads with `in`, writes with `out`: the reverse here.
            let stream = AesCtrStream::new(s, AesCtr::new(&out_key, &get(&o.out_iv, "out iv")?)?, AesCtr::new(&in_key, &get(&o.in_iv, "in iv")?)?);
            (Box::new(stream) as DataStream, Mode::Aes)
        }
        (Some(layer), _) => {
            let qop = layer.qop;
            (sasl_stream(s, layer, client.peer_maxbuf()), Mode::Sasl(qop))
        }
        (None, _) => (Box::new(s) as DataStream, Mode::Plain),
    })
}

// ------------------------------------------------ SASL-wrapped stream

/// Every write a SASL token (4-byte length first), as Hadoop's
/// `SaslInputStream` / `SaslOutputStream`; the slow path, for clients that
/// do not offer AES. Two tasks pump between the socket and the stream
/// handed back.
fn sasl_stream<S>(s: S, layer: SecurityLayer, peer_maxbuf: usize) -> DataStream
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let SecurityLayer { mut seal, mut open, .. } = layer;
    // Tokens at most the peer's maxbuf (16 bytes of MAC and trailer).
    let chunk = peer_maxbuf.clamp(1024, 64 * 1024) - 16;
    let (app, pump) = tokio::io::duplex(256 * 1024);
    let (mut net_r, mut net_w) = tokio::io::split(s);
    let (mut pump_r, mut pump_w) = tokio::io::split(pump);
    tokio::spawn(async move {
        let r: std::io::Result<()> = async {
            loop {
                let len = match net_r.read_u32().await {
                    Ok(n) => n as usize,
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                    Err(e) => return Err(e),
                };
                if len > pillar_common::digest_md5::MAX_BUFFER + 16 {
                    return Err(std::io::Error::other(format!("{} byte SASL token", len)));
                }
                let mut token = vec![0u8; len];
                net_r.read_exact(&mut token).await?;
                let plain = open.open(&token).map_err(std::io::Error::other)?;
                pump_w.write_all(&plain).await?;
            }
        }
        .await;
        if let Err(e) = r {
            warn!("SASL data stream (in): {}", e);
        }
        let _ = pump_w.shutdown().await;
    });
    tokio::spawn(async move {
        let mut buf = vec![0u8; chunk];
        let r: std::io::Result<()> = async {
            loop {
                let n = pump_r.read(&mut buf).await?;
                if n == 0 {
                    return Ok(());
                }
                let token = seal.seal(&buf[..n]).map_err(std::io::Error::other)?;
                net_w.write_u32(token.len() as u32).await?;
                net_w.write_all(&token).await?;
                net_w.flush().await?;
            }
        }
        .await;
        if let Err(e) = r {
            debug!("SASL data stream (out): {}", e);
        }
        let _ = net_w.shutdown().await;
    });
    Box::new(app)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pillar_common::hdfs::block_token::BlockTokenSecretManager;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tempfile::TempDir;

    /// A DataNode's storage with the NameNode's block keys and `security`.
    fn node(keys: &BlockTokenSecretManager, security: DataSecurity) -> (TempDir, Arc<BlockStorage>) {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(BlockStorage::new(dir.path().join("f"), dir.path().join("t")).unwrap());
        storage.tokens.update(&keys.export());
        storage.data_security.set(security).unwrap();
        (dir, storage)
    }

    fn keys() -> BlockTokenSecretManager {
        BlockTokenSecretManager::new("BP-1", Duration::from_secs(3600), Duration::from_secs(3600))
    }

    /// Records what one side writes to the network.
    struct Tap<S> {
        inner: S,
        seen: Arc<Mutex<Vec<u8>>>,
    }

    impl<S: AsyncRead + Unpin> AsyncRead for Tap<S> {
        fn poll_read(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &mut tokio::io::ReadBuf<'_>) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl<S: AsyncWrite + Unpin> AsyncWrite for Tap<S> {
        fn poll_write(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>, data: &[u8]) -> std::task::Poll<std::io::Result<usize>> {
            let r = std::pin::Pin::new(&mut self.inner).poll_write(cx, data);
            if let std::task::Poll::Ready(Ok(n)) = &r {
                self.seen.lock().unwrap().extend_from_slice(&data[..*n]);
            }
            r
        }
        fn poll_flush(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    /// Handshake between `client` and `server` (client offering AES or
    /// not), then a message each way and a large one; the modes negotiated
    /// and the bytes the client put on the wire.
    async fn exchange(client: &BlockStorage, server: Arc<BlockStorage>, offer_aes: bool) -> Result<(Mode, Mode, Vec<u8>)> {
        let (c, s) = tokio::io::duplex(64 * 1024);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let srv = tokio::spawn(async move {
            let mut s = s;
            assert_eq!(s.read_u32().await?, MAGIC);
            let (mut stream, mode) = accept(s, &server).await?;
            let mut buf = vec![0u8; 21 + 200_000];
            stream.read_exact(&mut buf).await?;
            stream.write_all(b"server: got ").await?;
            stream.write_all(&buf[..21]).await?;
            stream.flush().await?;
            Ok::<_, TransportError>((mode, buf))
        });
        let tapped = Tap { inner: c, seen: seen.clone() };
        let (mut stream, mode) = connect_with(tapped, client, 7, offer_aes).await?;
        let big: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        stream.write_all(b"secret block contents").await?;
        stream.write_all(&big[1..]).await?;
        stream.write_all(&[0]).await?;
        stream.flush().await?;
        let mut reply = vec![0u8; 12 + 21];
        stream.read_exact(&mut reply).await?;
        assert_eq!(&reply, b"server: got secret block contents");
        let (server_mode, got) = srv.await.unwrap()?;
        assert_eq!(&got[..21], b"secret block contents");
        assert!(got[21..21 + 199_999] == big[1..] && got[21 + 199_999] == 0, "the large message arrives intact");
        let wire = seen.lock().unwrap().clone();
        Ok((mode, server_mode, wire))
    }

    fn shows(wire: &[u8], text: &[u8]) -> bool {
        wire.windows(text.len()).any(|w| w == text)
    }

    #[tokio::test]
    async fn every_level_negotiates_and_carries_data() {
        let k = keys();
        let cases = [
            (DataSecurity { encrypt: true, protection: None }, true, Mode::Aes, false),
            (DataSecurity { encrypt: true, protection: None }, false, Mode::Sasl(Qop::Privacy), false),
            (DataSecurity { encrypt: false, protection: Some("privacy".parse().unwrap()) }, true, Mode::Aes, false),
            (DataSecurity { encrypt: false, protection: Some("integrity".parse().unwrap()) }, true, Mode::Sasl(Qop::Integrity), true),
            (DataSecurity { encrypt: false, protection: Some("authentication".parse().unwrap()) }, true, Mode::Plain, true),
        ];
        for (security, offer_aes, want, plaintext_on_wire) in cases {
            let (_d1, client) = node(&k, security.clone());
            let (_d2, server) = node(&k, security.clone());
            let (mode, server_mode, wire) = exchange(&client, server, offer_aes).await.unwrap();
            assert_eq!((mode, server_mode), (want, want), "{:?} offer_aes={}", security, offer_aes);
            assert_eq!(shows(&wire, b"secret block contents"), plaintext_on_wire, "{:?}: is the data readable on the wire?", security);
        }
    }

    #[tokio::test]
    async fn credentials_from_another_cluster_are_refused() {
        let (other, ours) = (keys(), keys());
        for security in [DataSecurity { encrypt: true, protection: None }, DataSecurity { encrypt: false, protection: Some("privacy".parse().unwrap()) }] {
            let (_d1, intruder) = node(&other, security.clone());
            let (_d2, server) = node(&ours, security.clone());
            let e = exchange(&intruder, server, true).await.err().expect("refused");
            assert!(e.to_string().contains("data transfer SASL refused"), "{e}");
        }
    }

    #[tokio::test]
    async fn the_namenodes_data_encryption_key_opens_a_connection() {
        // What a Hadoop client does with dfs.encrypt.data.transfer: a key
        // from getDataEncryptionKey, recomputed by the DataNode.
        let k = keys();
        let (_d, server) = node(&k, DataSecurity { encrypt: true, protection: None });
        let key = k.data_encryption_key();
        let b64 = &base64::engine::general_purpose::STANDARD;
        let username = format!("{} {} {}", key.key_id, key.block_pool_id, b64.encode(&key.nonce));
        assert_eq!(password_for(&server, &username), Some(b64.encode(&key.encryption_key)));
        // A key id the DataNode does not know (rolled away): unknown.
        let unknown = format!("{} {} {}", key.key_id.wrapping_add(99), key.block_pool_id, b64.encode(&key.nonce));
        assert_eq!(password_for(&server, &unknown), None);
    }

    #[tokio::test]
    async fn a_datanode_requiring_security_refuses_plain_connections() {
        let k = keys();
        let (_d, server) = node(&k, DataSecurity { encrypt: true, protection: None });
        let (mut c, s) = tokio::io::duplex(4096);
        let peer = "127.0.0.1:1".parse().unwrap();
        let srv = tokio::spawn(super::super::wire::serve_connection(s, server, peer));
        // A Hadoop DataTransferProtocol READ_BLOCK, unencrypted.
        c.write_all(&[0, 28, 81]).await.unwrap();
        c.flush().await.unwrap();
        let mut buf = [0u8; 1];
        assert_eq!(c.read(&mut buf).await.unwrap(), 0, "the DataNode closes the connection");
        srv.await.unwrap().unwrap();
    }
}

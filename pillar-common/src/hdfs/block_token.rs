//! Block access tokens (HDFS `BlockTokenSecretManager`).
//!
//! The NameNode hands every block it returns to a client together with a
//! token: an identifier (which block, which access, until when, under which
//! key) and its password, an HMAC-SHA1 of the identifier under a secret
//! block key. DataNodes hold the same keys (the NameNode sends them at
//! registration and when they roll) and check the token on every data
//! transfer, so a client can only read or write blocks the NameNode gave it.
//!
//! Keys roll every `key_update_interval` and stay valid until every token
//! they signed has expired. Tokens are opaque to clients, so the identifier
//! is Pillar's own compact encoding (only Pillar mints and checks them).

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use pillar_proto::hadoop::common::TokenProto;
use pillar_proto::hdfs::{BlockKeyProto, ExportedBlockKeysProto};
use sha1::Sha1;

/// `TokenProto.kind` of a block token.
pub const TOKEN_KIND: &str = "HDFS_BLOCK_TOKEN";

/// HDFS defaults: keys roll every 10 hours, tokens last 10 hours.
pub const DEFAULT_KEY_UPDATE_INTERVAL: Duration = Duration::from_secs(10 * 3600);
pub const DEFAULT_TOKEN_LIFETIME: Duration = Duration::from_secs(10 * 3600);

/// Access modes (HDFS `BlockTokenIdentifier.AccessMode`), as bits.
pub mod mode {
    pub const READ: u8 = 1;
    pub const WRITE: u8 = 2;
    /// Copy a replica to another DataNode (adding one to a pipeline).
    pub const COPY: u8 = 4;
    pub const REPLACE: u8 = 8;
}

const IDENTIFIER_MAGIC: &[u8; 4] = b"PBT1";

/// What a token grants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockTokenIdentifier {
    /// Milliseconds since the epoch.
    pub expiry_ms: u64,
    pub key_id: u32,
    /// Who it was issued to (informational, like HDFS's userId).
    pub user: String,
    pub block_pool_id: String,
    pub block_id: u64,
    /// [`mode`] bits.
    pub modes: u8,
}

impl BlockTokenIdentifier {
    pub fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(32 + self.user.len() + self.block_pool_id.len());
        v.extend_from_slice(IDENTIFIER_MAGIC);
        v.extend_from_slice(&self.expiry_ms.to_be_bytes());
        v.extend_from_slice(&self.key_id.to_be_bytes());
        v.extend_from_slice(&self.block_id.to_be_bytes());
        v.push(self.modes);
        for s in [&self.user, &self.block_pool_id] {
            let s = &s.as_bytes()[..s.len().min(u16::MAX as usize)];
            v.extend_from_slice(&(s.len() as u16).to_be_bytes());
            v.extend_from_slice(s);
        }
        v
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        let rest = b.strip_prefix(IDENTIFIER_MAGIC)?;
        let u64_at = |i: usize| Some(u64::from_be_bytes(rest.get(i..i + 8)?.try_into().ok()?));
        let expiry_ms = u64_at(0)?;
        let key_id = u32::from_be_bytes(rest.get(8..12)?.try_into().ok()?);
        let block_id = u64_at(12)?;
        let modes = *rest.get(20)?;
        let mut at = 21;
        let mut text = || {
            let n = u16::from_be_bytes(rest.get(at..at + 2)?.try_into().ok()?) as usize;
            let s = String::from_utf8(rest.get(at + 2..at + 2 + n)?.to_vec()).ok()?;
            at += 2 + n;
            Some(s)
        };
        let user = text()?;
        let block_pool_id = text()?;
        Some(Self { expiry_ms, key_id, user, block_pool_id, block_id, modes })
    }
}

/// A secret block key.
#[derive(Clone)]
pub struct BlockKey {
    pub id: u32,
    pub expiry_ms: u64,
    pub bytes: Vec<u8>,
}

impl std::fmt::Debug for BlockKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockKey").field("id", &self.id).field("expiry_ms", &self.expiry_ms).finish_non_exhaustive()
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = <Hmac<Sha1> as Mac>::new_from_slice(key).expect("HMAC takes any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Constant-time comparison of a password.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn token(identifier: &BlockTokenIdentifier, key: &BlockKey) -> TokenProto {
    let identifier = identifier.encode();
    TokenProto {
        password: hmac(&key.bytes, &identifier),
        identifier,
        kind: TOKEN_KIND.to_string(),
        service: String::new(),
    }
}

/// The NameNode side: keys, rolling, minting.
pub struct BlockTokenSecretManager {
    state: RwLock<Keys>,
    block_pool_id: String,
    key_update_interval: Duration,
    token_lifetime: Duration,
}

struct Keys {
    /// Oldest first; the last is current.
    keys: Vec<BlockKey>,
    next_id: u32,
    rolled: Instant,
    /// Bumped on every roll, so DataNodes can be sent the new set.
    version: u64,
}

impl BlockTokenSecretManager {
    pub fn new(block_pool_id: &str, key_update_interval: Duration, token_lifetime: Duration) -> Self {
        let keys = Keys { keys: Vec::new(), next_id: rand::random::<u32>() >> 1, rolled: Instant::now(), version: 0 };
        let m = Self { state: RwLock::new(keys), block_pool_id: block_pool_id.to_string(), key_update_interval, token_lifetime };
        m.add_key(&mut m.state.write().unwrap());
        m
    }

    fn add_key(&self, k: &mut Keys) {
        // Valid until every token it may sign has expired (HDFS: two update
        // intervals plus a token lifetime).
        let life = 2 * self.key_update_interval + self.token_lifetime;
        let key = BlockKey {
            id: k.next_id,
            expiry_ms: now_ms() + life.as_millis() as u64,
            bytes: (0..64).map(|_| rand::random::<u8>()).collect(),
        };
        k.next_id = k.next_id.wrapping_add(1);
        // Drop expired keys, but never the new current one.
        let now = now_ms();
        k.keys.retain(|key| key.expiry_ms > now);
        k.keys.push(key);
        k.rolled = Instant::now();
        k.version += 1;
    }

    /// Roll to a new key if the update interval has passed. Returns whether
    /// it did (DataNodes then need [`Self::export`]).
    pub fn roll_if_due(&self) -> bool {
        let mut k = self.state.write().unwrap();
        if k.rolled.elapsed() < self.key_update_interval {
            return false;
        }
        self.add_key(&mut k);
        true
    }

    /// The key set's version: changes when keys roll.
    pub fn version(&self) -> u64 {
        self.state.read().unwrap().version
    }

    /// A token for `user` to access `block_id` with `modes` ([`mode`] bits).
    pub fn generate(&self, user: &str, block_id: u64, modes: u8) -> TokenProto {
        let k = self.state.read().unwrap();
        let key = k.keys.last().expect("a current key");
        let id = BlockTokenIdentifier {
            expiry_ms: now_ms() + self.token_lifetime.as_millis() as u64,
            key_id: key.id,
            user: user.to_string(),
            block_pool_id: self.block_pool_id.clone(),
            block_id,
            modes,
        };
        token(&id, key)
    }

    /// Every live key, for DataNodes.
    pub fn export(&self) -> ExportedBlockKeysProto {
        let k = self.state.read().unwrap();
        let proto = |key: &BlockKey| BlockKeyProto { key_id: key.id, expiry_date: key.expiry_ms, key_bytes: Some(key.bytes.clone()) };
        ExportedBlockKeysProto {
            is_block_token_enabled: true,
            key_update_interval: self.key_update_interval.as_millis() as u64,
            token_life_time: self.token_lifetime.as_millis() as u64,
            current_key: proto(k.keys.last().expect("a current key")),
            all_keys: k.keys.iter().map(proto).collect(),
        }
    }
}

/// The DataNode side: the NameNode's keys; checks tokens, and mints its
/// own for transfers to other DataNodes (HDFS DataNodes do too).
#[derive(Default)]
pub struct BlockTokenVerifier {
    state: RwLock<VerifierState>,
}

#[derive(Default)]
struct VerifierState {
    enabled: bool,
    keys: HashMap<u32, BlockKey>,
    current: Option<u32>,
    token_lifetime: Duration,
}

impl BlockTokenVerifier {
    /// Take the NameNode's keys (registration, key update command).
    pub fn update(&self, exported: &ExportedBlockKeysProto) {
        let mut s = self.state.write().unwrap();
        s.enabled = exported.is_block_token_enabled;
        if !s.enabled {
            s.keys.clear();
            s.current = None;
            return;
        }
        let key = |p: &BlockKeyProto| BlockKey { id: p.key_id, expiry_ms: p.expiry_date, bytes: p.key_bytes.clone().unwrap_or_default() };
        for p in exported.all_keys.iter().chain(std::iter::once(&exported.current_key)) {
            s.keys.insert(p.key_id, key(p));
        }
        let now = now_ms();
        s.keys.retain(|_, k| k.expiry_ms > now);
        s.current = Some(exported.current_key.key_id);
        s.token_lifetime = Duration::from_millis(exported.token_life_time);
    }

    /// Whether tokens are checked (the NameNode enabled them).
    pub fn enabled(&self) -> bool {
        self.state.read().unwrap().enabled
    }

    /// The password of a token identifier, if one of our live keys signed
    /// it (SASL DIGEST-MD5 with a block token as the credential).
    pub fn password(&self, identifier: &[u8]) -> Option<(BlockTokenIdentifier, Vec<u8>)> {
        let id = BlockTokenIdentifier::decode(identifier)?;
        let s = self.state.read().unwrap();
        let key = s.keys.get(&id.key_id)?;
        (id.expiry_ms > now_ms()).then(|| (id, hmac(&key.bytes, identifier)))
    }

    /// Check that `token` grants `modes` on `block_id`. Always passes when
    /// tokens are not enabled.
    pub fn verify(&self, token: Option<&TokenProto>, block_id: u64, modes: u8) -> Result<(), String> {
        if !self.enabled() {
            return Ok(());
        }
        let token = token.filter(|t| !t.identifier.is_empty()).ok_or("no block access token")?;
        let (id, password) = self.password(&token.identifier).ok_or("block access token expired, unknown key or malformed")?;
        if !same(&password, &token.password) {
            return Err("block access token does not verify".into());
        }
        if id.block_id != block_id {
            return Err(format!("block access token is for block {}, not {}", id.block_id, block_id));
        }
        if id.modes & modes != modes {
            return Err(format!("block access token grants {:#x}, not {:#x}", id.modes, modes));
        }
        Ok(())
    }

    /// A token for this DataNode's own transfer of `block_id` to another
    /// DataNode, or `None` when tokens are off.
    pub fn mint(&self, block_id: u64, modes: u8) -> Option<TokenProto> {
        let s = self.state.read().unwrap();
        let key = s.keys.get(&s.current?)?;
        let id = BlockTokenIdentifier {
            expiry_ms: now_ms() + s.token_lifetime.max(Duration::from_secs(600)).as_millis() as u64,
            key_id: key.id,
            user: "datanode".to_string(),
            block_pool_id: String::new(),
            block_id,
            modes,
        };
        Some(token(&id, key))
    }
}

/// The IPC caller a block token authenticates (SASL DIGEST-MD5 on the
/// DataNode's IPC port): `blocktoken:<block id>:<modes>`.
pub fn token_caller(block_id: u64, modes: u8) -> String {
    format!("blocktoken:{}:{}", block_id, modes)
}

/// The block and modes of a [`token_caller`] name.
pub fn parse_token_caller(user: &str) -> Option<(u64, u8)> {
    let (block, modes) = user.strip_prefix("blocktoken:")?.split_once(':')?;
    Some((block.parse().ok()?, modes.parse().ok()?))
}

impl crate::ipc::TokenAuthority for BlockTokenVerifier {
    fn enabled(&self) -> bool {
        BlockTokenVerifier::enabled(self)
    }

    fn password(&self, identifier: &[u8]) -> Option<(String, Vec<u8>)> {
        if !BlockTokenVerifier::enabled(self) {
            return None;
        }
        let (id, password) = BlockTokenVerifier::password(self, identifier)?;
        Some((token_caller(id.block_id, id.modes), password))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair() -> (BlockTokenSecretManager, BlockTokenVerifier) {
        let m = BlockTokenSecretManager::new("BP-1", DEFAULT_KEY_UPDATE_INTERVAL, DEFAULT_TOKEN_LIFETIME);
        let v = BlockTokenVerifier::default();
        v.update(&m.export());
        (m, v)
    }

    #[test]
    fn identifiers_round_trip() {
        let id = BlockTokenIdentifier { expiry_ms: 123, key_id: 7, user: "alice".into(), block_pool_id: "BP-1".into(), block_id: 42, modes: mode::READ | mode::COPY };
        assert_eq!(BlockTokenIdentifier::decode(&id.encode()), Some(id));
        assert_eq!(BlockTokenIdentifier::decode(b"PBT1short"), None);
        assert_eq!(BlockTokenIdentifier::decode(b"nope"), None);
    }

    #[test]
    fn a_token_grants_its_block_and_modes_only() {
        let (m, v) = pair();
        let t = m.generate("alice", 42, mode::READ);
        assert!(v.verify(Some(&t), 42, mode::READ).is_ok());
        assert!(v.verify(Some(&t), 43, mode::READ).unwrap_err().contains("block 42"));
        assert!(v.verify(Some(&t), 42, mode::WRITE).is_err());
        assert!(v.verify(None, 42, mode::READ).unwrap_err().contains("no block access token"));
        // Tampering with the identifier (say, the block id) breaks the HMAC.
        let mut forged = t.clone();
        let mut id = BlockTokenIdentifier::decode(&forged.identifier).unwrap();
        id.block_id = 43;
        forged.identifier = id.encode();
        assert!(v.verify(Some(&forged), 43, mode::READ).unwrap_err().contains("does not verify"));
        // A key the DataNode never got.
        let other = BlockTokenSecretManager::new("BP-1", DEFAULT_KEY_UPDATE_INTERVAL, DEFAULT_TOKEN_LIFETIME);
        assert!(v.verify(Some(&other.generate("alice", 42, mode::READ)), 42, mode::READ).is_err());
    }

    #[test]
    fn expired_tokens_and_rolled_keys() {
        let m = BlockTokenSecretManager::new("BP-1", Duration::ZERO, Duration::ZERO);
        let v = BlockTokenVerifier::default();
        v.update(&m.export());
        let t = m.generate("alice", 1, mode::READ);
        std::thread::sleep(Duration::from_millis(5));
        assert!(v.verify(Some(&t), 1, mode::READ).unwrap_err().contains("expired"));

        let (m, v) = pair();
        let old = m.generate("alice", 1, mode::READ);
        let version = m.version();
        assert!(!m.roll_if_due());
        let m2 = BlockTokenSecretManager::new("BP-1", Duration::ZERO, DEFAULT_TOKEN_LIFETIME);
        assert!(m2.roll_if_due());
        assert_eq!(m.version(), version);
        // Tokens signed by an older key still verify after a roll.
        v.update(&m.export());
        assert!(v.verify(Some(&old), 1, mode::READ).is_ok());
        assert_eq!(m2.export().all_keys.len(), 2);
    }

    #[test]
    fn datanodes_mint_tokens_other_datanodes_accept() {
        let (_, v) = pair();
        let peer = BlockTokenVerifier::default();
        assert!(peer.mint(5, mode::WRITE).is_none(), "no keys, no tokens");
        let (m, _) = pair();
        v.update(&m.export());
        peer.update(&m.export());
        let t = v.mint(5, mode::WRITE).unwrap();
        assert!(peer.verify(Some(&t), 5, mode::WRITE).is_ok());
        let disabled = BlockTokenVerifier::default();
        assert!(disabled.verify(None, 5, mode::WRITE).is_ok(), "tokens off: nothing to check");
    }
}

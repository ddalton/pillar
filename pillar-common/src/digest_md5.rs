//! SASL DIGEST-MD5 (RFC 2831), server side, as Hadoop uses it for TOKEN
//! authentication: the username is a token's identifier and the password
//! its secret (each base64 encoded), so a client proves it holds a token
//! without sending the secret.
//!
//! Only what Hadoop's client negotiates is supported: `md5-sess`, UTF-8,
//! one authentication per exchange, and the protection the server is
//! configured for: `auth`, `auth-int` (RFC 2831 §2.3: an HMAC-MD5 on every
//! message) or `auth-conf` (§2.4: RC4 as well, the one cipher offered).

use std::collections::HashMap;

use base64::Engine;
use md5::{Digest, Md5};
use hmac::{Hmac, Mac};
use rand::RngCore;

use crate::sasl::{Open, Protection, Qop, Seal, SecurityLayer};

/// Largest message a peer may send through the security layer
/// (`maxbuf`, RFC 2831's maximum).
pub const MAX_BUFFER: usize = 16_777_215;

/// A DIGEST-MD5 server exchange: its challenge, then the client's response.
pub struct DigestServer {
    realm: String,
    /// `<protocol>/<server name>` the client must name.
    digest_uri: String,
    nonce: String,
    protection: Protection,
}

/// A successful exchange.
pub struct DigestOutcome {
    pub username: String,
    /// The final challenge, for the client to authenticate the server.
    pub rspauth: Vec<u8>,
    /// The security layer, unless the client chose `auth`.
    pub layer: Option<SecurityLayer>,
    /// The largest message the client takes (its `maxbuf`).
    pub peer_maxbuf: usize,
}

impl DigestServer {
    pub fn new(realm: &str, protocol: &str, server_name: &str, protection: &Protection) -> Self {
        let mut n = [0u8; 24];
        rand::thread_rng().fill_bytes(&mut n);
        Self {
            realm: realm.to_string(),
            digest_uri: format!("{}/{}", protocol, server_name),
            nonce: base64::engine::general_purpose::STANDARD.encode(n),
            protection: protection.clone(),
        }
    }

    /// The digest-challenge.
    pub fn challenge(&self) -> Vec<u8> {
        let qops: Vec<&str> = self.protection.levels().iter().map(|q| q.sasl_name()).collect();
        let mut c = format!(r#"realm="{}",nonce="{}",qop="{}""#, self.realm, self.nonce, qops.join(","));
        if self.protection.allows(Qop::Integrity) || self.protection.allows(Qop::Privacy) {
            c.push_str(&format!(",maxbuf={}", MAX_BUFFER));
        }
        if self.protection.allows(Qop::Privacy) {
            c.push_str(r#",cipher="rc4""#);
        }
        c.push_str(",charset=utf-8,algorithm=md5-sess");
        c.into_bytes()
    }

    /// Check the client's digest-response, `password` giving the password
    /// of a username. On success: the username and the `rspauth` the
    /// client checks to authenticate the server in turn.
    pub fn respond(&self, response: &[u8], password: impl FnOnce(&str) -> Option<String>) -> Result<DigestOutcome, String> {
        let f = parse(std::str::from_utf8(response).map_err(|_| "digest-response is not UTF-8")?)?;
        let get = |k: &str| f.get(k).map(String::as_str).ok_or_else(|| format!("digest-response has no {}", k));
        let username = get("username")?;
        if get("nonce")? != self.nonce {
            return Err("digest-response for another nonce".into());
        }
        if f.get("realm").map_or(false, |r| *r != self.realm) {
            return Err(format!("digest-response for realm {:?}", f["realm"]));
        }
        let qop_name = f.get("qop").map_or("auth", String::as_str);
        let qop = Qop::from_sasl_name(qop_name)
            .filter(|q| self.protection.allows(*q))
            .ok_or_else(|| format!("QOP {:?} is not offered (offered: {})", qop_name, self.protection))?;
        if qop == Qop::Privacy && f.get("cipher").map(String::as_str) != Some("rc4") {
            return Err(format!("cipher {:?} is not offered (rc4)", f.get("cipher")));
        }
        if get("digest-uri")? != self.digest_uri {
            return Err(format!("digest-uri {:?}, not {:?}", f["digest-uri"], self.digest_uri));
        }
        let (nc, cnonce) = (get("nc")?, get("cnonce")?);
        if nc != "00000001" {
            return Err(format!("nonce count {}", nc));
        }
        let authzid = f.get("authzid").map(String::as_str);
        if authzid.map_or(false, |a| a != username) {
            return Err(format!("{} may not act as {}", username, authzid.unwrap()));
        }
        let password = password(username).ok_or_else(|| format!("unknown or expired token {}", username))?;

        let realm = f.get("realm").map_or("", String::as_str);
        let mut a1 = md5(format!("{}:{}:{}", username, realm, password).as_bytes()).to_vec();
        a1.extend_from_slice(format!(":{}:{}", self.nonce, cnonce).as_bytes());
        if let Some(a) = authzid {
            a1.extend_from_slice(format!(":{}", a).as_bytes());
        }
        let h_a1 = md5(&a1);
        let ha1 = hex(&h_a1);
        let qop_name = qop.sasl_name();
        // auth-int and auth-conf digest A2 with a zero entity-body hash.
        let a2_tail = if qop == Qop::Authentication { String::new() } else { ":00000000000000000000000000000000".to_string() };
        let digest = |a2: &str| {
            hex(&md5(format!("{}:{}:{}:{}:{}:{}", ha1, self.nonce, nc, cnonce, qop_name, hex(&md5(format!("{}{}", a2, a2_tail).as_bytes()))).as_bytes()))
        };

        let expected = digest(&format!("AUTHENTICATE:{}", self.digest_uri));
        if !constant_time_eq(get("response")?.as_bytes(), expected.as_bytes()) {
            return Err("digest-response does not verify (wrong token password)".into());
        }
        let rspauth = format!("rspauth={}", digest(&format!(":{}", self.digest_uri)));
        let layer = (qop != Qop::Authentication).then(|| server_layer(&h_a1, qop));
        let peer_maxbuf = f.get("maxbuf").and_then(|m| m.parse().ok()).unwrap_or(65536);
        Ok(DigestOutcome { username: username.to_string(), rspauth: rspauth.into_bytes(), layer, peer_maxbuf })
    }
}

/// The client side: answer a server's challenge, check its `rspauth`.
pub struct DigestClient {
    username: String,
    password: String,
    digest_uri: String,
    /// The strongest level the client accepts that the server offers.
    qop: Option<Qop>,
    expected_rspauth: Option<String>,
    h_a1: [u8; 16],
    peer_maxbuf: usize,
}

impl DigestClient {
    /// `digest_uri` is `<protocol>/<server name>`; the client asks for the
    /// strongest level of `protection` the server offers.
    pub fn new(username: &str, password: &str, digest_uri: &str) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
            digest_uri: digest_uri.into(),
            qop: None,
            expected_rspauth: None,
            h_a1: [0; 16],
            peer_maxbuf: 65536,
        }
    }

    /// The digest-response to `challenge`, at the strongest level of
    /// `protection` the server offers. The authorization id is the
    /// username (as Hadoop's data transfer client sends it).
    pub fn respond(&mut self, challenge: &[u8], protection: &Protection) -> Result<Vec<u8>, String> {
        let c = parse(std::str::from_utf8(challenge).map_err(|_| "challenge is not UTF-8")?)?;
        let nonce = c.get("nonce").ok_or("challenge has no nonce")?;
        let realm = c.get("realm").map(String::as_str).unwrap_or("");
        let offered: Vec<Qop> = c.get("qop").map_or("auth", String::as_str).split(',').filter_map(|q| Qop::from_sasl_name(q.trim())).collect();
        let qop = protection
            .levels()
            .iter()
            .copied()
            .filter(|q| offered.contains(q))
            .filter(|q| *q != Qop::Privacy || c.get("cipher").is_some_and(|ciphers| ciphers.split(',').any(|x| x.trim() == "rc4")))
            .max()
            .ok_or_else(|| format!("No common protection layer between client and server (server offers {:?})", c.get("qop")))?;
        self.peer_maxbuf = c.get("maxbuf").and_then(|m| m.parse().ok()).unwrap_or(65536);
        let mut n = [0u8; 18];
        rand::thread_rng().fill_bytes(&mut n);
        let cnonce = base64::engine::general_purpose::STANDARD.encode(n);
        let authzid = &self.username;
        let mut a1 = md5(format!("{}:{}:{}", self.username, realm, self.password).as_bytes()).to_vec();
        a1.extend_from_slice(format!(":{}:{}:{}", nonce, cnonce, authzid).as_bytes());
        self.h_a1 = md5(&a1);
        let ha1 = hex(&self.h_a1);
        let tail = if qop == Qop::Authentication { String::new() } else { ":00000000000000000000000000000000".to_string() };
        let digest = |a2: &str| {
            hex(&md5(format!("{}:{}:00000001:{}:{}:{}", ha1, nonce, cnonce, qop.sasl_name(), hex(&md5(format!("{}{}", a2, tail).as_bytes()))).as_bytes()))
        };
        let response = digest(&format!("AUTHENTICATE:{}", self.digest_uri));
        self.expected_rspauth = Some(format!("rspauth={}", digest(&format!(":{}", self.digest_uri))));
        self.qop = Some(qop);
        let quote = |v: &str| v.replace('\\', "\\\\").replace('"', "\\\"");
        let mut out = format!(
            r#"charset=utf-8,username="{}",realm="{}",nonce="{}",nc=00000001,cnonce="{}",digest-uri="{}",maxbuf={},response={},qop={},authzid="{}""#,
            quote(&self.username),
            quote(realm),
            nonce,
            cnonce,
            self.digest_uri,
            MAX_BUFFER,
            response,
            qop.sasl_name(),
            quote(authzid)
        );
        if qop == Qop::Privacy {
            out.push_str(",cipher=rc4");
        }
        Ok(out.into_bytes())
    }

    /// Check the server's final message; the client's half of the layer
    /// (unless only `auth` was negotiated).
    pub fn finish(&self, rspauth: &[u8]) -> Result<Option<SecurityLayer>, String> {
        let expected = self.expected_rspauth.as_ref().ok_or("finish before respond")?;
        if !constant_time_eq(rspauth, expected.as_bytes()) {
            return Err("the server's rspauth does not verify".into());
        }
        let qop = self.qop.unwrap_or(Qop::Authentication);
        Ok((qop != Qop::Authentication).then(|| client_layer(&self.h_a1, qop)))
    }

    /// The largest message the server takes (its `maxbuf`).
    pub fn peer_maxbuf(&self) -> usize {
        self.peer_maxbuf
    }
}

/// The client's half of the layer: the mirror image of the server's.
fn client_layer(h_a1: &[u8; 16], qop: Qop) -> SecurityLayer {
    let cipher = |magic| (qop == Qop::Privacy).then(|| Rc4::new(&derive(h_a1, magic)));
    SecurityLayer {
        qop,
        seal: Box::new(Direction { ki: derive(h_a1, SIGN_C2S), cipher: cipher(SEAL_C2S), seq: 0 }),
        open: Box::new(Direction { ki: derive(h_a1, SIGN_S2C), cipher: cipher(SEAL_S2C), seq: 0 }),
    }
}

// ------------------------------------------------------- security layer

const SIGN_C2S: &[u8] = b"Digest session key to client-to-server signing key magic constant";
const SIGN_S2C: &[u8] = b"Digest session key to server-to-client signing key magic constant";
const SEAL_C2S: &[u8] = b"Digest H(A1) to client-to-server sealing key magic constant";
const SEAL_S2C: &[u8] = b"Digest H(A1) to server-to-client sealing key magic constant";

fn derive(prefix: &[u8], magic: &[u8]) -> [u8; 16] {
    md5(&[prefix, magic].concat())
}

/// The server's half of the layer: it opens with the client-to-server keys
/// and seals with the server-to-client ones.
fn server_layer(h_a1: &[u8; 16], qop: Qop) -> SecurityLayer {
    let (kic, kis) = (derive(h_a1, SIGN_C2S), derive(h_a1, SIGN_S2C));
    let cipher = |magic| (qop == Qop::Privacy).then(|| Rc4::new(&derive(h_a1, magic)));
    SecurityLayer {
        qop,
        seal: Box::new(Direction { ki: kis, cipher: cipher(SEAL_S2C), seq: 0 }),
        open: Box::new(Direction { ki: kic, cipher: cipher(SEAL_C2S), seq: 0 }),
    }
}

/// One direction of the layer: its signing key, its RC4 stream (privacy),
/// and the next sequence number (each side counts from 0).
struct Direction {
    ki: [u8; 16],
    cipher: Option<Rc4>,
    seq: u32,
}

impl Direction {
    fn mac(&self, seq: u32, message: &[u8]) -> [u8; 10] {
        let mut m = <Hmac<Md5> as Mac>::new_from_slice(&self.ki).expect("any key length");
        m.update(&seq.to_be_bytes());
        m.update(message);
        m.finalize().into_bytes()[..10].try_into().unwrap()
    }
}

/// RFC 2831: `msg || HMAC[0..10] || 0x0001 || seq` (integrity), or
/// `RC4(msg || HMAC[0..10]) || 0x0001 || seq` (confidentiality; RC4 is a
/// stream cipher, so there is no padding).
impl Seal for Direction {
    fn seal(&mut self, message: &[u8]) -> Result<Vec<u8>, String> {
        let seq = self.seq;
        self.seq = self.seq.wrapping_add(1);
        let mut out = Vec::with_capacity(message.len() + 16);
        out.extend_from_slice(message);
        out.extend_from_slice(&self.mac(seq, message));
        if let Some(c) = self.cipher.as_mut() {
            c.apply(&mut out);
        }
        out.extend_from_slice(&[0, 1]);
        out.extend_from_slice(&seq.to_be_bytes());
        Ok(out)
    }
}

impl Open for Direction {
    fn open(&mut self, token: &[u8]) -> Result<Vec<u8>, String> {
        if token.len() < 16 {
            return Err("DIGEST-MD5 message too short".into());
        }
        let (body, trailer) = token.split_at(token.len() - 6);
        if trailer[..2] != [0, 1] {
            return Err("DIGEST-MD5 message type is not 1".into());
        }
        let seq = u32::from_be_bytes(trailer[2..].try_into().unwrap());
        if seq != self.seq {
            return Err(format!("DIGEST-MD5 sequence number {} (expected {}): replayed or reordered", seq, self.seq));
        }
        let mut body = body.to_vec();
        if let Some(c) = self.cipher.as_mut() {
            c.apply(&mut body);
        }
        let (message, mac) = body.split_at(body.len() - 10);
        if !constant_time_eq(mac, &self.mac(seq, message)) {
            return Err("DIGEST-MD5 message integrity check failed".into());
        }
        self.seq = self.seq.wrapping_add(1);
        Ok(message.to_vec())
    }
}

/// RC4 (the `rc4` cipher of RFC 2831 §2.4; its state carries across
/// messages).
struct Rc4 {
    s: [u8; 256],
    i: u8,
    j: u8,
}

impl Rc4 {
    fn new(key: &[u8]) -> Self {
        let mut s = [0u8; 256];
        for (i, x) in s.iter_mut().enumerate() {
            *x = i as u8;
        }
        let mut j = 0u8;
        for i in 0..256 {
            j = j.wrapping_add(s[i]).wrapping_add(key[i % key.len()]);
            s.swap(i, j as usize);
        }
        Self { s, i: 0, j: 0 }
    }

    fn apply(&mut self, data: &mut [u8]) {
        for b in data {
            self.i = self.i.wrapping_add(1);
            self.j = self.j.wrapping_add(self.s[self.i as usize]);
            self.s.swap(self.i as usize, self.j as usize);
            *b ^= self.s[self.s[self.i as usize].wrapping_add(self.s[self.j as usize]) as usize];
        }
    }
}

fn md5(data: &[u8]) -> [u8; 16] {
    Md5::digest(data).into()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}

/// `key=value` and `key="quoted\"value"` pairs, comma separated.
fn parse(s: &str) -> Result<HashMap<String, String>, String> {
    let mut out = HashMap::new();
    let mut chars = s.chars().peekable();
    loop {
        while chars.peek().map_or(false, |c| *c == ',' || c.is_whitespace()) {
            chars.next();
        }
        if chars.peek().is_none() {
            return Ok(out);
        }
        let key: String = std::iter::from_fn(|| chars.next_if(|c| *c != '=')).collect();
        if chars.next() != Some('=') {
            return Err(format!("malformed digest-response near {:?}", key));
        }
        let mut value = String::new();
        if chars.peek() == Some(&'"') {
            chars.next();
            loop {
                match chars.next() {
                    Some('"') => break,
                    Some('\\') => value.extend(chars.next()),
                    Some(c) => value.push(c),
                    None => return Err("unterminated quoted string in digest-response".into()),
                }
            }
        } else {
            value.extend(std::iter::from_fn(|| chars.next_if(|c| *c != ',')));
        }
        out.insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A client side, as Java's DIGEST-MD5 client computes it.
    fn client_response(challenge: &[u8], user: &str, password: &str, uri: &str) -> String {
        client_response_with(challenge, user, password, uri, Qop::Authentication).0
    }

    /// The response for `qop`, and H(A1) (the layer's key material).
    fn client_response_with(challenge: &[u8], user: &str, password: &str, uri: &str, qop: Qop) -> (String, [u8; 16]) {
        let c = parse(std::str::from_utf8(challenge).unwrap()).unwrap();
        let (realm, nonce, cnonce) = (&c["realm"], &c["nonce"], "Q2xpZW50Tm9uY2U=");
        let mut a1 = md5(format!("{}:{}:{}", user, realm, password).as_bytes()).to_vec();
        a1.extend_from_slice(format!(":{}:{}", nonce, cnonce).as_bytes());
        let tail = if qop == Qop::Authentication { "" } else { ":00000000000000000000000000000000" };
        let ha2 = hex(&md5(format!("AUTHENTICATE:{}{}", uri, tail).as_bytes()));
        let q = qop.sasl_name();
        let response = hex(&md5(format!("{}:{}:00000001:{}:{}:{}", hex(&md5(&a1)), nonce, cnonce, q, ha2).as_bytes()));
        let cipher = if qop == Qop::Privacy { ",cipher=rc4" } else { "" };
        (
            format!(
                r#"charset=utf-8,username="{user}",realm="{realm}",nonce="{nonce}",nc=00000001,cnonce="{cnonce}",digest-uri="{uri}",maxbuf=65536,response={response},qop={q}{cipher}"#
            ),
            md5(&a1),
        )
    }

    fn client_halves(h_a1: &[u8; 16], qop: Qop) -> (Box<dyn Seal>, Box<dyn Open>) {
        let l = client_layer(h_a1, qop);
        (l.seal, l.open)
    }

    #[test]
    fn digest_client_and_server_agree_at_every_level() {
        for level in [Qop::Authentication, Qop::Integrity, Qop::Privacy] {
            let p: Protection = format!("{}", level).parse().unwrap();
            let server = DigestServer::new("0", "hdfs", "0", &p);
            let mut client = DigestClient::new("key 7 BP-1 bm9uY2U=", "cGFzc3dvcmQ=", "hdfs/0");
            let resp = client.respond(&server.challenge(), &"authentication,integrity,privacy".parse().unwrap()).unwrap();
            let done = server.respond(&resp, |u| (u == "key 7 BP-1 bm9uY2U=").then(|| "cGFzc3dvcmQ=".into())).unwrap();
            let layer = client.finish(&done.rspauth).unwrap();
            assert_eq!(layer.as_ref().map(|l| l.qop), (level != Qop::Authentication).then_some(level));
            if let (Some(mut c), Some(mut s)) = (layer, done.layer) {
                let t = c.seal.seal(b"an AES key").unwrap();
                assert_eq!(s.open.open(&t).unwrap(), b"an AES key");
                let t = s.seal.seal(b"another").unwrap();
                assert_eq!(c.open.open(&t).unwrap(), b"another");
            }
            assert!(client.finish(b"rspauth=00000000000000000000000000000000").is_err());
        }
        // A client that needs privacy refuses a server offering less.
        let server = DigestServer::new("0", "hdfs", "0", &Protection::default());
        let mut client = DigestClient::new("u", "p", "hdfs/0");
        assert!(client.respond(&server.challenge(), &"privacy".parse().unwrap()).unwrap_err().contains("No common protection layer"));
    }

    #[test]
    fn rc4_matches_the_published_test_vector() {
        let mut data = *b"Plaintext";
        Rc4::new(b"Key").apply(&mut data);
        assert_eq!(hex(&data), "bbf316e8d940af0ad3");
    }

    #[test]
    fn integrity_and_privacy_layers_protect_every_message() {
        for qop in [Qop::Integrity, Qop::Privacy] {
            let s = DigestServer::new("default", "", "default", &format!("{}", qop).parse().unwrap());
            let challenge = String::from_utf8(s.challenge()).unwrap();
            assert!(challenge.contains(&format!(r#"qop="{}""#, qop.sasl_name())), "{challenge}");
            assert_eq!(challenge.contains(r#"cipher="rc4""#), qop == Qop::Privacy);
            // A client wanting less than the server requires is refused.
            let weak = client_response(challenge.as_bytes(), "dG9rZW4=", "c2VjcmV0", "/default");
            assert!(s.respond(weak.as_bytes(), |_| Some("c2VjcmV0".into())).is_err());

            let (resp, h_a1) = client_response_with(challenge.as_bytes(), "dG9rZW4=", "c2VjcmV0", "/default", qop);
            let mut server = s.respond(resp.as_bytes(), |_| Some("c2VjcmV0".into())).unwrap().layer.expect("a layer");
            assert_eq!(server.qop, qop);
            let (mut c_seal, mut c_open) = client_halves(&h_a1, qop);
            for i in 0..3 {
                let msg = format!("call {i}: some RPC bytes");
                let sealed = c_seal.seal(msg.as_bytes()).unwrap();
                assert_eq!(sealed.windows(9).any(|w| w == b"RPC bytes"), qop == Qop::Integrity, "privacy hides the bytes");
                assert_eq!(server.open.open(&sealed).unwrap(), msg.as_bytes());
                let reply = server.seal.seal(b"the reply").unwrap();
                assert_eq!(c_open.open(&reply).unwrap(), b"the reply");
            }
            // Tampered, or replayed: refused (each on a fresh pair, as a
            // refusal ends the connection).
            let (mut c_seal, _) = client_halves(&h_a1, qop);
            let mut s_open = server_layer(&h_a1, qop).open;
            let mut sealed = c_seal.seal(b"pay alice").unwrap();
            sealed[2] ^= 1;
            assert!(s_open.open(&sealed).is_err(), "tampered");
            let (mut c_seal, _) = client_halves(&h_a1, qop);
            let mut s_open = server_layer(&h_a1, qop).open;
            let sealed = c_seal.seal(b"pay alice").unwrap();
            assert!(s_open.open(&sealed).is_ok());
            assert!(s_open.open(&sealed).is_err(), "replayed");
        }
    }


    #[test]
    fn a_client_holding_the_password_authenticates() {
        let s = DigestServer::new("default", "", "default", &Protection::default());
        let challenge = s.challenge();
        let resp = client_response(&challenge, "dG9rZW4=", "c2VjcmV0", "/default");
        let done = s.respond(resp.as_bytes(), |u| (u == "dG9rZW4=").then(|| "c2VjcmV0".to_string())).unwrap();
        assert_eq!(done.username, "dG9rZW4=");
        assert!(done.rspauth.starts_with(b"rspauth=") && done.rspauth.len() == 8 + 32);
        assert!(done.layer.is_none());

        // Wrong password, unknown user, another server's uri: refused.
        let wrong = client_response(&challenge, "dG9rZW4=", "bm9wZQ==", "/default");
        assert!(s.respond(wrong.as_bytes(), |_| Some("c2VjcmV0".into())).is_err());
        assert!(s.respond(resp.as_bytes(), |_| None).is_err());
        let elsewhere = client_response(&challenge, "dG9rZW4=", "c2VjcmV0", "nn/default");
        assert!(s.respond(elsewhere.as_bytes(), |_| Some("c2VjcmV0".into())).is_err());
        // A response to another server's nonce.
        let other = DigestServer::new("default", "", "default", &Protection::default());
        assert!(other.respond(resp.as_bytes(), |_| Some("c2VjcmV0".into())).is_err());
    }

    /// RFC 2831 section 4's example exchange (IMAP, "chris"/"secret").
    #[test]
    fn rfc_2831_example() {
        let s = DigestServer {
            realm: "elwood.innosoft.com".into(),
            digest_uri: "imap/elwood.innosoft.com".into(),
            nonce: "OA6MG9tEQGm2hh".into(),
            protection: Protection::default(),
        };
        let resp = r#"charset=utf-8,username="chris",realm="elwood.innosoft.com",nonce="OA6MG9tEQGm2hh",nc=00000001,cnonce="OA6MHXh6VqTrRk",digest-uri="imap/elwood.innosoft.com",response=d388dad90d4bbd760a152321f2143af7,qop=auth"#;
        let done = s.respond(resp.as_bytes(), |_| Some("secret".into())).unwrap();
        assert_eq!(done.username, "chris");
        assert_eq!(done.rspauth, b"rspauth=ea40f60335c427b5527b84dbabcdfffd");
    }
}

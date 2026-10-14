//! SASL DIGEST-MD5 (RFC 2831), server side, as Hadoop uses it for TOKEN
//! authentication: the username is a token's identifier and the password
//! its secret (each base64 encoded), so a client proves it holds a token
//! without sending the secret.
//!
//! Only what Hadoop's client negotiates is supported: QOP `auth` (no
//! security layer), `md5-sess`, UTF-8, one authentication per exchange.

use std::collections::HashMap;

use base64::Engine;
use md5::{Digest, Md5};
use rand::RngCore;

/// A DIGEST-MD5 server exchange: its challenge, then the client's response.
pub struct DigestServer {
    realm: String,
    /// `<protocol>/<server name>` the client must name.
    digest_uri: String,
    nonce: String,
}

impl DigestServer {
    pub fn new(realm: &str, protocol: &str, server_name: &str) -> Self {
        let mut n = [0u8; 24];
        rand::thread_rng().fill_bytes(&mut n);
        Self {
            realm: realm.to_string(),
            digest_uri: format!("{}/{}", protocol, server_name),
            nonce: base64::engine::general_purpose::STANDARD.encode(n),
        }
    }

    /// The digest-challenge.
    pub fn challenge(&self) -> Vec<u8> {
        format!(r#"realm="{}",nonce="{}",qop="auth",charset=utf-8,algorithm=md5-sess"#, self.realm, self.nonce).into_bytes()
    }

    /// Check the client's digest-response, `password` giving the password
    /// of a username. On success: the username and the `rspauth` the
    /// client checks to authenticate the server in turn.
    pub fn respond(&self, response: &[u8], password: impl FnOnce(&str) -> Option<String>) -> Result<(String, Vec<u8>), String> {
        let f = parse(std::str::from_utf8(response).map_err(|_| "digest-response is not UTF-8")?)?;
        let get = |k: &str| f.get(k).map(String::as_str).ok_or_else(|| format!("digest-response has no {}", k));
        let username = get("username")?;
        if get("nonce")? != self.nonce {
            return Err("digest-response for another nonce".into());
        }
        if f.get("realm").map_or(false, |r| *r != self.realm) {
            return Err(format!("digest-response for realm {:?}", f["realm"]));
        }
        if f.get("qop").map_or("auth", String::as_str) != "auth" {
            return Err(format!("QOP {:?} is not offered", f["qop"]));
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
        let ha1 = hex(&md5(&a1));
        let digest = |a2: &str| hex(&md5(format!("{}:{}:{}:{}:auth:{}", ha1, self.nonce, nc, cnonce, hex(&md5(a2.as_bytes()))).as_bytes()));

        let expected = digest(&format!("AUTHENTICATE:{}", self.digest_uri));
        if !constant_time_eq(get("response")?.as_bytes(), expected.as_bytes()) {
            return Err("digest-response does not verify (wrong token password)".into());
        }
        let rspauth = format!("rspauth={}", digest(&format!(":{}", self.digest_uri)));
        Ok((username.to_string(), rspauth.into_bytes()))
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
        let c = parse(std::str::from_utf8(challenge).unwrap()).unwrap();
        let (realm, nonce, cnonce) = (&c["realm"], &c["nonce"], "Q2xpZW50Tm9uY2U=");
        let mut a1 = md5(format!("{}:{}:{}", user, realm, password).as_bytes()).to_vec();
        a1.extend_from_slice(format!(":{}:{}", nonce, cnonce).as_bytes());
        let ha2 = hex(&md5(format!("AUTHENTICATE:{}", uri).as_bytes()));
        let response = hex(&md5(format!("{}:{}:00000001:{}:auth:{}", hex(&md5(&a1)), nonce, cnonce, ha2).as_bytes()));
        format!(
            r#"charset=utf-8,username="{user}",realm="{realm}",nonce="{nonce}",nc=00000001,cnonce="{cnonce}",digest-uri="{uri}",maxbuf=65536,response={response},qop=auth"#
        )
    }

    #[test]
    fn a_client_holding_the_password_authenticates() {
        let s = DigestServer::new("default", "", "default");
        let challenge = s.challenge();
        let resp = client_response(&challenge, "dG9rZW4=", "c2VjcmV0", "/default");
        let (user, rspauth) = s.respond(resp.as_bytes(), |u| (u == "dG9rZW4=").then(|| "c2VjcmV0".to_string())).unwrap();
        assert_eq!(user, "dG9rZW4=");
        assert!(rspauth.starts_with(b"rspauth=") && rspauth.len() == 8 + 32);

        // Wrong password, unknown user, another server's uri: refused.
        let wrong = client_response(&challenge, "dG9rZW4=", "bm9wZQ==", "/default");
        assert!(s.respond(wrong.as_bytes(), |_| Some("c2VjcmV0".into())).is_err());
        assert!(s.respond(resp.as_bytes(), |_| None).is_err());
        let elsewhere = client_response(&challenge, "dG9rZW4=", "c2VjcmV0", "nn/default");
        assert!(s.respond(elsewhere.as_bytes(), |_| Some("c2VjcmV0".into())).is_err());
        // A response to another server's nonce.
        let other = DigestServer::new("default", "", "default");
        assert!(other.respond(resp.as_bytes(), |_| Some("c2VjcmV0".into())).is_err());
    }

    /// RFC 2831 section 4's example exchange (IMAP, "chris"/"secret").
    #[test]
    fn rfc_2831_example() {
        let s = DigestServer {
            realm: "elwood.innosoft.com".into(),
            digest_uri: "imap/elwood.innosoft.com".into(),
            nonce: "OA6MG9tEQGm2hh".into(),
        };
        let resp = r#"charset=utf-8,username="chris",realm="elwood.innosoft.com",nonce="OA6MG9tEQGm2hh",nc=00000001,cnonce="OA6MHXh6VqTrRk",digest-uri="imap/elwood.innosoft.com",response=d388dad90d4bbd760a152321f2143af7,qop=auth"#;
        let (user, rspauth) = s.respond(resp.as_bytes(), |_| Some("secret".into())).unwrap();
        assert_eq!(user, "chris");
        assert_eq!(rspauth, b"rspauth=ea40f60335c427b5527b84dbabcdfffd");
    }
}

//! SASL quality of protection (Hadoop's `hadoop.rpc.protection`) and the
//! security layer a SASL exchange can leave behind, shared by GSSAPI
//! (Kerberos) and DIGEST-MD5 (tokens).
//!
//! With `integrity` or `privacy`, every RPC after authentication travels
//! inside the layer: signed, or signed and encrypted.

use std::fmt;
use std::str::FromStr;

/// One protection level, weakest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Qop {
    /// Authentication only (`auth`).
    Authentication,
    /// Every message signed (`auth-int`).
    Integrity,
    /// Every message signed and encrypted (`auth-conf`).
    Privacy,
}

impl Qop {
    /// The SASL name (`auth`, `auth-int`, `auth-conf`).
    pub fn sasl_name(self) -> &'static str {
        match self {
            Qop::Authentication => "auth",
            Qop::Integrity => "auth-int",
            Qop::Privacy => "auth-conf",
        }
    }

    pub fn from_sasl_name(s: &str) -> Option<Self> {
        match s {
            "auth" => Some(Qop::Authentication),
            "auth-int" => Some(Qop::Integrity),
            "auth-conf" => Some(Qop::Privacy),
            _ => None,
        }
    }

    /// RFC 4752 security layer bit.
    pub fn gssapi_bit(self) -> u8 {
        match self {
            Qop::Authentication => 1,
            Qop::Integrity => 2,
            Qop::Privacy => 4,
        }
    }
}

impl fmt::Display for Qop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Qop::Authentication => "authentication",
            Qop::Integrity => "integrity",
            Qop::Privacy => "privacy",
        })
    }
}

/// The levels a server accepts, as Hadoop's comma separated
/// `hadoop.rpc.protection` (`authentication`, `integrity`, `privacy`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Protection(Vec<Qop>);

impl Protection {
    pub fn levels(&self) -> &[Qop] {
        &self.0
    }

    /// Whether `qop` is one of the accepted levels.
    pub fn allows(&self, qop: Qop) -> bool {
        self.0.contains(&qop)
    }
}

impl Default for Protection {
    fn default() -> Self {
        Protection(vec![Qop::Authentication])
    }
}

impl FromStr for Protection {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let mut levels = Vec::new();
        for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let qop = match part.to_ascii_lowercase().as_str() {
                "authentication" => Qop::Authentication,
                "integrity" => Qop::Integrity,
                "privacy" => Qop::Privacy,
                other => return Err(format!("unknown RPC protection {:?} (authentication, integrity, privacy)", other)),
            };
            if !levels.contains(&qop) {
                levels.push(qop);
            }
        }
        if levels.is_empty() {
            return Err("no RPC protection level given".into());
        }
        Ok(Protection(levels))
    }
}

impl fmt::Display for Protection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<String> = self.0.iter().map(|q| q.to_string()).collect();
        f.write_str(&names.join(","))
    }
}

/// Protects what one side sends.
pub trait Seal: Send {
    fn seal(&mut self, message: &[u8]) -> Result<Vec<u8>, String>;
}

/// Checks (and decrypts) what the other side sent.
pub trait Open: Send {
    fn open(&mut self, token: &[u8]) -> Result<Vec<u8>, String>;
}

/// A negotiated security layer: the level, and each direction's half.
pub struct SecurityLayer {
    pub qop: Qop,
    pub seal: Box<dyn Seal>,
    pub open: Box<dyn Open>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protection_parses_like_hadoop_rpc_protection() {
        let p: Protection = "privacy, integrity".parse().unwrap();
        assert_eq!(p.levels(), &[Qop::Privacy, Qop::Integrity]);
        assert!(p.allows(Qop::Integrity) && !p.allows(Qop::Authentication));
        assert_eq!(Protection::default().levels(), &[Qop::Authentication]);
        assert!("secret".parse::<Protection>().is_err());
        assert!("".parse::<Protection>().is_err());
    }
}

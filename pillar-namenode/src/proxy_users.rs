//! Proxy users (Hadoop `hadoop.proxyuser.<user>.{hosts,users,groups}`): a
//! service such as Oozie or Livy authenticates as itself and acts as the
//! users it serves.
//!
//! `--proxy-users "oozie.hosts=10.0.0.0/8,gateway.example.com;oozie.groups=staff;livy.hosts=*;livy.users=alice,bob"`
//!
//! A real user may act as another user only if it has a rule naming that
//! user (`users`) or one of the user's groups (`groups`), `*` for anyone,
//! and only from one of its `hosts`: `*`, addresses, CIDR ranges or host
//! names (resolved when the NameNode starts). Rules are keyed by the real
//! user's short name (`oozie` for `oozie/host@REALM`).

use std::collections::HashMap;
use std::net::{IpAddr, ToSocketAddrs};

use tracing::warn;

/// Where a proxy user may connect from (Hadoop `MachineList`).
#[derive(Debug, Clone, Default)]
pub struct Hosts {
    any: bool,
    /// (network, prefix length).
    ranges: Vec<(IpAddr, u8)>,
}

impl Hosts {
    pub fn parse(spec: &str) -> Result<Self, String> {
        let mut hosts = Hosts::default();
        for entry in spec.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            if entry == "*" {
                hosts.any = true;
            } else if let Some((net, bits)) = entry.split_once('/') {
                let net: IpAddr = net.parse().map_err(|_| format!("{:?} is not a CIDR range", entry))?;
                let max = if net.is_ipv4() { 32 } else { 128 };
                let bits: u8 = bits.parse().ok().filter(|b| *b <= max).ok_or_else(|| format!("{:?} is not a CIDR range", entry))?;
                hosts.ranges.push((net, bits));
            } else if let Ok(ip) = entry.parse::<IpAddr>() {
                hosts.ranges.push((ip, if ip.is_ipv4() { 32 } else { 128 }));
            } else {
                // A host name: its addresses now (as Hadoop resolves them).
                match (entry, 0).to_socket_addrs() {
                    Ok(addrs) => hosts.ranges.extend(addrs.map(|a| (a.ip(), if a.is_ipv4() { 32 } else { 128 }))),
                    Err(e) => warn!("Proxy user host {} does not resolve: {}", entry, e),
                }
            }
        }
        Ok(hosts)
    }

    pub fn includes(&self, addr: IpAddr) -> bool {
        let addr = match addr {
            IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(addr),
            v4 => v4,
        };
        self.any || self.ranges.iter().any(|(net, bits)| in_range(addr, *net, *bits))
    }
}

fn in_range(addr: IpAddr, net: IpAddr, bits: u8) -> bool {
    let (a, n, width) = match (addr, net) {
        (IpAddr::V4(a), IpAddr::V4(n)) => (u32::from(a) as u128, u32::from(n) as u128, 32),
        (IpAddr::V6(a), IpAddr::V6(n)) => (u128::from(a), u128::from(n), 128),
        _ => return false,
    };
    let shift = width - bits as u32;
    shift >= width || (a >> shift) == (n >> shift)
}

/// Whom one real user may act as.
#[derive(Debug, Clone, Default)]
struct Rule {
    hosts: Option<Hosts>,
    users: Option<Vec<String>>,
    groups: Option<Vec<String>>,
}

/// Every proxy-user rule, and the users' groups (`--user-groups`).
#[derive(Debug, Clone, Default)]
pub struct ProxyUsers {
    rules: HashMap<String, Rule>,
    groups: HashMap<String, Vec<String>>,
}

impl ProxyUsers {
    /// Parse `<user>.<hosts|users|groups>=<list>` entries separated by `;`;
    /// `groups` are the users' groups, for `groups` rules.
    pub fn parse(spec: &str, groups: HashMap<String, Vec<String>>) -> Result<Self, String> {
        let mut rules: HashMap<String, Rule> = HashMap::new();
        for entry in spec.split(';').map(str::trim).filter(|e| !e.is_empty()) {
            let (key, value) = entry.split_once('=').ok_or_else(|| format!("{:?} is not <user>.<hosts|users|groups>=<list>", entry))?;
            let (user, what) = key.trim().rsplit_once('.').ok_or_else(|| format!("{:?} names no user", key))?;
            let list = || value.split(',').map(|v| v.trim().to_string()).filter(|v| !v.is_empty()).collect::<Vec<_>>();
            let rule = rules.entry(user.to_string()).or_default();
            match what {
                "hosts" => rule.hosts = Some(Hosts::parse(value)?),
                "users" => rule.users = Some(list()),
                "groups" => rule.groups = Some(list()),
                other => return Err(format!("{:?}: unknown proxy user setting {:?} (hosts, users, groups)", entry, other)),
            }
        }
        Ok(Self { rules, groups })
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Hadoop's `DefaultImpersonationProvider.authorize`: the effective user
    /// (by name or group) first, then the address, with Hadoop's messages.
    pub fn authorize(&self, real: &str, real_short: &str, effective: &str, addr: IpAddr) -> Result<(), String> {
        let rule = self.rules.get(real_short);
        let listed = |l: &Option<Vec<String>>, names: &[&str]| {
            l.as_ref().is_some_and(|l| l.iter().any(|x| x == "*" || names.contains(&x.as_str())))
        };
        let groups: Vec<&str> = self.groups.get(effective).map(|g| g.iter().map(String::as_str).collect()).unwrap_or_default();
        let allowed = rule.is_some_and(|r| listed(&r.users, &[effective]) || listed(&r.groups, &groups));
        if !allowed {
            return Err(format!("User: {} is not allowed to impersonate {}", real, effective));
        }
        if !rule.and_then(|r| r.hosts.as_ref()).is_some_and(|h| h.includes(addr)) {
            return Err(format!("Unauthorized connection for super-user: {} from IP {}", real, addr));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn policy() -> ProxyUsers {
        let groups = crate::permission::Permissions::parse_groups("bob=staff;carol=admins").unwrap();
        ProxyUsers::parse("oozie.hosts=10.1.0.0/16,127.0.0.1;oozie.groups=staff;livy.hosts=*;livy.users=alice, carol;hive.users=*", groups)
            .unwrap()
    }

    #[test]
    fn a_proxy_may_act_for_its_users_and_groups_from_its_hosts() {
        let p = policy();
        // oozie: staff members, from 10.1/16 or localhost.
        p.authorize("oozie/gw@R", "oozie", "bob", ip("10.1.200.7")).unwrap();
        p.authorize("oozie", "oozie", "bob", ip("::ffff:127.0.0.1")).unwrap();
        assert_eq!(
            p.authorize("oozie/gw@R", "oozie", "alice", ip("10.1.0.1")).unwrap_err(),
            "User: oozie/gw@R is not allowed to impersonate alice"
        );
        assert_eq!(
            p.authorize("oozie/gw@R", "oozie", "bob", ip("10.2.0.1")).unwrap_err(),
            "Unauthorized connection for super-user: oozie/gw@R from IP 10.2.0.1"
        );
        // livy: named users, from anywhere.
        p.authorize("livy", "livy", "carol", ip("192.168.1.1")).unwrap();
        assert!(p.authorize("livy", "livy", "bob", ip("192.168.1.1")).is_err());
        // hive: anyone, but no hosts at all, so from nowhere.
        assert!(p.authorize("hive", "hive", "bob", ip("127.0.0.1")).unwrap_err().starts_with("Unauthorized connection"));
        // Without a rule, nobody.
        assert!(p.authorize("eve", "eve", "bob", ip("127.0.0.1")).unwrap_err().contains("not allowed to impersonate"));
    }

    #[test]
    fn host_lists_take_addresses_ranges_names_and_star() {
        let h = Hosts::parse("10.0.0.0/8, 192.168.1.5, localhost, fe80::/10").unwrap();
        assert!(h.includes(ip("10.255.0.1")) && h.includes(ip("192.168.1.5")) && h.includes(ip("127.0.0.1")));
        assert!(h.includes(ip("fe80::1")) && !h.includes(ip("192.168.1.6")) && !h.includes(ip("11.0.0.1")));
        assert!(Hosts::parse("*").unwrap().includes(ip("8.8.8.8")));
        assert!(Hosts::parse("10.0.0.0/33").is_err());
        assert!(ProxyUsers::parse("oozie.hostz=*", HashMap::new()).is_err());
        assert!(ProxyUsers::parse("oozie", HashMap::new()).is_err());
    }
}

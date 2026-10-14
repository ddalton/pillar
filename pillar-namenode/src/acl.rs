//! POSIX ACLs (HDFS `AclTransformation`, `AclStorage`, and the ACL half of
//! `FSPermissionChecker`; `dfs.namenode.acls.enabled`).
//!
//! An ACL is a sorted list of entries: scope (access, default), type
//! (user, group, mask, other), an optional name, and rwx bits. As in HDFS,
//! an inode stores only what its permission bits cannot hold:
//!
//! - the permission bits keep `user::`, `other::` and, when the inode has
//!   extended entries, the **mask** in the group bits;
//! - the inode's entries ("feature") are the named users, `group::` and
//!   named groups of the access ACL, then every default entry.
//!
//! A directory's default ACL is copied to what is created in it, cut down
//! by the creation mode.

use serde::{Deserialize, Serialize};

/// Entry scope, in sort order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Scope {
    Access,
    Default,
}

/// Entry type, in sort order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Kind {
    User,
    Group,
    Mask,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AclEntry {
    pub scope: Scope,
    pub kind: Kind,
    /// A named user or group; `None` for the owner, owning group, mask and
    /// other.
    pub name: Option<String>,
    /// rwx bits.
    pub perm: u8,
}

/// HDFS's limit per scope.
const MAX_ENTRIES: usize = 32;

impl AclEntry {
    pub fn new(scope: Scope, kind: Kind, name: Option<&str>, perm: u8) -> Self {
        Self { scope, kind, name: name.map(str::to_string), perm: perm & 7 }
    }

    /// The sort key: scope, type, then name with unnamed first.
    fn key(&self) -> (Scope, Kind, Option<&str>) {
        (self.scope, self.kind, self.name.as_deref())
    }

    fn same_key(&self, other: &AclEntry) -> bool {
        self.key() == other.key()
    }
}

impl std::fmt::Display for AclEntry {
    /// `default:user:bob:r-x`, as `getfacl` prints it.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.scope == Scope::Default {
            f.write_str("default:")?;
        }
        let kind = match self.kind {
            Kind::User => "user",
            Kind::Group => "group",
            Kind::Mask => "mask",
            Kind::Other => "other",
        };
        let bit = |b: u8, c: char| if self.perm & b != 0 { c } else { '-' };
        write!(f, "{}:{}:{}{}{}", kind, self.name.as_deref().unwrap_or(""), bit(4, 'r'), bit(2, 'w'), bit(1, 'x'))
    }
}

/// An invalid ACL or operation; the message HDFS gives (`AclException`).
pub type AclError = String;

fn sort(acl: &mut [AclEntry]) {
    acl.sort_by(|a, b| a.key().cmp(&b.key()));
}

fn find<'a>(acl: &'a [AclEntry], scope: Scope, kind: Kind) -> Option<&'a AclEntry> {
    acl.iter().find(|e| e.scope == scope && e.kind == kind && e.name.is_none())
}

fn access(acl: &[AclEntry]) -> Vec<AclEntry> {
    acl.iter().filter(|e| e.scope == Scope::Access).cloned().collect()
}

fn defaults(acl: &[AclEntry]) -> Vec<AclEntry> {
    acl.iter().filter(|e| e.scope == Scope::Default).cloned().collect()
}

/// An ACL with only the owner, group and other entries.
fn is_minimal(acl: &[AclEntry]) -> bool {
    acl.len() == 3
}

/// The three entries a mode stands for.
fn minimal_acl(mode: u16) -> Vec<AclEntry> {
    vec![
        AclEntry::new(Scope::Access, Kind::User, None, (mode >> 6) as u8),
        AclEntry::new(Scope::Access, Kind::Group, None, (mode >> 3) as u8),
        AclEntry::new(Scope::Access, Kind::Other, None, mode as u8),
    ]
}

/// The whole ACL of an inode with `mode` and stored entries `feature`
/// (HDFS `readINodeLogicalAcl`).
pub fn logical_acl(mode: u16, feature: &[AclEntry]) -> Vec<AclEntry> {
    let access_entries = access(feature);
    let mut acl = Vec::with_capacity(feature.len() + 3);
    if access_entries.is_empty() {
        acl.extend(minimal_acl(mode));
    } else {
        acl.push(AclEntry::new(Scope::Access, Kind::User, None, (mode >> 6) as u8));
        acl.extend(access_entries);
        acl.push(AclEntry::new(Scope::Access, Kind::Mask, None, (mode >> 3) as u8));
        acl.push(AclEntry::new(Scope::Access, Kind::Other, None, mode as u8));
    }
    acl.extend(defaults(feature));
    acl
}

/// Store `acl` on an inode with `mode` and `is_dir` (HDFS
/// `updateINodeAcl`): its new mode (sticky bit kept) and entries.
pub fn store(mode: u16, is_dir: bool, acl: &[AclEntry]) -> Result<(u16, Vec<AclEntry>), AclError> {
    let sticky = mode & 0o1000;
    if is_minimal(acl) {
        let bits = |i: usize| acl[i].perm as u16;
        return Ok((sticky | bits(0) << 6 | bits(1) << 3 | bits(2), Vec::new()));
    }
    let access_entries = access(acl);
    let default_entries = defaults(acl);
    if !default_entries.is_empty() && !is_dir {
        return Err("Invalid ACL: only directories may have a default ACL.".into());
    }
    Ok((extended_mode(&access_entries, sticky), feature(&access_entries, &default_entries)))
}

/// The mode for an extended access ACL: owner, mask, other.
fn extended_mode(access_entries: &[AclEntry], sticky: u16) -> u16 {
    let n = access_entries.len();
    sticky | (access_entries[0].perm as u16) << 6 | (access_entries[n - 2].perm as u16) << 3 | access_entries[n - 1].perm as u16
}

/// What an inode stores: the access entries between the owner and the
/// mask (if extended), then the default entries.
fn feature(access_entries: &[AclEntry], default_entries: &[AclEntry]) -> Vec<AclEntry> {
    let mut f = Vec::new();
    if !is_minimal(access_entries) {
        f.extend_from_slice(&access_entries[1..access_entries.len() - 2]);
    }
    f.extend_from_slice(default_entries);
    f
}

/// removeAcl: the mode an inode with `feature` gets back, its group bits
/// the `group::` entry's (HDFS `unprotectedRemoveAcl`).
pub fn removed_mode(mode: u16, feature: &[AclEntry]) -> u16 {
    match find(feature, Scope::Access, Kind::Group) {
        Some(g) => (mode & !0o070) | (g.perm as u16) << 3,
        None => mode,
    }
}

/// Validate a client's ACL spec: sorted, within limits.
fn validated(mut spec: Vec<AclEntry>) -> Result<Vec<AclEntry>, AclError> {
    sort(&mut spec);
    check_max_entries(&spec)?;
    Ok(spec)
}

fn check_max_entries(acl: &[AclEntry]) -> Result<(), AclError> {
    for (scope, name) in [(Scope::Access, "access"), (Scope::Default, "default")] {
        let n = acl.iter().filter(|e| e.scope == scope).count();
        if n > MAX_ENTRIES {
            return Err(format!("Invalid ACL: ACL has {} {} entries, which exceeds maximum of {}.", n, name, MAX_ENTRIES));
        }
    }
    Ok(())
}

/// Masks to add back once the ACL is built (HDFS `calculateMasks`).
#[derive(Default)]
struct Masks {
    provided: [Option<AclEntry>; 2],
    dirty: [bool; 2],
    scope_dirty: [bool; 2],
}

fn idx(scope: Scope) -> usize {
    scope as usize
}

fn finish(mut acl: Vec<AclEntry>, masks: Masks) -> Result<Vec<AclEntry>, AclError> {
    copy_defaults_if_needed(&mut acl);
    // Masks.
    let mut found = [false; 2];
    let mut union = [0u8; 2];
    let mut needed = [false; 2];
    for e in &acl {
        let i = idx(e.scope);
        found[i] = true;
        if e.kind == Kind::Group || e.name.is_some() {
            union[i] |= e.perm;
        }
        if e.name.is_some() {
            needed[i] = true;
        }
    }
    for scope in [Scope::Access, Scope::Default] {
        let i = idx(scope);
        if !found[i] {
            continue;
        }
        let provided = masks.provided[i].clone();
        if provided.is_none() && needed[i] && masks.dirty[i] {
            return Err("Invalid ACL: mask is required and cannot be deleted.".into());
        } else if let Some(p) = provided.clone().filter(|_| !masks.scope_dirty[i] || masks.dirty[i]) {
            acl.push(p);
        } else if needed[i] || provided.is_some() {
            acl.push(AclEntry::new(scope, Kind::Mask, None, union[i]));
        }
    }
    build_and_validate(acl)
}

/// A default ACL lacking the owner, group or other entry takes it from
/// the access ACL.
fn copy_defaults_if_needed(acl: &mut Vec<AclEntry>) {
    sort(acl);
    if !acl.iter().any(|e| e.scope == Scope::Default) {
        return;
    }
    let mut copied = Vec::new();
    for kind in [Kind::User, Kind::Group, Kind::Other] {
        if find(acl, Scope::Default, kind).is_none() {
            if let Some(a) = find(acl, Scope::Access, kind) {
                copied.push(AclEntry::new(Scope::Default, kind, None, a.perm));
            }
        }
    }
    acl.extend(copied);
}

fn build_and_validate(mut acl: Vec<AclEntry>) -> Result<Vec<AclEntry>, AclError> {
    sort(&mut acl);
    for pair in acl.windows(2) {
        if pair[0].same_key(&pair[1]) {
            return Err("Invalid ACL: multiple entries with same scope, type and name.".into());
        }
    }
    if let Some(e) = acl.iter().find(|e| e.name.is_some() && matches!(e.kind, Kind::Mask | Kind::Other)) {
        return Err(format!("Invalid ACL: this entry type must not have a name: {}.", e));
    }
    check_max_entries(&acl)?;
    let has_defaults = acl.iter().any(|e| e.scope == Scope::Default);
    for kind in [Kind::User, Kind::Group, Kind::Other] {
        if find(&acl, Scope::Access, kind).is_none() {
            return Err("Invalid ACL: the user, group and other entries are required.".into());
        }
        if has_defaults && find(&acl, Scope::Default, kind).is_none() {
            return Err("Invalid default ACL: the user, group and other entries are required.".into());
        }
    }
    Ok(acl)
}

/// modifyAclEntries: `spec` entries replace or add to `existing`.
pub fn merge(existing: &[AclEntry], spec: Vec<AclEntry>) -> Result<Vec<AclEntry>, AclError> {
    let spec = validated(spec)?;
    let mut acl = Vec::new();
    let mut masks = Masks::default();
    let mut found_in_spec: Vec<&AclEntry> = Vec::new();
    for e in existing {
        let i = idx(e.scope);
        if let Some(s) = spec.iter().find(|s| s.same_key(e)) {
            found_in_spec.push(s);
            masks.scope_dirty[idx(s.scope)] = true;
            if s.kind == Kind::Mask {
                masks.provided[idx(s.scope)] = Some(s.clone());
                masks.dirty[idx(s.scope)] = true;
            } else {
                acl.push(s.clone());
            }
        } else if e.kind == Kind::Mask {
            masks.provided[i] = Some(e.clone());
        } else {
            acl.push(e.clone());
        }
    }
    for s in &spec {
        if !found_in_spec.iter().any(|f| f.same_key(s)) {
            let i = idx(s.scope);
            masks.scope_dirty[i] = true;
            if s.kind == Kind::Mask {
                masks.provided[i] = Some(s.clone());
                masks.dirty[i] = true;
            } else {
                acl.push(s.clone());
            }
        }
    }
    finish(acl, masks)
}

/// removeAclEntries: drop the entries `spec` names (by key).
pub fn filter_by_spec(existing: &[AclEntry], spec: Vec<AclEntry>) -> Result<Vec<AclEntry>, AclError> {
    let spec = validated(spec)?;
    let mut acl = Vec::new();
    let mut masks = Masks::default();
    for e in existing {
        let i = idx(e.scope);
        if spec.iter().any(|s| s.same_key(e)) {
            masks.scope_dirty[i] = true;
            if e.kind == Kind::Mask {
                masks.dirty[i] = true;
            }
        } else if e.kind == Kind::Mask {
            masks.provided[i] = Some(e.clone());
        } else {
            acl.push(e.clone());
        }
    }
    finish(acl, masks)
}

/// removeDefaultAcl: keep the access entries.
pub fn filter_default(existing: &[AclEntry]) -> Result<Vec<AclEntry>, AclError> {
    build_and_validate(access(existing))
}

/// setAcl: `spec` replaces every entry of the scopes it names.
pub fn replace(existing: &[AclEntry], spec: Vec<AclEntry>) -> Result<Vec<AclEntry>, AclError> {
    let spec = validated(spec)?;
    let mut acl = Vec::new();
    let mut masks = Masks::default();
    for s in &spec {
        let i = idx(s.scope);
        masks.scope_dirty[i] = true;
        if s.kind == Kind::Mask {
            masks.provided[i] = Some(s.clone());
            masks.dirty[i] = true;
        } else {
            acl.push(s.clone());
        }
    }
    for e in existing {
        let i = idx(e.scope);
        if !masks.scope_dirty[i] {
            if e.kind == Kind::Mask {
                masks.provided[i] = Some(e.clone());
            } else {
                acl.push(e.clone());
            }
        }
    }
    finish(acl, masks)
}

/// A new inode under a directory with `parent` entries (HDFS
/// `copyINodeDefaultAcl`): its mode and entries, from the parent's
/// default ACL cut down by the creation `mode`; `None` if the parent has
/// no default ACL.
pub fn inherit(parent: &[AclEntry], mode: u16, is_dir: bool) -> Option<(u16, Vec<AclEntry>)> {
    let parent_defaults = defaults(parent);
    if parent_defaults.is_empty() {
        return None;
    }
    let (u, g, o) = (((mode >> 6) & 7) as u8, ((mode >> 3) & 7) as u8, (mode & 7) as u8);
    let minimal = is_minimal(&parent_defaults);
    let access_entries: Vec<AclEntry> = parent_defaults
        .iter()
        .map(|e| {
            let perm = match (e.kind, e.name.is_some()) {
                (Kind::User, false) => e.perm & u,
                (Kind::Group, _) if minimal => e.perm & g,
                (Kind::Mask, _) => e.perm & g,
                (Kind::Other, _) => e.perm & o,
                _ => e.perm,
            };
            AclEntry::new(Scope::Access, e.kind, e.name.as_deref(), perm)
        })
        .collect();
    let default_entries = if is_dir { parent_defaults } else { Vec::new() };
    let sticky = mode & 0o1000;
    if !is_minimal(&access_entries) || !default_entries.is_empty() {
        Some((extended_mode(&access_entries, sticky), feature(&access_entries, &default_entries)))
    } else {
        let bits = |i: usize| access_entries[i].perm as u16;
        Some((sticky | bits(0) << 6 | bits(1) << 3 | bits(2), Vec::new()))
    }
}

/// Whether `user` (in the groups `member_of` says) has `access` under an
/// inode's extended ACL (HDFS `hasAclPermission`): the owner bits for the
/// owner; else a matching named user, or any matching group entry, masked;
/// else, if nothing matched, the other bits.
pub fn allows(
    user: &str,
    member_of: impl Fn(&str) -> bool,
    owner: &str,
    group: &str,
    mode: u16,
    feature: &[AclEntry],
    want: u8,
) -> bool {
    let owner_bits = ((mode >> 6) & 7) as u8;
    let mask = ((mode >> 3) & 7) as u8;
    if user == owner {
        return owner_bits & want == want;
    }
    let mut matched = false;
    for e in feature {
        if e.scope == Scope::Default {
            break;
        }
        match e.kind {
            Kind::User if e.name.as_deref() == Some(user) => {
                return e.perm & mask & want == want;
            }
            Kind::Group => {
                if member_of(e.name.as_deref().unwrap_or(group)) {
                    if e.perm & mask & want == want {
                        return true;
                    }
                    matched = true;
                }
            }
            _ => {}
        }
    }
    !matched && (mode & 7) as u8 & want == want
}

#[cfg(test)]
mod tests {
    use super::*;
    use Kind::*;
    use Scope::*;

    fn e(scope: Scope, kind: Kind, name: Option<&str>, perm: u8) -> AclEntry {
        AclEntry::new(scope, kind, name, perm)
    }

    fn show(acl: &[AclEntry]) -> Vec<String> {
        acl.iter().map(|e| e.to_string()).collect()
    }

    #[test]
    fn adding_a_named_user_adds_a_mask_and_moves_the_group_bits() {
        // rw-r----- plus user:bob:rwx.
        let existing = logical_acl(0o640, &[]);
        let acl = merge(&existing, vec![e(Access, User, Some("bob"), 7)]).unwrap();
        assert_eq!(show(&acl), ["user::rw-", "user:bob:rwx", "group::r--", "mask::rwx", "other::---"]);
        let (mode, feature) = store(0o640, false, &acl).unwrap();
        // The group bits now hold the mask.
        assert_eq!(mode, 0o670);
        assert_eq!(show(&feature), ["user:bob:rwx", "group::r--"]);
        assert_eq!(logical_acl(mode, &feature), acl);
        // Removing the ACL gives the group bits back.
        assert_eq!(removed_mode(mode, &feature), 0o640);
    }

    #[test]
    fn named_entries_are_limited_by_the_mask() {
        let acl = merge(&logical_acl(0o640, &[]), vec![e(Access, User, Some("bob"), 7), e(Access, Mask, None, 4)]).unwrap();
        let (mode, f) = store(0o640, false, &acl).unwrap();
        assert_eq!(mode, 0o640);
        let staff = |g: &str| g == "staff";
        assert!(allows("bob", |_| false, "alice", "staff", mode, &f, 4));
        assert!(!allows("bob", |_| false, "alice", "staff", mode, &f, 2), "the mask takes bob's w");
        // The owner is the owner bits, whatever the entries.
        assert!(allows("alice", |_| false, "alice", "staff", mode, &f, 6));
        // Group members: group::r--.
        assert!(allows("carol", staff, "alice", "staff", mode, &f, 4));
        assert!(!allows("carol", staff, "alice", "staff", mode, &f, 2));
        // A named user who matched and was denied does not fall to other.
        let open_other = (mode & !0o7) | 0o6;
        assert!(!allows("bob", |_| false, "alice", "staff", open_other, &f, 2));
        assert!(allows("dave", |_| false, "alice", "staff", open_other, &f, 2));
    }

    #[test]
    fn set_acl_replaces_a_scope_and_validates() {
        let spec = vec![e(Access, User, None, 7), e(Access, Group, None, 5), e(Access, Other, None, 0), e(Access, Group, Some("eng"), 7)];
        let acl = replace(&logical_acl(0o755, &[]), spec).unwrap();
        assert_eq!(show(&acl), ["user::rwx", "group::r-x", "group:eng:rwx", "mask::rwx", "other::---"]);
        // Required entries, duplicates, named masks.
        assert!(replace(&[], vec![e(Access, User, None, 7)]).unwrap_err().contains("are required"));
        let dup = vec![e(Access, User, Some("x"), 7), e(Access, User, Some("x"), 4)];
        assert!(merge(&logical_acl(0o755, &[]), dup).unwrap_err().contains("multiple entries"));
        assert!(merge(&logical_acl(0o755, &[]), vec![e(Access, Mask, Some("m"), 7)]).unwrap_err().contains("must not have a name"));
        // Only directories have defaults.
        let with_default = merge(&logical_acl(0o755, &[]), vec![e(Default, User, Some("bob"), 5)]).unwrap();
        assert!(store(0o644, false, &with_default).unwrap_err().contains("only directories"));
    }

    #[test]
    fn removing_entries_recomputes_or_refuses_the_mask() {
        let acl = merge(&logical_acl(0o750, &[]), vec![e(Access, User, Some("bob"), 7), e(Access, Group, Some("eng"), 5)]).unwrap();
        let acl2 = filter_by_spec(&acl, vec![e(Access, User, Some("bob"), 0)]).unwrap();
        // The mask is recomputed: the union of what is left (as HDFS does).
        assert_eq!(show(&acl2), ["user::rwx", "group::r-x", "group:eng:r-x", "mask::r-x", "other::---"]);
        assert!(filter_by_spec(&acl, vec![e(Access, Mask, None, 0)]).unwrap_err().contains("mask is required"));
        // Removing every named entry keeps a mask (recomputed), as HDFS does;
        // removeAcl is what drops the ACL.
        let acl3 = filter_by_spec(&acl, vec![e(Access, User, Some("bob"), 0), e(Access, Group, Some("eng"), 0)]).unwrap();
        assert_eq!(show(&acl3), ["user::rwx", "group::r-x", "mask::r-x", "other::---"]);
    }

    #[test]
    fn default_acls_are_inherited_and_cut_by_the_creation_mode() {
        // setfacl -m default:user:bob:rwx on a 755 directory.
        let dir = merge(&logical_acl(0o755, &[]), vec![e(Default, User, Some("bob"), 7)]).unwrap();
        assert_eq!(
            show(&defaults(&dir)),
            ["default:user::rwx", "default:user:bob:rwx", "default:group::r-x", "default:mask::rwx", "default:other::r-x"]
        );
        let (dmode, dfeature) = store(0o755, true, &dir).unwrap();
        assert_eq!(dmode, 0o755);
        // A file created 644 in it: bob gets rwx but the mask is r-- (from the mode's group bits).
        let (fmode, ffeature) = inherit(&dfeature, 0o644, false).unwrap();
        assert_eq!(show(&logical_acl(fmode, &ffeature)), ["user::rw-", "user:bob:rwx", "group::r-x", "mask::r--", "other::r--"]);
        assert!(!allows("bob", |_| false, "alice", "staff", fmode, &ffeature, 2));
        // A subdirectory inherits the default ACL itself.
        let (_, sub) = inherit(&dfeature, 0o755, true).unwrap();
        assert_eq!(defaults(&sub), defaults(&dfeature));
        // removeDefaultAcl.
        assert!(defaults(&filter_default(&logical_acl(dmode, &dfeature)).unwrap()).is_empty());
        // No default ACL, no inheritance.
        assert!(inherit(&[], 0o644, false).is_none());
    }
}

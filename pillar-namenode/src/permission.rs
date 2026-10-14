//! Permission enforcement (HDFS `FSPermissionChecker`,
//! `dfs.permissions.enabled`).
//!
//! POSIX-style: a caller is checked against an inode's owner bits if it
//! owns it, its group bits if it is in the inode's group, else its other
//! bits. Every operation checks:
//!
//! - **traverse**: EXECUTE on every existing directory above the path;
//! - optionally the nearest existing **ancestor**, the **parent**, the path
//!   itself, every directory of its **subtree**, or **ownership**;
//! - the **sticky bit**: in a sticky directory only the child's owner, the
//!   directory's owner or the superuser may delete or rename a child.
//!
//! The superuser (the NameNode's user, `hdfs` by default) and members of
//! the supergroup pass every check. Groups come from a static mapping
//! (`--user-groups "alice=staff,admins;bob=staff"`, as Hadoop's
//! `hadoop.user.group.static.mapping.overrides`); users not in it have no
//! groups.
//!
//! Checks run before an operation, without holding the namespace locked
//! across it (HDFS holds its namesystem lock): a concurrent chmod can race
//! an operation that already passed its check.

use std::collections::HashMap;

use tonic::Status;

use crate::metadata::namespace::InodeAttrs;
use crate::metadata::Namespace;

/// Access bits (HDFS `FsAction`).
pub mod action {
    pub const READ: u8 = 4;
    pub const WRITE: u8 = 2;
    pub const EXECUTE: u8 = 1;
    pub const READ_EXECUTE: u8 = READ | EXECUTE;
    pub const ALL: u8 = READ | WRITE | EXECUTE;

    /// HDFS's name for an access, as its messages print it.
    pub fn name(a: u8) -> &'static str {
        match a {
            0 => "NONE",
            1 => "EXECUTE",
            2 => "WRITE",
            3 => "WRITE_EXECUTE",
            4 => "READ",
            5 => "READ_EXECUTE",
            6 => "READ_WRITE",
            _ => "ALL",
        }
    }
}

const ACCESS_CONTROL: &str = "org.apache.hadoop.security.AccessControlException";

fn denied(message: String) -> Status {
    crate::ipc::hadoop_status(tonic::Code::PermissionDenied, ACCESS_CONTROL, message)
}

/// `drwxr-xr-x`, `-rw-r--r--`, `drwxrwxrwt` (sticky).
pub fn mode_string(is_dir: bool, mode: u16) -> String {
    let mut s = String::with_capacity(10);
    s.push(if is_dir { 'd' } else { '-' });
    for shift in [6, 3, 0] {
        let bits = (mode >> shift) & 7;
        s.push(if bits & 4 != 0 { 'r' } else { '-' });
        s.push(if bits & 2 != 0 { 'w' } else { '-' });
        let x = bits & 1 != 0;
        s.push(match (shift, mode & 0o1000 != 0) {
            (0, true) => if x { 't' } else { 'T' },
            _ => if x { 'x' } else { '-' },
        });
    }
    s
}

/// What an operation must be allowed (HDFS `checkPermission`'s arguments).
#[derive(Debug, Clone, Copy, Default)]
pub struct Check {
    /// The caller must own the path.
    pub owner: bool,
    /// Access on the nearest existing ancestor (creating parents).
    pub ancestor: Option<u8>,
    /// Access on the parent directory.
    pub parent: Option<u8>,
    /// Access on the path itself.
    pub access: Option<u8>,
    /// Access on every directory of the path's subtree.
    pub sub: Option<u8>,
    /// With `sub`: empty directories need no access (delete).
    pub ignore_empty_dir: bool,
    /// The sticky bit applies: removing or renaming the path (and, with
    /// `sub`, everything under it).
    pub sticky: bool,
}

/// The NameNode's permission policy.
#[derive(Debug, Clone)]
pub struct Permissions {
    pub enabled: bool,
    /// ACL operations allowed (HDFS dfs.namenode.acls.enabled).
    pub acls: bool,
    pub superuser: String,
    pub supergroup: String,
    /// user -> groups.
    pub groups: HashMap<String, Vec<String>>,
}

impl Default for Permissions {
    fn default() -> Self {
        Self { enabled: true, acls: true, superuser: "hdfs".into(), supergroup: "supergroup".into(), groups: HashMap::new() }
    }
}

impl Permissions {
    /// No checks at all (`dfs.permissions.enabled=false`).
    pub fn disabled() -> Self {
        Self { enabled: false, ..Default::default() }
    }

    /// Parse `alice=staff,admins;bob=staff` (users separated by `;`).
    pub fn parse_groups(s: &str) -> Result<HashMap<String, Vec<String>>, String> {
        let mut out = HashMap::new();
        for entry in s.split(';').map(str::trim).filter(|e| !e.is_empty()) {
            let (user, groups) = entry.split_once('=').ok_or_else(|| format!("{:?} is not user=group,group", entry))?;
            let groups: Vec<String> = groups.split(',').map(|g| g.trim().to_string()).filter(|g| !g.is_empty()).collect();
            out.insert(user.trim().to_string(), groups);
        }
        Ok(out)
    }

    pub fn groups_of(&self, user: &str) -> &[String] {
        self.groups.get(user).map(Vec::as_slice).unwrap_or_default()
    }

    pub fn is_superuser(&self, user: &str) -> bool {
        user == self.superuser || self.groups_of(user).iter().any(|g| *g == self.supergroup)
    }

    pub fn is_member(&self, user: &str, group: &str) -> bool {
        self.groups_of(user).iter().any(|g| g == group)
    }

    /// Whether `user` has `access` to an inode: by its ACL if it has an
    /// access ACL (a default ACL alone does not count, as in HDFS), else
    /// by its mode.
    pub fn allows(&self, user: &str, inode: &InodeAttrs, access: u8) -> bool {
        if inode.acl.iter().any(|e| e.scope == crate::acl::Scope::Access) {
            return crate::acl::allows(user, |g| self.is_member(user, g), &inode.owner, &inode.group, inode.mode, &inode.acl, access);
        }
        let bits = if user == inode.owner {
            (inode.mode >> 6) & 7
        } else if self.is_member(user, &inode.group) {
            (inode.mode >> 3) & 7
        } else {
            inode.mode & 7
        } as u8;
        bits & access == access
    }

    fn check_inode(&self, user: &str, inode: &InodeAttrs, path: &str, access: u8) -> Result<(), Status> {
        if self.allows(user, inode, access) {
            return Ok(());
        }
        Err(denied(format!(
            "Permission denied: user={}, access={}, inode=\"{}\":{}:{}:{}",
            user,
            action::name(access),
            path,
            inode.owner,
            inode.group,
            mode_string(inode.is_dir, inode.mode)
        )))
    }

    fn check_sticky(&self, user: &str, parent: &InodeAttrs, parent_path: &str, child: &InodeAttrs, path: &str) -> Result<(), Status> {
        if parent.mode & 0o1000 == 0 || user == parent.owner || user == child.owner {
            return Ok(());
        }
        Err(denied(format!(
            "Permission denied by sticky bit: user={}, path=\"{}\":{}:{}:{}, parent=\"{}\":{}:{}:{}",
            user,
            path,
            child.owner,
            child.group,
            mode_string(child.is_dir, child.mode),
            parent_path,
            parent.owner,
            parent.group,
            mode_string(parent.is_dir, parent.mode)
        )))
    }

    /// Only the superuser may (admin operations).
    pub fn check_superuser(&self, user: &str) -> Result<(), Status> {
        if !self.enabled || self.is_superuser(user) {
            return Ok(());
        }
        Err(denied(format!("Access denied for user {}. Superuser privilege is required", user)))
    }

    /// Check `user`'s right to do `check` on `path`.
    pub async fn check(&self, ns: &Namespace, user: &str, path: &str, check: Check) -> Result<(), Status> {
        if !self.enabled || self.is_superuser(user) {
            return Ok(());
        }
        let attrs = ns.resolve_attrs(path).await;
        let components: Vec<&str> = path.split('/').filter(|c| !c.is_empty()).collect();
        let path_of = |i: usize| if i == 0 { "/".to_string() } else { format!("/{}", components[..i].join("/")) };
        let n = attrs.len();
        let last = n - 1;

        // Traverse: EXECUTE on every existing directory above the path.
        let mut ancestor = last.checked_sub(1);
        while let Some(i) = ancestor {
            if attrs[i].is_some() {
                break;
            }
            ancestor = i.checked_sub(1);
        }
        if let Some(a) = ancestor {
            for (i, inode) in attrs[..=a].iter().enumerate() {
                let inode = inode.as_ref().expect("existing prefix");
                if inode.is_dir {
                    self.check_inode(user, inode, &path_of(i), action::EXECUTE)?;
                }
            }
        }
        if let (Some(access), Some(a)) = (check.ancestor, ancestor) {
            self.check_inode(user, attrs[a].as_ref().unwrap(), &path_of(a), access)?;
        }
        if let (Some(access), Some(p)) = (check.parent, last.checked_sub(1)) {
            if let Some(parent) = &attrs[p] {
                self.check_inode(user, parent, &path_of(p), access)?;
            }
        }
        let Some(inode) = &attrs[last] else { return Ok(()) };
        if check.sticky {
            if let Some(Some(parent)) = last.checked_sub(1).map(|p| &attrs[p]) {
                self.check_sticky(user, parent, &path_of(last - 1), inode, &path_of(last))?;
            }
        }
        if let Some(access) = check.access {
            self.check_inode(user, inode, &path_of(last), access)?;
        }
        if let Some(access) = check.sub {
            let dirs = ns.subtree_dirs(path).await.map_err(|e| Status::not_found(e.to_string()))?;
            for (dir_path, dir, children) in &dirs {
                if !(children.is_empty() && check.ignore_empty_dir) {
                    self.check_inode(user, dir, dir_path, access)?;
                }
                if check.sticky {
                    for child in children {
                        let child_path = format!("{}/...", dir_path.trim_end_matches('/'));
                        self.check_sticky(user, dir, dir_path, child, &child_path)?;
                    }
                }
            }
        }
        if check.owner && user != inode.owner {
            return Err(denied(format!("Permission denied. user={} is not the owner of inode={}", user, path_of(last))));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::namespace::FsPermission;

    fn perms() -> Permissions {
        Permissions { groups: Permissions::parse_groups("alice=staff;bob=staff;carol=admins;root=supergroup").unwrap(), ..Default::default() }
    }

    async fn tree() -> Namespace {
        let ns = Namespace::new();
        let mk = |p: &'static str, o: &'static str, g: &'static str, m: u16| {
            let ns = &ns;
            async move { ns.mkdir(p, o.into(), g.into(), FsPermission::new(m)).await.unwrap() }
        };
        mk("/tmp", "hdfs", "supergroup", 0o1777).await;
        mk("/tmp/alice", "alice", "staff", 0o755).await;
        mk("/home", "hdfs", "supergroup", 0o755).await;
        mk("/home/alice", "alice", "staff", 0o750).await;
        mk("/home/alice/private", "alice", "staff", 0o700).await;
        ns.create_file("/home/alice/notes", "alice".into(), "staff".into(), FsPermission::new(0o640), 3, 1024).await.unwrap();
        ns
    }

    fn is_denied<T>(r: Result<T, Status>) -> String {
        match r {
            Err(s) => {
                assert_eq!(s.code(), tonic::Code::PermissionDenied);
                s.message().to_string()
            }
            Ok(_) => panic!("allowed"),
        }
    }

    #[tokio::test]
    async fn owner_group_and_other_bits() {
        let (ns, p) = (tree().await, perms());
        let read = Check { access: Some(action::READ), ..Default::default() };
        p.check(&ns, "alice", "/home/alice/notes", read).await.unwrap();
        // bob is in staff: the group bits (r--) let him read, not write.
        p.check(&ns, "bob", "/home/alice/notes", read).await.unwrap();
        let write = Check { access: Some(action::WRITE), ..Default::default() };
        let m = is_denied(p.check(&ns, "bob", "/home/alice/notes", write).await);
        assert_eq!(m, r#"Permission denied: user=bob, access=WRITE, inode="/home/alice/notes":alice:staff:-rw-r-----"#);
        // carol is not: /home/alice (rwxr-x---) cannot even be traversed.
        let m = is_denied(p.check(&ns, "carol", "/home/alice/notes", read).await);
        assert!(m.contains(r#"access=EXECUTE, inode="/home/alice":alice:staff:drwxr-x---"#), "{m}");
        // The superuser and the supergroup pass everything.
        p.check(&ns, "hdfs", "/home/alice/private", Check { access: Some(action::ALL), ..Default::default() }).await.unwrap();
        p.check(&ns, "root", "/home/alice/private", Check { access: Some(action::ALL), ..Default::default() }).await.unwrap();
        // Disabled: nothing is checked.
        Permissions::disabled().check(&ns, "carol", "/home/alice/private", write).await.unwrap();
    }

    #[tokio::test]
    async fn creating_needs_write_on_the_nearest_existing_ancestor() {
        let (ns, p) = (tree().await, perms());
        let create = Check { ancestor: Some(action::WRITE), ..Default::default() };
        p.check(&ns, "alice", "/home/alice/a/b/c", create).await.unwrap();
        let m = is_denied(p.check(&ns, "bob", "/home/alice/a/b/c", create).await);
        assert!(m.contains(r#"access=WRITE, inode="/home/alice""#), "{m}");
        // Anyone may create in the sticky, world-writable /tmp.
        p.check(&ns, "carol", "/tmp/carol", create).await.unwrap();
    }

    #[tokio::test]
    async fn the_sticky_bit_protects_other_users_entries() {
        let (ns, p) = (tree().await, perms());
        let delete = Check { parent: Some(action::WRITE), sub: Some(action::ALL), ignore_empty_dir: true, sticky: true, ..Default::default() };
        // carol can write /tmp, but /tmp/alice is alice's and /tmp is sticky.
        let m = is_denied(p.check(&ns, "carol", "/tmp/alice", delete).await);
        assert!(m.starts_with("Permission denied by sticky bit: user=carol"), "{m}");
        p.check(&ns, "alice", "/tmp/alice", delete).await.unwrap();
    }

    #[tokio::test]
    async fn recursive_checks_cover_the_whole_subtree() {
        let (ns, p) = (tree().await, perms());
        let summary = Check { sub: Some(action::READ_EXECUTE), ..Default::default() };
        p.check(&ns, "alice", "/home", summary).await.unwrap();
        // bob may list /home/alice (group r-x) but not alice's private dir.
        let m = is_denied(p.check(&ns, "bob", "/home", summary).await);
        assert!(m.contains(r#"inode="/home/alice/private""#), "{m}");
    }

    #[tokio::test]
    async fn ownership_and_superuser_checks() {
        let (ns, p) = (tree().await, perms());
        let own = Check { owner: true, ..Default::default() };
        p.check(&ns, "alice", "/home/alice/notes", own).await.unwrap();
        assert!(is_denied(p.check(&ns, "bob", "/home/alice/notes", own).await).contains("is not the owner"));
        p.check_superuser("hdfs").unwrap();
        p.check_superuser("root").unwrap();
        assert!(is_denied(p.check_superuser("alice")).contains("Superuser privilege is required"));
    }

    #[test]
    fn modes_print_like_ls() {
        assert_eq!(mode_string(true, 0o755), "drwxr-xr-x");
        assert_eq!(mode_string(true, 0o1777), "drwxrwxrwt");
        assert_eq!(mode_string(false, 0o1640), "-rw-r----T");
        assert!(Permissions::parse_groups("alice").is_err());
        assert_eq!(Permissions::parse_groups("a=x, y ;b=").unwrap()["a"], vec!["x", "y"]);
    }
}

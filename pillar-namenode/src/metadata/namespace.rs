use dashmap::DashMap;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum NamespaceError {
    #[error("Path not found: {0}")]
    PathNotFound(String),

    #[error("Path already exists: {0}")]
    PathAlreadyExists(String),

    #[error("Not a directory: {0}")]
    NotADirectory(String),

    #[error("Directory not empty: {0}")]
    DirectoryNotEmpty(String),

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),
}

pub type Result<T> = std::result::Result<T, NamespaceError>;

/// Represents HDFS permissions (rwxrwxrwx)
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct FsPermission {
    pub mode: u16,  // Unix-style permission bits
}

impl FsPermission {
    /// Permission bits and the sticky bit (0o1000).
    pub fn new(mode: u16) -> Self {
        Self { mode: mode & 0o1777 }
    }

    pub fn sticky(&self) -> bool {
        self.mode & 0o1000 != 0
    }

    pub fn default_dir() -> Self {
        Self::new(0o755)
    }

    pub fn default_file() -> Self {
        Self::new(0o644)
    }
}

/// Block information for a file
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BlockInfo {
    pub block_id: u64,
    pub generation_stamp: u64,
    pub num_bytes: u64,
}

/// A file open for writing (HDFS INodeFile under construction): the client
/// holding its lease, and how many blocks it had when opened. Blocks from
/// that index on were allocated by this writer; the last of them is the one
/// being written until the next addBlock or complete commits it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UnderConstruction {
    pub holder: String,
    pub blocks_at_open: u32,
}

/// What a permission check needs to know of an inode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InodeAttrs {
    pub owner: String,
    pub group: String,
    pub mode: u16,
    pub is_dir: bool,
}

impl InodeAttrs {
    fn of(inode: &INode) -> Self {
        Self { owner: inode.owner.clone(), group: inode.group.clone(), mode: inode.permissions.mode, is_dir: inode.is_dir }
    }
}

/// INode (Index Node) - represents a file or directory in HDFS
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct INode {
    pub id: u64,
    pub name: String,
    pub is_dir: bool,
    pub permissions: FsPermission,
    pub owner: String,
    pub group: String,
    pub mtime: u64,  // Modification time (milliseconds since epoch)
    pub atime: u64,  // Access time (milliseconds since epoch)
    pub replication: u16,  // Replication factor (files only)
    pub block_size: u64,   // Block size in bytes (files only)
    #[serde(skip)]
    pub children: Option<DashMap<String, u64>>,  // For directories: name -> inode_id
    pub blocks: Option<Vec<BlockInfo>>,          // For files: ordered list of blocks
    /// Set while the file is open for writing.
    #[serde(default)]
    pub under_construction: Option<UnderConstruction>,
}

impl INode {
    pub fn new_dir(id: u64, name: String, owner: String, group: String) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            id,
            name,
            is_dir: true,
            permissions: FsPermission::default_dir(),
            owner,
            group,
            mtime: now,
            atime: now,
            replication: 0,
            block_size: 0,
            children: Some(DashMap::new()),
            blocks: None,
            under_construction: None,
        }
    }

    pub fn new_file(id: u64, name: String, owner: String, group: String, replication: u16, block_size: u64) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            id,
            name,
            is_dir: false,
            permissions: FsPermission::default_file(),
            owner,
            group,
            mtime: now,
            atime: now,
            replication,
            block_size,
            children: None,
            blocks: Some(Vec::new()),
            under_construction: None,
        }
    }

    pub fn add_block(&mut self, block: BlockInfo) {
        if let Some(blocks) = &mut self.blocks {
            blocks.push(block);
        }
    }

    /// The block being written, if the file is open and its writer has
    /// allocated one since opening it.
    pub fn open_block(&self) -> Option<&BlockInfo> {
        let uc = self.under_construction.as_ref()?;
        let blocks = self.blocks.as_ref()?;
        (blocks.len() > uc.blocks_at_open as usize).then(|| blocks.last()).flatten()
    }
}


/// An inode removed from the namespace by [`Namespace::delete_and_collect`].
#[derive(Debug, Clone)]
pub struct RemovedInode {
    pub id: u64,
    pub is_dir: bool,
    /// Blocks of a removed file (empty for directories); callers use these to
    /// release block-manager state.
    pub blocks: Vec<BlockInfo>,
    /// The lease holder, if the file was open for writing.
    pub holder: Option<String>,
}

/// The HDFS namespace - manages the entire filesystem tree
///
/// Paths are resolved by walking `children` maps from the root; there is no
/// full-path cache, so rename and delete only touch the entries they change.
///
/// Locking rules (to stay deadlock-free and never block a tokio worker):
/// * no DashMap guard (on `inodes` or a `children` map) is ever held across
///   an `.await`;
/// * lookups hold at most one inode read lock at a time;
/// * a new child is linked while holding the parent's read lock, after
///   checking the parent is still live; delete removes a directory from
///   `inodes` while holding its write lock, so a create can't link into a
///   directory that is being deleted.
pub struct Namespace {
    /// All inodes indexed by ID
    inodes: DashMap<u64, Arc<RwLock<INode>>>,

    /// Root inode ID (always 1)
    root_id: u64,

    /// Next inode ID to allocate (lock-free atomic counter)
    next_inode_id: AtomicU64,
}

impl Namespace {
    pub fn new() -> Self {
        let root_id = 1u64;
        let next_inode_id = 2u64;

        let root = Arc::new(RwLock::new(INode::new_dir(
            root_id,
            "/".to_string(),
            "hdfs".to_string(),
            "supergroup".to_string(),
        )));

        let inodes = DashMap::new();
        inodes.insert(root_id, root);

        Self {
            inodes,
            root_id,
            next_inode_id: AtomicU64::new(next_inode_id),
        }
    }

    /// Allocate a new inode ID (lock-free using atomic fetch_add)
    fn allocate_inode_id(&self) -> u64 {
        self.next_inode_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Split a path into components, filtering out empty strings
    fn split_path(path: &str) -> Vec<&str> {
        path.split('/')
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Split a path into (parent components, final component).
    /// Fails for the root path, which has no final component.
    fn split_parent(path: &str) -> Result<(Vec<&str>, &str)> {
        let mut components = Self::split_path(path);
        let name = components
            .pop()
            .ok_or_else(|| NamespaceError::InvalidPath(path.to_string()))?;
        Ok((components, name))
    }

    /// Clone an inode handle out of the map (the map guard is released immediately).
    fn inode_arc(&self, inode_id: u64) -> Option<Arc<RwLock<INode>>> {
        self.inodes.get(&inode_id).map(|entry| entry.value().clone())
    }

    /// Resolve path components to an inode id by walking from the root.
    async fn resolve(&self, components: &[&str], path: &str) -> Result<u64> {
        let mut current_id = self.root_id;

        for component in components {
            let current = self.inode_arc(current_id)
                .ok_or_else(|| NamespaceError::PathNotFound(path.to_string()))?;
            let current_inode = current.read().await;

            if !current_inode.is_dir {
                return Err(NamespaceError::NotADirectory(path.to_string()));
            }

            let children = current_inode.children.as_ref()
                .ok_or_else(|| NamespaceError::NotADirectory(path.to_string()))?;

            current_id = children
                .get(*component)
                .map(|entry| *entry.value())
                .ok_or_else(|| NamespaceError::PathNotFound(path.to_string()))?;
        }

        Ok(current_id)
    }

    /// What permission checks need of each component of `path`, root
    /// first: its attributes, or `None` from the first that does not exist.
    pub async fn resolve_attrs(&self, path: &str) -> Vec<Option<InodeAttrs>> {
        let components = Self::split_path(path);
        let mut out = Vec::with_capacity(components.len() + 1);
        let mut current = Some(self.root_id);
        for i in 0..=components.len() {
            let Some(id) = current else {
                out.push(None);
                continue;
            };
            let Some(inode) = self.inode_arc(id) else {
                out.push(None);
                current = None;
                continue;
            };
            let inode = inode.read().await;
            out.push(Some(InodeAttrs::of(&inode)));
            current = components.get(i).and_then(|c| inode.children.as_ref()?.get(*c).map(|e| *e.value()));
        }
        out
    }

    /// The directories of the subtree at `path` (itself first), each with
    /// the attributes of its children, for checks over a whole subtree.
    pub async fn subtree_dirs(&self, path: &str) -> Result<Vec<(String, InodeAttrs, Vec<InodeAttrs>)>> {
        let mut out = Vec::new();
        let root = self.resolve(&Self::split_path(path), path).await?;
        let mut stack = vec![(path.trim_end_matches('/').to_string(), root)];
        while let Some((dir_path, id)) = stack.pop() {
            let Some(inode) = self.inode_arc(id) else { continue };
            let inode = inode.read().await;
            if !inode.is_dir {
                continue;
            }
            let mut children = Vec::new();
            if let Some(c) = inode.children.as_ref() {
                let ids: Vec<(String, u64)> = c.iter().map(|e| (e.key().clone(), *e.value())).collect();
                for (name, child_id) in ids {
                    if let Some(child) = self.inode_arc(child_id) {
                        let child = child.read().await;
                        if child.is_dir {
                            stack.push((format!("{}/{}", dir_path, name), child_id));
                        }
                        children.push(InodeAttrs::of(&child));
                    }
                }
            }
            let shown = if dir_path.is_empty() { "/".to_string() } else { dir_path.clone() };
            out.push((shown, InodeAttrs::of(&inode), children));
        }
        Ok(out)
    }

    /// Lookup an inode by path
    pub async fn lookup(&self, path: &str) -> Result<Arc<RwLock<INode>>> {
        let components = Self::split_path(path);
        let inode_id = self.resolve(&components, path).await?;
        self.inode_arc(inode_id)
            .ok_or_else(|| NamespaceError::PathNotFound(path.to_string()))
    }

    /// Atomically link a new inode at `path`, failing if the name exists.
    /// `id` is the inode id to use (edit-log replay); `None` allocates one.
    async fn insert_child(
        &self,
        path: &str,
        id: Option<u64>,
        make_inode: impl FnOnce(u64, String) -> INode,
    ) -> Result<u64> {
        let (parent_components, name) = Self::split_parent(path)?;
        let parent_id = self.resolve(&parent_components, path).await?;
        let parent = self.inode_arc(parent_id)
            .ok_or_else(|| NamespaceError::PathNotFound(path.to_string()))?;
        let parent_inode = parent.read().await;

        // The parent may have been deleted between resolve and lock.
        if !self.inodes.contains_key(&parent_id) {
            return Err(NamespaceError::PathNotFound(path.to_string()));
        }

        if !parent_inode.is_dir {
            return Err(NamespaceError::NotADirectory(path.to_string()));
        }

        let parent_children = parent_inode.children.as_ref()
            .ok_or_else(|| NamespaceError::NotADirectory(path.to_string()))?;

        let result = match parent_children.entry(name.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(_) => {
                Err(NamespaceError::PathAlreadyExists(path.to_string()))
            }
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                let inode_id = match id {
                    Some(id) => {
                        if self.inodes.contains_key(&id) {
                            return Err(NamespaceError::InvalidPath(format!("inode {} already exists", id)));
                        }
                        self.next_inode_id.fetch_max(id + 1, Ordering::SeqCst);
                        id
                    }
                    None => self.allocate_inode_id(),
                };
                let inode = make_inode(inode_id, name.to_string());
                self.inodes.insert(inode_id, Arc::new(RwLock::new(inode)));
                slot.insert(inode_id);
                Ok(inode_id)
            }
        };
        result
    }

    /// Create a directory
    pub async fn mkdir(&self, path: &str, owner: String, group: String, permissions: FsPermission) -> Result<u64> {
        self.insert_child(path, None, |inode_id, name| {
            let mut inode = INode::new_dir(inode_id, name, owner, group);
            inode.permissions = permissions;
            inode
        })
        .await
    }

    /// Create a file
    pub async fn create_file(
        &self,
        path: &str,
        owner: String,
        group: String,
        permissions: FsPermission,
        replication: u16,
        block_size: u64,
    ) -> Result<u64> {
        self.insert_child(path, None, |inode_id, name| {
            let mut inode = INode::new_file(inode_id, name, owner, group, replication, block_size);
            inode.permissions = permissions;
            inode
        })
        .await
    }

    /// Link `inode` at `path`, keeping its id (edit-log replay).
    pub async fn add_inode_at(&self, path: &str, inode: INode) -> Result<u64> {
        self.insert_child(path, Some(inode.id), |_, name| INode { name, ..inode }).await
    }

    /// Delete a file or directory
    pub async fn delete(&self, path: &str, recursive: bool) -> Result<()> {
        self.delete_and_collect(path, recursive).await.map(|_| ())
    }

    /// Delete a file or directory, returning every inode that was removed
    /// (the target and, for a recursive delete, all of its descendants).
    pub async fn delete_and_collect(&self, path: &str, recursive: bool) -> Result<Vec<RemovedInode>> {
        let (parent_components, name) = Self::split_parent(path)
            .map_err(|_| NamespaceError::PermissionDenied("Cannot delete root".to_string()))?;

        let parent_id = self.resolve(&parent_components, path).await?;
        let parent = self.inode_arc(parent_id)
            .ok_or_else(|| NamespaceError::PathNotFound(path.to_string()))?;

        let target_id = {
            let parent_inode = parent.read().await;
            if !parent_inode.is_dir {
                return Err(NamespaceError::NotADirectory(path.to_string()));
            }
            parent_inode.children.as_ref()
                .and_then(|children| children.get(name).map(|entry| *entry.value()))
                .ok_or_else(|| NamespaceError::PathNotFound(path.to_string()))?
        };
        let target = self.inode_arc(target_id)
            .ok_or_else(|| NamespaceError::PathNotFound(path.to_string()))?;

        let mut removed = Vec::new();
        let mut pending = Vec::new();

        {
            let mut target_inode = target.write().await;

            // Lost a race with another delete.
            if !self.inodes.contains_key(&target_id) {
                return Err(NamespaceError::PathNotFound(path.to_string()));
            }

            if target_inode.is_dir && !recursive {
                if let Some(children) = &target_inode.children {
                    if !children.is_empty() {
                        return Err(NamespaceError::DirectoryNotEmpty(path.to_string()));
                    }
                }
            }

            // Unlink from the parent, only if the name still refers to this inode.
            let unlinked = {
                let parent_inode = parent.read().await;
                parent_inode.children.as_ref()
                    .and_then(|children| children.remove_if(name, |_, id| *id == target_id))
                    .is_some()
            };
            if !unlinked {
                return Err(NamespaceError::PathNotFound(path.to_string()));
            }

            removed.push(self.detach_inode(target_id, &mut target_inode, &mut pending));
        }

        // Remove descendants, one inode lock at a time.
        while let Some(inode_id) = pending.pop() {
            let Some(inode) = self.inode_arc(inode_id) else { continue };
            let mut guard = inode.write().await;
            if !self.inodes.contains_key(&inode_id) {
                continue;
            }
            removed.push(self.detach_inode(inode_id, &mut guard, &mut pending));
        }

        Ok(removed)
    }

    /// Remove an inode (whose write lock the caller holds) from the inode
    /// table, queueing its children for removal.
    fn detach_inode(&self, inode_id: u64, inode: &mut INode, pending: &mut Vec<u64>) -> RemovedInode {
        if let Some(children) = &inode.children {
            pending.extend(children.iter().map(|entry| *entry.value()));
            children.clear();
        }
        self.inodes.remove(&inode_id);
        RemovedInode {
            id: inode_id,
            is_dir: inode.is_dir,
            blocks: inode.blocks.as_ref().cloned().unwrap_or_default(),
            holder: inode.under_construction.as_ref().map(|uc| uc.holder.clone()),
        }
    }

    /// List directory contents
    pub async fn list_directory(&self, path: &str) -> Result<Vec<Arc<RwLock<INode>>>> {
        let inode = self.lookup(path).await?;
        let inode_guard = inode.read().await;

        if !inode_guard.is_dir {
            return Err(NamespaceError::NotADirectory(path.to_string()));
        }

        let children = inode_guard.children.as_ref()
            .ok_or_else(|| NamespaceError::NotADirectory(path.to_string()))?;

        let child_ids: Vec<u64> = children.iter().map(|entry| *entry.value()).collect();

        Ok(child_ids
            .into_iter()
            .filter_map(|child_id| self.inode_arc(child_id))
            .collect())
    }

    /// Rename a file or directory (atomic with respect to the name tables)
    ///
    /// Only the two directory entries involved change, so the cost is
    /// independent of the size of the namespace or of the renamed subtree.
    pub async fn rename(&self, src: &str, dst: &str) -> Result<()> {
        let (src_parent_components, src_name) = Self::split_parent(src)
            .map_err(|_| NamespaceError::PermissionDenied("Cannot rename root".to_string()))?;
        let (dst_parent_components, dst_name) = Self::split_parent(dst)
            .map_err(|_| NamespaceError::InvalidPath("Cannot rename to root".to_string()))?;

        let src_components = Self::split_path(src);
        let dst_components = Self::split_path(dst);
        if dst_components == src_components {
            return Err(NamespaceError::PathAlreadyExists(dst.to_string()));
        }
        if dst_components.starts_with(&src_components) {
            return Err(NamespaceError::InvalidPath(format!(
                "Cannot rename {} into its own subtree {}",
                src, dst
            )));
        }

        // Resolve the source entry
        let src_parent_id = self.resolve(&src_parent_components, src).await?;
        let src_parent = self.inode_arc(src_parent_id)
            .ok_or_else(|| NamespaceError::PathNotFound(src.to_string()))?;
        let src_id = {
            let src_parent_inode = src_parent.read().await;
            if !src_parent_inode.is_dir {
                return Err(NamespaceError::NotADirectory(src.to_string()));
            }
            src_parent_inode.children.as_ref()
                .and_then(|children| children.get(src_name).map(|entry| *entry.value()))
                .ok_or_else(|| NamespaceError::PathNotFound(src.to_string()))?
        };

        // Resolve the destination parent
        let dst_parent_id = self.resolve(&dst_parent_components, dst).await?;
        let dst_parent = self.inode_arc(dst_parent_id)
            .ok_or_else(|| NamespaceError::PathNotFound(dst.to_string()))?;

        // Link at the destination first, so the inode is never unreachable.
        {
            let dst_parent_inode = dst_parent.read().await;
            if !self.inodes.contains_key(&dst_parent_id) {
                return Err(NamespaceError::PathNotFound(dst.to_string()));
            }
            if !dst_parent_inode.is_dir {
                return Err(NamespaceError::NotADirectory(dst.to_string()));
            }
            let dst_children = dst_parent_inode.children.as_ref()
                .ok_or_else(|| NamespaceError::NotADirectory(dst.to_string()))?;
            match dst_children.entry(dst_name.to_string()) {
                dashmap::mapref::entry::Entry::Occupied(_) => {
                    return Err(NamespaceError::PathAlreadyExists(dst.to_string()));
                }
                dashmap::mapref::entry::Entry::Vacant(slot) => {
                    slot.insert(src_id);
                }
            };
        }

        // Unlink from the source; this fails if a concurrent rename/delete
        // already moved or removed it.
        let unlinked = {
            let src_parent_inode = src_parent.read().await;
            src_parent_inode.children.as_ref()
                .and_then(|children| children.remove_if(src_name, |_, id| *id == src_id))
                .is_some()
        };

        if !unlinked || !self.inodes.contains_key(&src_id) {
            // Roll back the destination link.
            let dst_parent_inode = dst_parent.read().await;
            if let Some(children) = dst_parent_inode.children.as_ref() {
                children.remove_if(dst_name, |_, id| *id == src_id);
            }
            return Err(NamespaceError::PathNotFound(src.to_string()));
        }

        // Update inode name
        if let Some(inode) = self.inode_arc(src_id) {
            inode.write().await.name = dst_name.to_string();
        }

        Ok(())
    }

    /// Get inode by ID
    pub fn get_inode(&self, inode_id: u64) -> Option<Arc<RwLock<INode>>> {
        self.inode_arc(inode_id)
    }

    /// Total number of inodes, including the root
    pub fn inode_count(&self) -> usize {
        self.inodes.len()
    }

    /// Every open file: (inode id, its under-construction record).
    pub async fn open_files(&self) -> Vec<(u64, UnderConstruction)> {
        let handles: Vec<_> = self.inodes.iter().map(|e| e.value().clone()).collect();
        let mut out = Vec::new();
        for h in handles {
            let inode = h.read().await;
            if let Some(uc) = &inode.under_construction {
                out.push((inode.id, uc.clone()));
            }
        }
        out
    }

    /// The path of inode `id` (walking parents is not possible: inodes do
    /// not record them), found by searching from the root. For the few
    /// open files listOpenFiles reports.
    pub async fn path_of(&self, id: u64) -> Option<String> {
        let mut stack = vec![(self.root_id, String::new())];
        while let Some((dir, prefix)) = stack.pop() {
            let inode = self.inode_arc(dir)?;
            let inode = inode.read().await;
            for e in inode.children.iter().flatten() {
                let path = format!("{}/{}", prefix, e.key());
                if *e.value() == id {
                    return Some(path);
                }
                stack.push((*e.value(), path));
            }
        }
        None
    }

    /// The id the next new inode will get.
    pub fn next_inode_id(&self) -> u64 {
        self.next_inode_id.load(Ordering::SeqCst)
    }

    /// Every inode reachable from the root, parents before children (an
    /// fsimage). Consistent only if no mutation runs meanwhile, i.e. under
    /// the edit log's writer lock.
    pub async fn records(&self) -> Vec<crate::metadata::edit_log::InodeRecord> {
        let mut out = Vec::with_capacity(self.inodes.len());
        let mut queue = VecDeque::from([(self.root_id, 0u64)]);
        while let Some((id, parent)) = queue.pop_front() {
            let Some(inode) = self.inode_arc(id) else { continue };
            let inode = inode.read().await;
            if let Some(children) = &inode.children {
                queue.extend(children.iter().map(|e| (*e.value(), id)));
            }
            out.push(crate::metadata::edit_log::InodeRecord::from_inode(&inode, parent));
        }
        out
    }

    /// Rebuild a namespace from [`Namespace::records`] output.
    pub fn from_records(
        records: Vec<crate::metadata::edit_log::InodeRecord>,
        open: Vec<(u64, UnderConstruction)>,
        next_inode_id: u64,
    ) -> std::result::Result<Self, String> {
        let mut records = records.into_iter();
        let root = records.next().ok_or("image has no root inode")?;
        if root.parent != 0 || !root.is_dir {
            return Err(format!("image root is not a directory: {:?}", root.name));
        }
        let root_id = root.id;
        let mut max_id = root_id;
        let mut inodes: HashMap<u64, INode> = HashMap::new();
        inodes.insert(root_id, root.into_inode());
        for r in records {
            let (id, parent, name) = (r.id, r.parent, r.name.clone());
            let parent = inodes.get(&parent).ok_or_else(|| format!("inode {} has unknown parent {}", id, parent))?;
            let children = parent.children.as_ref().ok_or_else(|| format!("parent of inode {} is a file", id))?;
            if children.insert(name, id).is_some() {
                return Err(format!("duplicate name under inode {}", r.parent));
            }
            if inodes.insert(id, r.into_inode()).is_some() {
                return Err(format!("duplicate inode {}", id));
            }
            max_id = max_id.max(id);
        }
        for (id, uc) in open {
            let inode = inodes.get_mut(&id).ok_or_else(|| format!("open file {} is not in the image", id))?;
            if inode.is_dir {
                return Err(format!("open file {} is a directory", id));
            }
            inode.under_construction = Some(uc);
        }
        Ok(Self {
            inodes: inodes.into_iter().map(|(id, inode)| (id, Arc::new(RwLock::new(inode)))).collect(),
            root_id,
            next_inode_id: AtomicU64::new(next_inode_id.max(max_id + 1)),
        })
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_namespace_basic() {
        let ns = Namespace::new();

        // Create directory
        let dir_id = ns.mkdir("/test", "user".to_string(), "group".to_string(), FsPermission::default_dir())
            .await
            .unwrap();

        // Lookup directory
        let inode = ns.lookup("/test").await.unwrap();
        let inode_guard = inode.read().await;
        assert_eq!(inode_guard.id, dir_id);
        assert_eq!(inode_guard.name, "test");
        assert!(inode_guard.is_dir);

        // Create file
        let file_id = ns.create_file(
            "/test/file.txt",
            "user".to_string(),
            "group".to_string(),
            FsPermission::default_file(),
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Lookup file
        let file_inode = ns.lookup("/test/file.txt").await.unwrap();
        let file_guard = file_inode.read().await;
        assert_eq!(file_guard.id, file_id);
        assert!(!file_guard.is_dir);

        // List directory
        let children = ns.list_directory("/test").await.unwrap();
        assert_eq!(children.len(), 1);
    }

    #[tokio::test]
    async fn test_rename_file() {
        let ns = Namespace::new();

        // Create directory
        ns.mkdir("/src", "user".to_string(), "group".to_string(), FsPermission::default_dir())
            .await
            .unwrap();
        ns.mkdir("/dst", "user".to_string(), "group".to_string(), FsPermission::default_dir())
            .await
            .unwrap();

        // Create file
        let file_id = ns.create_file(
            "/src/file.txt",
            "user".to_string(),
            "group".to_string(),
            FsPermission::default_file(),
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Add some blocks to the file
        let file_inode = ns.lookup("/src/file.txt").await.unwrap();
        {
            let mut file_guard = file_inode.write().await;
            file_guard.add_block(BlockInfo {
                block_id: 100,
                generation_stamp: 1000,
                num_bytes: 1024,
            });
            file_guard.add_block(BlockInfo {
                block_id: 101,
                generation_stamp: 1001,
                num_bytes: 2048,
            });
        }

        // Rename file
        ns.rename("/src/file.txt", "/dst/renamed.txt").await.unwrap();

        // Verify old path doesn't exist
        assert!(ns.lookup("/src/file.txt").await.is_err());

        // Verify new path exists with same inode ID and blocks
        let renamed_inode = ns.lookup("/dst/renamed.txt").await.unwrap();
        let renamed_guard = renamed_inode.read().await;
        assert_eq!(renamed_guard.id, file_id);
        assert_eq!(renamed_guard.name, "renamed.txt");
        assert_eq!(renamed_guard.blocks.as_ref().unwrap().len(), 2);
        assert_eq!(renamed_guard.blocks.as_ref().unwrap()[0].block_id, 100);
        assert_eq!(renamed_guard.blocks.as_ref().unwrap()[1].block_id, 101);

        // Verify source directory is empty
        let src_children = ns.list_directory("/src").await.unwrap();
        assert_eq!(src_children.len(), 0);

        // Verify destination directory has the file
        let dst_children = ns.list_directory("/dst").await.unwrap();
        assert_eq!(dst_children.len(), 1);
    }

    #[tokio::test]
    async fn test_rename_directory() {
        let ns = Namespace::new();

        // Create directory structure
        ns.mkdir("/old", "user".to_string(), "group".to_string(), FsPermission::default_dir())
            .await
            .unwrap();
        ns.mkdir("/old/subdir", "user".to_string(), "group".to_string(), FsPermission::default_dir())
            .await
            .unwrap();
        ns.create_file(
            "/old/file1.txt",
            "user".to_string(),
            "group".to_string(),
            FsPermission::default_file(),
            3,
            128 * 1024 * 1024,
        ).await.unwrap();
        ns.create_file(
            "/old/subdir/file2.txt",
            "user".to_string(),
            "group".to_string(),
            FsPermission::default_file(),
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Rename directory
        ns.rename("/old", "/new").await.unwrap();

        // Verify old paths don't exist
        assert!(ns.lookup("/old").await.is_err());
        assert!(ns.lookup("/old/file1.txt").await.is_err());
        assert!(ns.lookup("/old/subdir").await.is_err());
        assert!(ns.lookup("/old/subdir/file2.txt").await.is_err());

        // Verify new paths exist
        assert!(ns.lookup("/new").await.is_ok());
        assert!(ns.lookup("/new/file1.txt").await.is_ok());
        assert!(ns.lookup("/new/subdir").await.is_ok());
        assert!(ns.lookup("/new/subdir/file2.txt").await.is_ok());

        // Verify directory has correct children
        let new_children = ns.list_directory("/new").await.unwrap();
        assert_eq!(new_children.len(), 2); // file1.txt and subdir

        let subdir_children = ns.list_directory("/new/subdir").await.unwrap();
        assert_eq!(subdir_children.len(), 1); // file2.txt
    }

    #[tokio::test]
    async fn test_rename_errors() {
        let ns = Namespace::new();

        // Create a file
        ns.create_file(
            "/file.txt",
            "user".to_string(),
            "group".to_string(),
            FsPermission::default_file(),
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Cannot rename root
        assert!(ns.rename("/", "/newroot").await.is_err());

        // Cannot rename to existing path
        ns.create_file(
            "/existing.txt",
            "user".to_string(),
            "group".to_string(),
            FsPermission::default_file(),
            3,
            128 * 1024 * 1024,
        ).await.unwrap();
        assert!(ns.rename("/file.txt", "/existing.txt").await.is_err());

        // Cannot rename non-existent path
        assert!(ns.rename("/nonexistent", "/new").await.is_err());
    }

    async fn dir(ns: &Namespace, path: &str) -> Result<u64> {
        ns.mkdir(path, "user".to_string(), "group".to_string(), FsPermission::default_dir()).await
    }

    async fn file(ns: &Namespace, path: &str) -> Result<u64> {
        ns.create_file(path, "user".to_string(), "group".to_string(),
            FsPermission::default_file(), 3, 1024).await
    }

    async fn id_of(ns: &Namespace, path: &str) -> Option<u64> {
        match ns.lookup(path).await {
            Ok(inode) => Some(inode.read().await.id),
            Err(_) => None,
        }
    }

    #[tokio::test]
    async fn test_rename_dir_then_lookup_old_and_new_child_paths() {
        let ns = Namespace::new();
        dir(&ns, "/a").await.unwrap();
        dir(&ns, "/a/b").await.unwrap();
        let f = file(&ns, "/a/b/f").await.unwrap();
        let g = file(&ns, "/a/g").await.unwrap();

        // Warm any lookup state on the old paths before renaming.
        assert_eq!(id_of(&ns, "/a/b/f").await, Some(f));
        assert_eq!(id_of(&ns, "/a/g").await, Some(g));

        ns.rename("/a", "/x").await.unwrap();

        assert_eq!(id_of(&ns, "/a").await, None);
        assert_eq!(id_of(&ns, "/a/b").await, None);
        assert_eq!(id_of(&ns, "/a/b/f").await, None);
        assert_eq!(id_of(&ns, "/a/g").await, None);
        assert_eq!(id_of(&ns, "/x/b/f").await, Some(f));
        assert_eq!(id_of(&ns, "/x/g").await, Some(g));

        // Rename a nested dir into a different parent, then reuse the old name.
        dir(&ns, "/y").await.unwrap();
        ns.rename("/x/b", "/y/c").await.unwrap();
        assert_eq!(id_of(&ns, "/x/b/f").await, None);
        assert_eq!(id_of(&ns, "/y/c/f").await, Some(f));
        let new_b = dir(&ns, "/x/b").await.unwrap();
        assert_eq!(id_of(&ns, "/x/b").await, Some(new_b));
        assert_eq!(id_of(&ns, "/x/b/f").await, None);
        assert_eq!(ns.lookup("/y/c").await.unwrap().read().await.name, "c");
    }

    #[tokio::test]
    async fn test_rename_into_own_subtree_rejected() {
        let ns = Namespace::new();
        dir(&ns, "/a").await.unwrap();
        dir(&ns, "/a/b").await.unwrap();
        assert!(matches!(ns.rename("/a", "/a/b/c").await, Err(NamespaceError::InvalidPath(_))));
        assert!(ns.rename("/a", "/a").await.is_err());
        // Tree is untouched.
        assert!(ns.lookup("/a/b").await.is_ok());
        // A sibling sharing a name prefix is not a subtree.
        ns.rename("/a", "/ab").await.unwrap();
        assert!(ns.lookup("/ab/b").await.is_ok());
    }

    #[tokio::test]
    async fn test_recursive_delete_then_recreate_gives_fresh_inodes() {
        let ns = Namespace::new();
        let old_d = dir(&ns, "/d").await.unwrap();
        let old_e = dir(&ns, "/d/e").await.unwrap();
        let old_f = file(&ns, "/d/e/f").await.unwrap();
        {
            let inode = ns.lookup("/d/e/f").await.unwrap();
            inode.write().await.add_block(BlockInfo { block_id: 7, generation_stamp: 1, num_bytes: 10 });
        }
        file(&ns, "/d/g").await.unwrap();

        // Non-recursive delete of a non-empty dir fails and changes nothing.
        assert!(matches!(ns.delete("/d", false).await, Err(NamespaceError::DirectoryNotEmpty(_))));
        assert_eq!(id_of(&ns, "/d/e/f").await, Some(old_f));

        ns.delete("/d", true).await.unwrap();
        assert_eq!(id_of(&ns, "/d").await, None);
        assert_eq!(id_of(&ns, "/d/e").await, None);
        assert_eq!(id_of(&ns, "/d/e/f").await, None);

        // Recreate the same paths: must be new, empty inodes.
        let new_d = dir(&ns, "/d").await.unwrap();
        assert_ne!(new_d, old_d);
        assert!(ns.list_directory("/d").await.unwrap().is_empty());
        assert!(ns.lookup("/d/e").await.is_err());
        let new_e = dir(&ns, "/d/e").await.unwrap();
        assert_ne!(new_e, old_e);
        let new_f = file(&ns, "/d/e/f").await.unwrap();
        assert_ne!(new_f, old_f);
        let f = ns.lookup("/d/e/f").await.unwrap();
        assert!(f.read().await.blocks.as_ref().unwrap().is_empty());
        assert!(ns.get_inode(old_f).is_none());
        assert!(ns.get_inode(old_e).is_none());
    }

    #[tokio::test]
    async fn test_recursive_delete_does_not_leak_inodes() {
        let ns = Namespace::new();
        assert_eq!(ns.inode_count(), 1); // root

        dir(&ns, "/keep").await.unwrap();
        file(&ns, "/keep/k").await.unwrap();
        let baseline = ns.inode_count();

        dir(&ns, "/t").await.unwrap();
        for i in 0..5 {
            dir(&ns, &format!("/t/d{}", i)).await.unwrap();
            for j in 0..4 {
                file(&ns, &format!("/t/d{}/f{}", i, j)).await.unwrap();
            }
            dir(&ns, &format!("/t/d{}/sub", i)).await.unwrap();
            file(&ns, &format!("/t/d{}/sub/deep", i)).await.unwrap();
        }
        let created = 1 + 5 * (1 + 4 + 1 + 1);
        assert_eq!(ns.inode_count(), baseline + created);

        let removed = ns.delete_and_collect("/t", true).await.unwrap();
        assert_eq!(removed.len(), created);
        assert_eq!(removed.iter().filter(|r| !r.is_dir).count(), 5 * 5);
        assert_eq!(ns.inode_count(), baseline);
        assert!(ns.lookup("/keep/k").await.is_ok());

        // Deleting a single file also releases it.
        let removed = ns.delete_and_collect("/keep/k", false).await.unwrap();
        assert_eq!(removed.len(), 1);
        assert_eq!(ns.inode_count(), baseline - 1);
        assert!(ns.delete("/", true).await.is_err());
    }

    #[tokio::test]
    async fn test_create_exclusive_under_concurrency() {
        let ns = Arc::new(Namespace::new());
        dir(&ns, "/c").await.unwrap();
        let mut handles = Vec::new();
        for _ in 0..16 {
            let ns = ns.clone();
            handles.push(tokio::spawn(async move { file(&ns, "/c/same").await.is_ok() }));
        }
        let mut ok = 0;
        for h in handles {
            if h.await.unwrap() {
                ok += 1;
            }
        }
        assert_eq!(ok, 1);
        assert_eq!(ns.inode_count(), 3);
    }

    #[tokio::test]
    async fn test_lookup_does_not_hold_map_guard_across_await() {
        // A writer holding a directory's lock must not block unrelated
        // inode-table operations performed while a lookup waits on it.
        let ns = Arc::new(Namespace::new());
        dir(&ns, "/w").await.unwrap();
        let w = ns.lookup("/w").await.unwrap();
        let guard = w.write().await;

        let ns2 = ns.clone();
        let waiting = tokio::spawn(async move { ns2.lookup("/w/x").await.is_ok() });
        tokio::task::yield_now().await;

        // These touch the inode table (possibly the same shard) and must not hang.
        let done = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            for i in 0..64 {
                file(&ns, &format!("/f{}", i)).await.unwrap();
            }
        })
        .await;
        assert!(done.is_ok());

        drop(guard);
        assert!(!waiting.await.unwrap());
    }
}

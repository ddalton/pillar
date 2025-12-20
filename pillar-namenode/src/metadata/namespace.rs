use dashmap::DashMap;
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
#[derive(Debug, Clone, Copy)]
pub struct FsPermission {
    pub mode: u16,  // Unix-style permission bits
}

impl FsPermission {
    pub fn new(mode: u16) -> Self {
        Self { mode: mode & 0o777 }
    }

    pub fn default_dir() -> Self {
        Self::new(0o755)
    }

    pub fn default_file() -> Self {
        Self::new(0o644)
    }
}

/// Block information for a file
#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub block_id: u64,
    pub generation_stamp: u64,
    pub num_bytes: u64,
}

/// INode (Index Node) - represents a file or directory in HDFS
#[derive(Debug, Clone)]
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
    pub children: Option<DashMap<String, u64>>,  // For directories: name -> inode_id
    pub blocks: Option<Vec<BlockInfo>>,          // For files: ordered list of blocks
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
        }
    }

    pub fn add_block(&mut self, block: BlockInfo) {
        if let Some(blocks) = &mut self.blocks {
            blocks.push(block);
        }
    }
}

/// The HDFS namespace - manages the entire filesystem tree
pub struct Namespace {
    /// All inodes indexed by ID
    inodes: DashMap<u64, Arc<RwLock<INode>>>,

    /// Path cache for quick lookups (path -> inode_id)
    path_cache: DashMap<String, u64>,

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

        let path_cache = DashMap::new();
        path_cache.insert("/".to_string(), root_id);

        Self {
            inodes,
            path_cache,
            root_id,
            next_inode_id: AtomicU64::new(next_inode_id),
        }
    }

    /// Allocate a new inode ID (lock-free using atomic fetch_add)
    async fn allocate_inode_id(&self) -> u64 {
        self.next_inode_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Split a path into components, filtering out empty strings
    fn split_path(path: &str) -> Vec<&str> {
        path.split('/')
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Lookup an inode by path
    pub async fn lookup(&self, path: &str) -> Result<Arc<RwLock<INode>>> {
        // Normalize path
        let path = if path.is_empty() { "/" } else { path };

        // Check cache first
        if let Some(inode_id) = self.path_cache.get(path) {
            if let Some(inode) = self.inodes.get(inode_id.value()) {
                return Ok(inode.clone());
            }
        }

        // Walk the tree
        let components = Self::split_path(path);
        if components.is_empty() {
            // Root path "/"
            return self.inodes.get(&self.root_id)
                .map(|entry| entry.clone())
                .ok_or_else(|| NamespaceError::PathNotFound(path.to_string()));
        }

        let mut current_id = self.root_id;

        for component in components {
            let current = self.inodes.get(&current_id)
                .ok_or_else(|| NamespaceError::PathNotFound(path.to_string()))?;

            let current_inode = current.read().await;

            if !current_inode.is_dir {
                return Err(NamespaceError::NotADirectory(path.to_string()));
            }

            let children = current_inode.children.as_ref()
                .ok_or_else(|| NamespaceError::NotADirectory(path.to_string()))?;

            current_id = *children.get(component)
                .ok_or_else(|| NamespaceError::PathNotFound(path.to_string()))?
                .value();
        }

        let result = self.inodes.get(&current_id)
            .map(|entry| entry.clone())
            .ok_or_else(|| NamespaceError::PathNotFound(path.to_string()))?;

        // Cache the result
        self.path_cache.insert(path.to_string(), current_id);

        Ok(result)
    }

    /// Create a directory
    pub async fn mkdir(&self, path: &str, owner: String, group: String, permissions: FsPermission) -> Result<u64> {
        // Check if already exists
        if self.lookup(path).await.is_ok() {
            return Err(NamespaceError::PathAlreadyExists(path.to_string()));
        }

        // Find parent
        let parent_path = path.rfind('/').map(|i| &path[..i.max(1)]).unwrap_or("/");
        let name = path.rfind('/').map(|i| &path[i+1..]).unwrap_or(path);

        if name.is_empty() {
            return Err(NamespaceError::InvalidPath(path.to_string()));
        }

        let parent = self.lookup(parent_path).await?;
        let parent_inode = parent.read().await;

        if !parent_inode.is_dir {
            return Err(NamespaceError::NotADirectory(parent_path.to_string()));
        }

        let parent_children = parent_inode.children.as_ref()
            .ok_or_else(|| NamespaceError::NotADirectory(parent_path.to_string()))?;

        // Allocate new inode
        let inode_id = self.allocate_inode_id().await;
        let mut new_inode = INode::new_dir(inode_id, name.to_string(), owner, group);
        new_inode.permissions = permissions;

        // Insert into tree
        let new_inode_arc = Arc::new(RwLock::new(new_inode));
        self.inodes.insert(inode_id, new_inode_arc);
        parent_children.insert(name.to_string(), inode_id);

        // Cache the path
        self.path_cache.insert(path.to_string(), inode_id);

        Ok(inode_id)
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
        // Check if already exists
        if self.lookup(path).await.is_ok() {
            return Err(NamespaceError::PathAlreadyExists(path.to_string()));
        }

        // Find parent
        let parent_path = path.rfind('/').map(|i| &path[..i.max(1)]).unwrap_or("/");
        let name = path.rfind('/').map(|i| &path[i+1..]).unwrap_or(path);

        if name.is_empty() {
            return Err(NamespaceError::InvalidPath(path.to_string()));
        }

        let parent = self.lookup(parent_path).await?;
        let parent_inode = parent.read().await;

        if !parent_inode.is_dir {
            return Err(NamespaceError::NotADirectory(parent_path.to_string()));
        }

        let parent_children = parent_inode.children.as_ref()
            .ok_or_else(|| NamespaceError::NotADirectory(parent_path.to_string()))?;

        // Allocate new inode
        let inode_id = self.allocate_inode_id().await;
        let mut new_inode = INode::new_file(inode_id, name.to_string(), owner, group, replication, block_size);
        new_inode.permissions = permissions;

        // Insert into tree
        let new_inode_arc = Arc::new(RwLock::new(new_inode));
        self.inodes.insert(inode_id, new_inode_arc);
        parent_children.insert(name.to_string(), inode_id);

        // Cache the path
        self.path_cache.insert(path.to_string(), inode_id);

        Ok(inode_id)
    }

    /// Delete a file or directory
    pub async fn delete(&self, path: &str, recursive: bool) -> Result<()> {
        if path == "/" {
            return Err(NamespaceError::PermissionDenied("Cannot delete root".to_string()));
        }

        let inode = self.lookup(path).await?;
        let inode_guard = inode.read().await;

        // Check if directory is empty
        if inode_guard.is_dir && !recursive {
            if let Some(children) = &inode_guard.children {
                if !children.is_empty() {
                    return Err(NamespaceError::DirectoryNotEmpty(path.to_string()));
                }
            }
        }

        let inode_id = inode_guard.id;
        drop(inode_guard);

        // Remove from parent
        let parent_path = path.rfind('/').map(|i| &path[..i.max(1)]).unwrap_or("/");
        let name = path.rfind('/').map(|i| &path[i+1..]).unwrap_or(path);

        let parent = self.lookup(parent_path).await?;
        let parent_inode = parent.read().await;

        if let Some(children) = &parent_inode.children {
            children.remove(name);
        }

        // Remove from inodes and cache
        self.inodes.remove(&inode_id);
        self.path_cache.remove(path);

        Ok(())
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

        let mut result = Vec::new();
        for entry in children.iter() {
            let child_id = *entry.value();
            if let Some(child) = self.inodes.get(&child_id) {
                result.push(child.clone());
            }
        }

        Ok(result)
    }

    /// Get inode by ID
    pub fn get_inode(&self, inode_id: u64) -> Option<Arc<RwLock<INode>>> {
        self.inodes.get(&inode_id).map(|entry| entry.clone())
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
}

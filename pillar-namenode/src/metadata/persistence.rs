use async_trait::async_trait;
use rusqlite::{params, Connection};
use std::path::Path;
use thiserror::Error;

use super::namespace::{INode, FsPermission};
use super::block_manager::BlockId;

#[derive(Error, Debug)]
pub enum PersistenceError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] rusqlite::Error),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Not found: {0}")]
    NotFound(String),
}

pub type Result<T> = std::result::Result<T, PersistenceError>;

/// Trait for metadata persistence
#[async_trait]
pub trait MetadataStore: Send + Sync {
    /// Save an inode to persistent storage
    async fn save_inode(&self, inode: &INode) -> Result<()>;

    /// Load an inode from persistent storage
    async fn load_inode(&self, inode_id: u64) -> Result<INode>;

    /// Delete an inode from persistent storage
    async fn delete_inode(&self, inode_id: u64) -> Result<()>;

    /// Save block mapping for a file
    async fn save_block_mapping(&self, inode_id: u64, block_id: BlockId, block_index: u32) -> Result<()>;

    /// Load all blocks for a file
    async fn load_blocks(&self, inode_id: u64) -> Result<Vec<(u32, BlockId)>>;

    /// Initialize the storage (create tables, etc.)
    async fn initialize(&self) -> Result<()>;
}

/// SQLite-based metadata store for MVP
pub struct SqliteMetadataStore {
    db_path: String,
}

impl SqliteMetadataStore {
    pub fn new(db_path: impl AsRef<Path>) -> Self {
        Self {
            db_path: db_path.as_ref().to_string_lossy().to_string(),
        }
    }

    fn get_connection(&self) -> Result<Connection> {
        Ok(Connection::open(&self.db_path)?)
    }
}

#[async_trait]
impl MetadataStore for SqliteMetadataStore {
    async fn initialize(&self) -> Result<()> {
        let conn = self.get_connection()?;

        // Create inodes table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS inodes (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                is_dir INTEGER NOT NULL,
                permissions INTEGER NOT NULL,
                owner TEXT NOT NULL,
                group_name TEXT NOT NULL,
                mtime INTEGER NOT NULL,
                atime INTEGER NOT NULL,
                replication INTEGER NOT NULL,
                block_size INTEGER NOT NULL
            )",
            [],
        )?;

        // Create blocks table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS blocks (
                inode_id INTEGER NOT NULL,
                block_index INTEGER NOT NULL,
                block_id INTEGER NOT NULL,
                generation_stamp INTEGER NOT NULL,
                PRIMARY KEY (inode_id, block_index),
                FOREIGN KEY (inode_id) REFERENCES inodes(id)
            )",
            [],
        )?;

        // Create directory entries table (parent-child relationships)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dir_entries (
                parent_id INTEGER NOT NULL,
                child_name TEXT NOT NULL,
                child_id INTEGER NOT NULL,
                PRIMARY KEY (parent_id, child_name),
                FOREIGN KEY (parent_id) REFERENCES inodes(id),
                FOREIGN KEY (child_id) REFERENCES inodes(id)
            )",
            [],
        )?;

        Ok(())
    }

    async fn save_inode(&self, inode: &INode) -> Result<()> {
        let conn = self.get_connection()?;

        conn.execute(
            "INSERT OR REPLACE INTO inodes
             (id, name, is_dir, permissions, owner, group_name, mtime, atime, replication, block_size)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                inode.id,
                inode.name,
                if inode.is_dir { 1 } else { 0 },
                inode.permissions.mode,
                inode.owner,
                inode.group,
                inode.mtime,
                inode.atime,
                inode.replication,
                inode.block_size,
            ],
        )?;

        Ok(())
    }

    async fn load_inode(&self, inode_id: u64) -> Result<INode> {
        let conn = self.get_connection()?;

        let mut stmt = conn.prepare(
            "SELECT id, name, is_dir, permissions, owner, group_name, mtime, atime, replication, block_size
             FROM inodes WHERE id = ?1"
        )?;

        let inode = stmt.query_row(params![inode_id], |row| {
            let is_dir: i32 = row.get(2)?;
            let permissions: u16 = row.get(3)?;
            let replication: u16 = row.get(8)?;
            let block_size: u64 = row.get(9)?;

            Ok(INode {
                id: row.get(0)?,
                name: row.get(1)?,
                is_dir: is_dir != 0,
                permissions: FsPermission::new(permissions),
                owner: row.get(4)?,
                group: row.get(5)?,
                mtime: row.get(6)?,
                atime: row.get(7)?,
                replication,
                block_size,
                children: if is_dir != 0 { Some(dashmap::DashMap::new()) } else { None },
                blocks: if is_dir == 0 { Some(Vec::new()) } else { None },
                under_construction: None,
                acl: Vec::new(),
            })
        }).map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => {
                PersistenceError::NotFound(format!("Inode {} not found", inode_id))
            }
            other => PersistenceError::DatabaseError(other),
        })?;

        Ok(inode)
    }

    async fn delete_inode(&self, inode_id: u64) -> Result<()> {
        let conn = self.get_connection()?;

        conn.execute("DELETE FROM inodes WHERE id = ?1", params![inode_id])?;
        conn.execute("DELETE FROM blocks WHERE inode_id = ?1", params![inode_id])?;
        conn.execute("DELETE FROM dir_entries WHERE child_id = ?1", params![inode_id])?;

        Ok(())
    }

    async fn save_block_mapping(&self, inode_id: u64, block_id: BlockId, block_index: u32) -> Result<()> {
        let conn = self.get_connection()?;

        conn.execute(
            "INSERT OR REPLACE INTO blocks (inode_id, block_index, block_id, generation_stamp)
             VALUES (?1, ?2, ?3, ?4)",
            params![inode_id, block_index, block_id.block_id, block_id.generation_stamp],
        )?;

        Ok(())
    }

    async fn load_blocks(&self, inode_id: u64) -> Result<Vec<(u32, BlockId)>> {
        let conn = self.get_connection()?;

        let mut stmt = conn.prepare(
            "SELECT block_index, block_id, generation_stamp
             FROM blocks
             WHERE inode_id = ?1
             ORDER BY block_index"
        )?;

        let blocks = stmt.query_map(params![inode_id], |row| {
            Ok((
                row.get(0)?,
                BlockId::new(row.get(1)?, row.get(2)?),
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(blocks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_sqlite_persistence() {
        let temp_file = NamedTempFile::new().unwrap();
        let store = SqliteMetadataStore::new(temp_file.path());

        store.initialize().await.unwrap();

        // Create and save inode
        let inode = INode::new_dir(1, "test".to_string(), "user".to_string(), "group".to_string());
        store.save_inode(&inode).await.unwrap();

        // Load inode
        let loaded = store.load_inode(1).await.unwrap();
        assert_eq!(loaded.id, 1);
        assert_eq!(loaded.name, "test");
        assert!(loaded.is_dir);
    }
}

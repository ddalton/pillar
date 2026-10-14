//! The identity a NameNode gives its namespace when it formats a name
//! directory, and that every DataNode volume joining it records (HDFS
//! `StorageInfo` / `NamespaceInfo`, kept in `current/VERSION` files).
//!
//! A DataNode only registers with a NameNode whose identity matches the one
//! on its volume, and only adopts an identity while its volume is empty. So
//! a NameNode started on a fresh or wrong name directory cannot receive the
//! block reports of an existing cluster's DataNodes, and so can never order
//! their replicas deleted as unknown.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageIdentity {
    pub cluster_id: String,
    pub namespace_id: u32,
    pub block_pool_id: String,
}

impl std::fmt::Display for StorageIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "clusterID={} namespaceID={} blockpoolID={}", self.cluster_id, self.namespace_id, self.block_pool_id)
    }
}

impl StorageIdentity {
    /// Read a `VERSION` file; `Ok(None)` if there is none.
    pub fn read(path: &Path) -> io::Result<Option<Self>> {
        let text = match fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let get = |key: &str| {
            text.lines()
                .filter(|l| !l.starts_with('#'))
                .find_map(|l| l.strip_prefix(key)?.strip_prefix('='))
                .map(str::trim)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("{}: no {}", path.display(), key)))
        };
        let namespace_id = get("namespaceID")?
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, format!("{}: bad namespaceID", path.display())))?;
        Ok(Some(Self {
            cluster_id: get("clusterID")?.to_string(),
            namespace_id,
            block_pool_id: get("blockpoolID")?.to_string(),
        }))
    }

    /// Write a `VERSION` file atomically (temporary file, fsync, rename).
    /// `storage_type` is `NAME_NODE` or `DATA_NODE`, as in HDFS.
    pub fn write(&self, path: &Path, storage_type: &str) -> io::Result<()> {
        let dir = path.parent().ok_or_else(|| io::Error::other("VERSION path has no parent"))?;
        fs::create_dir_all(dir)?;
        let tmp = path.with_extension("tmp");
        let mut f = File::create(&tmp)?;
        write!(
            f,
            "#Pillar storage identity\nclusterID={}\nnamespaceID={}\nblockpoolID={}\nstorageType={}\nlayoutVersion=1\n",
            self.cluster_id, self.namespace_id, self.block_pool_id, storage_type
        )?;
        f.sync_all()?;
        fs::rename(&tmp, path)?;
        File::open(dir)?.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_reports_absence() {
        let dir = std::env::temp_dir().join(format!("pillar-identity-{}", std::process::id()));
        let path = dir.join("current").join("VERSION");
        let _ = fs::remove_dir_all(&dir);
        assert_eq!(StorageIdentity::read(&path).unwrap(), None);
        let id = StorageIdentity { cluster_id: "CID-1".into(), namespace_id: 42, block_pool_id: "BP-42-x-1".into() };
        id.write(&path, "DATA_NODE").unwrap();
        assert_eq!(StorageIdentity::read(&path).unwrap(), Some(id));
        fs::write(&path, "clusterID=CID-1\n").unwrap();
        assert_eq!(StorageIdentity::read(&path).unwrap_err().kind(), io::ErrorKind::InvalidData);
        fs::remove_dir_all(&dir).unwrap();
    }
}

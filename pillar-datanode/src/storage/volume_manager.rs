use std::path::{Path, PathBuf};
use std::fs;
use nix::sys::statvfs::statvfs;
use thiserror::Error;
use tracing::{info, warn};

#[derive(Error, Debug)]
pub enum VolumeError {
    #[error("Volume not mounted: {0}")]
    NotMounted(String),

    #[error("Failed to create directory: {0}")]
    CreateDirectoryFailed(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("System error: {0}")]
    SystemError(String),

    /// Another DataNode process has the volume (its `in_use.lock`).
    #[error("Volume {0} is in use by another DataNode ({1})")]
    InUse(String, String),
}

pub type Result<T> = std::result::Result<T, VolumeError>;

/// Volume information and statistics
#[derive(Debug, Clone)]
pub struct VolumeInfo {
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub remaining_bytes: u64,
    pub block_pool_used_bytes: u64,
}

impl VolumeInfo {
    pub fn utilization(&self) -> f64 {
        if self.capacity_bytes == 0 {
            0.0
        } else {
            self.used_bytes as f64 / self.capacity_bytes as f64
        }
    }
}

/// Take `<mount>/in_use.lock` exclusively (released when the process
/// exits, however it exits), recording `pid@host` in it.
fn lock_volume(mount_point: &Path) -> Result<fs::File> {
    use std::io::{Read, Seek, Write};
    use std::os::fd::AsRawFd;
    let path = mount_point.join("in_use.lock");
    let mut f = fs::OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path)?;
    #[allow(deprecated)]
    if let Err(e) = nix::fcntl::flock(f.as_raw_fd(), nix::fcntl::FlockArg::LockExclusiveNonblock) {
        if e == nix::errno::Errno::EWOULDBLOCK {
            let mut holder = String::new();
            let _ = f.read_to_string(&mut holder);
            return Err(VolumeError::InUse(mount_point.display().to_string(), holder.trim().to_string()));
        }
        return Err(VolumeError::SystemError(format!("locking {}: {}", path.display(), e)));
    }
    let host = std::env::var("HOSTNAME")
        .or_else(|_| fs::read_to_string("/etc/hostname"))
        .map(|h| h.trim().to_string())
        .unwrap_or_default();
    f.set_len(0)?;
    f.rewind()?;
    write!(f, "{}@{}", std::process::id(), host)?;
    Ok(f)
}

/// Manages a Flint RWO volume for DataNode storage
pub struct VolumeManager {
    /// Mount point of the Flint volume (e.g., /mnt/pillar-data)
    mount_point: PathBuf,

    /// Block pool ID
    block_pool_id: String,

    /// Base directory for blocks
    base_dir: PathBuf,

    /// Current directory for finalized blocks
    current_dir: PathBuf,

    /// `in_use.lock`, held (flock) while this VolumeManager lives.
    _lock: fs::File,

    /// Temporary directory for blocks being written
    tmp_dir: PathBuf,
}

impl VolumeManager {
    /// Initialize a volume manager
    ///
    /// Expected directory structure:
    /// /mnt/pillar-data/
    ///   └── current/
    ///       └── BP-<pool-id>/
    ///           ├── current/
    ///           │   └── finalized/
    ///           └── tmp/
    pub fn new(mount_point: impl AsRef<Path>, block_pool_id: String) -> Result<Self> {
        let mount_point = mount_point.as_ref().to_path_buf();

        // Verify mount point exists
        if !mount_point.exists() {
            return Err(VolumeError::NotMounted(
                mount_point.display().to_string()
            ));
        }

        // One DataNode process per volume (HDFS's `in_use.lock`): a
        // replacement started while the old process still runs (a pod
        // force-deleted and recreated on its volume) would serve and report
        // replicas the other is still changing.
        let lock = lock_volume(&mount_point)?;

        // Create HDFS directory structure
        let base_dir = mount_point.join("current").join(&block_pool_id);
        let current_dir = base_dir.join("current").join("finalized");
        let tmp_dir = base_dir.join("tmp");

        // Create directories
        fs::create_dir_all(&current_dir)
            .map_err(|e| VolumeError::CreateDirectoryFailed(format!("{}: {}", current_dir.display(), e)))?;

        fs::create_dir_all(&tmp_dir)
            .map_err(|e| VolumeError::CreateDirectoryFailed(format!("{}: {}", tmp_dir.display(), e)))?;

        info!("Initialized volume at: {}", mount_point.display());
        info!("  Block pool: {}", block_pool_id);
        info!("  Finalized blocks: {}", current_dir.display());
        info!("  Temporary blocks: {}", tmp_dir.display());

        Ok(Self {
            mount_point,
            block_pool_id,
            base_dir,
            current_dir,
            tmp_dir,
            _lock: lock,
        })
    }

    /// `current/VERSION`: the namespace identity this volume belongs to.
    pub fn version_file(&self) -> PathBuf {
        self.mount_point.join("current").join("VERSION")
    }

    /// Volume statistics with block-pool usage supplied by the caller
    /// (`BlockStorage::total_bytes`): one `statvfs`, no directory walk.
    /// This is what heartbeats should use.
    pub fn get_volume_stats(&self, block_pool_used_bytes: u64) -> Result<VolumeInfo> {
        let stats = statvfs(&self.mount_point)
            .map_err(|e| VolumeError::SystemError(format!("statvfs failed: {}", e)))?;

        let block_size = stats.block_size();
        let capacity_bytes = stats.blocks() as u64 * block_size;
        let available_bytes = stats.blocks_available() as u64 * block_size;

        Ok(VolumeInfo {
            capacity_bytes,
            used_bytes: capacity_bytes.saturating_sub(available_bytes),
            remaining_bytes: available_bytes,
            block_pool_used_bytes,
        })
    }

    /// Get volume statistics, walking the block pool directory to size it.
    /// Prefer [`Self::get_volume_stats`] on hot paths.
    pub fn get_volume_info(&self) -> Result<VolumeInfo> {
        let stats = statvfs(&self.mount_point)
            .map_err(|e| VolumeError::SystemError(format!("statvfs failed: {}", e)))?;

        let block_size = stats.block_size();
        let capacity_bytes = stats.blocks() as u64 * block_size;
        let available_bytes = stats.blocks_available() as u64 * block_size;
        let used_bytes = capacity_bytes - available_bytes;

        // Calculate block pool usage
        let block_pool_used_bytes = self.calculate_directory_size(&self.base_dir)?;

        Ok(VolumeInfo {
            capacity_bytes,
            used_bytes,
            remaining_bytes: available_bytes,
            block_pool_used_bytes,
        })
    }

    /// Calculate total size of a directory
    fn calculate_directory_size(&self, dir: &Path) -> Result<u64> {
        let mut total_size = 0u64;

        if dir.is_dir() {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();

                if path.is_file() {
                    total_size += entry.metadata()?.len();
                } else if path.is_dir() {
                    total_size += self.calculate_directory_size(&path)?;
                }
            }
        }

        Ok(total_size)
    }

    /// Get the base directory for finalized blocks
    pub fn get_finalized_dir(&self) -> &Path {
        &self.current_dir
    }

    /// Get the temporary directory
    pub fn get_tmp_dir(&self) -> &Path {
        &self.tmp_dir
    }

    /// Get the mount point
    pub fn get_mount_point(&self) -> &Path {
        &self.mount_point
    }

    /// Get the block pool ID
    pub fn get_block_pool_id(&self) -> &str {
        &self.block_pool_id
    }

    /// Check if volume is healthy
    pub fn is_healthy(&self) -> bool {
        // Basic health check: verify directories exist and are writable
        self.current_dir.exists() &&
        self.tmp_dir.exists() &&
        self.test_write().is_ok()
    }

    /// Test if we can write to the volume
    fn test_write(&self) -> Result<()> {
        let test_file = self.tmp_dir.join(".health_check");
        fs::write(&test_file, b"test")?;
        fs::remove_file(&test_file)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_volume_manager_initialization() {
        let temp_dir = TempDir::new().unwrap();
        let mount_point = temp_dir.path();

        let volume_manager = VolumeManager::new(mount_point, "BP-test-001".to_string()).unwrap();

        assert!(volume_manager.get_finalized_dir().exists());
        assert!(volume_manager.get_tmp_dir().exists());
        assert_eq!(volume_manager.get_block_pool_id(), "BP-test-001");
    }

    #[test]
    fn test_volume_info() {
        let temp_dir = TempDir::new().unwrap();
        let mount_point = temp_dir.path();

        let volume_manager = VolumeManager::new(mount_point, "BP-test-001".to_string()).unwrap();
        let info = volume_manager.get_volume_info().unwrap();

        assert!(info.capacity_bytes > 0);
        assert!(info.remaining_bytes <= info.capacity_bytes);
    }

    #[test]
    fn test_health_check() {
        let temp_dir = TempDir::new().unwrap();
        let mount_point = temp_dir.path();

        let volume_manager = VolumeManager::new(mount_point, "BP-test-001".to_string()).unwrap();
        assert!(volume_manager.is_healthy());
    }

    #[test]
    fn one_datanode_process_per_volume() {
        let temp_dir = TempDir::new().unwrap();
        let first = VolumeManager::new(temp_dir.path(), "BP-test-001".to_string()).unwrap();
        match VolumeManager::new(temp_dir.path(), "BP-test-001".to_string()) {
            Err(VolumeError::InUse(_, holder)) => assert!(holder.starts_with(&format!("{}@", std::process::id())), "{holder}"),
            other => panic!("a second VolumeManager on the volume: {:?}", other.map(|_| ())),
        }
        drop(first);
        VolumeManager::new(temp_dir.path(), "BP-test-001".to_string()).expect("free once the first is gone");
    }
}

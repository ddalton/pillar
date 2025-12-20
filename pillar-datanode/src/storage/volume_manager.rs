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
        })
    }

    /// Get volume statistics
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
}

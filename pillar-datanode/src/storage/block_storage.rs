use std::path::{Path, PathBuf};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use dashmap::DashMap;
use bytes::Bytes;
use thiserror::Error;
use tracing::{info, warn, debug};

#[derive(Error, Debug)]
pub enum BlockStorageError {
    #[error("Block not found: {0}")]
    BlockNotFound(u64),

    #[error("Block already exists: {0}")]
    BlockAlreadyExists(u64),

    #[error("Invalid block size")]
    InvalidBlockSize,

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Checksum mismatch")]
    ChecksumMismatch,
}

pub type Result<T> = std::result::Result<T, BlockStorageError>;

/// Block identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockId {
    pub block_id: u64,
    pub generation_stamp: u64,
}

impl BlockId {
    pub fn new(block_id: u64, generation_stamp: u64) -> Self {
        Self { block_id, generation_stamp }
    }
}

/// Metadata for a stored block
#[derive(Debug, Clone)]
pub struct BlockMetadata {
    pub block_id: BlockId,
    pub num_bytes: u64,
    pub file_path: PathBuf,
    pub meta_path: PathBuf,
}

/// Manages HDFS block storage on the filesystem
pub struct BlockStorage {
    /// Base directory for finalized blocks
    finalized_dir: PathBuf,

    /// Temporary directory for blocks being written
    tmp_dir: PathBuf,

    /// Block metadata cache: block_id -> BlockMetadata
    blocks: DashMap<u64, BlockMetadata>,
}

impl BlockStorage {
    pub fn new(finalized_dir: impl AsRef<Path>, tmp_dir: impl AsRef<Path>) -> Result<Self> {
        let finalized_dir = finalized_dir.as_ref().to_path_buf();
        let tmp_dir = tmp_dir.as_ref().to_path_buf();

        let storage = Self {
            finalized_dir,
            tmp_dir,
            blocks: DashMap::new(),
        };

        // Scan and load existing blocks
        storage.scan_blocks()?;

        Ok(storage)
    }

    /// Scan the finalized directory and load all blocks into memory
    fn scan_blocks(&self) -> Result<()> {
        if !self.finalized_dir.exists() {
            return Ok(());
        }

        let mut count = 0;

        for entry in fs::read_dir(&self.finalized_dir)? {
            let entry = entry?;
            let path = entry.path();

            // Look for block files (blk_<blockid>_<genstamp>)
            if let Some(file_name) = path.file_name() {
                if let Some(name_str) = file_name.to_str() {
                    if name_str.starts_with("blk_") && !name_str.ends_with(".meta") {
                        if let Some(block_id) = self.parse_block_filename(name_str) {
                            let meta_path = path.with_extension("meta");
                            let num_bytes = entry.metadata()?.len();

                            let metadata = BlockMetadata {
                                block_id,
                                num_bytes,
                                file_path: path,
                                meta_path,
                            };

                            self.blocks.insert(block_id.block_id, metadata);
                            count += 1;
                        }
                    }
                }
            }
        }

        info!("Loaded {} existing blocks from {}", count, self.finalized_dir.display());

        Ok(())
    }

    /// Parse a block filename to extract block ID and generation stamp
    /// Format: blk_<blockid>_<genstamp>
    fn parse_block_filename(&self, filename: &str) -> Option<BlockId> {
        let parts: Vec<&str> = filename.split('_').collect();
        if parts.len() != 3 || parts[0] != "blk" {
            return None;
        }

        let block_id = parts[1].parse::<u64>().ok()?;
        let generation_stamp = parts[2].parse::<u64>().ok()?;

        Some(BlockId::new(block_id, generation_stamp))
    }

    /// Get the path for a block file
    fn get_block_path(&self, block_id: &BlockId, finalized: bool) -> PathBuf {
        let dir = if finalized { &self.finalized_dir } else { &self.tmp_dir };
        dir.join(format!("blk_{}_{}", block_id.block_id, block_id.generation_stamp))
    }

    /// Get the path for a block metadata file
    fn get_meta_path(&self, block_id: &BlockId, finalized: bool) -> PathBuf {
        self.get_block_path(block_id, finalized).with_extension("meta")
    }

    /// Create a new block (in temporary directory)
    pub fn create_block(&self, block_id: BlockId) -> Result<()> {
        if self.blocks.contains_key(&block_id.block_id) {
            return Err(BlockStorageError::BlockAlreadyExists(block_id.block_id));
        }

        let file_path = self.get_block_path(&block_id, false);
        let meta_path = self.get_meta_path(&block_id, false);

        // Create empty block file
        File::create(&file_path)?;
        File::create(&meta_path)?;

        debug!("Created block: {} at {}", block_id.block_id, file_path.display());

        Ok(())
    }

    /// Write data to a block at a specific offset
    pub fn write_block(&self, block_id: &BlockId, offset: u64, data: &[u8]) -> Result<u64> {
        let file_path = self.get_block_path(block_id, false);

        if !file_path.exists() {
            return Err(BlockStorageError::BlockNotFound(block_id.block_id));
        }

        let mut file = OpenOptions::new()
            .write(true)
            .open(&file_path)?;

        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        file.flush()?;

        debug!("Wrote {} bytes to block {} at offset {}", data.len(), block_id.block_id, offset);

        Ok(data.len() as u64)
    }

    /// Read data from a block
    pub fn read_block(&self, block_id: &BlockId, offset: u64, length: u64) -> Result<Bytes> {
        let metadata = self.blocks.get(&block_id.block_id)
            .ok_or(BlockStorageError::BlockNotFound(block_id.block_id))?;

        let mut file = File::open(&metadata.file_path)?;

        file.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; length as usize];
        let bytes_read = file.read(&mut buffer)?;
        buffer.truncate(bytes_read);

        debug!("Read {} bytes from block {} at offset {}", bytes_read, block_id.block_id, offset);

        Ok(Bytes::from(buffer))
    }

    /// Finalize a block (move from tmp to finalized directory)
    pub fn finalize_block(&self, block_id: &BlockId) -> Result<u64> {
        let tmp_path = self.get_block_path(block_id, false);
        let finalized_path = self.get_block_path(block_id, true);

        if !tmp_path.exists() {
            return Err(BlockStorageError::BlockNotFound(block_id.block_id));
        }

        // Get file size
        let num_bytes = fs::metadata(&tmp_path)?.len();

        // Move block file
        fs::rename(&tmp_path, &finalized_path)?;

        // Move metadata file
        let tmp_meta = self.get_meta_path(block_id, false);
        let finalized_meta = self.get_meta_path(block_id, true);
        if tmp_meta.exists() {
            fs::rename(&tmp_meta, &finalized_meta)?;
        }

        // Add to blocks cache
        let metadata = BlockMetadata {
            block_id: *block_id,
            num_bytes,
            file_path: finalized_path.clone(),
            meta_path: finalized_meta,
        };

        self.blocks.insert(block_id.block_id, metadata);

        info!("Finalized block {} ({} bytes) at {}", block_id.block_id, num_bytes, finalized_path.display());

        Ok(num_bytes)
    }

    /// Delete a block
    pub fn delete_block(&self, block_id: &BlockId) -> Result<()> {
        if let Some((_, metadata)) = self.blocks.remove(&block_id.block_id) {
            // Delete block file
            if metadata.file_path.exists() {
                fs::remove_file(&metadata.file_path)?;
            }

            // Delete metadata file
            if metadata.meta_path.exists() {
                fs::remove_file(&metadata.meta_path)?;
            }

            info!("Deleted block {}", block_id.block_id);
        }

        Ok(())
    }

    /// Get all blocks (for block reporting)
    pub fn get_all_blocks(&self) -> Vec<(BlockId, u64)> {
        self.blocks
            .iter()
            .map(|entry| {
                let metadata = entry.value();
                (metadata.block_id, metadata.num_bytes)
            })
            .collect()
    }

    /// Get block metadata
    pub fn get_block_metadata(&self, block_id: u64) -> Option<BlockMetadata> {
        self.blocks.get(&block_id).map(|entry| entry.value().clone())
    }

    /// Get total number of blocks
    pub fn get_block_count(&self) -> usize {
        self.blocks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_block_storage() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();

        // Create block
        let block_id = BlockId::new(12345, 1000);
        storage.create_block(block_id).unwrap();

        // Write data
        let data = b"Hello, HDFS!";
        storage.write_block(&block_id, 0, data).unwrap();

        // Read data
        let read_data = storage.read_block(&block_id, 0, data.len() as u64).unwrap();
        assert_eq!(&read_data[..], data);

        // Finalize
        let size = storage.finalize_block(&block_id).unwrap();
        assert_eq!(size, data.len() as u64);

        // Verify in blocks cache
        assert_eq!(storage.get_block_count(), 1);

        // Delete block
        storage.delete_block(&block_id).unwrap();
        assert_eq!(storage.get_block_count(), 0);
    }
}

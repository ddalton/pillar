use pillar_common::hdfs::block_token::BlockTokenVerifier;
use std::path::{Path, PathBuf};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
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

    /// Sum of `num_bytes` over finalized blocks, so usage reports need no
    /// directory walk.
    total_bytes: AtomicU64,

    /// Finalize/delete events awaiting an incremental block report; `None`
    /// until a NameNode client asks for them.
    events: Mutex<Option<Vec<BlockEvent>>>,

    /// Woken on every event, so the report goes out at once (as HDFS
    /// DataNodes do): a client's complete() waits for it.
    events_notify: tokio::sync::Notify,

    /// Replicas being written: bytes the whole pipeline has acknowledged
    /// (HDFS `bytesAcked`). Readers of a file still being written may read
    /// that far: it is what hflush guarantees.
    visible: DashMap<u64, (BlockId, u64)>,

    /// Block access token keys from the NameNode; every data transfer
    /// checks its token here (a no-op until the NameNode sends keys).
    pub tokens: BlockTokenVerifier,
}

/// A change to the set of finalized replicas, for incremental block reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockEvent {
    Received { block: BlockId, num_bytes: u64 },
    Deleted { block: BlockId },
}

impl BlockStorage {
    pub fn new(finalized_dir: impl AsRef<Path>, tmp_dir: impl AsRef<Path>) -> Result<Self> {
        let finalized_dir = finalized_dir.as_ref().to_path_buf();
        let tmp_dir = tmp_dir.as_ref().to_path_buf();

        let storage = Self {
            finalized_dir,
            tmp_dir,
            blocks: DashMap::new(),
            total_bytes: AtomicU64::new(0),
            events: Mutex::new(None),
            events_notify: tokio::sync::Notify::new(),
            tokens: BlockTokenVerifier::default(),
            visible: DashMap::new(),
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
                            self.total_bytes.fetch_add(num_bytes, Ordering::Relaxed);
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

    /// Append data to an existing block
    pub fn append_block(&self, block_id: &BlockId, data: &[u8]) -> Result<u64> {
        // Check if block exists in finalized or tmp directory
        let tmp_path = self.get_block_path(block_id, false);
        let finalized_path = self.get_block_path(block_id, true);

        let file_path = if tmp_path.exists() {
            tmp_path
        } else if finalized_path.exists() {
            finalized_path
        } else {
            return Err(BlockStorageError::BlockNotFound(block_id.block_id));
        };

        // Open file in append mode
        let mut file = OpenOptions::new()
            .append(true)
            .open(&file_path)?;

        // Write data at the end
        file.write_all(data)?;
        file.flush()?;

        // Get new file size
        let new_size = file.metadata()?.len();

        debug!("Appended {} bytes to block {} (new size: {})", data.len(), block_id.block_id, new_size);

        Ok(new_size)
    }

    /// Read data from a finalized block.
    ///
    /// `length` is clamped to what the block holds past `offset`, so a
    /// caller asking for more than exists gets the remainder (and the
    /// allocation is bounded by the block, not by the request).
    pub fn read_block(&self, block_id: &BlockId, offset: u64, length: u64) -> Result<Bytes> {
        let (mut file, available) = self.open_for_read(block_id, offset, length)?;
        let mut buffer = vec![0u8; available as usize];
        file.read_exact(&mut buffer)?;

        debug!("Read {} bytes from block {} at offset {}", available, block_id.block_id, offset);

        Ok(Bytes::from(buffer))
    }

    /// Open a finalized block positioned at `offset`, returning the file and
    /// how many bytes may be read from there: `min(length, size - offset)`.
    pub fn open_for_read(&self, block_id: &BlockId, offset: u64, length: u64) -> Result<(File, u64)> {
        let path = self.blocks.get(&block_id.block_id)
            .map(|m| m.file_path.clone())
            .ok_or(BlockStorageError::BlockNotFound(block_id.block_id))?;

        let mut file = File::open(&path)?;
        let size = file.metadata()?.len();
        let available = length.min(size.saturating_sub(offset));
        file.seek(SeekFrom::Start(offset.min(size)))?;
        Ok((file, available))
    }

    /// Start receiving a complete block: create (or truncate a stale) temp
    /// file once and return it for sequential writing. Finish with
    /// [`Self::finalize_block`]; on failure call [`Self::abort_write`].
    ///
    /// Refuses a block that is already finalized here, so a duplicate
    /// transfer cannot clobber a good replica.
    pub fn begin_write(&self, block_id: &BlockId) -> Result<File> {
        if self.blocks.contains_key(&block_id.block_id) {
            return Err(BlockStorageError::BlockAlreadyExists(block_id.block_id));
        }
        File::create(self.get_meta_path(block_id, false))?;
        Ok(File::create(self.get_block_path(block_id, false))?)
    }

    /// The temp `.meta` file of a block being written (created by
    /// [`Self::begin_write`]), opened for writing and truncated.
    pub fn open_tmp_meta(&self, block_id: &BlockId) -> Result<File> {
        Ok(OpenOptions::new().write(true).truncate(true).create(true).open(self.get_meta_path(block_id, false))?)
    }

    /// The replica of `block_id` being written here (in tmp), with whatever
    /// generation stamp it has, and its length.
    pub fn find_rbw(&self, block_id: u64) -> Option<(BlockId, u64)> {
        let prefix = format!("blk_{}_", block_id);
        fs::read_dir(&self.tmp_dir).ok()?.flatten().find_map(|e| {
            let name = e.file_name().into_string().ok()?;
            if !name.starts_with(&prefix) || name.ends_with(".meta") {
                return None;
            }
            let id = self.parse_block_filename(&name)?;
            Some((id, e.metadata().ok()?.len()))
        })
    }

    /// What block recovery needs to know of the replica of `block_id` here:
    /// whether it is finalized, its id and its length (on disk, for one
    /// being written).
    pub fn recovery_info(&self, block_id: u64) -> Option<(bool, BlockId, u64)> {
        if let Some(m) = self.blocks.get(&block_id) {
            return Some((true, m.block_id, m.num_bytes));
        }
        self.find_rbw(block_id).map(|(id, len)| (false, id, len))
    }

    /// Every replica being written here (block reports list them as under
    /// construction), with its length on disk.
    pub fn get_rbw_blocks(&self) -> Vec<(BlockId, u64)> {
        let Ok(entries) = fs::read_dir(&self.tmp_dir) else { return Vec::new() };
        entries
            .flatten()
            .filter_map(|e| {
                let name = e.file_name().into_string().ok()?;
                if name.ends_with(".meta") || name.ends_with(".tmp") {
                    return None;
                }
                Some((self.parse_block_filename(&name)?, e.metadata().ok()?.len()))
            })
            .collect()
    }

    /// Give the replica being written of `block_id` the generation stamp
    /// `new_gs` (pipeline recovery), returning its new id and length. A
    /// replica with a newer stamp than `new_gs` is refused: it belongs to a
    /// later pipeline.
    pub fn restamp_rbw(&self, block_id: u64, new_gs: u64) -> Result<(BlockId, u64)> {
        let (old, len) = self.find_rbw(block_id).ok_or(BlockStorageError::BlockNotFound(block_id))?;
        if old.generation_stamp > new_gs {
            return Err(BlockStorageError::BlockAlreadyExists(block_id));
        }
        let new = BlockId::new(block_id, new_gs);
        if old != new {
            fs::rename(self.get_block_path(&old, false), self.get_block_path(&new, false))?;
            let meta = self.get_meta_path(&old, false);
            if meta.exists() {
                fs::rename(meta, self.get_meta_path(&new, false))?;
            }
        }
        Ok((new, len))
    }

    /// The pipeline acknowledged `len` bytes of the replica being written
    /// `block` (never moves backwards for the same stamp).
    pub fn set_visible(&self, block: &BlockId, len: u64) {
        let mut e = self.visible.entry(block.block_id).or_insert((*block, len));
        if e.0 != *block || e.1 < len {
            *e = (*block, len);
        }
    }

    /// Reset the visible length of a reopened replica (recovery cuts it to
    /// the acknowledged length; append starts from the finalized length).
    pub fn reset_visible(&self, block: &BlockId, len: u64) {
        self.visible.insert(block.block_id, (*block, len));
    }

    /// The replica of `block_id` here and how much of it a reader may see:
    /// all of a finalized replica, the acknowledged bytes of one being
    /// written (HDFS `getReplicaVisibleLength`).
    pub fn replica_visible_length(&self, block_id: u64) -> Option<(BlockId, u64)> {
        if let Some(m) = self.blocks.get(&block_id) {
            return Some((m.block_id, m.num_bytes));
        }
        let (id, _) = self.find_rbw(block_id)?;
        let len = self.visible.get(&block_id).filter(|v| v.0 == id).map_or(0, |v| v.1);
        Some((id, len))
    }

    /// Open a replica being written for reading `length` bytes from `offset`.
    pub fn open_rbw_for_read(&self, block_id: &BlockId, offset: u64, length: u64) -> Result<(File, u64)> {
        let mut file = File::open(self.get_block_path(block_id, false))?;
        let size = file.metadata()?.len();
        let available = length.min(size.saturating_sub(offset));
        file.seek(SeekFrom::Start(offset.min(size)))?;
        Ok((file, available))
    }

    /// The `.meta` path of a replica being written.
    pub fn rbw_meta_path(&self, block_id: &BlockId) -> PathBuf {
        self.get_meta_path(block_id, false)
    }

    /// Open a replica being written, data and `.meta`, for reading and
    /// writing in place.
    pub fn open_rbw(&self, block_id: &BlockId) -> Result<(File, File)> {
        let open = |p: PathBuf| OpenOptions::new().read(true).write(true).open(p);
        Ok((open(self.get_block_path(block_id, false))?, open(self.get_meta_path(block_id, false))?))
    }

    /// Reopen a finalized replica for writing (append): it moves back to
    /// tmp under `new_gs` and leaves the finalized set until finalized
    /// again. Returns its new id and length.
    pub fn unfinalize(&self, block_id: &BlockId, new_gs: u64) -> Result<(BlockId, u64)> {
        // Only this exact replica: a different stamp must leave it in place.
        let (_, meta) = self
            .blocks
            .remove_if(&block_id.block_id, |_, m| m.block_id == *block_id)
            .ok_or(BlockStorageError::BlockNotFound(block_id.block_id))?;
        self.total_bytes.fetch_sub(meta.num_bytes, Ordering::Relaxed);
        let new = BlockId::new(block_id.block_id, new_gs);
        fs::rename(&meta.file_path, self.get_block_path(&new, false))?;
        if meta.meta_path.exists() {
            fs::rename(&meta.meta_path, self.get_meta_path(&new, false))?;
        }
        if new != *block_id {
            self.record(BlockEvent::Deleted { block: *block_id });
        }
        Ok((new, meta.num_bytes))
    }

    /// Give a finalized replica of `block_id` the generation stamp `new_gs`
    /// (close recovery: the data was complete, the pipeline broke while
    /// closing). Returns its length.
    pub fn restamp_finalized(&self, block_id: u64, new_gs: u64) -> Result<u64> {
        let (_, mut meta) = self.blocks.remove(&block_id).ok_or(BlockStorageError::BlockNotFound(block_id))?;
        let old = meta.block_id;
        let new = BlockId::new(block_id, new_gs);
        if old != new {
            let file_path = self.get_block_path(&new, true);
            let meta_path = self.get_meta_path(&new, true);
            fs::rename(&meta.file_path, &file_path)?;
            if meta.meta_path.exists() {
                fs::rename(&meta.meta_path, &meta_path)?;
            }
            meta = BlockMetadata { block_id: new, file_path, meta_path, ..meta };
            self.record(BlockEvent::Deleted { block: old });
            self.record(BlockEvent::Received { block: new, num_bytes: meta.num_bytes });
        }
        let len = meta.num_bytes;
        self.blocks.insert(block_id, meta);
        Ok(len)
    }

    /// Delete replicas being written that nobody has touched for
    /// `older_than`: their writer is gone and its lease long recovered.
    /// Returns how many were removed.
    pub fn sweep_stale_rbw(&self, older_than: std::time::Duration) -> usize {
        let Ok(entries) = fs::read_dir(&self.tmp_dir) else { return 0 };
        let mut removed = 0;
        for e in entries.flatten() {
            let stale = e
                .metadata()
                .and_then(|m| m.modified())
                .map(|t| t.elapsed().unwrap_or_default() >= older_than)
                .unwrap_or(false);
            if stale && fs::remove_file(e.path()).is_ok() {
                if !e.file_name().to_string_lossy().ends_with(".meta") {
                    removed += 1;
                }
            }
        }
        if removed > 0 {
            info!("Removed {} abandoned replicas being written from {}", removed, self.tmp_dir.display());
        }
        removed
    }

    /// Discard a partially received block.
    pub fn abort_write(&self, block_id: &BlockId) {
        self.visible.remove_if(&block_id.block_id, |_, v| v.0 == *block_id);
        for path in [self.get_block_path(block_id, false), self.get_meta_path(block_id, false)] {
            if let Err(e) = fs::remove_file(&path) {
                if e.kind() != io::ErrorKind::NotFound {
                    warn!("Failed to remove partial block file {}: {}", path.display(), e);
                }
            }
        }
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

        self.visible.remove(&block_id.block_id);

        // Add to blocks cache
        let metadata = BlockMetadata {
            block_id: *block_id,
            num_bytes,
            file_path: finalized_path.clone(),
            meta_path: finalized_meta,
        };

        if let Some(old) = self.blocks.insert(block_id.block_id, metadata) {
            self.total_bytes.fetch_sub(old.num_bytes, Ordering::Relaxed);
        }
        self.total_bytes.fetch_add(num_bytes, Ordering::Relaxed);
        self.record(BlockEvent::Received { block: *block_id, num_bytes });

        info!("Finalized block {} ({} bytes) at {}", block_id.block_id, num_bytes, finalized_path.display());

        Ok(num_bytes)
    }

    /// Delete a block
    pub fn delete_block(&self, block_id: &BlockId) -> Result<()> {
        if let Some((_, metadata)) = self.blocks.remove(&block_id.block_id) {
            self.total_bytes.fetch_sub(metadata.num_bytes, Ordering::Relaxed);
            self.record(BlockEvent::Deleted { block: metadata.block_id });
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

    /// Bytes held in finalized blocks.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Start recording finalize/delete events for incremental block reports.
    pub fn enable_events(&self) {
        let mut ev = self.events.lock().unwrap();
        if ev.is_none() {
            *ev = Some(Vec::new());
        }
    }

    /// Take the events recorded since the last call.
    pub fn take_events(&self) -> Vec<BlockEvent> {
        self.events.lock().unwrap().as_mut().map(std::mem::take).unwrap_or_default()
    }

    /// Put back events that could not be reported, ahead of newer ones.
    pub fn requeue_events(&self, mut older: Vec<BlockEvent>) {
        if let Some(ev) = self.events.lock().unwrap().as_mut() {
            older.append(ev);
            *ev = older;
        }
    }

    fn record(&self, event: BlockEvent) {
        if let Some(ev) = self.events.lock().unwrap().as_mut() {
            ev.push(event);
            self.events_notify.notify_one();
        }
    }

    /// Resolves when an event is recorded (one may already be pending).
    pub async fn event_recorded(&self) {
        self.events_notify.notified().await
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
    use std::os::unix::fs::FileExt;

    fn new_storage() -> (TempDir, BlockStorage) {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");
        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();
        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        (temp_dir, storage)
    }

    fn write_rbw(storage: &BlockStorage, id: BlockId, data: &[u8]) {
        storage.begin_write(&id).unwrap().write_all(data).unwrap();
    }

    #[test]
    fn a_replica_being_written_is_restamped_for_recovery_but_not_backwards() {
        let (_d, storage) = new_storage();
        assert!(storage.find_rbw(7).is_none());
        write_rbw(&storage, BlockId::new(7, 100), b"0123456789");
        assert_eq!(storage.find_rbw(7), Some((BlockId::new(7, 100), 10)));

        let (id, len) = storage.restamp_rbw(7, 105).unwrap();
        assert_eq!((id, len), (BlockId::new(7, 105), 10));
        assert_eq!(storage.find_rbw(7), Some((BlockId::new(7, 105), 10)));
        // A stale pipeline (older stamp) cannot take it back.
        assert!(matches!(storage.restamp_rbw(7, 101), Err(BlockStorageError::BlockAlreadyExists(7))));
        let (data, _) = storage.open_rbw(&id).unwrap();
        data.write_all_at(b"AB", 10).unwrap();
        assert_eq!(storage.finalize_block(&id).unwrap(), 12);
        assert!(storage.find_rbw(7).is_none());
        assert_eq!(storage.get_block_metadata(7).unwrap().block_id, id);
    }

    #[test]
    fn a_finalized_replica_reopens_for_append_and_restamps_for_close_recovery() {
        let (_d, storage) = new_storage();
        storage.enable_events();
        let old = BlockId::new(9, 200);
        write_rbw(&storage, old, b"hello");
        storage.finalize_block(&old).unwrap();
        let _ = storage.take_events();

        // Append: leaves the finalized set (and usage) until finalized again.
        let (id, len) = storage.unfinalize(&old, 201).unwrap();
        assert_eq!((id, len), (BlockId::new(9, 201), 5));
        assert!(storage.get_block_metadata(9).is_none());
        assert_eq!(storage.total_bytes(), 0);
        assert_eq!(storage.take_events(), vec![BlockEvent::Deleted { block: old }]);
        storage.open_rbw(&id).unwrap().0.write_all_at(b" world", 5).unwrap();
        assert_eq!(storage.finalize_block(&id).unwrap(), 11);
        // Wrong stamp: not this replica.
        assert!(storage.unfinalize(&old, 202).is_err());

        // Close recovery of a finalized replica: renamed under the new stamp.
        assert_eq!(storage.restamp_finalized(9, 210).unwrap(), 11);
        let m = storage.get_block_metadata(9).unwrap();
        assert_eq!(m.block_id, BlockId::new(9, 210));
        assert!(m.file_path.exists() && !storage.get_block_path(&id, true).exists());
        let ev = storage.take_events();
        assert!(ev.contains(&BlockEvent::Received { block: BlockId::new(9, 210), num_bytes: 11 }));
    }

    #[test]
    fn abandoned_replicas_being_written_are_swept() {
        let (_d, storage) = new_storage();
        write_rbw(&storage, BlockId::new(1, 1), b"x");
        assert_eq!(storage.sweep_stale_rbw(std::time::Duration::from_secs(3600)), 0);
        assert_eq!(storage.sweep_stale_rbw(std::time::Duration::ZERO), 1);
        assert!(storage.find_rbw(1).is_none());
    }

    #[test]
    fn a_write_session_writes_sequentially_and_finalizes() {
        let (_d, storage) = new_storage();
        let b = BlockId::new(1, 1);
        let mut f = storage.begin_write(&b).unwrap();
        f.write_all(b"hello ").unwrap();
        f.write_all(b"world").unwrap();
        drop(f);
        assert_eq!(storage.finalize_block(&b).unwrap(), 11);
        assert_eq!(&storage.read_block(&b, 0, 11).unwrap()[..], b"hello world");
        assert!(matches!(storage.begin_write(&b), Err(BlockStorageError::BlockAlreadyExists(1))));
    }

    #[test]
    fn an_aborted_write_leaves_nothing() {
        let (d, storage) = new_storage();
        let b = BlockId::new(2, 1);
        storage.begin_write(&b).unwrap().write_all(b"partial").unwrap();
        storage.abort_write(&b);
        assert_eq!(fs::read_dir(d.path().join("tmp")).unwrap().count(), 0);
        assert!(storage.finalize_block(&b).is_err());
    }

    #[test]
    fn usage_and_events_track_finalize_and_delete() {
        let (_d, storage) = new_storage();
        let (a, b) = (BlockId::new(10, 1), BlockId::new(11, 1));
        storage.begin_write(&a).unwrap().write_all(&[0; 100]).unwrap();
        storage.finalize_block(&a).unwrap();
        assert!(storage.take_events().is_empty(), "events are off until enabled");

        storage.enable_events();
        storage.begin_write(&b).unwrap().write_all(&[0; 50]).unwrap();
        storage.finalize_block(&b).unwrap();
        storage.delete_block(&a).unwrap();
        assert_eq!(storage.total_bytes(), 50);
        assert_eq!(
            storage.take_events(),
            vec![BlockEvent::Received { block: b, num_bytes: 50 }, BlockEvent::Deleted { block: a }]
        );
        assert!(storage.take_events().is_empty());
    }

    #[test]
    fn reads_are_clamped_to_the_block() {
        let (_d, storage) = new_storage();
        let b = BlockId::new(3, 1);
        storage.begin_write(&b).unwrap().write_all(b"0123456789").unwrap();
        storage.finalize_block(&b).unwrap();
        assert_eq!(&storage.read_block(&b, 4, 100).unwrap()[..], b"456789");
        assert_eq!(&storage.read_block(&b, 0, u64::MAX).unwrap()[..], b"0123456789");
        assert!(storage.read_block(&b, 50, 10).unwrap().is_empty());
    }

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

        // Finalize (blocks must be finalized before reading)
        let size = storage.finalize_block(&block_id).unwrap();
        assert_eq!(size, data.len() as u64);

        // Read data (after finalization)
        let read_data = storage.read_block(&block_id, 0, data.len() as u64).unwrap();
        assert_eq!(&read_data[..], data);

        // Verify in blocks cache
        assert_eq!(storage.get_block_count(), 1);

        // Delete block
        storage.delete_block(&block_id).unwrap();
        assert_eq!(storage.get_block_count(), 0);
    }

    #[test]
    fn test_seek_to_beginning() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(1001, 1);

        storage.create_block(block_id).unwrap();
        let data = b"0123456789ABCDEF";
        storage.write_block(&block_id, 0, data).unwrap();
        storage.finalize_block(&block_id).unwrap();

        // Seek to offset 0 (beginning)
        let read_data = storage.read_block(&block_id, 0, 5).unwrap();
        assert_eq!(&read_data[..], b"01234");
    }

    #[test]
    fn test_seek_to_middle() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(1002, 1);

        storage.create_block(block_id).unwrap();
        let data = b"0123456789ABCDEF";
        storage.write_block(&block_id, 0, data).unwrap();
        storage.finalize_block(&block_id).unwrap();

        // Seek to middle (offset 5)
        let read_data = storage.read_block(&block_id, 5, 5).unwrap();
        assert_eq!(&read_data[..], b"56789");
    }

    #[test]
    fn test_seek_to_end() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(1003, 1);

        storage.create_block(block_id).unwrap();
        let data = b"0123456789ABCDEF";
        storage.write_block(&block_id, 0, data).unwrap();
        storage.finalize_block(&block_id).unwrap();

        // Seek to near end
        let read_data = storage.read_block(&block_id, 12, 4).unwrap();
        assert_eq!(&read_data[..], b"CDEF");
    }

    #[test]
    fn test_seek_beyond_end() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(1004, 1);

        storage.create_block(block_id).unwrap();
        let data = b"0123456789ABCDEF";
        storage.write_block(&block_id, 0, data).unwrap();
        storage.finalize_block(&block_id).unwrap();

        // Seek beyond file end - should return less data than requested
        let read_data = storage.read_block(&block_id, 14, 10).unwrap();
        assert_eq!(&read_data[..], b"EF"); // Only 2 bytes available
    }

    #[test]
    fn test_seek_backwards() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(1005, 1);

        storage.create_block(block_id).unwrap();
        let data = b"0123456789ABCDEF";
        storage.write_block(&block_id, 0, data).unwrap();
        storage.finalize_block(&block_id).unwrap();

        // First seek to offset 10
        let read_data1 = storage.read_block(&block_id, 10, 3).unwrap();
        assert_eq!(&read_data1[..], b"ABC");

        // Then seek backwards to offset 2
        let read_data2 = storage.read_block(&block_id, 2, 4).unwrap();
        assert_eq!(&read_data2[..], b"2345");
    }

    #[test]
    fn test_seek_and_read_specific_bytes() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(1006, 1);

        storage.create_block(block_id).unwrap();
        let data = b"The quick brown fox jumps over the lazy dog";
        storage.write_block(&block_id, 0, data).unwrap();
        storage.finalize_block(&block_id).unwrap();

        // Read "quick" at offset 4
        let read_data = storage.read_block(&block_id, 4, 5).unwrap();
        assert_eq!(&read_data[..], b"quick");

        // Read "fox" at offset 16
        let read_data = storage.read_block(&block_id, 16, 3).unwrap();
        assert_eq!(&read_data[..], b"fox");

        // Read "lazy" at offset 35
        let read_data = storage.read_block(&block_id, 35, 4).unwrap();
        assert_eq!(&read_data[..], b"lazy");
    }

    #[test]
    fn test_multiple_seeks() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(1007, 1);

        storage.create_block(block_id).unwrap();
        let data = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        storage.write_block(&block_id, 0, data).unwrap();
        storage.finalize_block(&block_id).unwrap();

        // Multiple sequential seeks
        let positions: Vec<(u64, u64, &[u8])> = vec![
            (0, 3, b"ABC"),
            (5, 3, b"FGH"),
            (10, 3, b"KLM"),
            (20, 3, b"UVW"),
            (25, 1, b"Z"),
        ];

        for (offset, length, expected) in positions {
            let read_data = storage.read_block(&block_id, offset, length).unwrap();
            assert_eq!(&read_data[..], expected);
        }
    }

    #[test]
    fn test_append_to_block() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(2001, 1);

        // Create and write initial data
        storage.create_block(block_id).unwrap();
        storage.write_block(&block_id, 0, b"Hello, ").unwrap();

        // Append more data
        let new_size = storage.append_block(&block_id, b"World!").unwrap();
        assert_eq!(new_size, 13); // "Hello, World!" is 13 bytes

        // Finalize and read to verify
        storage.finalize_block(&block_id).unwrap();
        let read_data = storage.read_block(&block_id, 0, 13).unwrap();
        assert_eq!(&read_data[..], b"Hello, World!");
    }

    #[test]
    fn test_append_to_finalized_block() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(2002, 1);

        // Create, write, and finalize
        storage.create_block(block_id).unwrap();
        storage.write_block(&block_id, 0, b"Initial").unwrap();
        storage.finalize_block(&block_id).unwrap();

        // Append to finalized block
        let new_size = storage.append_block(&block_id, b" Data").unwrap();
        assert_eq!(new_size, 12); // "Initial Data" is 12 bytes

        // Read to verify
        let read_data = storage.read_block(&block_id, 0, 12).unwrap();
        assert_eq!(&read_data[..], b"Initial Data");
    }

    #[test]
    fn test_multiple_appends() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(2003, 1);

        storage.create_block(block_id).unwrap();
        storage.write_block(&block_id, 0, b"A").unwrap();

        // Multiple appends
        storage.append_block(&block_id, b"B").unwrap();
        storage.append_block(&block_id, b"C").unwrap();
        storage.append_block(&block_id, b"D").unwrap();
        let final_size = storage.append_block(&block_id, b"E").unwrap();

        assert_eq!(final_size, 5); // "ABCDE"

        // Finalize and verify
        storage.finalize_block(&block_id).unwrap();
        let read_data = storage.read_block(&block_id, 0, 5).unwrap();
        assert_eq!(&read_data[..], b"ABCDE");
    }

    #[test]
    fn test_append_large_data() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(2004, 1);

        storage.create_block(block_id).unwrap();

        // Write initial 1KB
        let initial_data = vec![0xAA; 1024];
        storage.write_block(&block_id, 0, &initial_data).unwrap();

        // Append 2KB
        let append_data = vec![0xBB; 2048];
        let new_size = storage.append_block(&block_id, &append_data).unwrap();

        assert_eq!(new_size, 3072); // 1KB + 2KB

        // Verify first KB is 0xAA
        storage.finalize_block(&block_id).unwrap();
        let read_first = storage.read_block(&block_id, 0, 1024).unwrap();
        assert!(read_first.iter().all(|&b| b == 0xAA));

        // Verify last 2KB is 0xBB
        let read_last = storage.read_block(&block_id, 1024, 2048).unwrap();
        assert!(read_last.iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn test_append_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(2005, 1);

        storage.create_block(block_id).unwrap();
        storage.write_block(&block_id, 0, b"Line 1\n").unwrap();
        storage.append_block(&block_id, b"Line 2\n").unwrap();
        storage.append_block(&block_id, b"Line 3\n").unwrap();

        storage.finalize_block(&block_id).unwrap();

        // Read entire content
        let read_data = storage.read_block(&block_id, 0, 100).unwrap();
        assert_eq!(&read_data[..], b"Line 1\nLine 2\nLine 3\n");

        // Read specific line
        let read_line2 = storage.read_block(&block_id, 7, 7).unwrap();
        assert_eq!(&read_line2[..], b"Line 2\n");
    }

    #[test]
    fn test_append_to_nonexistent_block() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(9999, 1);

        // Try to append to non-existent block
        let result = storage.append_block(&block_id, b"data");
        assert!(result.is_err());
        match result.unwrap_err() {
            BlockStorageError::BlockNotFound(id) => assert_eq!(id, 9999),
            _ => panic!("Expected BlockNotFound error"),
        }
    }

    #[test]
    fn test_append_empty_data() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(2006, 1);

        storage.create_block(block_id).unwrap();
        storage.write_block(&block_id, 0, b"Data").unwrap();

        // Append empty data (should succeed but not change size)
        let new_size = storage.append_block(&block_id, b"").unwrap();
        assert_eq!(new_size, 4);

        storage.finalize_block(&block_id).unwrap();
        let read_data = storage.read_block(&block_id, 0, 4).unwrap();
        assert_eq!(&read_data[..], b"Data");
    }

    // ========================================================================
    // Block Corruption Detection Tests
    // ========================================================================

    #[test]
    fn test_detect_corrupted_block() {
        use pillar_common::hdfs::checksums::{compute_checksum, verify_checksum};

        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(3001, 1);

        // Write original data
        let original_data = b"Important data that must not be corrupted";
        storage.create_block(block_id).unwrap();
        storage.write_block(&block_id, 0, original_data).unwrap();
        storage.finalize_block(&block_id).unwrap();

        // Compute checksum of original data
        let original_checksum = compute_checksum(original_data);

        // Read data and verify it matches
        let read_data = storage.read_block(&block_id, 0, original_data.len() as u64).unwrap();
        assert!(verify_checksum(&read_data, &original_checksum));

        // Simulate corruption by tampering with the block file
        let block_path = storage.get_block_path(&block_id, true);
        let mut corrupted_data = original_data.to_vec();
        corrupted_data[10] = b'X'; // Flip a byte
        fs::write(&block_path, &corrupted_data).unwrap();

        // Read corrupted data
        let read_corrupted = storage.read_block(&block_id, 0, original_data.len() as u64).unwrap();

        // Checksum validation should fail
        assert!(!verify_checksum(&read_corrupted, &original_checksum),
                "Corrupted block should fail checksum validation");
    }

    #[test]
    fn test_checksum_validation_on_read() {
        use pillar_common::hdfs::checksums::{compute_checksum, verify_checksum};

        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(3002, 1);

        // Write data and compute checksum
        let data = b"Data that should maintain integrity";
        storage.create_block(block_id).unwrap();
        storage.write_block(&block_id, 0, data).unwrap();
        storage.finalize_block(&block_id).unwrap();

        let expected_checksum = compute_checksum(data);

        // Read data multiple times
        for _ in 0..5 {
            let read_data = storage.read_block(&block_id, 0, data.len() as u64).unwrap();

            // Validate checksum on each read
            assert!(verify_checksum(&read_data, &expected_checksum),
                    "Block data should pass checksum validation");
            assert_eq!(&read_data[..], data);
        }
    }

    #[test]
    fn test_partial_block_corruption() {
        use pillar_common::hdfs::checksums::{compute_checksum, verify_checksum};

        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();
        let block_id = BlockId::new(3003, 1);

        // Write large block
        let large_data: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
        storage.create_block(block_id).unwrap();
        storage.write_block(&block_id, 0, &large_data).unwrap();
        storage.finalize_block(&block_id).unwrap();

        let original_checksum = compute_checksum(&large_data);

        // Corrupt middle of block
        let block_path = storage.get_block_path(&block_id, true);
        let mut corrupted_data = large_data.clone();
        corrupted_data[512] = 0xFF; // Flip byte in middle
        fs::write(&block_path, &corrupted_data).unwrap();

        // Read full block
        let read_data = storage.read_block(&block_id, 0, 1024).unwrap();

        // Should detect corruption even in middle of large block
        assert!(!verify_checksum(&read_data, &original_checksum),
                "Partial corruption should be detected");
    }

    #[test]
    fn test_multiple_blocks_corruption_isolation() {
        use pillar_common::hdfs::checksums::{compute_checksum, verify_checksum};

        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let storage = BlockStorage::new(&finalized_dir, &tmp_dir).unwrap();

        // Create multiple blocks
        let block1 = BlockId::new(3004, 1);
        let block2 = BlockId::new(3005, 1);
        let block3 = BlockId::new(3006, 1);

        let data1 = b"Block 1 data";
        let data2 = b"Block 2 data";
        let data3 = b"Block 3 data";

        for (block_id, data) in [(block1, data1), (block2, data2), (block3, data3)] {
            storage.create_block(block_id).unwrap();
            storage.write_block(&block_id, 0, data).unwrap();
            storage.finalize_block(&block_id).unwrap();
        }

        let checksum1 = compute_checksum(data1);
        let checksum2 = compute_checksum(data2);
        let checksum3 = compute_checksum(data3);

        // Corrupt only block 2
        let block2_path = storage.get_block_path(&block2, true);
        fs::write(&block2_path, b"Corrupted!!!").unwrap();

        // Verify block 1 is still good
        let read1 = storage.read_block(&block1, 0, data1.len() as u64).unwrap();
        assert!(verify_checksum(&read1, &checksum1), "Block 1 should be intact");

        // Verify block 2 is corrupted
        let read2 = storage.read_block(&block2, 0, data2.len() as u64).unwrap();
        assert!(!verify_checksum(&read2, &checksum2), "Block 2 should be corrupted");

        // Verify block 3 is still good
        let read3 = storage.read_block(&block3, 0, data3.len() as u64).unwrap();
        assert!(verify_checksum(&read3, &checksum3), "Block 3 should be intact");
    }
}

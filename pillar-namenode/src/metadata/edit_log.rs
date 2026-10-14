//! Durable namespace: an HDFS-style edit log plus checkpoint images.
//!
//! Every namespace mutation runs inside an [`EditTxn`]. The transaction
//! holds one writer lock while the in-memory change is made and its
//! [`EditOp`] is appended to a buffer, so the log order is the order in
//! which changes became visible. [`EditTxn::commit`] releases the lock and
//! then waits until the buffer is on disk; concurrent commits share one
//! fsync (group commit, as in HDFS `FSEditLog.logSync`). A client is only
//! answered once its change is durable. If a write or fsync fails, the log
//! refuses further transactions: memory and disk may have diverged.
//!
//! On disk, in `<name_dir>/current`:
//! * `VERSION`: the namespace's [`StorageIdentity`], made when the
//!   directory is formatted; DataNodes refuse a NameNode whose identity
//!   differs from the one on their volumes;
//! * `fsimage_<txid>`: the whole namespace as of transaction `txid`;
//! * `edits_<txid>`: the transactions from `txid` on, one segment per
//!   checkpoint.
//!
//! Both files are a magic header and a sequence of frames,
//! `len: u32 LE | crc32c(payload): u32 LE | payload` (postcard). Startup
//! loads the newest readable image, replays the edits after it, and cuts a
//! torn final frame (a crash mid-write; that transaction was never
//! acknowledged). A bad frame anywhere else stops startup.
//!
//! Block locations are not persisted: as in HDFS, they are rebuilt from
//! DataNode block reports.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use pillar_common::hdfs::storage_identity::StorageIdentity;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, MutexGuard};
use tracing::{info, warn};

use super::block_manager::BlockManager;
use super::namespace::{BlockInfo, FsPermission, INode, Namespace, UnderConstruction};

const EDITS_MAGIC: &[u8; 8] = b"PLRNEDT1";
const IMAGE_MAGIC: &[u8; 8] = b"PLRNIMG1";
/// Largest frame accepted on read; a bigger length is a torn or corrupt frame.
const MAX_FRAME: usize = 256 << 20;
/// Images kept on disk; older ones, and the edits they cover, are removed.
const KEEP_IMAGES: usize = 2;

/// One inode as stored in an image (and in [`EditOp::AddInode`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InodeRecord {
    pub id: u64,
    /// Parent inode id; 0 for the root (and unused in `AddInode`).
    pub parent: u64,
    pub name: String,
    pub is_dir: bool,
    pub permission: u16,
    pub owner: String,
    pub group: String,
    pub mtime: u64,
    pub atime: u64,
    pub replication: u16,
    pub block_size: u64,
    pub blocks: Vec<BlockInfo>,
}

impl InodeRecord {
    pub fn from_inode(inode: &INode, parent: u64) -> Self {
        Self {
            id: inode.id,
            parent,
            name: inode.name.clone(),
            is_dir: inode.is_dir,
            permission: inode.permissions.mode,
            owner: inode.owner.clone(),
            group: inode.group.clone(),
            mtime: inode.mtime,
            atime: inode.atime,
            replication: inode.replication,
            block_size: inode.block_size,
            blocks: inode.blocks.clone().unwrap_or_default(),
        }
    }

    pub fn into_inode(self) -> INode {
        let mut inode = if self.is_dir {
            INode::new_dir(self.id, self.name, self.owner, self.group)
        } else {
            let mut f = INode::new_file(self.id, self.name, self.owner, self.group, self.replication, self.block_size);
            f.blocks = Some(self.blocks);
            f
        };
        inode.permissions = FsPermission::new(self.permission);
        inode.mtime = self.mtime;
        inode.atime = self.atime;
        inode.replication = self.replication;
        inode.block_size = self.block_size;
        inode
    }
}

/// A logged namespace change. Paths are resolved at replay time, which is
/// sound because ops are logged in the order they were applied.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EditOp {
    /// mkdir or create, with the inode id and attributes it was given.
    AddInode { path: String, inode: InodeRecord },
    /// Recursive delete (a non-recursive delete that succeeded is the same).
    Delete { path: String },
    /// rename; with `overwrite`, an existing destination is deleted first.
    Rename { src: String, dst: String, overwrite: bool },
    AddBlock { inode: u64, block: BlockInfo },
    /// The client reported a block's final length.
    CommitBlock { inode: u64, block_id: u64, num_bytes: u64 },
    UpdateGenerationStamp { inode: u64, block_id: u64, generation_stamp: u64 },
    /// Replace a file's block list (abandonBlock, truncate).
    SetBlocks { inode: u64, blocks: Vec<BlockInfo> },
    /// Move the sources' blocks to the end of `target`, then delete them.
    Concat { target: String, sources: Vec<String> },
    /// setPermission / setOwner / setTimes / setReplication: the fields
    /// that are `Some` change. (Appended last: postcard encodes the variant
    /// index, so existing logs keep decoding.)
    SetAttributes { path: String, attrs: InodeAttributes },
    /// The file is open for writing by `holder` (create, append).
    Open { inode: u64, holder: String, blocks_at_open: u32 },
    /// The file is closed: complete(), or lease recovery.
    Close { inode: u64 },
}

/// Open files, after the inode records in an image (absent in images
/// written before files had an open state).
type OpenFiles = Vec<(u64, UnderConstruction)>;

/// Attribute changes carried by [`EditOp::SetAttributes`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct InodeAttributes {
    pub permission: Option<u16>,
    pub owner: Option<String>,
    pub group: Option<String>,
    pub mtime: Option<u64>,
    pub atime: Option<u64>,
    pub replication: Option<u16>,
}

impl InodeAttributes {
    /// Apply to `inode` (replication only to files).
    pub fn apply(&self, inode: &mut INode) {
        if let Some(p) = self.permission {
            inode.permissions = FsPermission::new(p);
        }
        if let Some(o) = &self.owner {
            inode.owner = o.clone();
        }
        if let Some(g) = &self.group {
            inode.group = g.clone();
        }
        if let Some(t) = self.mtime {
            inode.mtime = t;
        }
        if let Some(t) = self.atime {
            inode.atime = t;
        }
        if let (Some(r), false) = (self.replication, inode.is_dir) {
            inode.replication = r;
        }
    }
}

/// Id allocators, persisted so ids are never reused after a restart (a
/// DataNode may still hold a replica of a deleted block).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counters {
    pub next_inode_id: u64,
    pub next_block_id: u64,
    pub next_generation_stamp: u64,
}

impl Counters {
    pub fn of(namespace: &Namespace, blocks: &BlockManager) -> Self {
        Self {
            next_inode_id: namespace.next_inode_id(),
            next_block_id: blocks.next_block_id(),
            next_generation_stamp: blocks.next_generation_stamp(),
        }
    }

    fn observe_block(&mut self, b: &BlockInfo) {
        self.next_block_id = self.next_block_id.max(b.block_id + 1);
        self.next_generation_stamp = self.next_generation_stamp.max(b.generation_stamp + 1);
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ImageHeader {
    txid: u64,
    counters: Counters,
    inodes: u64,
}

/// A new namespace identity (HDFS `NNStorage.newClusterID` and friends):
/// `cluster_id` if given, else a random `CID-<uuid>`; a random positive
/// namespace id; a block pool id derived from both.
pub fn new_identity(cluster_id: Option<&str>) -> StorageIdentity {
    let namespace_id = rand::random::<u32>() % (i32::MAX as u32) + 1;
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    StorageIdentity {
        cluster_id: cluster_id.map(str::to_string).unwrap_or_else(|| format!("CID-{}", uuid::Uuid::new_v4())),
        namespace_id,
        block_pool_id: format!("BP-{}-pillar-{}", namespace_id, millis),
    }
}

/// What [`EditLog::open`] recovered.
pub struct Recovered {
    pub identity: StorageIdentity,
    pub namespace: Namespace,
    pub counters: Counters,
    /// Last transaction applied.
    pub txid: u64,
    /// Edits replayed on top of the image.
    pub replayed: u64,
}

impl Recovered {
    /// Rebuild the block manager's file-to-block map and counters from the
    /// recovered namespace.
    pub async fn restore_blocks(&self, blocks: &BlockManager) {
        for r in self.namespace.records().await {
            if !r.is_dir {
                blocks.restore_file(r.id, &r.blocks);
            }
        }
        blocks.restore_counters(self.counters.next_block_id, self.counters.next_generation_stamp);
    }
}

struct Pending {
    buf: Vec<u8>,
    /// Last transaction id handed out.
    last_txid: u64,
}

struct Disk {
    dir: PathBuf,
    segment: Mutex<Arc<File>>,
    last_checkpoint_txid: AtomicU64,
    checkpointing: Mutex<()>,
    /// Held for the NameNode's lifetime: one NameNode per name directory.
    _lock: File,
}

/// The NameNode edit log. [`EditLog::in_memory`] keeps the transaction
/// discipline without writing anything (tests, or no name directory).
pub struct EditLog {
    writer: Mutex<()>,
    pending: StdMutex<Pending>,
    synced_txid: AtomicU64,
    failed: AtomicBool,
    disk: Option<Disk>,
}

/// A namespace transaction: see the module docs.
pub struct EditTxn<'a> {
    log: &'a EditLog,
    guard: MutexGuard<'a, ()>,
    last: Option<u64>,
}

impl EditTxn<'_> {
    /// Append `op`; it becomes durable at [`EditTxn::commit`].
    pub fn log(&mut self, op: EditOp) {
        let mut p = self.log.pending.lock().unwrap();
        p.last_txid += 1;
        let txid = p.last_txid;
        if self.log.disk.is_some() {
            let payload = postcard::to_stdvec(&(txid, &op)).expect("edit ops serialize");
            put_frame(&mut p.buf, &payload);
        }
        self.last = Some(txid);
    }

    /// Release the writer lock, then wait until this transaction's edits
    /// are on disk.
    pub async fn commit(self) -> io::Result<()> {
        let EditTxn { log, guard, last } = self;
        drop(guard);
        match last {
            Some(txid) => log.sync(txid).await,
            None => Ok(()),
        }
    }
}

impl EditLog {
    pub fn in_memory() -> Self {
        Self::new(0, None)
    }

    fn new(txid: u64, disk: Option<Disk>) -> Self {
        Self {
            writer: Mutex::new(()),
            pending: StdMutex::new(Pending { buf: Vec::new(), last_txid: txid }),
            synced_txid: AtomicU64::new(txid),
            failed: AtomicBool::new(false),
            disk,
        }
    }

    pub fn is_durable(&self) -> bool {
        self.disk.is_some()
    }

    /// Start a transaction (takes the writer lock).
    pub async fn begin(&self) -> io::Result<EditTxn<'_>> {
        let guard = self.writer.lock().await;
        if self.failed.load(Ordering::Acquire) {
            return Err(io::Error::other("edit log failed earlier; namespace is read-only"));
        }
        Ok(EditTxn { log: self, guard, last: None })
    }

    /// Last transaction id handed out.
    pub fn last_txid(&self) -> u64 {
        self.pending.lock().unwrap().last_txid
    }

    /// Transactions since the last checkpoint.
    pub fn txns_since_checkpoint(&self) -> u64 {
        match &self.disk {
            Some(d) => self.last_txid().saturating_sub(d.last_checkpoint_txid.load(Ordering::Acquire)),
            None => 0,
        }
    }

    /// Make every edit up to `txid` durable. Whoever holds the segment lock
    /// writes everything buffered so far, so waiters behind it usually find
    /// their edits already synced.
    async fn sync(&self, txid: u64) -> io::Result<()> {
        let Some(disk) = &self.disk else { return Ok(()) };
        if self.synced_txid.load(Ordering::Acquire) >= txid {
            return Ok(());
        }
        let file = disk.segment.lock().await;
        if self.synced_txid.load(Ordering::Acquire) >= txid {
            return Ok(());
        }
        if self.failed.load(Ordering::Acquire) {
            return Err(io::Error::other("edit log failed earlier"));
        }
        let (buf, last) = {
            let mut p = self.pending.lock().unwrap();
            (std::mem::take(&mut p.buf), p.last_txid)
        };
        let f = file.clone();
        let res = tokio::task::spawn_blocking(move || {
            (&*f).write_all(&buf)?;
            f.sync_data()
        })
        .await
        .map_err(io::Error::other)
        .and_then(|r| r);
        if let Err(e) = res {
            self.failed.store(true, Ordering::Release);
            tracing::error!("Edit log write failed; refusing further namespace changes: {}", e);
            return Err(e);
        }
        self.synced_txid.fetch_max(last, Ordering::AcqRel);
        Ok(())
    }

    /// Write an image of the current namespace and start a new edits
    /// segment. Mutations wait while the namespace is copied; the image is
    /// written after they resume.
    pub async fn checkpoint(&self, namespace: &Namespace, blocks: &BlockManager) -> io::Result<()> {
        let Some(disk) = &self.disk else { return Ok(()) };
        let _one = disk.checkpointing.lock().await;
        let (txid, records, open, counters) = {
            let _writer = self.writer.lock().await;
            let txid = self.last_txid();
            if txid == disk.last_checkpoint_txid.load(Ordering::Acquire) {
                return Ok(());
            }
            self.sync(txid).await?;
            let next = open_segment(&disk.dir, txid + 1)?;
            *disk.segment.lock().await = Arc::new(next);
            (txid, namespace.records().await, namespace.open_files().await, Counters::of(namespace, blocks))
        };
        let dir = disk.dir.clone();
        tokio::task::spawn_blocking(move || {
            write_image(&dir, txid, counters, &records, &open)?;
            purge(&dir)
        })
        .await
        .map_err(io::Error::other)??;
        disk.last_checkpoint_txid.store(txid, Ordering::Release);
        info!("Checkpoint written at transaction {}", txid);
        Ok(())
    }

    /// Open (or format) the name directory, recover the namespace, and
    /// return a log ready for new transactions.
    pub async fn open(name_dir: &Path) -> io::Result<(Self, Recovered)> {
        Self::open_with(name_dir, None).await
    }

    /// [`EditLog::open`]; `cluster_id` names the cluster when formatting,
    /// and must match the directory's when it is already formatted.
    pub async fn open_with(name_dir: &Path, cluster_id: Option<&str>) -> io::Result<(Self, Recovered)> {
        let dir = name_dir.join("current");
        fs::create_dir_all(&dir)?;
        let lock = File::create(name_dir.join("in_use.lock"))?;
        lock.try_lock().map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("{} is in use by another NameNode", name_dir.display()),
            )
        })?;

        let version = dir.join("VERSION");
        let mut identity = StorageIdentity::read(&version)?;
        let mut images = list(&dir, "fsimage_")?;
        if images.is_empty() {
            if !list(&dir, "edits_")?.is_empty() {
                return Err(invalid(format!("{}: edits but no fsimage", dir.display())));
            }
            let id = identity.take().unwrap_or_else(|| new_identity(cluster_id));
            id.write(&version, "NAME_NODE")?;
            info!("Formatting empty name directory {} ({})", name_dir.display(), id);
            identity = Some(id);
            let ns = Namespace::new();
            write_image(&dir, 0, Counters::of(&ns, &BlockManager::new()), &ns.records().await, &[])?;
            images = list(&dir, "fsimage_")?;
        }
        let identity = match identity {
            Some(id) => id,
            None => {
                // Formatted before identities existed.
                let id = new_identity(cluster_id);
                warn!("{} has no VERSION; assigning {}", dir.display(), id);
                id.write(&version, "NAME_NODE")?;
                id
            }
        };
        if let Some(want) = cluster_id {
            if want != identity.cluster_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{} belongs to cluster {}, not {}", name_dir.display(), identity.cluster_id, want),
                ));
            }
        }

        // Newest readable image (an older one is kept for this case).
        let mut loaded = None;
        for (txid, path) in images.iter().rev() {
            match read_image(path) {
                Ok(img) => {
                    loaded = Some(img);
                    break;
                }
                Err(e) => warn!("Skipping unreadable image {} (txid {}): {}", path.display(), txid, e),
            }
        }
        let (header, records, open) = loaded.ok_or_else(|| invalid(format!("{}: no readable fsimage", dir.display())))?;
        let namespace = Namespace::from_records(records, open, header.counters.next_inode_id).map_err(invalid)?;
        let mut counters = header.counters;
        let mut txid = header.txid;
        let image_txid = txid;
        info!("Loaded fsimage at transaction {} ({} inodes)", txid, namespace.inode_count());

        let segments = list(&dir, "edits_")?;
        let mut replayed = 0u64;
        for (i, (_, path)) in segments.iter().enumerate() {
            let last_segment = i + 1 == segments.len();
            for (offset, frame) in read_frames(path, EDITS_MAGIC)? {
                let decoded = frame.and_then(|p| postcard::from_bytes::<(u64, EditOp)>(&p).ok());
                let Some((t, op)) = decoded else {
                    if !last_segment {
                        return Err(invalid(format!("{}: corrupt edit at byte {}", path.display(), offset)));
                    }
                    warn!("{}: discarding torn edit at byte {} (crash during write)", path.display(), offset);
                    OpenOptions::new().write(true).open(path)?.set_len(offset)?;
                    break;
                };
                if t <= txid {
                    continue;
                }
                if t != txid + 1 {
                    return Err(invalid(format!("{}: edit {} follows {}", path.display(), t, txid)));
                }
                apply(&namespace, &op, &mut counters)
                    .await
                    .map_err(|e| invalid(format!("{}: replaying edit {} {:?}: {}", path.display(), t, op, e)))?;
                txid = t;
                replayed += 1;
            }
        }
        info!("Replayed {} edits; namespace at transaction {}", replayed, txid);

        if txid != image_txid {
            write_image(&dir, txid, counters, &namespace.records().await, &namespace.open_files().await)?;
            purge(&dir)?;
        }
        let segment = open_segment(&dir, txid + 1)?;
        let disk = Disk {
            dir,
            segment: Mutex::new(Arc::new(segment)),
            last_checkpoint_txid: AtomicU64::new(txid),
            checkpointing: Mutex::new(()),
            _lock: lock,
        };
        Ok((Self::new(txid, Some(disk)), Recovered { identity, namespace, counters, txid, replayed }))
    }
}

/// Apply one logged op to the namespace (replay).
async fn apply(ns: &Namespace, op: &EditOp, counters: &mut Counters) -> Result<(), String> {
    let file = |id: u64| ns.get_inode(id).ok_or_else(|| format!("inode {} not found", id));
    match op {
        EditOp::AddInode { path, inode } => {
            for b in &inode.blocks {
                counters.observe_block(b);
            }
            counters.next_inode_id = counters.next_inode_id.max(inode.id + 1);
            ns.add_inode_at(path, inode.clone().into_inode()).await.map_err(|e| e.to_string())?;
        }
        EditOp::Delete { path } => {
            ns.delete(path, true).await.map_err(|e| e.to_string())?;
        }
        EditOp::Rename { src, dst, overwrite } => {
            if *overwrite && ns.lookup(dst).await.is_ok() {
                ns.delete(dst, false).await.map_err(|e| e.to_string())?;
            }
            ns.rename(src, dst).await.map_err(|e| e.to_string())?;
        }
        EditOp::AddBlock { inode, block } => {
            counters.observe_block(block);
            file(*inode)?.write().await.add_block(block.clone());
        }
        EditOp::CommitBlock { inode, block_id, num_bytes } => {
            let f = file(*inode)?;
            let mut f = f.write().await;
            let b = f.blocks.iter_mut().flatten().find(|b| b.block_id == *block_id);
            b.ok_or_else(|| format!("block {} not in inode {}", block_id, inode))?.num_bytes = *num_bytes;
        }
        EditOp::UpdateGenerationStamp { inode, block_id, generation_stamp } => {
            counters.next_generation_stamp = counters.next_generation_stamp.max(generation_stamp + 1);
            let f = file(*inode)?;
            let mut f = f.write().await;
            let b = f.blocks.iter_mut().flatten().find(|b| b.block_id == *block_id);
            b.ok_or_else(|| format!("block {} not in inode {}", block_id, inode))?.generation_stamp = *generation_stamp;
        }
        EditOp::SetBlocks { inode, blocks } => {
            for b in blocks {
                counters.observe_block(b);
            }
            file(*inode)?.write().await.blocks = Some(blocks.clone());
        }
        EditOp::SetAttributes { path, attrs } => {
            let inode = ns.lookup(path).await.map_err(|e| e.to_string())?;
            attrs.apply(&mut *inode.write().await);
        }
        EditOp::Open { inode, holder, blocks_at_open } => {
            file(*inode)?.write().await.under_construction =
                Some(UnderConstruction { holder: holder.clone(), blocks_at_open: *blocks_at_open });
        }
        EditOp::Close { inode } => {
            file(*inode)?.write().await.under_construction = None;
        }
        EditOp::Concat { target, sources } => {
            let t = ns.lookup(target).await.map_err(|e| e.to_string())?;
            for s in sources {
                let moved = ns.lookup(s).await.map_err(|e| e.to_string())?.read().await.blocks.clone();
                t.write().await.blocks.get_or_insert_with(Vec::new).extend(moved.unwrap_or_default());
            }
            for s in sources {
                ns.delete(s, false).await.map_err(|e| e.to_string())?;
            }
        }
    }
    Ok(())
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn put_frame(out: &mut Vec<u8>, payload: &[u8]) {
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
    out.extend_from_slice(payload);
}

/// Read up to `buf.len()` bytes; fewer only at end of file.
fn read_full(r: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

/// The frames of a file, each with its byte offset; `None` marks a torn or
/// corrupt frame, after which nothing more is read. A file shorter than its
/// magic (created but never written) has no frames.
fn read_frames(path: &Path, magic: &[u8; 8]) -> io::Result<Vec<(u64, Option<Vec<u8>>)>> {
    let mut r = BufReader::new(File::open(path)?);
    let mut m = [0u8; 8];
    let got = read_full(&mut r, &mut m)?;
    if got < m.len() {
        return Ok(if got == 0 { Vec::new() } else { vec![(0, None)] });
    }
    if &m != magic {
        return Err(invalid(format!("{}: bad magic", path.display())));
    }
    let mut offset = m.len() as u64;
    let mut frames = Vec::new();
    loop {
        let mut head = [0u8; 8];
        match read_full(&mut r, &mut head)? {
            0 => break,
            8 => {}
            _ => {
                frames.push((offset, None));
                break;
            }
        }
        let len = u32::from_le_bytes(head[..4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(head[4..].try_into().unwrap());
        if len > MAX_FRAME {
            frames.push((offset, None));
            break;
        }
        let mut payload = vec![0u8; len];
        if read_full(&mut r, &mut payload)? < len || crc32c::crc32c(&payload) != crc {
            frames.push((offset, None));
            break;
        }
        frames.push((offset, Some(payload)));
        offset += 8 + len as u64;
    }
    Ok(frames)
}

fn edits_name(first_txid: u64) -> String {
    format!("edits_{:019}", first_txid)
}

fn image_name(txid: u64) -> String {
    format!("fsimage_{:019}", txid)
}

/// Files named `<prefix><txid>`, sorted by txid.
fn list(dir: &Path, prefix: &str) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(txid) = name.strip_prefix(prefix).and_then(|t| t.parse::<u64>().ok()) {
            out.push((txid, entry.path()));
        }
    }
    out.sort();
    Ok(out)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Create (or reset) the segment starting at `first_txid`. Reusing a name
/// is safe: replay consumed every complete edit, so any file already there
/// holds none past the current transaction.
fn open_segment(dir: &Path, first_txid: u64) -> io::Result<File> {
    let path = dir.join(edits_name(first_txid));
    let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(&path)?;
    f.write_all(EDITS_MAGIC)?;
    f.sync_all()?;
    sync_dir(dir)?;
    Ok(f)
}

fn write_image(dir: &Path, txid: u64, counters: Counters, records: &[InodeRecord], open: &[(u64, UnderConstruction)]) -> io::Result<()> {
    let path = dir.join(image_name(txid));
    let tmp = dir.join(format!("{}.tmp", image_name(txid)));
    let mut w = BufWriter::new(File::create(&tmp)?);
    w.write_all(IMAGE_MAGIC)?;
    let mut frame = Vec::new();
    let header = ImageHeader { txid, counters, inodes: records.len() as u64 };
    put_frame(&mut frame, &postcard::to_stdvec(&header).map_err(io::Error::other)?);
    for r in records {
        put_frame(&mut frame, &postcard::to_stdvec(r).map_err(io::Error::other)?);
        if frame.len() >= 1 << 20 {
            w.write_all(&frame)?;
            frame.clear();
        }
    }
    put_frame(&mut frame, &postcard::to_stdvec(open).map_err(io::Error::other)?);
    w.write_all(&frame)?;
    let f = w.into_inner().map_err(|e| e.into_error())?;
    f.sync_all()?;
    fs::rename(&tmp, &path)?;
    sync_dir(dir)
}

fn read_image(path: &Path) -> io::Result<(ImageHeader, Vec<InodeRecord>, OpenFiles)> {
    let mut frames = read_frames(path, IMAGE_MAGIC)?.into_iter();
    let bad = || invalid(format!("{}: truncated or corrupt", path.display()));
    let head = frames.next().and_then(|(_, f)| f).ok_or_else(bad)?;
    let header: ImageHeader = postcard::from_bytes(&head).map_err(|_| bad())?;
    let mut records = Vec::with_capacity(header.inodes as usize);
    for _ in 0..header.inodes {
        let (_, f) = frames.next().ok_or_else(bad)?;
        records.push(postcard::from_bytes(&f.ok_or_else(bad)?).map_err(|_| bad())?);
    }
    let open = match frames.next() {
        None => Vec::new(),
        Some((_, f)) => postcard::from_bytes(&f.ok_or_else(bad)?).map_err(|_| bad())?,
    };
    if frames.next().is_some() {
        return Err(bad());
    }
    Ok((header, records, open))
}

/// Keep the newest [`KEEP_IMAGES`] images and the edits needed on top of
/// the oldest of them; remove the rest (and stray temporary images).
fn purge(dir: &Path) -> io::Result<()> {
    let images = list(dir, "fsimage_")?;
    if images.len() > KEEP_IMAGES {
        for (_, path) in &images[..images.len() - KEEP_IMAGES] {
            fs::remove_file(path)?;
        }
    }
    let Some((oldest_kept, _)) = list(dir, "fsimage_")?.first().cloned() else { return Ok(()) };
    let segments = list(dir, "edits_")?;
    // A segment ends where the next begins; the newest is still open.
    for pair in segments.windows(2) {
        let ((_, path), (next_start, _)) = (&pair[0], &pair[1]);
        if next_start - 1 <= oldest_kept {
            fs::remove_file(path)?;
        }
    }
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "tmp") {
            fs::remove_file(path)?;
        }
    }
    sync_dir(dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datanode::manager::StorageInfo;
    use crate::datanode::{BlockPlacementPolicy, DatanodeManager};
    use crate::grpc::ClientNamenodeService;
    use crate::metadata::block_manager::{BlockId, DatanodeId};
    use tempfile::TempDir;

    struct Nn {
        svc: ClientNamenodeService,
        ns: Arc<Namespace>,
        blocks: Arc<BlockManager>,
        log: Arc<EditLog>,
        replayed: u64,
    }

    /// A NameNode's metadata services over `dir`, with three live DataNodes.
    async fn open(dir: &Path) -> Nn {
        let (log, rec) = EditLog::open(dir).await.unwrap();
        let blocks = Arc::new(BlockManager::new());
        rec.restore_blocks(&blocks).await;
        let replayed = rec.replayed;
        let ns = Arc::new(rec.namespace);
        let dns = Arc::new(DatanodeManager::new());
        for i in 0..3 {
            let uuid = format!("dn{i}");
            dns.register(DatanodeId::new(uuid.clone(), "h".into(), 1), 1 << 30).unwrap();
            let storage = StorageInfo { capacity: 1 << 30, used: 0, remaining: 1 << 30, block_pool_used: 0 };
            dns.heartbeat(&uuid, storage, 0, 0).unwrap();
        }
        let placement = Arc::new(BlockPlacementPolicy::new(dns.clone(), 3));
        let log = Arc::new(log);
        let svc = ClientNamenodeService::new(ns.clone(), blocks.clone(), dns, placement).with_edit_log(log.clone());
        Nn { svc, ns, blocks, log, replayed }
    }

    /// Write a closed file: each block committed and reported by a
    /// DataNode, then complete().
    async fn file(nn: &Nn, path: &str, block_lens: &[u64]) {
        nn.svc.handle_create(path, "u".into(), "g".into(), 0o640, 3, 1 << 20).await.unwrap();
        for &len in block_lens {
            let b = nn.svc.handle_add_block(path).await.unwrap();
            nn.svc.handle_commit_block(path, b.block_id, len).await.unwrap();
            let id = BlockId::new(b.block_id, b.generation_stamp);
            nn.blocks.report_block(DatanodeId::new("dn0".into(), "h".into(), 1), id, len);
        }
        assert!(nn.svc.handle_complete(path).await.unwrap());
    }

    /// Everything persisted: the tree, attributes, block lists, and the
    /// block manager's view of each file.
    async fn state(nn: &Nn) -> (Vec<InodeRecord>, Vec<(u64, Vec<(BlockId, u64)>)>) {
        let mut records = nn.ns.records().await;
        records.sort_by_key(|r| r.id);
        let mut files = Vec::new();
        for r in records.iter().filter(|r| !r.is_dir) {
            let ids = nn.blocks.get_file_blocks(r.id).unwrap_or_default();
            let lens = ids.iter().map(|id| (*id, nn.blocks.get_block_info(id).unwrap().num_bytes)).collect();
            files.push((r.id, lens));
        }
        (records, files)
    }

    #[tokio::test]
    async fn every_kind_of_change_survives_a_restart() {
        let dir = TempDir::new().unwrap();
        let nn = open(dir.path()).await;
        let svc = &nn.svc;
        svc.handle_mkdirs("/a/b/c", "u".into(), "g".into(), 0o750, true).await.unwrap();
        file(&nn, "/a/b/f", &[100, 50]).await;
        file(&nn, "/a/t", &[100, 100, 30]).await;
        file(&nn, "/a/s1", &[7]).await;
        file(&nn, "/a/s2", &[9, 4]).await;
        file(&nn, "/a/old", &[1]).await;
        svc.handle_rename("/a/b/f", "/a/f").await.unwrap();
        svc.handle_rename2("/a/s1", "/a/old", true).await.unwrap();
        svc.handle_truncate("/a/t", 150).await.unwrap();
        svc.handle_concat("/a/f", &["/a/old".to_string(), "/a/s2".to_string()]).await.unwrap();
        // Reopen /a/f: abandon a block, then write one through a pipeline
        // update (new generation stamp), and close it.
        svc.handle_append("/a/f", true).await.unwrap();
        let b = svc.handle_add_block("/a/f").await.unwrap();
        svc.handle_abandon_block("/a/f", b.block_id).await.unwrap();
        let w = svc.handle_add_block("/a/f").await.unwrap();
        let bumped = svc.handle_update_block_for_pipeline(w.block_id, w.generation_stamp).await.unwrap();
        svc.handle_commit_block("/a/f", w.block_id, 6).await.unwrap();
        nn.blocks.report_block(DatanodeId::new("dn0".into(), "h".into(), 1), BlockId::new(w.block_id, bumped.generation_stamp), 6);
        assert!(svc.handle_complete("/a/f").await.unwrap());
        // And one file left open across the restart, mid-block.
        svc.handle_create("/a/open", "u".into(), "g".into(), 0o644, 3, 1 << 20).await.unwrap();
        let o = svc.handle_add_block("/a/open").await.unwrap();
        svc.handle_delete("/a/b", true).await.unwrap();
        let attrs = InodeAttributes {
            permission: Some(0o600),
            owner: Some("alice".into()),
            group: Some("eng".into()),
            mtime: Some(1_000),
            atime: Some(2_000),
            replication: None,
        };
        svc.handle_set_attributes("/a/f", attrs).await.unwrap();
        assert!(svc.handle_set_replication("/a/f", 2).await.unwrap());
        assert!(!svc.handle_set_replication("/a", 2).await.unwrap(), "a directory has no replication");
        // complete() retries re-commit the same length; that logs nothing.
        let txid = nn.log.last_txid();
        svc.handle_commit_block("/a/f", w.block_id, 6).await.unwrap();
        assert_eq!(nn.log.last_txid(), txid);

        let before = state(&nn).await;
        let counters = Counters::of(&nn.ns, &nn.blocks);
        drop(nn);

        let nn = open(dir.path()).await;
        assert!(nn.replayed > 10);
        assert_eq!(state(&nn).await, before);
        let f = nn.ns.lookup("/a/f").await.unwrap();
        assert_eq!(f.read().await.blocks.as_ref().unwrap()[5].generation_stamp, bumped.generation_stamp);
        let lens: Vec<u64> = f.read().await.blocks.as_ref().unwrap().iter().map(|b| b.num_bytes).collect();
        assert_eq!(lens, vec![100, 50, 7, 9, 4, 6]);
        assert!(f.read().await.under_construction.is_none());
        // The open file is still open, by the same holder, its block still
        // the one being written.
        let open = nn.ns.lookup("/a/open").await.unwrap();
        let open = open.read().await;
        assert_eq!(open.under_construction, Some(UnderConstruction { holder: String::new(), blocks_at_open: 0 }));
        assert_eq!(open.open_block().map(|b| b.block_id), Some(o.block_id));
        drop(open);
        assert!(nn.ns.lookup("/a/s1").await.is_err() && nn.ns.lookup("/a/b").await.is_err());
        {
            let f = f.read().await;
            assert_eq!((f.permissions.mode, f.owner.as_str(), f.group.as_str()), (0o600, "alice", "eng"));
            assert_eq!((f.mtime, f.atime, f.replication), (1_000, 2_000, 2));
        }

        // Ids are never reused, including the abandoned block's.
        assert!(Counters::of(&nn.ns, &nn.blocks).next_block_id >= counters.next_block_id);
        assert!(nn.blocks.next_block_id() > b.block_id);
        file(&nn, "/a/new", &[1]).await;
        let new = nn.ns.lookup("/a/new").await.unwrap();
        assert!(new.read().await.id >= counters.next_inode_id);
    }

    #[tokio::test]
    async fn restart_after_a_checkpoint_replays_only_later_edits() {
        let dir = TempDir::new().unwrap();
        let nn = open(dir.path()).await;
        for i in 0..20 {
            file(&nn, &format!("/f{i}"), &[i]).await;
        }
        // An open file goes into the image with its holder.
        nn.svc.handle_create_with("/open", "u".into(), "g".into(), 0o644, 3, 1 << 20, false, false, "w1").await.unwrap();
        nn.log.checkpoint(&nn.ns, &nn.blocks).await.unwrap();
        assert_eq!(nn.log.txns_since_checkpoint(), 0);
        nn.svc.handle_delete("/f3", false).await.unwrap();
        nn.svc.handle_rename("/f4", "/g4").await.unwrap();
        let before = state(&nn).await;
        drop(nn);

        let nn = open(dir.path()).await;
        assert_eq!(nn.replayed, 2);
        assert_eq!(state(&nn).await, before);
        assert_eq!(nn.ns.open_files().await.into_iter().map(|(_, uc)| uc.holder).collect::<Vec<_>>(), ["w1"]);
        assert_eq!(nn.svc.restore_leases().await, 1);
        drop(nn);

        // Opening wrote a new image; the oldest images and covered edits go.
        let current = dir.path().join("current");
        assert_eq!(list(&current, "fsimage_").unwrap().len(), KEEP_IMAGES);
        assert!(list(&current, "edits_").unwrap().len() <= 2);
        assert_eq!(open(dir.path()).await.replayed, 0);
    }

    #[tokio::test]
    async fn a_torn_final_edit_is_discarded() {
        let dir = TempDir::new().unwrap();
        let nn = open(dir.path()).await;
        file(&nn, "/kept", &[10]).await;
        let before = state(&nn).await;
        drop(nn);

        // A crash mid-write: half a frame at the end of the open segment.
        let (_, seg) = list(&dir.path().join("current"), "edits_").unwrap().pop().unwrap();
        let mut frame = Vec::new();
        put_frame(&mut frame, &postcard::to_stdvec(&(99u64, EditOp::Delete { path: "/kept".into() })).unwrap());
        OpenOptions::new().append(true).open(&seg).unwrap().write_all(&frame[..frame.len() / 2]).unwrap();

        let nn = open(dir.path()).await;
        assert_eq!(state(&nn).await, before);
        file(&nn, "/after", &[]).await;
        drop(nn);
        let nn = open(dir.path()).await;
        assert!(nn.ns.lookup("/after").await.is_ok() && nn.ns.lookup("/kept").await.is_ok());
    }

    #[tokio::test]
    async fn a_corrupt_edit_before_the_last_segment_stops_startup() {
        let dir = TempDir::new().unwrap();
        let nn = open(dir.path()).await;
        file(&nn, "/a", &[]).await;
        file(&nn, "/b", &[]).await;
        drop(nn);
        let current = dir.path().join("current");
        // Damage the segment holding the edits, then add a later (empty)
        // segment so the damaged one is not the last.
        let (start, seg) = list(&current, "edits_").unwrap().into_iter().find(|(_, p)| fs::metadata(p).unwrap().len() > 8).unwrap();
        let mut bytes = fs::read(&seg).unwrap();
        let n = bytes.len();
        bytes[n - 1] ^= 0xff;
        fs::write(&seg, bytes).unwrap();
        open_segment(&current, start + 100).unwrap();
        let err = EditLog::open(dir.path()).await.err().expect("corrupt edit must stop startup");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
        assert!(err.to_string().contains("corrupt edit"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_commits_are_all_durable() {
        let dir = TempDir::new().unwrap();
        let nn = Arc::new(open(dir.path()).await);
        let tasks: Vec<_> = (0..64)
            .map(|i| {
                let nn = nn.clone();
                tokio::spawn(async move { file(&nn, &format!("/c{i}"), &[i as u64 + 1]).await })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
        let before = state(&nn).await;
        drop(nn);
        let nn = open(dir.path()).await;
        assert_eq!(nn.ns.inode_count(), 65);
        assert_eq!(state(&nn).await, before);
    }

    #[tokio::test]
    async fn one_namenode_per_name_directory() {
        let dir = TempDir::new().unwrap();
        let nn = open(dir.path()).await;
        let err = EditLog::open(dir.path()).await.err().expect("second open must fail");
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        drop(nn);
        EditLog::open(dir.path()).await.unwrap();
    }

    #[tokio::test]
    async fn the_identity_is_made_at_format_and_kept() {
        let dir = TempDir::new().unwrap();
        let (_, first) = EditLog::open_with(dir.path(), Some("CID-test")).await.unwrap();
        assert_eq!(first.identity.cluster_id, "CID-test");
        assert!(first.identity.namespace_id > 0);
        let (_, again) = EditLog::open(dir.path()).await.unwrap();
        assert_eq!(again.identity, first.identity);
        let err = EditLog::open_with(dir.path(), Some("CID-other")).await.err().expect("wrong cluster id");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        // A fresh directory is a different namespace.
        let other = TempDir::new().unwrap();
        let (_, fresh) = EditLog::open(other.path()).await.unwrap();
        assert_ne!(fresh.identity, first.identity);
    }
}

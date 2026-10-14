use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::metadata::{Namespace, BlockManager, EditLog, EditOp, EditTxn};
use crate::metadata::edit_log::{InodeAttributes, InodeRecord};
use crate::metadata::lease_manager::LeaseManager;
use crate::metadata::namespace::{FsPermission, INode, UnderConstruction};
use crate::metadata::block_manager::{BlockId, DatanodeId};
use crate::datanode::{DatanodeManager, BlockPlacementPolicy};

/// ClientNamenodeProtocol service - handles communication from HDFS clients to NameNode
pub struct ClientNamenodeService {
    namespace: Arc<Namespace>,
    block_manager: Arc<BlockManager>,
    datanode_manager: Arc<DatanodeManager>,
    placement_policy: Arc<BlockPlacementPolicy>,
    /// Every namespace change is a transaction in this log.
    edits: Arc<EditLog>,
    /// getServerDefaults: block size and replication (NameNode config).
    default_block_size: u64,
    default_replication: u16,
    /// This namespace's block pool, named in every block handed out.
    block_pool_id: String,
    /// Who holds which open files.
    leases: Arc<LeaseManager>,
}

impl ClientNamenodeService {
    pub fn new(
        namespace: Arc<Namespace>,
        block_manager: Arc<BlockManager>,
        datanode_manager: Arc<DatanodeManager>,
        placement_policy: Arc<BlockPlacementPolicy>,
    ) -> Self {
        Self {
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
            edits: Arc::new(EditLog::in_memory()),
            default_block_size: 128 * 1024 * 1024,
            default_replication: 3,
            block_pool_id: "BP-pillar-001".to_string(),
            leases: Arc::new(LeaseManager::default()),
        }
    }

    /// Use `leases` (e.g. with other soft/hard limits).
    pub fn with_leases(mut self, leases: Arc<LeaseManager>) -> Self {
        self.leases = leases;
        self
    }

    /// After loading a namespace: every open file's holder gets a fresh
    /// lease (HDFS does the same), so a live writer carries on and a dead
    /// one is recovered after the hard limit.
    pub async fn restore_leases(&self) -> usize {
        let open = self.namespace.open_files().await;
        for (id, uc) in &open {
            self.leases.add(&uc.holder, *id);
        }
        open.len()
    }

    /// renewLease.
    pub fn handle_renew_lease(&self, client: &str) {
        self.leases.renew(client);
    }

    /// isFileClosed.
    pub async fn handle_is_file_closed(&self, path: &str) -> Result<bool, Status> {
        let inode = self.namespace.lookup(path).await.map_err(namespace_status)?;
        let closed = inode.read().await.under_construction.is_none();
        Ok(closed)
    }

    /// recoverLease: close the file now (see [`Self::recover_file`]).
    /// True once it is closed, which it always is on return.
    pub async fn handle_recover_lease(&self, path: &str, client: &str) -> Result<bool, Status> {
        let inode = self.namespace.lookup(path).await.map_err(namespace_status)?;
        let (id, holder) = {
            let inode = inode.read().await;
            if inode.is_dir {
                return Err(Status::not_found(format!("{} is a directory", path)));
            }
            (inode.id, inode.under_construction.as_ref().map(|uc| uc.holder.clone()))
        };
        if let Some(holder) = holder {
            info!("recoverLease({}) by {}: recovering {}'s lease", path, client, holder);
            self.recover_file(id).await?;
        }
        Ok(true)
    }

    /// listOpenFiles: (inode id, path, holder) of every open file.
    pub async fn handle_list_open_files(&self) -> Vec<(u64, String, String)> {
        let mut out = Vec::new();
        for (id, uc) in self.namespace.open_files().await {
            if let Some(path) = self.namespace.path_of(id).await {
                out.push((id, path, uc.holder));
            }
        }
        out.sort();
        out
    }

    /// Recover the files of every lease past the hard limit; returns how
    /// many. Run periodically (HDFS LeaseManager.Monitor).
    pub async fn check_leases(&self) -> usize {
        let expired = self.leases.hard_expired();
        for (holder, id) in &expired {
            warn!("Lease of {} on inode {} passed the hard limit; recovering the file", holder, id);
            if let Err(e) = self.recover_file(*id).await {
                warn!("Recovering inode {}: {}", id, e);
            }
        }
        expired.len()
    }

    /// Lease recovery: close an open file whose writer is gone. The block
    /// being written keeps the length a DataNode reported when it finished
    /// the block; if no DataNode finished it, no data in it was ever
    /// acknowledged (Pillar has no hflush yet), so it is dropped.
    async fn recover_file(&self, inode_id: u64) -> Result<(), Status> {
        let mut tx = self.begin().await?;
        let Some(inode) = self.namespace.get_inode(inode_id) else { return Ok(()) };
        let mut guard = inode.write().await;
        let Some(uc) = guard.under_construction.clone() else { return Ok(()) };
        if let Some(open) = guard.open_block().cloned() {
            let id = BlockId::new(open.block_id, open.generation_stamp);
            let blocks = guard.blocks.as_mut().expect("a file");
            if self.block_manager.get_replica_count(&id) > 0 {
                let len = self.block_manager.get_block_info(&id).map_or(open.num_bytes, |b| b.num_bytes);
                blocks.last_mut().expect("open block").num_bytes = len;
                info!("Lease recovery of inode {}: block {} kept, {} bytes", inode_id, open.block_id, len);
            } else {
                blocks.pop();
                self.block_manager.remove_block(&id);
                info!("Lease recovery of inode {}: block {} dropped (no DataNode finished it)", inode_id, open.block_id);
            }
            tx.log(EditOp::SetBlocks { inode: inode_id, blocks: blocks.clone() });
        }
        guard.under_construction = None;
        drop(guard);
        tx.log(EditOp::Close { inode: inode_id });
        tx.commit().await.map_err(edit_status)?;
        self.leases.remove(&uc.holder, inode_id);
        Ok(())
    }

    /// Before `client` opens an existing file: if another writer has it
    /// open, take it over once that writer's lease is past the soft limit
    /// (recovering the file first), else AlreadyBeingCreatedException.
    async fn take_over_if_open(&self, path: &str, client: &str, op: &str) -> Result<(), Status> {
        let Ok(inode) = self.namespace.lookup(path).await else { return Ok(()) };
        let (id, holder) = {
            let inode = inode.read().await;
            match &inode.under_construction {
                Some(uc) => (inode.id, uc.holder.clone()),
                None => return Ok(()),
            }
        };
        if holder != client && self.leases.soft_expired(&holder) {
            info!("{} {} by {}: {}'s lease expired; recovering the file", op, path, client, holder);
            return self.recover_file(id).await;
        }
        Err(already_being_created(format!(
            "Failed to {} {} for {}: the file is open for writing by {}",
            op, path, client, holder
        )))
    }

    /// Server defaults reported to clients (getServerDefaults).
    pub fn with_defaults(mut self, block_size: u64, replication: u16) -> Self {
        self.default_block_size = block_size;
        self.default_replication = replication;
        self
    }

    /// The namespace's block pool id (from its identity).
    pub fn with_block_pool_id(mut self, block_pool_id: String) -> Self {
        self.block_pool_id = block_pool_id;
        self
    }

    pub fn default_block_size(&self) -> u64 {
        self.default_block_size
    }

    pub fn default_replication(&self) -> u16 {
        self.default_replication
    }

    pub fn block_pool_id(&self) -> &str {
        &self.block_pool_id
    }

    /// Last edit-log transaction id (getCurrentEditLogTxid).
    pub fn last_txid(&self) -> u64 {
        self.edits.last_txid()
    }

    /// setPermission / setOwner / setTimes: change the attributes that are
    /// `Some`, durably.
    pub async fn handle_set_attributes(&self, path: &str, attrs: InodeAttributes) -> Result<(), Status> {
        let mut tx = self.begin().await?;
        let inode = self.namespace.lookup(path).await.map_err(namespace_status)?;
        attrs.apply(&mut *inode.write().await);
        tx.log(EditOp::SetAttributes { path: path.to_string(), attrs });
        tx.commit().await.map_err(edit_status)
    }

    /// setReplication: false for a directory (as HDFS). The replication
    /// monitor then adds or deletes replicas.
    pub async fn handle_set_replication(&self, path: &str, replication: u32) -> Result<bool, Status> {
        if !(1..=512).contains(&replication) {
            return Err(Status::invalid_argument(format!(
                "replication {} is outside 1..=512", replication
            )));
        }
        if self.namespace.lookup(path).await.map_err(namespace_status)?.read().await.is_dir {
            return Ok(false);
        }
        let attrs = InodeAttributes { replication: Some(replication as u16), ..Default::default() };
        self.handle_set_attributes(path, attrs).await?;
        info!("Set replication of {} to {}", path, replication);
        Ok(true)
    }

    /// getPreferredBlockSize: the file's block size.
    pub async fn handle_preferred_block_size(&self, path: &str) -> Result<u64, Status> {
        let inode = self.namespace.lookup(path).await.map_err(namespace_status)?;
        let inode = inode.read().await;
        if inode.is_dir {
            return Err(Status::not_found(format!("{} is a directory", path)));
        }
        Ok(inode.block_size)
    }

    /// checkAccess: the path must exist. Permissions are not enforced (as
    /// HDFS with dfs.permissions.enabled=false).
    pub async fn handle_check_access(&self, path: &str) -> Result<(), Status> {
        self.namespace.lookup(path).await.map(|_| ()).map_err(namespace_status)
    }

    /// reportBadBlocks: drop each named replica (unless it is the block's
    /// last) so the replication monitor copies a good one in its place.
    pub fn handle_report_bad_replicas(&self, replicas: &[(BlockId, String)]) {
        for (block, uuid) in replicas {
            if self.block_manager.drop_corrupt_replica(block, uuid) {
                warn!("Replica of block {} on {} reported corrupt; deleting it", block.block_id, uuid);
            } else {
                warn!("Replica of block {} on {} reported corrupt; kept (unknown or last replica)", block.block_id, uuid);
            }
        }
    }

    /// Log namespace changes to `edits` (default: an in-memory log).
    pub fn with_edit_log(mut self, edits: Arc<EditLog>) -> Self {
        self.edits = edits;
        self
    }

    async fn begin(&self) -> Result<EditTxn<'_>, Status> {
        self.edits.begin().await.map_err(edit_status)
    }

    /// The record logged for a new inode.
    async fn inode_record(&self, id: u64) -> InodeRecord {
        let inode = self.namespace.get_inode(id).expect("inode just created");
        let inode = inode.read().await;
        InodeRecord::from_inode(&inode, 0)
    }

    /// Get file/directory info
    pub async fn handle_get_file_info(&self, path: &str) -> Result<FileInfo, Status> {
        let inode = self.namespace
            .lookup(path)
            .await
            .map_err(|e| Status::not_found(format!("Path not found: {}", e)))?;

        let inode_guard = inode.read().await;

        Ok(FileInfo {
            path: path.to_string(),
            file_id: inode_guard.id,
            is_dir: inode_guard.is_dir,
            length: self.get_file_length(&*inode_guard).await,
            block_size: inode_guard.block_size,
            replication: inode_guard.replication,
            modification_time: inode_guard.mtime,
            access_time: inode_guard.atime,
            owner: inode_guard.owner.clone(),
            group: inode_guard.group.clone(),
            permissions: inode_guard.permissions.mode,
        })
    }

    /// List directory contents
    pub async fn handle_get_listing(&self, path: &str) -> Result<Vec<FileInfo>, Status> {
        let children = self.namespace
            .list_directory(path)
            .await
            .map_err(|e| Status::not_found(format!("Directory not found: {}", e)))?;

        let mut result = Vec::new();

        for child in children {
            let child_guard = child.read().await;
            let child_path = if path == "/" {
                format!("/{}", child_guard.name)
            } else {
                format!("{}/{}", path, child_guard.name)
            };

            result.push(FileInfo {
                path: child_path,
                file_id: child_guard.id,
                is_dir: child_guard.is_dir,
                length: self.get_file_length(&*child_guard).await,
                block_size: child_guard.block_size,
                replication: child_guard.replication,
                modification_time: child_guard.mtime,
                access_time: child_guard.atime,
                owner: child_guard.owner.clone(),
                group: child_guard.group.clone(),
                permissions: child_guard.permissions.mode,
            });
        }

        Ok(result)
    }

    /// Create a directory
    pub async fn handle_mkdir(
        &self,
        path: &str,
        owner: String,
        group: String,
        permissions: u16,
    ) -> Result<(), Status> {
        let mut tx = self.begin().await?;
        let id = self.namespace
            .mkdir(path, owner, group, FsPermission::new(permissions))
            .await
            .map_err(namespace_status)?;
        tx.log(EditOp::AddInode { path: path.to_string(), inode: self.inode_record(id).await });
        tx.commit().await.map_err(edit_status)?;

        info!("Created directory: {}", path);

        Ok(())
    }

    /// HDFS `mkdirs`: succeeds if `path` already is a directory; with
    /// `create_parent`, creates missing ancestors first (`mkdir -p`).
    pub async fn handle_mkdirs(
        &self,
        path: &str,
        owner: String,
        group: String,
        permissions: u16,
        create_parent: bool,
    ) -> Result<(), Status> {
        let mut prefixes: Vec<String> = Vec::new();
        let mut acc = String::new();
        for part in path.split('/').filter(|p| !p.is_empty()) {
            acc.push('/');
            acc.push_str(part);
            prefixes.push(acc.clone());
        }
        let (ancestors, target) = match prefixes.split_last() {
            Some((t, a)) => (a, t.as_str()),
            None => return Ok(()), // "/"
        };
        if create_parent {
            for dir in ancestors {
                self.ensure_dir(dir, &owner, &group, permissions).await?;
            }
        }
        self.ensure_dir(target, &owner, &group, permissions).await
    }

    async fn ensure_dir(&self, path: &str, owner: &str, group: &str, permissions: u16) -> Result<(), Status> {
        match self.namespace.lookup(path).await {
            Ok(inode) => {
                if inode.read().await.is_dir {
                    Ok(())
                } else {
                    Err(Status::already_exists(format!("Path is not a directory: {}", path)))
                }
            }
            Err(_) => match self.handle_mkdir(path, owner.to_string(), group.to_string(), permissions).await {
                // Lost a race with a concurrent mkdirs: fine if it made a directory.
                Err(e) if e.code() == tonic::Code::AlreadyExists => match self.namespace.lookup(path).await {
                    Ok(inode) if inode.read().await.is_dir => Ok(()),
                    _ => Err(e),
                },
                r => r,
            },
        }
    }

    /// Create a file
    pub async fn handle_create(
        &self,
        path: &str,
        owner: String,
        group: String,
        permissions: u16,
        replication: u16,
        block_size: u64,
    ) -> Result<(), Status> {
        self.handle_create_with(path, owner, group, permissions, replication, block_size, false, false, "").await.map(|_| ())
    }

    /// Mark inode `id` open for writing by `client`.
    async fn open_for(&self, tx: &mut EditTxn<'_>, id: u64, client: &str, blocks_at_open: u32) {
        let inode = self.namespace.get_inode(id).expect("inode just created or looked up");
        inode.write().await.under_construction =
            Some(UnderConstruction { holder: client.to_string(), blocks_at_open });
        tx.log(EditOp::Open { inode: id, holder: client.to_string(), blocks_at_open });
        self.leases.add(client, id);
    }

    /// HDFS `create`: `create_parent` makes missing ancestors (as
    /// `mkdirs -p`); `overwrite` (CreateFlag.OVERWRITE) replaces an existing
    /// file, deleting it and creating the new one in one transaction.
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_create_with(
        &self,
        path: &str,
        owner: String,
        group: String,
        permissions: u16,
        replication: u16,
        block_size: u64,
        create_parent: bool,
        overwrite: bool,
        client: &str,
    ) -> Result<u64, Status> {
        self.take_over_if_open(path, client, "create").await?;
        if create_parent {
            if let Some((parent, _)) = path.trim_end_matches('/').rsplit_once('/') {
                if !parent.is_empty() {
                    self.handle_mkdirs(parent, owner.clone(), group.clone(), 0o755, true).await?;
                }
            }
        }
        let mut tx = self.begin().await?;
        if let Ok(existing) = self.namespace.lookup(path).await {
            if existing.read().await.is_dir {
                return Err(Status::already_exists(format!("{} is a directory", path)));
            }
            if !overwrite {
                return Err(Status::already_exists(format!("{} already exists", path)));
            }
            self.delete_in(&mut tx, path, false).await?;
        }
        let id = self.namespace
            .create_file(
                path,
                owner,
                group,
                FsPermission::new(permissions),
                replication,
                block_size,
            )
            .await
            .map_err(namespace_status)?;
        tx.log(EditOp::AddInode { path: path.to_string(), inode: self.inode_record(id).await });
        self.open_for(&mut tx, id, client, 0).await;
        tx.commit().await.map_err(edit_status)?;

        info!("Created file: {} (open for {})", path, client);

        Ok(id)
    }

    /// Delete a file or directory
    pub async fn handle_delete(&self, path: &str, recursive: bool) -> Result<bool, Status> {
        let mut tx = self.begin().await?;
        self.delete_in(&mut tx, path, recursive).await?;
        tx.commit().await.map_err(edit_status)?;
        Ok(true)
    }

    /// Delete `path` and release its blocks, logging it in `tx`.
    async fn delete_in(&self, tx: &mut EditTxn<'_>, path: &str, recursive: bool) -> Result<(), Status> {
        let removed = self.namespace
            .delete_and_collect(path, recursive)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete: {}", e)))?;

        self.release_blocks(&removed);
        for r in &removed {
            if let Some(holder) = &r.holder {
                self.leases.remove(holder, r.id);
            }
        }

        tx.log(EditOp::Delete { path: path.to_string() });
        info!("Deleted: {} ({} inodes)", path, removed.len());

        Ok(())
    }

    /// Release block-manager state for every removed file (including
    /// descendants of a recursively deleted directory).
    fn release_blocks(&self, removed: &[crate::metadata::namespace::RemovedInode]) {
        for inode in removed.iter().filter(|inode| !inode.is_dir) {
            for block in &inode.blocks {
                self.block_manager
                    .remove_block(&BlockId::new(block.block_id, block.generation_stamp));
            }
            self.block_manager.delete_file_blocks(inode.id);
        }
    }

    /// Rename a file or directory (atomic operation)
    pub async fn handle_rename(&self, src: &str, dst: &str) -> Result<bool, Status> {
        let mut tx = self.begin().await?;
        self.namespace
            .rename(src, dst)
            .await
            .map_err(namespace_status)?;
        tx.log(EditOp::Rename { src: src.to_string(), dst: dst.to_string(), overwrite: false });
        tx.commit().await.map_err(edit_status)?;

        info!("Renamed: {} -> {}", src, dst);

        Ok(true)
    }

    /// Get the blocks of a file overlapping `[offset, offset + length)`.
    ///
    /// Convenience wrapper around [`Self::handle_get_located_blocks`] that
    /// returns only the block list.
    pub async fn handle_get_block_locations(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Vec<LocatedBlock>, Status> {
        Ok(self.handle_get_located_blocks(path, offset, length).await?.blocks)
    }

    /// getBlockLocations with HDFS semantics.
    ///
    /// Block offsets are the cumulative sizes of the preceding blocks (blocks
    /// can have unequal sizes after append, concat or truncate). Only blocks
    /// overlapping `[offset, offset + length)` are returned; as in HDFS, the
    /// block containing `offset` is returned even when `length` is 0, and an
    /// offset at or past EOF yields no blocks. `file_length`, `last_block`
    /// and `is_last_block_complete` always describe the whole file.
    pub async fn handle_get_located_blocks(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<LocatedBlocks, Status> {
        let inode = self.namespace
            .lookup(path)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        // Copy out what we need and release the inode lock before resolving
        // locations.
        let (selected, file_length, last, open) = {
            let inode_guard = inode.read().await;

            if inode_guard.is_dir {
                return Err(Status::invalid_argument("Cannot get blocks for directory"));
            }

            // An open file shows its finished blocks; the one being written
            // has no length a reader could rely on (no hflush yet).
            let blocks = inode_guard.finished_blocks();
            let file_length: u64 = blocks.iter().map(|b| b.num_bytes).sum();
            let last = blocks.last().map(|b| (b.clone(), file_length - b.num_bytes));
            let open = inode_guard.under_construction.is_some();
            (Self::blocks_in_range(blocks, offset, length), file_length, last, open)
        };

        let blocks = selected
            .into_iter()
            .map(|(block, block_offset)| self.locate(&block, block_offset))
            .collect();

        let is_last_block_complete = last.is_some();
        let last_block = last.map(|(block, block_offset)| self.locate(&block, block_offset));

        Ok(LocatedBlocks {
            file_length,
            blocks,
            last_block,
            is_last_block_complete,
            under_construction: open,
        })
    }

    /// Select the blocks overlapping `[offset, offset + length)`, paired with
    /// their starting offsets in the file (HDFS `createLocatedBlockList`).
    fn blocks_in_range(
        blocks: &[crate::metadata::namespace::BlockInfo],
        offset: u64,
        length: u64,
    ) -> Vec<(crate::metadata::namespace::BlockInfo, u64)> {
        // Find the block containing `offset`.
        let mut cur_pos = 0u64;
        let mut cur_blk = 0usize;
        while cur_blk < blocks.len() {
            if cur_pos + blocks[cur_blk].num_bytes > offset {
                break;
            }
            cur_pos += blocks[cur_blk].num_bytes;
            cur_blk += 1;
        }

        let end = offset.saturating_add(length);
        let mut result = Vec::new();
        while cur_blk < blocks.len() {
            result.push((blocks[cur_blk].clone(), cur_pos));
            cur_pos += blocks[cur_blk].num_bytes;
            cur_blk += 1;
            if cur_pos >= end {
                break;
            }
        }
        result
    }

    /// Build a LocatedBlock for a block at a given file offset.
    fn locate(&self, block: &crate::metadata::namespace::BlockInfo, offset: u64) -> LocatedBlock {
        let block_id = BlockId::new(block.block_id, block.generation_stamp);
        LocatedBlock {
            block_id: block.block_id,
            generation_stamp: block.generation_stamp,
            num_bytes: block.num_bytes,
            offset,
            locations: self.block_manager
                .get_block_locations(&block_id)
                .unwrap_or_default(),
        }
    }

    /// Add a new block to a file (called by client when writing)
    pub async fn handle_add_block(&self, path: &str) -> Result<LocatedBlock, Status> {
        self.handle_add_block_as(path, "").await
    }

    /// addBlock by `client`, which must hold the file's lease.
    pub async fn handle_add_block_as(&self, path: &str, client: &str) -> Result<LocatedBlock, Status> {
        let inode = self.namespace
            .lookup(path)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        // Read what placement needs, then release the inode lock: choosing
        // targets scans all datanodes and must not serialize other operations
        // on this file.
        let replication = {
            let inode_guard = inode.read().await;
            if inode_guard.is_dir {
                return Err(Status::invalid_argument("Cannot add block to directory"));
            }
            inode_guard.replication
        };

        // Select datanodes for replica placement (before allocating, so a
        // placement failure doesn't leak an allocated block)
        let targets = self.placement_policy
            .choose_targets(Some(replication))
            .map_err(|e| Status::resource_exhausted(format!("Failed to choose targets: {}", e)))?;

        let mut tx = self.begin().await?;
        let mut inode_guard = inode.write().await;

        if inode_guard.is_dir {
            return Err(Status::invalid_argument("Cannot add block to directory"));
        }
        check_holder(&inode_guard, path, client)?;

        // Allocate new block
        let block_id = self.block_manager
            .allocate_block(inode_guard.id)
            .await
            .map_err(|e| Status::internal(format!("Failed to allocate block: {}", e)))?;

        // Add block to inode
        let block = crate::metadata::namespace::BlockInfo {
            block_id: block_id.block_id,
            generation_stamp: block_id.generation_stamp,
            num_bytes: 0,
        };
        inode_guard.add_block(block.clone());
        tx.log(EditOp::AddBlock { inode: inode_guard.id, block });
        drop(inode_guard);
        tx.commit().await.map_err(edit_status)?;

        info!("Allocated block {} for file: {}", block_id.block_id, path);

        Ok(LocatedBlock {
            block_id: block_id.block_id,
            generation_stamp: block_id.generation_stamp,
            num_bytes: 0,
            offset: 0,
            locations: targets,
        })
    }

    /// HDFS `rename2`: like rename, but an existing destination is an error
    /// unless `overwrite` is set, in which case a destination file (or empty
    /// directory) is replaced.
    pub async fn handle_rename2(&self, src: &str, dst: &str, overwrite: bool) -> Result<(), Status> {
        let mut tx = self.begin().await?;
        let replaced = self.namespace.lookup(dst).await.is_ok();
        if replaced {
            if !overwrite {
                return Err(Status::already_exists(format!("rename destination {} already exists", dst)));
            }
            // Non-recursive: a non-empty destination directory is refused,
            // as in HDFS. Logged as one op below, so a crash can't leave
            // the destination deleted but the source not moved.
            let removed = self.namespace.delete_and_collect(dst, false).await.map_err(namespace_status)?;
            self.release_blocks(&removed);
        }
        self.namespace.rename(src, dst).await.map_err(namespace_status)?;
        tx.log(EditOp::Rename { src: src.to_string(), dst: dst.to_string(), overwrite: replaced });
        tx.commit().await.map_err(edit_status)?;
        info!("Renamed: {} -> {}{}", src, dst, if replaced { " (replaced)" } else { "" });
        Ok(())
    }

    /// Record the final length of a block the client finished writing
    /// (`addBlock`'s `previous`, `complete`'s `last`). The file's length is
    /// the sum of its blocks.
    pub async fn handle_commit_block(&self, path: &str, block_id: u64, num_bytes: u64) -> Result<(), Status> {
        self.handle_commit_block_as(path, block_id, num_bytes, "").await
    }

    /// Record a block's final length for `client`, which must hold the
    /// file's lease (a closed file is left alone: a retried complete).
    pub async fn handle_commit_block_as(&self, path: &str, block_id: u64, num_bytes: u64, client: &str) -> Result<(), Status> {
        let inode = self.namespace.lookup(path).await.map_err(namespace_status)?;
        let mut tx = self.begin().await?;
        let mut guard = inode.write().await;
        if guard.under_construction.is_none() {
            return Ok(());
        }
        check_holder(&guard, path, client)?;
        let inode_id = guard.id;
        let block = guard
            .blocks
            .as_mut()
            .and_then(|bs| bs.iter_mut().find(|b| b.block_id == block_id))
            .ok_or_else(|| Status::invalid_argument(format!("block {} is not part of {}", block_id, path)))?;
        // complete() is retried until a replica is reported; don't log
        // (and fsync) the same length each time.
        if block.num_bytes == num_bytes {
            return Ok(());
        }
        block.num_bytes = num_bytes;
        let id = crate::metadata::block_manager::BlockId::new(block.block_id, block.generation_stamp);
        drop(guard);
        let _ = self.block_manager.update_block_size(&id, num_bytes);
        tx.log(EditOp::CommitBlock { inode: inode_id, block_id, num_bytes });
        tx.commit().await.map_err(edit_status)
    }

    /// Complete a file (finalize writes)
    pub async fn handle_complete(&self, path: &str) -> Result<bool, Status> {
        self.handle_complete_as(path, "").await
    }

    /// complete(): close the file once every block has a reported replica.
    /// Completing an already closed file succeeds (a retried complete).
    pub async fn handle_complete_as(&self, path: &str, client: &str) -> Result<bool, Status> {
        let inode = self.namespace
            .lookup(path)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        let mut tx = self.begin().await?;
        let mut inode_guard = inode.write().await;

        if inode_guard.is_dir {
            return Err(Status::invalid_argument("Cannot complete directory"));
        }
        if inode_guard.under_construction.is_none() {
            return Ok(true);
        }
        check_holder(&inode_guard, path, client)?;

        // HDFS dfs.namenode.replication.min (1): the file is complete only
        // once every block has a replica a DataNode has reported. Until then
        // answer false; clients retry complete() with backoff.
        let unreported = inode_guard
            .blocks
            .iter()
            .flatten()
            .find(|b| {
                let id = crate::metadata::block_manager::BlockId::new(b.block_id, b.generation_stamp);
                self.block_manager.get_block_locations(&id).map_or(true, |l| l.is_empty())
            })
            .map(|b| b.block_id);
        if let Some(block_id) = unreported {
            debug!("complete({}): block {} has no reported replica yet", path, block_id);
            return Ok(false);
        }

        let id = inode_guard.id;
        inode_guard.under_construction = None;
        drop(inode_guard);
        tx.log(EditOp::Close { inode: id });
        tx.commit().await.map_err(edit_status)?;
        self.leases.remove(client, id);
        info!("Completed file: {}", path);

        Ok(true)
    }

    /// HDFS `append`. Appending starts a new block: answering with no last
    /// block tells the client to addBlock, which is what HDFS answers when
    /// the last block is full. Writing into a partially filled last block needs the DataNode
    /// append pipeline, which Pillar lacks, so that is refused unless the
    /// client asked for a new block (CreateFlag.NEW_BLOCK).
    pub async fn handle_append(&self, path: &str, new_block: bool) -> Result<(), Status> {
        self.handle_append_as(path, new_block, "").await
    }

    /// append by `client`: the file becomes open for it (see handle_append).
    pub async fn handle_append_as(&self, path: &str, new_block: bool, client: &str) -> Result<(), Status> {
        self.take_over_if_open(path, client, "append").await?;
        let inode = self.namespace
            .lookup(path)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        let mut tx = self.begin().await?;
        let inode_guard = inode.read().await;
        if inode_guard.under_construction.is_some() {
            return Err(already_being_created(format!("Failed to append {}: it was opened meanwhile", path)));
        }

        if inode_guard.is_dir {
            return Err(Status::invalid_argument("Cannot append to directory"));
        }
        let partial = inode_guard
            .blocks
            .as_ref()
            .and_then(|b| b.last())
            .is_some_and(|last| last.num_bytes < inode_guard.block_size);
        if partial && !new_block {
            return Err(Status::unimplemented(format!(
                "append to {}: its last block is partially filled, and Pillar cannot extend a block yet; \
                 append with CreateFlag.NEW_BLOCK to start a new one",
                path
            )));
        }
        let (id, blocks) = (inode_guard.id, inode_guard.blocks.as_ref().map_or(0, |b| b.len()) as u32);
        drop(inode_guard);
        self.open_for(&mut tx, id, client, blocks).await;
        tx.commit().await.map_err(edit_status)?;
        info!("Append to {} by {} starts a new block", path, client);
        Ok(())
    }

    /// Abandon a block allocation (cleanup failed writes)
    pub async fn handle_abandon_block(&self, path: &str, block_id: u64) -> Result<(), Status> {
        self.handle_abandon_block_as(path, block_id, "").await
    }

    /// abandonBlock by `client`, which must hold the file's lease.
    pub async fn handle_abandon_block_as(&self, path: &str, block_id: u64, client: &str) -> Result<(), Status> {
        let inode = self.namespace
            .lookup(path)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        let mut tx = self.begin().await?;
        let mut inode_guard = inode.write().await;
        let inode_id = inode_guard.id;
        check_holder(&inode_guard, path, client)?;

        if let Some(blocks) = &mut inode_guard.blocks {
            let mut abandoned = Vec::new();
            blocks.retain(|b| {
                if b.block_id == block_id {
                    abandoned.push(BlockId::new(b.block_id, b.generation_stamp));
                    false
                } else {
                    true
                }
            });
            for block in &abandoned {
                self.block_manager.remove_block(block);
            }
            if !abandoned.is_empty() {
                tx.log(EditOp::SetBlocks { inode: inode_id, blocks: blocks.clone() });
            }
            info!("Abandoned block {} for file: {}", block_id, path);
        }
        drop(inode_guard);

        tx.commit().await.map_err(edit_status)
    }

    /// Get DataNode report (cluster health)
    pub async fn handle_get_datanode_report(&self) -> Result<Vec<DatanodeReportInfo>, Status> {
        let datanodes = self.datanode_manager.get_live_datanodes();

        let mut report = Vec::new();
        for dn in datanodes {
            report.push(DatanodeReportInfo {
                hostname: dn.id.hostname.clone(),
                uuid: dn.id.uuid.clone(),
                data_port: dn.id.data_port,
                http_port: dn.id.http_port,
                rpc_port: dn.id.rpc_port,
                capacity: dn.storage.capacity,
                used: dn.storage.used,
                remaining: dn.storage.remaining,
                block_pool_used: dn.storage.block_pool_used,
                num_blocks: dn.num_blocks as u32,
                // Milliseconds since the epoch, as HDFS reports it.
                last_update: (std::time::SystemTime::now() - dn.last_update.elapsed())
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0),
                supported_transports: dn.id.supported_transports.clone(),
                rdma_port: dn.id.rdma_port,
            });
        }

        Ok(report)
    }

    /// Get filesystem stats from actual DataNodes
    pub async fn handle_get_fs_stats_real(&self) -> Result<FsStats, Status> {
        let total_capacity = self.datanode_manager.get_total_capacity();
        let total_used = self.datanode_manager.get_total_used();
        let total_remaining = self.datanode_manager.get_total_remaining();

        Ok(FsStats {
            capacity: total_capacity,
            used: total_used,
            remaining: total_remaining,
            under_replicated: 0, // TODO: Calculate from block manager
            corrupt_blocks: 0,
            missing_blocks: 0,
        })
    }

    /// Get additional datanode for pipeline recovery
    /// Called when a DataNode fails during write pipeline
    pub async fn handle_get_additional_datanode(
        &self,
        block_id: u64,
        generation_stamp: u64,
        existing_datanodes: &[DatanodeId],
        excluded_datanodes: &[DatanodeId],
        num_additional_nodes: u32,
    ) -> Result<LocatedBlock, Status> {
        info!(
            "Getting {} additional datanode(s) for block {} (excluding {} nodes)",
            num_additional_nodes,
            block_id,
            existing_datanodes.len() + excluded_datanodes.len()
        );

        // Choose the best live datanodes, excluding existing and excluded ones
        let exclude: Vec<&str> = existing_datanodes
            .iter()
            .chain(excluded_datanodes.iter())
            .map(|dn| dn.uuid.as_str())
            .collect();

        let new_locations = self.placement_policy
            .choose_targets_excluding(num_additional_nodes as usize, &exclude)
            .map_err(|e| Status::resource_exhausted(format!(
                "Not enough datanodes available: need {}: {}",
                num_additional_nodes, e
            )))?;

        // Combine existing and new locations
        let mut all_locations = existing_datanodes.to_vec();
        all_locations.extend(new_locations);

        info!("Selected {} additional datanode(s) for block {}", num_additional_nodes, block_id);

        Ok(LocatedBlock {
            block_id,
            generation_stamp,
            num_bytes: 0,
            offset: 0,
            locations: all_locations,
        })
    }

    /// Update block generation stamp for pipeline
    pub async fn handle_update_block_for_pipeline(
        &self,
        block_id: u64,
        old_generation_stamp: u64,
    ) -> Result<LocatedBlock, Status> {
        self.handle_update_block_for_pipeline_as(block_id, old_generation_stamp, "").await
    }

    /// updateBlockForPipeline by `client`, which must hold the lease on the
    /// block's file.
    pub async fn handle_update_block_for_pipeline_as(
        &self,
        block_id: u64,
        old_generation_stamp: u64,
        client: &str,
    ) -> Result<LocatedBlock, Status> {
        info!(
            "Updating block {} generation stamp from {}",
            block_id, old_generation_stamp
        );

        let mut tx = self.begin().await?;

        // Allocate new generation stamp
        let new_generation_stamp = self.block_manager
            .allocate_generation_stamp()
            .await;

        // Update the block with new generation stamp
        let old_block_id = BlockId::new(block_id, old_generation_stamp);
        let new_block_id = BlockId::new(block_id, new_generation_stamp);

        // Get the block info
        let block_info = self.block_manager
            .get_block_info(&old_block_id)
            .ok_or_else(|| Status::not_found(format!("Block not found: {}", block_id)))?;
        if let Some(inode) = self.namespace.get_inode(block_info.inode_id) {
            let inode = inode.read().await;
            check_holder(&inode, &format!("inode {}", block_info.inode_id), client)?;
        }

        // Get current locations
        let locations = self.block_manager
            .get_block_locations(&old_block_id)
            .unwrap_or_default();

        // Update block manager with new generation stamp
        self.block_manager.update_generation_stamp(old_block_id, new_block_id);

        // ...and the file's block list, which reads and the image use.
        if let Some(inode) = self.namespace.get_inode(block_info.inode_id) {
            let mut inode = inode.write().await;
            if let Some(b) = inode.blocks.iter_mut().flatten().find(|b| b.block_id == block_id) {
                b.generation_stamp = new_generation_stamp;
            }
        }
        tx.log(EditOp::UpdateGenerationStamp {
            inode: block_info.inode_id,
            block_id,
            generation_stamp: new_generation_stamp,
        });
        tx.commit().await.map_err(edit_status)?;

        info!(
            "Updated block {} generation stamp: {} -> {}",
            block_id, old_generation_stamp, new_generation_stamp
        );

        Ok(LocatedBlock {
            block_id,
            generation_stamp: new_generation_stamp,
            num_bytes: block_info.num_bytes,
            offset: 0,
            locations,
        })
    }

    /// Truncate a file to a new length
    pub async fn handle_truncate(
        &self,
        path: &str,
        new_length: u64,
    ) -> Result<bool, Status> {
        info!("Truncating file {} to {} bytes", path, new_length);

        let inode = self.namespace
            .lookup(path)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        let mut tx = self.begin().await?;
        let mut inode_guard = inode.write().await;

        if inode_guard.is_dir {
            return Err(Status::invalid_argument("Cannot truncate directory"));
        }
        if inode_guard.under_construction.is_some() {
            return Err(file_is_open("truncate", path));
        }

        // Calculate current file length
        let current_length = inode_guard.blocks.as_ref()
            .map(|blocks| blocks.iter().map(|b| b.num_bytes).sum())
            .unwrap_or(0);

        // Validate new length
        if new_length > current_length {
            return Err(Status::invalid_argument(format!(
                "Cannot truncate to length {} greater than current length {}",
                new_length, current_length
            )));
        }

        // If new length is same as current, it's a no-op
        if new_length == current_length {
            info!("Truncate is no-op: new length equals current length");
            return Ok(true);
        }

        if let Some(blocks) = &mut inode_guard.blocks {
            // Walk real block sizes (blocks can be partial after
            // append/concat): keep every block that starts before
            // new_length, and cut the last kept one at new_length.
            let mut num_blocks_to_keep = 0usize;
            let mut kept_bytes = 0u64;
            while num_blocks_to_keep < blocks.len() && kept_bytes < new_length {
                kept_bytes += blocks[num_blocks_to_keep].num_bytes;
                num_blocks_to_keep += 1;
            }

            let dropped: Vec<BlockId> = blocks
                .drain(num_blocks_to_keep..)
                .map(|b| BlockId::new(b.block_id, b.generation_stamp))
                .collect();
            for block in &dropped {
                self.block_manager.remove_block(block);
            }

            if let Some(last_block) = blocks.last_mut() {
                let last_block_size = last_block.num_bytes - (kept_bytes - new_length);
                last_block.num_bytes = last_block_size;
                let _ = self.block_manager.update_block_size(
                    &BlockId::new(last_block.block_id, last_block.generation_stamp),
                    last_block_size,
                );
                info!(
                    "Updated last block {} size to {} bytes",
                    last_block.block_id,
                    last_block_size
                );
            }

            info!(
                "Truncated {} from {} to {} bytes ({} blocks remaining, {} removed)",
                path, current_length, new_length, blocks.len(), dropped.len()
            );
            let blocks = blocks.clone();
            tx.log(EditOp::SetBlocks { inode: inode_guard.id, blocks });
        }
        drop(inode_guard);
        tx.commit().await.map_err(edit_status)?;

        Ok(true)
    }

    /// Concatenate source files into target file
    pub async fn handle_concat(
        &self,
        target: &str,
        sources: &[String],
    ) -> Result<(), Status> {
        info!("Concatenating {} files into {}", sources.len(), target);

        // Validate sources array is not empty
        if sources.is_empty() {
            return Err(Status::invalid_argument("Sources array cannot be empty"));
        }

        let mut tx = self.begin().await?;

        // Look up target file
        let target_inode = self.namespace
            .lookup(target)
            .await
            .map_err(|e| Status::not_found(format!("Target file not found: {}", e)))?;

        let mut target_guard = target_inode.write().await;

        if target_guard.is_dir {
            return Err(Status::invalid_argument("Target cannot be a directory"));
        }
        if target_guard.under_construction.is_some() {
            return Err(file_is_open("concat", target));
        }

        // Validate and collect all source files
        let mut source_blocks = Vec::new();
        let mut source_ids: Vec<u64> = Vec::new();
        for source_path in sources {
            let source_inode = self.namespace
                .lookup(source_path)
                .await
                .map_err(|e| Status::not_found(format!("Source file not found: {}", e)))?;

            // The target is write-locked: locking it again would deadlock,
            // and deleting it afterwards would lose the result.
            if Arc::ptr_eq(&source_inode, &target_inode) {
                return Err(Status::invalid_argument(format!(
                    "Source {} is the concat target", source_path
                )));
            }

            let source_guard = source_inode.read().await;

            if source_ids.contains(&source_guard.id) {
                return Err(Status::invalid_argument(format!(
                    "Source {} is listed more than once", source_path
                )));
            }
            source_ids.push(source_guard.id);

            if source_guard.is_dir {
                return Err(Status::invalid_argument(format!(
                    "Source {} is a directory",
                    source_path
                )));
            }
            if source_guard.under_construction.is_some() {
                return Err(file_is_open("concat", source_path));
            }

            // Check block size compatibility
            if source_guard.block_size != target_guard.block_size {
                return Err(Status::invalid_argument(format!(
                    "Source {} has different block size ({} vs {})",
                    source_path, source_guard.block_size, target_guard.block_size
                )));
            }

            // Collect blocks from this source
            if let Some(blocks) = &source_guard.blocks {
                source_blocks.extend(blocks.clone());
            }
        }

        // Append source blocks to target
        if let Some(target_blocks) = &mut target_guard.blocks {
            target_blocks.extend(source_blocks);
        } else {
            target_guard.blocks = Some(source_blocks);
        }

        // The moved blocks now belong to the target in the block manager too.
        let target_id = target_guard.id;
        for source_id in &source_ids {
            self.block_manager.move_file_blocks(*source_id, target_id);
        }

        // Release target lock before deleting sources
        drop(target_guard);

        // Delete all source files
        for source_path in sources {
            self.namespace
                .delete(source_path, false)
                .await
                .map_err(|e| Status::internal(format!(
                    "Failed to delete source file {}: {}",
                    source_path, e
                )))?;
        }

        tx.log(EditOp::Concat { target: target.to_string(), sources: sources.to_vec() });
        tx.commit().await.map_err(edit_status)?;

        info!("Successfully concatenated {} files into {}", sources.len(), target);

        Ok(())
    }

    /// HDFS getContentSummary (`hdfs dfs -count`, `-du -s`): totals over
    /// the subtree at `path`, which counts itself if it is a directory.
    pub async fn handle_get_content_summary(&self, path: &str) -> Result<ContentSummary, Status> {
        let root = self.namespace.lookup(path).await.map_err(namespace_status)?;
        let mut summary = ContentSummary::default();
        let mut stack = vec![root];
        while let Some(inode) = stack.pop() {
            let guard = inode.read().await;
            if guard.is_dir {
                summary.directory_count += 1;
                let children: Vec<u64> =
                    guard.children.iter().flat_map(|c| c.iter().map(|e| *e.value())).collect();
                drop(guard);
                stack.extend(children.into_iter().filter_map(|id| self.namespace.get_inode(id)));
            } else {
                let length: u64 = guard.blocks.iter().flatten().map(|b| b.num_bytes).sum();
                summary.file_count += 1;
                summary.length += length;
                summary.space_consumed += length * guard.replication as u64;
            }
        }
        Ok(summary)
    }

    /// Get file length
    async fn get_file_length(&self, inode: &INode) -> u64 {
        if inode.is_dir {
            return 0;
        }

        inode.blocks.as_ref()
            .map(|blocks| blocks.iter().map(|b| b.num_bytes).sum())
            .unwrap_or(0)
    }
}

/// Totals over a subtree (HDFS ContentSummary).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContentSummary {
    pub length: u64,
    pub file_count: u64,
    pub directory_count: u64,
    /// Bytes stored counting every replica.
    pub space_consumed: u64,
}

/// DataNode report information
#[derive(Debug, Clone)]
pub struct DatanodeReportInfo {
    pub hostname: String,
    pub uuid: String,
    pub data_port: u16,
    pub http_port: u16,
    pub rpc_port: u16,
    pub capacity: u64,
    pub used: u64,
    pub remaining: u64,
    pub block_pool_used: u64,
    pub num_blocks: u32,
    pub last_update: u64,
    pub supported_transports: Vec<String>,
    pub rdma_port: u16,
}

/// Filesystem statistics
#[derive(Debug, Clone)]
pub struct FsStats {
    pub capacity: u64,
    pub used: u64,
    pub remaining: u64,
    pub under_replicated: u64,
    pub corrupt_blocks: u64,
    pub missing_blocks: u64,
}

/// File information returned to client
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    /// The inode id: HDFS clients key their open files (and leases) by it.
    pub file_id: u64,
    pub is_dir: bool,
    pub length: u64,
    pub block_size: u64,
    pub replication: u16,
    pub modification_time: u64,
    pub access_time: u64,
    pub owner: String,
    pub group: String,
    pub permissions: u16,
}

/// Result of getBlockLocations (mirrors HDFS LocatedBlocks)
#[derive(Debug, Clone)]
pub struct LocatedBlocks {
    /// Length of the whole file
    pub file_length: u64,
    /// Blocks overlapping the requested range
    pub blocks: Vec<LocatedBlock>,
    /// Last block of the file (None for an empty file)
    pub last_block: Option<LocatedBlock>,
    pub is_last_block_complete: bool,
    pub under_construction: bool,
}

/// Block location information
#[derive(Debug, Clone)]
pub struct LocatedBlock {
    pub block_id: u64,
    pub generation_stamp: u64,
    pub num_bytes: u64,
    pub offset: u64,
    pub locations: Vec<DatanodeId>,
}

/// `client` must hold the lease on `inode` (open for writing by it); else
/// LeaseExpiredException, as HDFS answers a writer that lost its lease.
fn check_holder(inode: &INode, path: &str, client: &str) -> Result<(), Status> {
    match &inode.under_construction {
        Some(uc) if uc.holder == client => Ok(()),
        Some(uc) => Err(lease_expired(format!(
            "No lease on {} (inode {}): held by {}, not {}", path, inode.id, uc.holder, client
        ))),
        None => Err(lease_expired(format!(
            "No lease on {} (inode {}): File is not open for writing. Holder {} does not have any open files.",
            path, inode.id, client
        ))),
    }
}

fn lease_expired(message: String) -> Status {
    crate::ipc::hadoop_status(
        tonic::Code::FailedPrecondition,
        "org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException",
        message,
    )
}

fn already_being_created(message: String) -> Status {
    crate::ipc::hadoop_status(
        tonic::Code::AlreadyExists,
        "org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException",
        message,
    )
}

/// The file is open for writing, and the operation needs it closed.
fn file_is_open(op: &str, path: &str) -> Status {
    Status::failed_precondition(format!("{}: {} is open for writing", op, path))
}

/// The edit log could not make a change durable.
fn edit_status(e: std::io::Error) -> Status {
    Status::unavailable(format!("edit log: {}", e))
}

/// Map a namespace error onto the status (and so the Hadoop exception)
/// clients expect: a missing parent is FileNotFound, not AlreadyExists.
fn namespace_status(e: crate::metadata::namespace::NamespaceError) -> Status {
    use crate::metadata::namespace::NamespaceError as E;
    match e {
        E::PathNotFound(_) => Status::not_found(e.to_string()),
        E::PathAlreadyExists(_) => Status::already_exists(e.to_string()),
        E::NotADirectory(_) | E::DirectoryNotEmpty(_) => Status::failed_precondition(e.to_string()),
        E::InvalidPath(_) => Status::invalid_argument(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Finish writing `path` as a client does: the DataNodes report each
    /// block (as they would on finishing it), then complete() closes it.
    async fn close_file(service: &ClientNamenodeService, path: &str) {
        let inode = service.namespace.lookup(path).await.unwrap();
        let blocks = inode.read().await.blocks.clone().unwrap_or_default();
        for b in blocks {
            service.block_manager.report_block(
                DatanodeId::new("dn-test".to_string(), "h".to_string(), 1),
                BlockId::new(b.block_id, b.generation_stamp),
                b.num_bytes,
            );
        }
        assert!(service.handle_complete(path).await.unwrap(), "complete({path})");
    }

    #[tokio::test]
    async fn content_summary_totals_the_subtree() {
        let namespace = Arc::new(Namespace::new());
        let datanodes = Arc::new(DatanodeManager::new());
        let placement = Arc::new(BlockPlacementPolicy::new(datanodes.clone(), 3));
        let service = ClientNamenodeService::new(namespace.clone(), Arc::new(BlockManager::new()), datanodes, placement);
        service.handle_mkdirs("/t/a/b", "u".into(), "g".into(), 0o755, true).await.unwrap();
        for (path, lens, replication) in [("/t/f", vec![10u64, 3], 3u16), ("/t/a/g", vec![5], 2), ("/t/a/b/empty", vec![], 3)] {
            service.handle_create(path, "u".into(), "g".into(), 0o644, replication, 1 << 20).await.unwrap();
            let inode = namespace.lookup(path).await.unwrap();
            let mut inode = inode.write().await;
            for (i, len) in lens.into_iter().enumerate() {
                inode.add_block(crate::metadata::namespace::BlockInfo { block_id: i as u64, generation_stamp: 1, num_bytes: len });
            }
        }
        let s = service.handle_get_content_summary("/t").await.unwrap();
        assert_eq!(s, ContentSummary { length: 18, file_count: 3, directory_count: 3, space_consumed: 13 * 3 + 5 * 2 });
        let f = service.handle_get_content_summary("/t/f").await.unwrap();
        assert_eq!((f.file_count, f.directory_count, f.length), (1, 0, 13));
        assert_eq!(service.handle_get_content_summary("/nope").await.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_file_operations() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager,
            placement_policy,
        );

        // Create directory
        service.handle_mkdir("/test", "user".to_string(), "group".to_string(), 0o755)
            .await
            .unwrap();

        // Get file info
        let info = service.handle_get_file_info("/test").await.unwrap();
        assert!(info.is_dir);
        assert_eq!(info.path, "/test");

        // List directory
        let listing = service.handle_get_listing("/").await.unwrap();
        assert_eq!(listing.len(), 1);
    }

    #[tokio::test]
    async fn test_append_operations() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1)); // Use 1 for testing

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode for block allocation
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "localhost".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000, // 1GB
        ).unwrap();

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,  // replication = 1 for test
            128 * 1024 * 1024,
        ).await.unwrap();

        // Add a partial block (not full)
        let block1 = service.handle_add_block("/test.txt").await.unwrap();

        // Simulate partial block by updating its size
        let file_inode = namespace.lookup("/test.txt").await.unwrap();
        {
            let mut file_guard = file_inode.write().await;
            if let Some(blocks) = &mut file_guard.blocks {
                if let Some(last_block) = blocks.last_mut() {
                    last_block.num_bytes = 64 * 1024 * 1024; // Half full
                }
            }
        }

        let blocks = || async { file_inode.read().await.blocks.as_ref().unwrap().len() };

        // An open file cannot be appended to, even by its writer.
        let err = service.handle_append("/test.txt", true).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::AlreadyExists);
        close_file(&service, "/test.txt").await;

        // A partially filled last block cannot be extended: refused, unless
        // the client asks for a new block. Either way append allocates
        // nothing itself (the client's addBlock does), and opens the file.
        let err = service.handle_append("/test.txt", false).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        service.handle_append("/test.txt", true).await.unwrap();
        assert_eq!(blocks().await, 1);
        assert!(!service.handle_is_file_closed("/test.txt").await.unwrap());
        let _ = block1;
        close_file(&service, "/test.txt").await;

        // Mark block as full
        {
            let mut file_guard = file_inode.write().await;
            if let Some(blocks) = &mut file_guard.blocks {
                if let Some(last_block) = blocks.last_mut() {
                    last_block.num_bytes = 128 * 1024 * 1024; // Full
                }
            }
        }

        // A full last block: append is fine, and still allocates nothing
        // (allocating here as well as in the client's addBlock left an empty
        // block no DataNode ever reported, so complete() never succeeded).
        service.handle_append("/test.txt", false).await.unwrap();
        assert_eq!(blocks().await, 1);
    }

    #[tokio::test]
    async fn test_abandon_block() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1)); // Use 1 for testing

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "localhost".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create a file and add blocks
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,  // replication = 1 for test
            128 * 1024 * 1024,
        ).await.unwrap();

        let block1 = service.handle_add_block("/test.txt").await.unwrap();
        let block2 = service.handle_add_block("/test.txt").await.unwrap();

        // Verify 2 blocks exist
        let file_inode = namespace.lookup("/test.txt").await.unwrap();
        {
            let file_guard = file_inode.read().await;
            assert_eq!(file_guard.blocks.as_ref().unwrap().len(), 2);
        }

        // Abandon the second block
        service.handle_abandon_block("/test.txt", block2.block_id).await.unwrap();

        // Verify only 1 block remains
        {
            let file_guard = file_inode.read().await;
            assert_eq!(file_guard.blocks.as_ref().unwrap().len(), 1);
            assert_eq!(file_guard.blocks.as_ref().unwrap()[0].block_id, block1.block_id);
        }
    }

    #[tokio::test]
    async fn test_rename_service() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager,
            placement_policy,
        );

        // Create directories and file
        service.handle_mkdir("/src", "user".to_string(), "group".to_string(), 0o755)
            .await
            .unwrap();
        service.handle_mkdir("/dst", "user".to_string(), "group".to_string(), 0o755)
            .await
            .unwrap();
        service.handle_create(
            "/src/file.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Rename file
        let result = service.handle_rename("/src/file.txt", "/dst/renamed.txt").await.unwrap();
        assert!(result);

        // Verify old path doesn't exist
        assert!(service.handle_get_file_info("/src/file.txt").await.is_err());

        // Verify new path exists
        let info = service.handle_get_file_info("/dst/renamed.txt").await.unwrap();
        assert_eq!(info.path, "/dst/renamed.txt");
        assert!(!info.is_dir);

        // Test directory rename
        service.handle_mkdir("/olddir", "user".to_string(), "group".to_string(), 0o755)
            .await
            .unwrap();
        service.handle_create(
            "/olddir/file.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        let result = service.handle_rename("/olddir", "/newdir").await.unwrap();
        assert!(result);

        // Verify directory rename worked
        assert!(service.handle_get_file_info("/olddir").await.is_err());
        assert!(service.handle_get_file_info("/newdir").await.is_ok());
        assert!(service.handle_get_file_info("/newdir/file.txt").await.is_ok());
    }

    #[tokio::test]
    async fn test_datanode_report() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace,
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Initially no datanodes
        let report = service.handle_get_datanode_report().await.unwrap();
        assert_eq!(report.len(), 0);

        // Register datanodes
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000, // 1GB
        ).unwrap();

        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn2".to_string(),
                hostname: "host2".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            2_000_000_000, // 2GB
        ).unwrap();

        // Get report
        let report = service.handle_get_datanode_report().await.unwrap();
        assert_eq!(report.len(), 2);

        // Verify datanode info
        let dn1 = report.iter().find(|dn| dn.uuid == "dn1").unwrap();
        assert_eq!(dn1.hostname, "host1");
        assert_eq!(dn1.capacity, 1_000_000_000);

        let dn2 = report.iter().find(|dn| dn.uuid == "dn2").unwrap();
        assert_eq!(dn2.hostname, "host2");
        assert_eq!(dn2.capacity, 2_000_000_000);
    }

    #[tokio::test]
    async fn test_fs_stats() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager.clone(),
            placement_policy,
        );

        // Initially no capacity
        let stats = service.handle_get_fs_stats_real().await.unwrap();
        assert_eq!(stats.capacity, 0);
        assert_eq!(stats.used, 0);
        assert_eq!(stats.remaining, 0);

        // Register datanodes with different capacities
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000, // 1GB
        ).unwrap();

        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn2".to_string(),
                hostname: "host2".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            2_000_000_000, // 2GB
        ).unwrap();

        // Simulate heartbeat to update storage stats
        use crate::datanode::manager::StorageInfo;
        datanode_manager.heartbeat(
            "dn1",
            StorageInfo {
                capacity: 1_000_000_000,
                used: 100_000_000,
                remaining: 900_000_000,
                block_pool_used: 100_000_000,
            },
            0,
            0,
        ).unwrap();

        datanode_manager.heartbeat(
            "dn2",
            StorageInfo {
                capacity: 2_000_000_000,
                used: 500_000_000,
                remaining: 1_500_000_000,
                block_pool_used: 500_000_000,
            },
            0,
            0,
        ).unwrap();

        // Get aggregated stats
        let stats = service.handle_get_fs_stats_real().await.unwrap();
        assert_eq!(stats.capacity, 3_000_000_000);
        assert_eq!(stats.used, 600_000_000);
        assert_eq!(stats.remaining, 2_400_000_000);
    }

    #[tokio::test]
    async fn test_get_additional_datanode() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register multiple datanodes
        for i in 1..=5 {
            datanode_manager.register(
                crate::metadata::block_manager::DatanodeId {
                    uuid: format!("dn{}", i),
                    hostname: format!("host{}", i),
                    rpc_port: 9867,
                    data_port: 50010,
                    http_port: 50075,
                    supported_transports: vec!["tcp".to_string()],
                    rdma_port: 0,
                },
                1_000_000_000,
            ).unwrap();
        }

        // Simulate existing datanodes in the pipeline
        let existing_dns = vec![
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn2".to_string(),
                hostname: "host2".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
        ];

        let excluded_dns = vec![];

        // Request 1 additional datanode
        let result = service.handle_get_additional_datanode(
            1000,  // block_id
            1,     // generation_stamp
            &existing_dns,
            &excluded_dns,
            1,     // num_additional_nodes
        ).await.unwrap();

        // Should have 3 locations total (2 existing + 1 new)
        assert_eq!(result.locations.len(), 3);

        // The new datanode should not be dn1 or dn2
        let new_dn = &result.locations[2];
        assert_ne!(new_dn.uuid, "dn1");
        assert_ne!(new_dn.uuid, "dn2");
    }

    #[tokio::test]
    async fn test_get_additional_datanode_insufficient() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register only 2 datanodes
        for i in 1..=2 {
            datanode_manager.register(
                crate::metadata::block_manager::DatanodeId {
                    uuid: format!("dn{}", i),
                    hostname: format!("host{}", i),
                    rpc_port: 9867,
                    data_port: 50010,
                    http_port: 50075,
                    supported_transports: vec!["tcp".to_string()],
                    rdma_port: 0,
                },
                1_000_000_000,
            ).unwrap();
        }

        // Both are already in use
        let existing_dns = vec![
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn2".to_string(),
                hostname: "host2".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
        ];

        // Request 1 more, but none available
        let result = service.handle_get_additional_datanode(
            1000,
            1,
            &existing_dns,
            &[],
            1,
        ).await;

        // Should fail with resource_exhausted
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_block_for_pipeline() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create a file and allocate a block
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        let block = service.handle_add_block("/test.txt").await.unwrap();
        let old_generation_stamp = block.generation_stamp;

        // Update block generation stamp
        let updated_block = service.handle_update_block_for_pipeline(
            block.block_id,
            old_generation_stamp,
        ).await.unwrap();

        // Generation stamp should be different
        assert_eq!(updated_block.block_id, block.block_id);
        assert_ne!(updated_block.generation_stamp, old_generation_stamp);
        assert!(updated_block.generation_stamp > old_generation_stamp);
    }

    #[tokio::test]
    async fn test_recover_lease() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Add a block
        let _block = service.handle_add_block("/test.txt").await.unwrap();

        // Recover lease
        let result = service.handle_recover_lease("/test.txt", "client1").await.unwrap();
        assert!(result);
    }

    #[tokio::test]
    async fn test_recover_lease_nonexistent_file() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager,
            placement_policy,
        );

        // Try to recover lease for non-existent file
        let result = service.handle_recover_lease("/nonexistent.txt", "client1").await;
        assert!(result.is_err());
    }

    // ========================================================================
    // Atomicity Tests - Concurrent Operations
    // ========================================================================

    #[tokio::test]
    async fn test_concurrent_rename_different_files() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create two files
        service.handle_create(
            "/file1.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();
        service.handle_create(
            "/file2.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Concurrently rename different files
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_rename("/file1.txt", "/renamed1.txt").await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_rename("/file2.txt", "/renamed2.txt").await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        // Both renames should succeed
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert!(result1.unwrap());
        assert!(result2.unwrap());

        // Verify both files were renamed
        assert!(service.handle_get_file_info("/renamed1.txt").await.is_ok());
        assert!(service.handle_get_file_info("/renamed2.txt").await.is_ok());
        assert!(service.handle_get_file_info("/file1.txt").await.is_err());
        assert!(service.handle_get_file_info("/file2.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_concurrent_rename_same_file() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create a file
        service.handle_create(
            "/file.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Concurrently rename same file to different destinations
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_rename("/file.txt", "/renamed1.txt").await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_rename("/file.txt", "/renamed2.txt").await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        // One should succeed, one should fail
        let success_count = [result1.is_ok(), result2.is_ok()].iter().filter(|&&x| x).count();
        assert_eq!(success_count, 1, "Exactly one rename should succeed");

        // Original file should not exist
        assert!(service.handle_get_file_info("/file.txt").await.is_err());

        // Exactly one of the renamed files should exist
        let renamed1_exists = service.handle_get_file_info("/renamed1.txt").await.is_ok();
        let renamed2_exists = service.handle_get_file_info("/renamed2.txt").await.is_ok();
        assert!(renamed1_exists ^ renamed2_exists, "Exactly one renamed file should exist");
    }

    #[tokio::test]
    async fn test_rename_read_race() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create a file
        service.handle_create(
            "/file.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Concurrently rename and read
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_rename("/file.txt", "/renamed.txt").await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_get_file_info("/file.txt").await
        });

        let rename_result = handle1.await.unwrap();
        let read_result = handle2.await.unwrap();

        // Rename should succeed
        assert!(rename_result.is_ok());

        // Read might succeed or fail depending on timing, but shouldn't corrupt state
        // After rename, new name should exist
        assert!(service.handle_get_file_info("/renamed.txt").await.is_ok());
    }

    #[tokio::test]
    async fn test_rename_delete_race() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create a file
        service.handle_create(
            "/file.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Concurrently rename and delete
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_rename("/file.txt", "/renamed.txt").await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_delete("/file.txt", false).await
        });

        let rename_result = handle1.await.unwrap();
        let delete_result = handle2.await.unwrap();

        // One should succeed, one should fail
        let success_count = [rename_result.is_ok(), delete_result.is_ok()].iter().filter(|&&x| x).count();
        assert_eq!(success_count, 1, "Exactly one operation should succeed");

        // Original file should not exist
        assert!(service.handle_get_file_info("/file.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_rename_directory_concurrent() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create two directories
        service.handle_mkdir("/dir1", "user".to_string(), "group".to_string(), 0o755).await.unwrap();
        service.handle_mkdir("/dir2", "user".to_string(), "group".to_string(), 0o755).await.unwrap();

        // Concurrently rename different directories
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_rename("/dir1", "/renamed_dir1").await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_rename("/dir2", "/renamed_dir2").await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        // Both renames should succeed
        assert!(result1.is_ok());
        assert!(result2.is_ok());

        // Verify both directories were renamed
        assert!(service.handle_get_file_info("/renamed_dir1").await.is_ok());
        assert!(service.handle_get_file_info("/renamed_dir2").await.is_ok());
    }

    #[tokio::test]
    async fn test_concurrent_create_same_file() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Concurrently create same file
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_create(
                "/file.txt",
                "user".to_string(),
                "group".to_string(),
                0o644,
                3,
                128 * 1024 * 1024,
            ).await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_create(
                "/file.txt",
                "user".to_string(),
                "group".to_string(),
                0o644,
                3,
                128 * 1024 * 1024,
            ).await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        // One should succeed, one should fail (file already exists)
        let success_count = [result1.is_ok(), result2.is_ok()].iter().filter(|&&x| x).count();
        assert_eq!(success_count, 1, "Exactly one create should succeed");

        // File should exist
        assert!(service.handle_get_file_info("/file.txt").await.is_ok());
    }

    #[tokio::test]
    async fn test_concurrent_create_different_files() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Concurrently create different files
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_create(
                "/file1.txt",
                "user".to_string(),
                "group".to_string(),
                0o644,
                3,
                128 * 1024 * 1024,
            ).await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_create(
                "/file2.txt",
                "user".to_string(),
                "group".to_string(),
                0o644,
                3,
                128 * 1024 * 1024,
            ).await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        // Both creates should succeed
        assert!(result1.is_ok());
        assert!(result2.is_ok());

        // Both files should exist
        assert!(service.handle_get_file_info("/file1.txt").await.is_ok());
        assert!(service.handle_get_file_info("/file2.txt").await.is_ok());
    }

    #[tokio::test]
    async fn test_create_read_race() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Concurrently create and read
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_create(
                "/file.txt",
                "user".to_string(),
                "group".to_string(),
                0o644,
                3,
                128 * 1024 * 1024,
            ).await
        });

        let handle2 = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            service2.handle_get_file_info("/file.txt").await
        });

        let create_result = handle1.await.unwrap();
        let read_result = handle2.await.unwrap();

        // Create should succeed
        assert!(create_result.is_ok());

        // Read might succeed or fail depending on timing
        // After create completes, file should exist
        assert!(service.handle_get_file_info("/file.txt").await.is_ok());
    }

    #[tokio::test]
    async fn test_concurrent_delete_same_file() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create a file
        service.handle_create(
            "/file.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Concurrently delete same file
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_delete("/file.txt", false).await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_delete("/file.txt", false).await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        // One should succeed, one should fail gracefully
        let success_count = [result1.is_ok(), result2.is_ok()].iter().filter(|&&x| x).count();
        assert_eq!(success_count, 1, "Exactly one delete should succeed");

        // File should not exist
        assert!(service.handle_get_file_info("/file.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_concurrent_delete_different_files() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create two files
        service.handle_create(
            "/file1.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();
        service.handle_create(
            "/file2.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Concurrently delete different files
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_delete("/file1.txt", false).await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_delete("/file2.txt", false).await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        // Both deletes should succeed
        assert!(result1.is_ok());
        assert!(result2.is_ok());

        // Both files should not exist
        assert!(service.handle_get_file_info("/file1.txt").await.is_err());
        assert!(service.handle_get_file_info("/file2.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_delete_while_reading() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create a file
        service.handle_create(
            "/file.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Concurrently delete and read
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_delete("/file.txt", false).await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_get_file_info("/file.txt").await
        });

        let delete_result = handle1.await.unwrap();
        let _read_result = handle2.await.unwrap();

        // Delete should succeed
        assert!(delete_result.is_ok());

        // Read might succeed or fail depending on timing
        // After delete, file should not exist
        assert!(service.handle_get_file_info("/file.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_delete_directory_while_creating_files() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create a directory
        service.handle_mkdir("/dir", "user".to_string(), "group".to_string(), 0o755).await.unwrap();

        // Concurrently delete directory and create file in it
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_delete("/dir", true).await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_create(
                "/dir/file.txt",
                "user".to_string(),
                "group".to_string(),
                0o644,
                3,
                128 * 1024 * 1024,
            ).await
        });

        let delete_result = handle1.await.unwrap();
        let create_result = handle2.await.unwrap();

        // One should succeed, one should fail
        let success_count = [delete_result.is_ok(), create_result.is_ok()].iter().filter(|&&x| x).count();
        assert_eq!(success_count, 1, "Exactly one operation should succeed");
    }

    // ========================================================================
    // DataNode Failure Tests
    // ========================================================================

    #[tokio::test]
    async fn test_insufficient_datanodes() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager.clone(),
            placement_policy,
        );

        // Register only 2 datanodes, but replication factor is 3
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn2".to_string(),
                hostname: "host2".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create a file - should fail because we need 3 datanodes but only have 2
        let result = service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3, // Replication factor 3
            128 * 1024 * 1024,
        ).await;

        // Creation should succeed (metadata operation)
        assert!(result.is_ok());

        // But adding a block should fail due to insufficient datanodes
        let add_block_result = service.handle_add_block("/test.txt").await;
        assert!(add_block_result.is_err());
    }

    #[tokio::test]
    async fn test_read_with_datanode_failure() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace,
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register 3 datanodes
        for i in 1..=3 {
            datanode_manager.register(
                crate::metadata::block_manager::DatanodeId {
                    uuid: format!("dn{}", i),
                    hostname: format!("host{}", i),
                    rpc_port: 9867,
                    data_port: 50010,
                    http_port: 50075,
                    supported_transports: vec!["tcp".to_string()],
                    rdma_port: 0,
                },
                1_000_000_000,
            ).unwrap();
        }

        // All 3 datanodes should be alive initially
        assert_eq!(datanode_manager.get_live_datanodes().len(), 3);
        assert_eq!(datanode_manager.get_dead_datanodes().len(), 0);

        // Mark dn1 as dead
        datanode_manager.mark_datanode_dead_for_test("dn1");

        // Verify failure detection
        let live_datanodes = datanode_manager.get_live_datanodes();
        let dead_datanodes = datanode_manager.get_dead_datanodes();
        assert_eq!(live_datanodes.len(), 2);
        assert_eq!(dead_datanodes.len(), 1);
        assert_eq!(dead_datanodes[0].id.uuid, "dn1");

        // Live datanodes should only be dn2 and dn3
        let live_uuids: Vec<String> = live_datanodes.iter().map(|dn| dn.id.uuid.clone()).collect();
        assert!(live_uuids.contains(&"dn2".to_string()));
        assert!(live_uuids.contains(&"dn3".to_string()));
        assert!(!live_uuids.contains(&"dn1".to_string()));

        // Client would select datanodes for read, should only get live ones
        let selected = datanode_manager.select_datanodes(2).unwrap();
        assert_eq!(selected.len(), 2);

        // Selected should not include dead dn1
        for node in &selected {
            assert_ne!(node.uuid, "dn1", "Dead datanode should not be selected");
        }
    }

    #[tokio::test]
    async fn test_write_with_datanode_failure_recovery() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace,
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register 4 datanodes (one extra for replacement)
        for i in 1..=4 {
            datanode_manager.register(
                crate::metadata::block_manager::DatanodeId {
                    uuid: format!("dn{}", i),
                    hostname: format!("host{}", i),
                    rpc_port: 9867,
                    data_port: 50010,
                    http_port: 50075,
                    supported_transports: vec!["tcp".to_string()],
                    rdma_port: 0,
                },
                1_000_000_000,
            ).unwrap();
        }

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Add a block
        let block = service.handle_add_block("/test.txt").await.unwrap();
        let original_locations = block.locations.clone();

        // Simulate one datanode failing during write
        let failed_datanode = &original_locations[0];

        // Client calls get_additional_datanode to get replacement
        let existing: Vec<crate::metadata::block_manager::DatanodeId> = original_locations.clone();
        let excluded: Vec<crate::metadata::block_manager::DatanodeId> = vec![failed_datanode.clone()];

        let replacement_block = service.handle_get_additional_datanode(
            block.block_id,
            block.generation_stamp,
            &existing,
            &excluded,
            1,
        ).await.unwrap();

        // Replacement block should have original + new datanodes
        assert!(replacement_block.locations.len() >= 3);

        // New datanode should not be the failed one
        let new_locations: Vec<String> = replacement_block.locations
            .iter()
            .map(|loc| loc.uuid.clone())
            .collect();

        // Should have at least one datanode that wasn't in the original list
        let has_new_datanode = new_locations.iter()
            .any(|uuid| !existing.iter().any(|orig| &orig.uuid == uuid));

        assert!(has_new_datanode || new_locations.len() > existing.len(),
                "Should have added a new datanode for pipeline recovery");
    }

    // ========================================================================
    // Truncate Operation Tests
    // ========================================================================

    #[tokio::test]
    async fn test_truncate_to_shorter_length() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create a file with a block
        let block_size = 128 * 1024 * 1024; // 128MB
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            block_size,
        ).await.unwrap();

        service.handle_add_block("/test.txt").await.unwrap();

        // Simulate a full block
        let inode = namespace.lookup("/test.txt").await.unwrap();
        {
            let mut inode_guard = inode.write().await;
            if let Some(blocks) = &mut inode_guard.blocks {
                if let Some(block) = blocks.last_mut() {
                    block.num_bytes = block_size; // Full block
                }
            }
        }

        // Truncate to half the block size
        let new_length = block_size / 2;
        close_file(&service, "/test.txt").await;
        let result = service.handle_truncate("/test.txt", new_length).await.unwrap();
        assert!(result);

        // Verify the file length
        let file_info = service.handle_get_file_info("/test.txt").await.unwrap();
        assert_eq!(file_info.length, new_length);

        // Verify block was updated
        let inode_guard = inode.read().await;
        assert_eq!(inode_guard.blocks.as_ref().unwrap().len(), 1);
        assert_eq!(inode_guard.blocks.as_ref().unwrap()[0].num_bytes, new_length);
    }

    #[tokio::test]
    async fn test_truncate_to_same_length() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        service.handle_add_block("/test.txt").await.unwrap();

        let inode = namespace.lookup("/test.txt").await.unwrap();
        let current_length = 64 * 1024 * 1024;
        {
            let mut inode_guard = inode.write().await;
            if let Some(blocks) = &mut inode_guard.blocks {
                if let Some(block) = blocks.last_mut() {
                    block.num_bytes = current_length;
                }
            }
        }

        // Truncate to same length (no-op)
        close_file(&service, "/test.txt").await;
        let result = service.handle_truncate("/test.txt", current_length).await.unwrap();
        assert!(result);

        // Verify nothing changed
        let file_info = service.handle_get_file_info("/test.txt").await.unwrap();
        assert_eq!(file_info.length, current_length);
    }

    #[tokio::test]
    async fn test_truncate_beyond_length_fails() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        service.handle_add_block("/test.txt").await.unwrap();

        let inode = namespace.lookup("/test.txt").await.unwrap();
        let current_length = 64 * 1024 * 1024;
        {
            let mut inode_guard = inode.write().await;
            if let Some(blocks) = &mut inode_guard.blocks {
                if let Some(block) = blocks.last_mut() {
                    block.num_bytes = current_length;
                }
            }
        }

        // Try to truncate beyond current length - should fail
        let result = service.handle_truncate("/test.txt", current_length + 1000).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_truncate_removes_complete_blocks() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create a file with multiple blocks
        let block_size = 128 * 1024 * 1024;
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            block_size,
        ).await.unwrap();

        // Add 3 blocks
        service.handle_add_block("/test.txt").await.unwrap();
        service.handle_add_block("/test.txt").await.unwrap();
        service.handle_add_block("/test.txt").await.unwrap();

        let inode = namespace.lookup("/test.txt").await.unwrap();
        {
            let mut inode_guard = inode.write().await;
            if let Some(blocks) = &mut inode_guard.blocks {
                // Make all blocks full
                for block in blocks.iter_mut() {
                    block.num_bytes = block_size;
                }
            }
        }

        // Verify we have 3 blocks
        {
            let inode_guard = inode.read().await;
            assert_eq!(inode_guard.blocks.as_ref().unwrap().len(), 3);
        }

        // Truncate to 1.5 blocks
        let new_length = block_size + (block_size / 2);
        close_file(&service, "/test.txt").await;
        let result = service.handle_truncate("/test.txt", new_length).await.unwrap();
        assert!(result);

        // Verify we now have 2 blocks
        let inode_guard = inode.read().await;
        assert_eq!(inode_guard.blocks.as_ref().unwrap().len(), 2);

        // First block should be full, second should be half
        assert_eq!(inode_guard.blocks.as_ref().unwrap()[0].num_bytes, block_size);
        assert_eq!(inode_guard.blocks.as_ref().unwrap()[1].num_bytes, block_size / 2);
    }

    #[tokio::test]
    async fn test_truncate_to_zero() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create a file with blocks
        let block_size = 128 * 1024 * 1024;
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            block_size,
        ).await.unwrap();

        service.handle_add_block("/test.txt").await.unwrap();
        service.handle_add_block("/test.txt").await.unwrap();

        // Set block sizes
        let inode = namespace.lookup("/test.txt").await.unwrap();
        {
            let mut inode_guard = inode.write().await;
            if let Some(blocks) = &mut inode_guard.blocks {
                for block in blocks.iter_mut() {
                    block.num_bytes = block_size / 2; // Half full blocks
                }
            }
        }

        // Truncate to zero
        close_file(&service, "/test.txt").await;
        let result = service.handle_truncate("/test.txt", 0).await.unwrap();
        assert!(result);

        // Verify all blocks were removed
        let inode_guard = inode.read().await;
        assert!(inode_guard.blocks.as_ref().unwrap().is_empty());

        // Verify file length is zero
        let file_info = service.handle_get_file_info("/test.txt").await.unwrap();
        assert_eq!(file_info.length, 0);
    }

    #[tokio::test]
    async fn test_truncate_nonexistent_file() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager,
            placement_policy,
        );

        // Try to truncate non-existent file
        let result = service.handle_truncate("/nonexistent.txt", 100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_truncate_directory_fails() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager,
            placement_policy,
        );

        // Create a directory
        service.handle_mkdir("/dir", "user".to_string(), "group".to_string(), 0o755).await.unwrap();

        // Try to truncate directory - should fail
        let result = service.handle_truncate("/dir", 0).await;
        assert!(result.is_err());
    }

    // ========================================================================
    // Concat Operation Tests
    // ========================================================================

    #[tokio::test]
    async fn test_concat_multiple_files() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        let block_size = 128 * 1024 * 1024;

        // Create target file
        service.handle_create(
            "/target.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            block_size,
        ).await.unwrap();
        service.handle_add_block("/target.txt").await.unwrap();

        // Create source files
        for i in 1..=3 {
            let path = format!("/source{}.txt", i);
            service.handle_create(
                &path,
                "user".to_string(),
                "group".to_string(),
                0o644,
                1,
                block_size,
            ).await.unwrap();
            service.handle_add_block(&path).await.unwrap();
        }

        // Concat source files into target
        let sources = vec![
            "/source1.txt".to_string(),
            "/source2.txt".to_string(),
            "/source3.txt".to_string(),
        ];

        for p in ["/target.txt"].iter().copied().chain(sources.iter().map(|s| s.as_str())) {
            close_file(&service, p).await;
        }
        let result = service.handle_concat("/target.txt", &sources).await;
        assert!(result.is_ok());

        // Verify target file has all blocks (1 original + 3 from sources = 4 total)
        let target_inode = namespace.lookup("/target.txt").await.unwrap();
        let target_guard = target_inode.read().await;
        assert_eq!(target_guard.blocks.as_ref().unwrap().len(), 4);

        // Verify source files were deleted
        for i in 1..=3 {
            let path = format!("/source{}.txt", i);
            assert!(namespace.lookup(&path).await.is_err());
        }
    }

    #[tokio::test]
    async fn test_concat_empty_sources_fails() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager,
            placement_policy,
        );

        // Create target file
        service.handle_create(
            "/target.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Try to concat with empty sources - should fail
        let result = service.handle_concat("/target.txt", &[]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_concat_nonexistent_source_fails() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create target file
        service.handle_create(
            "/target.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Try to concat with non-existent source
        let sources = vec!["/nonexistent.txt".to_string()];
        let result = service.handle_concat("/target.txt", &sources).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_concat_different_block_sizes_fails() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create target file with 128MB blocks
        service.handle_create(
            "/target.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Create source file with 64MB blocks
        service.handle_create(
            "/source.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            64 * 1024 * 1024,
        ).await.unwrap();

        // Try to concat - should fail due to different block sizes
        let sources = vec!["/source.txt".to_string()];
        let result = service.handle_concat("/target.txt", &sources).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_concat_directory_fails() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create a directory and a file
        service.handle_mkdir("/dir", "user".to_string(), "group".to_string(), 0o755).await.unwrap();
        service.handle_create(
            "/file.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Try to concat directory as target - should fail
        let sources = vec!["/file.txt".to_string()];
        let result = service.handle_concat("/dir", &sources).await;
        assert!(result.is_err());
    }

    // ========================================================================
    // Idempotency Tests
    // ========================================================================

    #[tokio::test]
    async fn test_create_idempotency() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager,
            placement_policy,
        );

        // Create a file
        let result1 = service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await;
        assert!(result1.is_ok());

        // Retry create - should fail with already exists
        let result2 = service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await;
        assert!(result2.is_err());

        // File should still exist and be intact
        let info = service.handle_get_file_info("/test.txt").await.unwrap();
        assert_eq!(info.path, "/test.txt");
    }

    #[tokio::test]
    async fn test_delete_idempotency() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager,
            placement_policy,
        );

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Delete the file
        let result1 = service.handle_delete("/test.txt", false).await;
        assert!(result1.is_ok());

        // Retry delete - should fail (file doesn't exist)
        let result2 = service.handle_delete("/test.txt", false).await;
        assert!(result2.is_err());

        // File should not exist
        assert!(service.handle_get_file_info("/test.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_rename_idempotency() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager,
            placement_policy,
        );

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Rename the file
        let result1 = service.handle_rename("/test.txt", "/renamed.txt").await;
        assert!(result1.is_ok());

        // Retry rename - should fail (source doesn't exist)
        let result2 = service.handle_rename("/test.txt", "/renamed.txt").await;
        assert!(result2.is_err());

        // Renamed file should exist
        assert!(service.handle_get_file_info("/renamed.txt").await.is_ok());
        // Original should not exist
        assert!(service.handle_get_file_info("/test.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_truncate_idempotency() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create a file with data
        let block_size = 128 * 1024 * 1024;
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            block_size,
        ).await.unwrap();

        service.handle_add_block("/test.txt").await.unwrap();

        let inode = namespace.lookup("/test.txt").await.unwrap();
        {
            let mut inode_guard = inode.write().await;
            if let Some(blocks) = &mut inode_guard.blocks {
                if let Some(block) = blocks.last_mut() {
                    block.num_bytes = block_size;
                }
            }
        }

        // Truncate the file
        let new_length = block_size / 2;
        close_file(&service, "/test.txt").await;
        let result1 = service.handle_truncate("/test.txt", new_length).await;
        assert!(result1.is_ok());

        // Retry truncate to same length - should succeed (idempotent)
        let result2 = service.handle_truncate("/test.txt", new_length).await;
        assert!(result2.is_ok());

        // File length should be the same
        let info = service.handle_get_file_info("/test.txt").await.unwrap();
        assert_eq!(info.length, new_length);
    }

    #[tokio::test]
    async fn test_mkdir_idempotency() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager,
            placement_policy,
        );

        // Create a directory
        let result1 = service.handle_mkdir("/dir", "user".to_string(), "group".to_string(), 0o755).await;
        assert!(result1.is_ok());

        // Retry mkdir - should fail (already exists)
        let result2 = service.handle_mkdir("/dir", "user".to_string(), "group".to_string(), 0o755).await;
        assert!(result2.is_err());

        // Directory should still exist
        let info = service.handle_get_file_info("/dir").await.unwrap();
        assert!(info.is_dir);
    }

    #[tokio::test]
    async fn test_append_idempotency() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Appending to an empty file allocates nothing (the blocks come from
        // the client's addBlock calls) and opens it; a second append while
        // it is open is refused, as in HDFS, and works again once closed.
        close_file(&service, "/test.txt").await;
        service.handle_append("/test.txt", false).await.unwrap();
        let err = service.handle_append("/test.txt", false).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::AlreadyExists);
        close_file(&service, "/test.txt").await;
        service.handle_append("/test.txt", false).await.unwrap();
        let inode = namespace.lookup("/test.txt").await.unwrap();
        assert!(inode.read().await.blocks.as_ref().unwrap().is_empty());
        assert_eq!(block_manager.get_total_blocks(), 0);
    }

    #[tokio::test]
    async fn test_complete_idempotency() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Complete the file
        let result1 = service.handle_complete("/test.txt").await;
        assert!(result1.is_ok());

        // Retry complete - should succeed (idempotent)
        let result2 = service.handle_complete("/test.txt").await;
        assert!(result2.is_ok());

        // File should still exist
        assert!(service.handle_get_file_info("/test.txt").await.is_ok());
    }

    // ========================================================================
    // Namespace / block-manager consistency
    // ========================================================================

    /// Service with `num_datanodes` registered datanodes and replication 1.
    fn service_with_datanodes(
        num_datanodes: usize,
    ) -> (ClientNamenodeService, Arc<Namespace>, Arc<BlockManager>) {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        for i in 0..num_datanodes {
            datanode_manager
                .register(DatanodeId::new(format!("dn{}", i), format!("host{}", i), 9867), 1 << 40)
                .unwrap();
        }
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));
        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager,
            placement_policy,
        );
        (service, namespace, block_manager)
    }

    /// A service whose leases expire after `soft` / `hard`.
    fn service_with_leases(soft_ms: u64, hard_ms: u64) -> (ClientNamenodeService, Arc<BlockManager>) {
        let (service, _, blocks) = service_with_datanodes(1);
        let leases = LeaseManager::new(
            std::time::Duration::from_millis(soft_ms),
            std::time::Duration::from_millis(hard_ms),
        );
        (service.with_leases(Arc::new(leases)), blocks)
    }

    async fn open_as(service: &ClientNamenodeService, path: &str, client: &str) {
        service
            .handle_create_with(path, "u".into(), "g".into(), 0o644, 1, 1024, false, false, client)
            .await
            .unwrap();
    }

    fn hadoop_class(status: &Status) -> &str {
        status.metadata().get("x-hadoop-exception").and_then(|v| v.to_str().ok()).unwrap_or("")
    }

    #[tokio::test]
    async fn only_the_lease_holder_may_write() {
        let (service, blocks) = service_with_leases(60_000, 1_200_000);
        open_as(&service, "/f", "alice").await;
        assert!(!service.handle_is_file_closed("/f").await.unwrap());

        let e = service.handle_add_block_as("/f", "bob").await.unwrap_err();
        assert_eq!(hadoop_class(&e), "org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException");
        let e = service.handle_complete_as("/f", "bob").await.unwrap_err();
        assert_eq!(hadoop_class(&e), "org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException");
        // Another writer cannot create it (overwrite or not) or append to it.
        let e = service
            .handle_create_with("/f", "u".into(), "g".into(), 0o644, 1, 1024, false, true, "bob")
            .await
            .unwrap_err();
        assert_eq!(hadoop_class(&e), "org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException");
        assert!(service.handle_append_as("/f", true, "bob").await.is_err());

        let b = service.handle_add_block_as("/f", "alice").await.unwrap();
        service.handle_commit_block_as("/f", b.block_id, 10, "alice").await.unwrap();
        blocks.report_block(DatanodeId::new("dn0".into(), "h".into(), 1), BlockId::new(b.block_id, b.generation_stamp), 10);
        assert!(service.handle_complete_as("/f", "alice").await.unwrap());
        assert!(service.handle_is_file_closed("/f").await.unwrap());
        // A retried complete succeeds; writing to a closed file does not.
        assert!(service.handle_complete_as("/f", "alice").await.unwrap());
        let e = service.handle_add_block_as("/f", "alice").await.unwrap_err();
        assert_eq!(hadoop_class(&e), "org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException");
        assert_eq!(service.leases.open_file_count(), 0);
    }

    #[tokio::test]
    async fn after_the_soft_limit_another_client_takes_over() {
        let (service, _) = service_with_leases(100, 60_000);
        open_as(&service, "/f", "alice").await;
        service.handle_add_block_as("/f", "alice").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        // Alice stopped renewing: Bob's create (overwrite) recovers the file
        // first. The unfinished block is dropped, and Bob gets a new file.
        service
            .handle_create_with("/f", "u".into(), "g".into(), 0o644, 1, 1024, false, true, "bob")
            .await
            .unwrap();
        let st = service.handle_list_open_files().await;
        assert_eq!(st.len(), 1);
        assert_eq!((st[0].1.as_str(), st[0].2.as_str()), ("/f", "bob"));
        let e = service.handle_add_block_as("/f", "alice").await.unwrap_err();
        assert_eq!(hadoop_class(&e), "org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException");
    }

    #[tokio::test]
    async fn renewals_keep_the_lease() {
        let (service, _) = service_with_leases(100, 60_000);
        open_as(&service, "/f", "alice").await;
        for _ in 0..4 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            service.handle_renew_lease("alice");
        }
        let e = service.handle_append_as("/f", true, "bob").await.unwrap_err();
        assert_eq!(hadoop_class(&e), "org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException");
    }

    #[tokio::test]
    async fn the_hard_limit_recovers_files_keeping_only_finished_blocks() {
        let (service, blocks) = service_with_leases(50, 100);
        let dn = DatanodeId::new("dn0".into(), "h".into(), 1);
        // /done: its last block was finished by a DataNode (reported, 700
        // bytes) but the writer died before complete().
        open_as(&service, "/done", "alice").await;
        let b1 = service.handle_add_block_as("/done", "alice").await.unwrap();
        service.handle_commit_block_as("/done", b1.block_id, 1024, "alice").await.unwrap();
        blocks.report_block(dn.clone(), BlockId::new(b1.block_id, b1.generation_stamp), 1024);
        let b2 = service.handle_add_block_as("/done", "alice").await.unwrap();
        blocks.report_block(dn.clone(), BlockId::new(b2.block_id, b2.generation_stamp), 700);
        // /torn: the writer died mid-block; no DataNode finished it.
        open_as(&service, "/torn", "alice").await;
        let t1 = service.handle_add_block_as("/torn", "alice").await.unwrap();
        // Readers see only finished blocks meanwhile.
        assert_eq!(service.handle_get_located_blocks("/done", 0, u64::MAX).await.unwrap().file_length, 1024);

        assert_eq!(service.check_leases().await, 0, "not yet past the hard limit");
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert_eq!(service.check_leases().await, 2);

        assert!(service.handle_is_file_closed("/done").await.unwrap());
        let done = service.handle_get_located_blocks("/done", 0, u64::MAX).await.unwrap();
        assert_eq!((done.file_length, done.blocks.len(), done.under_construction), (1724, 2, false));
        assert!(service.handle_is_file_closed("/torn").await.unwrap());
        assert_eq!(service.handle_get_file_info("/torn").await.unwrap().length, 0);
        assert!(blocks.get_block_info(&BlockId::new(t1.block_id, t1.generation_stamp)).is_none());
        assert_eq!(service.leases.open_file_count(), 0);
    }

    #[tokio::test]
    async fn recover_lease_closes_now_and_delete_releases_the_lease() {
        let (service, _) = service_with_leases(60_000, 1_200_000);
        open_as(&service, "/r", "alice").await;
        assert!(service.handle_recover_lease("/r", "bob").await.unwrap());
        assert!(service.handle_is_file_closed("/r").await.unwrap());
        assert!(service.handle_recover_lease("/r", "bob").await.unwrap(), "already closed");

        open_as(&service, "/d", "alice").await;
        assert_eq!(service.leases.open_file_count(), 1);
        service.handle_delete("/d", false).await.unwrap();
        assert_eq!(service.leases.open_file_count(), 0);
        assert!(service.handle_recover_lease("/missing", "bob").await.is_err());
    }

    async fn create(service: &ClientNamenodeService, path: &str, block_size: u64) {
        service
            .handle_create(path, "u".to_string(), "g".to_string(), 0o644, 1, block_size)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_recursive_delete_releases_descendant_blocks() {
        let (service, namespace, block_manager) = service_with_datanodes(1);
        let inodes_before = namespace.inode_count();

        service.handle_mkdir("/d", "u".to_string(), "g".to_string(), 0o755).await.unwrap();
        service.handle_mkdir("/d/sub", "u".to_string(), "g".to_string(), 0o755).await.unwrap();
        for path in ["/d/a", "/d/sub/b"] {
            create(&service, path, 1024).await;
            service.handle_add_block(path).await.unwrap();
            service.handle_add_block(path).await.unwrap();
        }
        create(&service, "/keep", 1024).await;
        service.handle_add_block("/keep").await.unwrap();
        assert_eq!(block_manager.get_total_blocks(), 5);

        assert!(service.handle_delete("/d", false).await.is_err());
        service.handle_delete("/d", true).await.unwrap();

        assert_eq!(block_manager.get_total_blocks(), 1);
        assert_eq!(namespace.inode_count(), inodes_before + 1);

        // Recreating the same paths yields fresh, empty files.
        service.handle_mkdir("/d", "u".to_string(), "g".to_string(), 0o755).await.unwrap();
        service.handle_mkdir("/d/sub", "u".to_string(), "g".to_string(), 0o755).await.unwrap();
        create(&service, "/d/sub/b", 1024).await;
        assert_eq!(service.handle_get_file_info("/d/sub/b").await.unwrap().length, 0);
        assert!(service.handle_get_block_locations("/d/sub/b", 0, 1 << 20).await.unwrap().is_empty());
        assert_eq!(service.handle_get_listing("/d").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_concat_moves_block_ownership_and_rejects_self() {
        let (service, namespace, block_manager) = service_with_datanodes(1);
        create(&service, "/t", 1024).await;
        create(&service, "/s", 1024).await;
        service.handle_add_block("/t").await.unwrap();
        service.handle_add_block("/s").await.unwrap();
        // Open files cannot be concatenated.
        assert!(service.handle_concat("/t", &["/s".to_string()]).await.is_err());
        close_file(&service, "/t").await;
        close_file(&service, "/s").await;

        // Concatenating a file onto itself must fail, not deadlock or delete it.
        let res = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            service.handle_concat("/t", &["/t".to_string()]),
        )
        .await
        .expect("concat onto itself must not deadlock");
        assert!(res.is_err());
        assert!(namespace.lookup("/t").await.is_ok());

        let dup = ["/s".to_string(), "/s".to_string()];
        assert!(service.handle_concat("/t", &dup).await.is_err());

        service.handle_concat("/t", &["/s".to_string()]).await.unwrap();
        let t_id = namespace.lookup("/t").await.unwrap().read().await.id;
        assert_eq!(block_manager.get_file_blocks(t_id).unwrap().len(), 2);

        // Deleting the target releases the moved block as well.
        service.handle_delete("/t", false).await.unwrap();
        assert_eq!(block_manager.get_total_blocks(), 0);
    }

    // ========================================================================
    // getBlockLocations: real offsets and range filtering
    // ========================================================================

    /// Create `path` with blocks of the given sizes (block_size 128).
    async fn file_with_block_sizes(
        service: &ClientNamenodeService,
        namespace: &Namespace,
        path: &str,
        sizes: &[u64],
    ) {
        create(service, path, 128).await;
        for _ in sizes {
            service.handle_add_block(path).await.unwrap();
        }
        let inode = namespace.lookup(path).await.unwrap();
        let mut guard = inode.write().await;
        for (block, size) in guard.blocks.as_mut().unwrap().iter_mut().zip(sizes) {
            block.num_bytes = *size;
        }
        drop(guard);
        close_file(service, path).await;
    }

    fn offsets(blocks: &[LocatedBlock]) -> Vec<u64> {
        blocks.iter().map(|b| b.offset).collect()
    }

    #[tokio::test]
    async fn test_get_block_locations_unequal_block_sizes() {
        let (service, namespace, block_manager) = service_with_datanodes(1);
        file_with_block_sizes(&service, &namespace, "/f", &[100, 50, 100, 30]).await;

        let all = service.handle_get_located_blocks("/f", 0, u64::MAX).await.unwrap();
        assert_eq!(offsets(&all.blocks), vec![0, 100, 150, 250]);
        assert_eq!(all.file_length, 280);

        // Replica locations come from the block manager.
        let second = BlockId::new(all.blocks[1].block_id, all.blocks[1].generation_stamp);
        block_manager.add_block_location(second, DatanodeId::new("dn0".to_string(), "host0".to_string(), 9867));
        let span = service.handle_get_located_blocks("/f", 100, 1).await.unwrap();
        // dn-test (the replica close_file reported) and dn0.
        let uuids: Vec<_> = span.blocks[0].locations.iter().map(|l| l.uuid.as_str()).collect();
        assert_eq!(uuids, ["dn-test", "dn0"]);

        // Only blocks overlapping [offset, offset+length) are returned.
        let mid = service.handle_get_located_blocks("/f", 120, 10).await.unwrap();
        assert_eq!(offsets(&mid.blocks), vec![100]);
        let span = service.handle_get_located_blocks("/f", 149, 2).await.unwrap();
        assert_eq!(offsets(&span.blocks), vec![100, 150]);
        let edge = service.handle_get_located_blocks("/f", 100, 50).await.unwrap();
        assert_eq!(offsets(&edge.blocks), vec![100]);
        let tail = service.handle_get_located_blocks("/f", 200, 1000).await.unwrap();
        assert_eq!(offsets(&tail.blocks), vec![150, 250]);
        // HDFS returns the block containing offset even for length 0.
        let zero = service.handle_get_located_blocks("/f", 150, 0).await.unwrap();
        assert_eq!(offsets(&zero.blocks), vec![150]);
        // At or beyond EOF: no blocks.
        assert!(service.handle_get_located_blocks("/f", 280, 10).await.unwrap().blocks.is_empty());
        assert!(service.handle_get_located_blocks("/f", 10_000, 10).await.unwrap().blocks.is_empty());

        // File-level fields describe the whole file regardless of range.
        assert_eq!(mid.file_length, 280);
        let last = mid.last_block.as_ref().unwrap();
        assert_eq!((last.offset, last.num_bytes), (250, 30));
        assert_eq!(last.block_id, all.blocks[3].block_id);
        assert!(mid.is_last_block_complete);
        assert!(!mid.under_construction);

        // The Vec-returning wrapper applies the same filtering.
        let v = service.handle_get_block_locations("/f", 120, 10).await.unwrap();
        assert_eq!(offsets(&v), vec![100]);
    }

    #[tokio::test]
    async fn test_get_block_locations_empty_file() {
        let (service, _, _) = service_with_datanodes(1);
        create(&service, "/empty", 128).await;
        let located = service.handle_get_located_blocks("/empty", 0, u64::MAX).await.unwrap();
        assert!(located.blocks.is_empty());
        assert_eq!(located.file_length, 0);
        assert!(located.last_block.is_none());
        assert!(!located.is_last_block_complete);
    }

    #[tokio::test]
    async fn test_block_offsets_after_truncate_concat_and_append() {
        let (service, namespace, block_manager) = service_with_datanodes(1);
        file_with_block_sizes(&service, &namespace, "/t", &[100, 50, 100]).await;

        // Truncate inside the second block: [100, 50, 100] -> [100, 20].
        service.handle_truncate("/t", 120).await.unwrap();
        let located = service.handle_get_located_blocks("/t", 0, u64::MAX).await.unwrap();
        assert_eq!(offsets(&located.blocks), vec![0, 100]);
        assert_eq!(located.blocks[1].num_bytes, 20);
        assert_eq!(located.file_length, 120);
        // The dropped block is released from the block manager.
        assert_eq!(block_manager.get_total_blocks(), 2);

        // Concat a source with a partial block: offsets keep accumulating.
        file_with_block_sizes(&service, &namespace, "/s", &[70]).await;
        service.handle_concat("/t", &["/s".to_string()]).await.unwrap();
        let located = service.handle_get_located_blocks("/t", 0, u64::MAX).await.unwrap();
        assert_eq!(offsets(&located.blocks), vec![0, 100, 120]);
        assert_eq!(located.file_length, 190);
        let last = located.last_block.unwrap();
        assert_eq!((last.offset, last.num_bytes), (120, 70));
        let hit = service.handle_get_located_blocks("/t", 125, 1).await.unwrap();
        assert_eq!(offsets(&hit.blocks), vec![120]);

        // Appending with a new block after the partial one: the new block
        // starts at the file's real end.
        service.handle_append("/t", true).await.unwrap();
        let b = service.handle_add_block("/t").await.unwrap();
        service.handle_commit_block("/t", b.block_id, 5).await.unwrap();
        // The block being written stays hidden until the file is closed.
        let open = service.handle_get_located_blocks("/t", 0, u64::MAX).await.unwrap();
        assert_eq!((offsets(&open.blocks), open.under_construction), (vec![0, 100, 120], true));
        close_file(&service, "/t").await;
        let located = service.handle_get_located_blocks("/t", 0, u64::MAX).await.unwrap();
        assert_eq!(offsets(&located.blocks), vec![0, 100, 120, 190]);
        assert_eq!(located.file_length, 195);

        // Truncate across unequal blocks to the exact boundary.
        service.handle_truncate("/t", 100).await.unwrap();
        let located = service.handle_get_located_blocks("/t", 0, u64::MAX).await.unwrap();
        assert_eq!(offsets(&located.blocks), vec![0]);
        assert_eq!(located.file_length, 100);
    }
}

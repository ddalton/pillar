//! Block recovery (HDFS `BlockRecoveryWorker`): the NameNode picks one
//! DataNode of a block's pipeline to settle a block whose writer is gone.
//! It asks every DataNode of the pipeline what replica it holds, agrees on
//! a length, has each replica cut to it, stamped with the recovery's
//! generation stamp and finalized, and reports the outcome to the NameNode
//! (`commitBlockSynchronization`), which closes the file.
//!
//! The length is a finalized replica's, if one exists (the block was
//! complete), else the shortest replica being written: every byte below it
//! is on all of them. The DataNode-to-DataNode calls use Pillar's data
//! protocol ([`crate::transport::wire::replica_info`] and
//! [`crate::transport::wire::recover_replica`]) in place of HDFS's
//! InterDatanodeProtocol.

use tracing::{info, warn};

use pillar_common::hdfs::block_token::mode;

use crate::storage::block_storage::BlockStorage;

use crate::transport::wire;

/// A block to recover, from the NameNode's BlockRecoveryCommand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveringBlock {
    pub block_id: u64,
    /// The block's generation stamp at the NameNode: older replicas are stale.
    pub generation_stamp: u64,
    /// The stamp the recovered replicas take.
    pub new_generation_stamp: u64,
    /// The pipeline: DataNode uuid and data address.
    pub targets: Vec<(String, String)>,
}

/// The outcome: the agreed length and the DataNodes that finalized their
/// replica at it, or `delete` if no DataNode had a replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recovered {
    pub length: u64,
    pub holders: Vec<String>,
    pub delete: bool,
}

/// `storage` mints the block access tokens the other DataNodes ask for,
/// and secures the connections to them as this DataNode requires.
pub async fn recover(b: &RecoveringBlock, storage: &BlockStorage) -> Result<Recovered, String> {
    // Every replica of the pipeline that is current.
    let mut replicas = Vec::new();
    for (uuid, addr) in &b.targets {
        match wire::replica_info(storage, addr, b.block_id, storage.tokens.mint(b.block_id, mode::READ)).await {
            Ok(Some((finalized, gs, len))) if gs >= b.generation_stamp => replicas.push((uuid, addr, finalized, len)),
            Ok(Some((_, gs, _))) => info!("Block {} on {} is stale (stamp {})", b.block_id, uuid, gs),
            Ok(None) => info!("Block {}: {} has no replica", b.block_id, uuid),
            Err(e) => warn!("Block {}: {} unreachable: {}", b.block_id, uuid, e),
        }
    }
    if replicas.is_empty() {
        return Ok(Recovered { length: 0, holders: Vec::new(), delete: true });
    }
    let length = match replicas.iter().find(|r| r.2) {
        Some(finalized) => finalized.3,
        None => replicas.iter().map(|r| r.3).min().expect("some replica"),
    };
    let mut holders = Vec::new();
    for (uuid, addr, finalized, len) in replicas {
        let usable = if finalized { len == length } else { len >= length };
        if !usable {
            continue;
        }
        match wire::recover_replica(storage, addr, b.block_id, b.new_generation_stamp, length, storage.tokens.mint(b.block_id, mode::WRITE)).await {
            Ok(_) => holders.push(uuid.clone()),
            Err(e) => warn!("Block {}: recovering the replica on {} failed: {}", b.block_id, uuid, e),
        }
    }
    if holders.is_empty() {
        return Err(format!("block {}: no replica could be recovered", b.block_id));
    }
    info!("Recovered block {} at {} bytes on {:?} (stamp {})", b.block_id, length, holders, b.new_generation_stamp);
    Ok(Recovered { length, holders, delete: false })
}

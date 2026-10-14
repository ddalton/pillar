//! HDFS `BlockListAsLongs`, the legacy (non-buffer) block report encoding
//! carried in `StorageBlockReportProto.blocks`.
//!
//! ```text
//! [ numFinalized, numUnderConstruction,
//!   (blockId, numBytes, genStamp) × numFinalized,
//!   -1, -1, -1,                                     delimiter
//!   (blockId, numBytes, genStamp, replicaState) × numUnderConstruction ]
//! ```
//!
//! Values are unsigned 64-bit on the wire; the delimiter is `u64::MAX`.
//! Replicas being written are reported as under construction (state RBW),
//! so a NameNode that restarted learns where a block being written is and
//! can recover it.

/// HDFS `ReplicaState.RBW`, the state of an under-construction replica.
const REPLICA_STATE_RBW: u64 = 1;

/// One replica in a block report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReportedBlock {
    pub block_id: u64,
    pub num_bytes: u64,
    pub generation_stamp: u64,
    /// Being written (not finalized).
    pub under_construction: bool,
}

const DELIMITER: u64 = u64::MAX;

/// Encode replicas: finalized ones, the delimiter, then those being written.
pub fn encode_block_list(blocks: &[ReportedBlock]) -> Vec<u64> {
    let (uc, finalized): (Vec<&ReportedBlock>, Vec<&ReportedBlock>) = blocks.iter().partition(|b| b.under_construction);
    let mut v = Vec::with_capacity(2 + 3 * finalized.len() + 3 + 4 * uc.len());
    v.push(finalized.len() as u64);
    v.push(uc.len() as u64);
    for b in finalized {
        v.extend_from_slice(&[b.block_id, b.num_bytes, b.generation_stamp]);
    }
    v.extend_from_slice(&[DELIMITER; 3]);
    for b in uc {
        v.extend_from_slice(&[b.block_id, b.num_bytes, b.generation_stamp, REPLICA_STATE_RBW]);
    }
    v
}

/// Decode a block list; under-construction replicas are returned too.
pub fn decode_block_list(longs: &[u64]) -> Result<Vec<ReportedBlock>, String> {
    if longs.len() < 2 {
        return if longs.is_empty() { Ok(Vec::new()) } else { Err("block list shorter than its header".into()) };
    }
    let (finalized, uc) = (longs[0] as usize, longs[1] as usize);
    let overflow = || "block list counts overflow".to_string();
    let finalized_end = finalized.checked_mul(3).and_then(|n| n.checked_add(2)).ok_or_else(overflow)?;
    let need = if uc == 0 {
        finalized_end
    } else {
        uc.checked_mul(4).and_then(|n| n.checked_add(finalized_end + 3)).ok_or_else(overflow)?
    };
    if longs.len() < need {
        return Err(format!("block list of {} longs, header needs {}", longs.len(), need));
    }
    let mut out = Vec::with_capacity(finalized + uc);
    for t in longs[2..finalized_end].chunks_exact(3) {
        out.push(ReportedBlock { block_id: t[0], num_bytes: t[1], generation_stamp: t[2], under_construction: false });
    }
    if uc > 0 {
        let uc_start = finalized_end + 3;
        if longs[uc_start - 3..uc_start] != [DELIMITER; 3] {
            return Err("missing delimiter before under-construction replicas".into());
        }
        for q in longs[uc_start..uc_start + 4 * uc].chunks_exact(4) {
            out.push(ReportedBlock { block_id: q[0], num_bytes: q[1], generation_stamp: q[2], under_construction: true });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_matches_the_hdfs_layout() {
        let blocks = [
            ReportedBlock { block_id: 7, num_bytes: 100, generation_stamp: 1001, under_construction: false },
            ReportedBlock { block_id: 8, num_bytes: 0, generation_stamp: 1002, under_construction: false },
        ];
        let enc = encode_block_list(&blocks);
        assert_eq!(enc, vec![2, 0, 7, 100, 1001, 8, 0, 1002, u64::MAX, u64::MAX, u64::MAX]);
        assert_eq!(decode_block_list(&enc).unwrap(), blocks);
    }

    #[test]
    fn empty_and_truncated_lists() {
        assert!(decode_block_list(&[]).unwrap().is_empty());
        assert!(decode_block_list(&encode_block_list(&[])).unwrap().is_empty());
        assert!(decode_block_list(&[3, 0, 1, 2]).is_err());
        assert!(decode_block_list(&[u64::MAX, 0]).is_err());
    }

    #[test]
    fn under_construction_replicas_follow_the_delimiter() {
        let longs = [1, 1, 5, 10, 900, u64::MAX, u64::MAX, u64::MAX, 6, 20, 901, 1];
        let got = decode_block_list(&longs).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[1], ReportedBlock { block_id: 6, num_bytes: 20, generation_stamp: 901, under_construction: true });
        // And they encode back the same way (state RBW).
        assert_eq!(encode_block_list(&got), longs);
    }
}

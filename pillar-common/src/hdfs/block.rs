// HDFS block abstractions

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockId(pub u64);

impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "blk_{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub block_id: BlockId,
    pub size: u64,
    pub generation_stamp: u64,
    pub num_bytes: u64,
}

impl BlockInfo {
    pub fn new(block_id: BlockId, generation_stamp: u64) -> Self {
        Self {
            block_id,
            size: 0,
            generation_stamp,
            num_bytes: 0,
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_id_creation() {
        let block_id = BlockId(12345);
        assert_eq!(block_id.0, 12345);
    }

    #[test]
    fn test_block_id_display() {
        let block_id = BlockId(67890);
        assert_eq!(format!("{}", block_id), "blk_67890");
    }

    #[test]
    fn test_block_id_equality() {
        let block1 = BlockId(100);
        let block2 = BlockId(100);
        let block3 = BlockId(200);

        assert_eq!(block1, block2);
        assert_ne!(block1, block3);
    }

    #[test]
    fn test_block_info_creation() {
        let block_id = BlockId(1000);
        let gen_stamp = 42;
        let block_info = BlockInfo::new(block_id, gen_stamp);

        assert_eq!(block_info.block_id, block_id);
        assert_eq!(block_info.generation_stamp, gen_stamp);
        assert_eq!(block_info.size, 0);
        assert_eq!(block_info.num_bytes, 0);
    }

    #[test]
    fn test_block_info_with_data() {
        let block_id = BlockId(2000);
        let mut block_info = BlockInfo::new(block_id, 1);

        block_info.size = 1024;
        block_info.num_bytes = 1024;

        assert_eq!(block_info.size, 1024);
        assert_eq!(block_info.num_bytes, 1024);
    }
}

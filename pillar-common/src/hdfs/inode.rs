// HDFS INode utilities

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InodeId(pub u64);

impl InodeId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inode_id_creation() {
        let inode = InodeId::new(100);
        assert_eq!(inode.0, 100);
    }

    #[test]
    fn test_inode_id_equality() {
        let inode1 = InodeId(500);
        let inode2 = InodeId(500);
        let inode3 = InodeId(600);

        assert_eq!(inode1, inode2);
        assert_ne!(inode1, inode3);
    }

    #[test]
    fn test_inode_id_hash_consistency() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(InodeId(1));
        set.insert(InodeId(2));
        set.insert(InodeId(1)); // Duplicate

        assert_eq!(set.len(), 2);
        assert!(set.contains(&InodeId(1)));
        assert!(set.contains(&InodeId(2)));
    }
}

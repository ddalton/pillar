// HDFS replication logic

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationFactor(pub u32);

impl Default for ReplicationFactor {
    fn default() -> Self {
        Self(3) // HDFS default replication factor
    }
}

impl ReplicationFactor {
    pub fn new(factor: u32) -> Self {
        Self(factor.max(1)) // Minimum 1 replica
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_factor_default() {
        let rf = ReplicationFactor::default();
        assert_eq!(rf.as_u32(), 3);
    }

    #[test]
    fn test_replication_factor_creation() {
        let rf = ReplicationFactor::new(5);
        assert_eq!(rf.as_u32(), 5);
    }

    #[test]
    fn test_replication_factor_minimum() {
        // Should not allow zero, minimum is 1
        let rf = ReplicationFactor::new(0);
        assert_eq!(rf.as_u32(), 1);
    }

    #[test]
    fn test_replication_factor_one() {
        let rf = ReplicationFactor::new(1);
        assert_eq!(rf.as_u32(), 1);
    }

    #[test]
    fn test_replication_factor_large() {
        let rf = ReplicationFactor::new(100);
        assert_eq!(rf.as_u32(), 100);
    }

    #[test]
    fn test_replication_factor_equality() {
        let rf1 = ReplicationFactor::new(3);
        let rf2 = ReplicationFactor::new(3);
        let rf3 = ReplicationFactor::new(5);

        assert_eq!(rf1, rf2);
        assert_ne!(rf1, rf3);
    }
}

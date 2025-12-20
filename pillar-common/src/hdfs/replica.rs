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

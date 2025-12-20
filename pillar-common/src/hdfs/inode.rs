// HDFS INode utilities

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InodeId(pub u64);

impl InodeId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

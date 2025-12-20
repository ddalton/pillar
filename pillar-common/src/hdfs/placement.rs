// HDFS block placement policies

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementPolicy {
    RoundRobin,
    Random,
    LocalityAware,
}

impl Default for PlacementPolicy {
    fn default() -> Self {
        Self::RoundRobin
    }
}

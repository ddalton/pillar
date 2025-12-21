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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_placement_policy_default() {
        let policy = PlacementPolicy::default();
        assert_eq!(policy, PlacementPolicy::RoundRobin);
    }

    #[test]
    fn test_placement_policy_variants() {
        let round_robin = PlacementPolicy::RoundRobin;
        let random = PlacementPolicy::Random;
        let locality = PlacementPolicy::LocalityAware;

        assert_ne!(round_robin, random);
        assert_ne!(round_robin, locality);
        assert_ne!(random, locality);
    }

    #[test]
    fn test_placement_policy_clone() {
        let policy1 = PlacementPolicy::LocalityAware;
        let policy2 = policy1.clone();

        assert_eq!(policy1, policy2);
    }

    #[test]
    fn test_placement_policy_debug() {
        let policy = PlacementPolicy::RoundRobin;
        let debug_str = format!("{:?}", policy);
        assert!(debug_str.contains("RoundRobin"));
    }
}

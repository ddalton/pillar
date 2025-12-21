//! Replication strategy selection
//!
//! This module implements adaptive strategy selection based on:
//! - Block size
//! - Target transport capabilities (RDMA vs TCP)
//! - Number of replicas
//! - Available transports
//!
//! Based on OSU HiBD research findings:
//! - **Pipeline**: Baseline, works with any transport
//! - **Parallel**: 2.9x with RDMA, 1.3x with TCP (for 3-way replication)
//! - **Overlapped**: 4.1x with RDMA, 1.5x with TCP (for large blocks >64MB)

use tracing::{debug, info};
use std::sync::Arc;

use super::ReplicationTarget;
use crate::transport::{DataTransport, TransportType};

/// Replication strategy types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationStrategy {
    /// Sequential replication (baseline)
    /// - DN1 → DN2 → DN3
    /// - Works with any transport
    /// - Best for: small blocks, mixed clusters
    Pipeline,

    /// Parallel replication (OSU HiBD)
    /// - DN1 → [DN2, DN3] (concurrent)
    /// - Expected: 2.9x with RDMA, 1.3x with TCP
    /// - Best for: medium blocks with RDMA peers
    Parallel,

    /// Overlapped replication (SOR-HDFS)
    /// - SEDA-based pipelining of disk reads, RDMA writes, and acks
    /// - Expected: 4.1x with RDMA, 1.5x with TCP
    /// - Best for: large blocks (>64MB) with RDMA peers
    Overlapped,
}

impl std::fmt::Display for ReplicationStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicationStrategy::Pipeline => write!(f, "Pipeline"),
            ReplicationStrategy::Parallel => write!(f, "Parallel"),
            ReplicationStrategy::Overlapped => write!(f, "Overlapped"),
        }
    }
}

/// Strategy selector that chooses the best replication strategy
pub struct StrategySelector {
    /// Threshold for large blocks (64 MB)
    /// Blocks larger than this use overlapped strategy with RDMA
    large_block_threshold: u64,

    /// Minimum replicas for parallel strategies
    /// Need at least 2 replicas to benefit from parallelization
    min_replicas_for_parallel: usize,
}

impl StrategySelector {
    /// Create a new strategy selector with default thresholds
    pub fn new() -> Self {
        Self {
            large_block_threshold: 64 * 1024 * 1024, // 64 MB
            min_replicas_for_parallel: 2,
        }
    }

    /// Create with custom thresholds (mainly for testing)
    #[allow(dead_code)]
    pub fn with_thresholds(
        large_block_threshold: u64,
        min_replicas_for_parallel: usize,
    ) -> Self {
        Self {
            large_block_threshold,
            min_replicas_for_parallel,
        }
    }

    /// Select the best replication strategy based on the request parameters
    pub fn select_strategy(
        &self,
        targets: &[ReplicationTarget],
        block_size: u64,
        transports: &[Arc<dyn DataTransport>],
    ) -> ReplicationStrategy {
        // If not enough targets, always use pipeline
        if targets.len() < self.min_replicas_for_parallel {
            debug!(
                "Using Pipeline strategy: only {} target(s)",
                targets.len()
            );
            return ReplicationStrategy::Pipeline;
        }

        // Check if RDMA transport is available
        let has_rdma_transport = transports.iter().any(|t| {
            #[cfg(feature = "rdma")]
            {
                matches!(t.transport_type(), TransportType::Rdma)
            }
            #[cfg(not(feature = "rdma"))]
            {
                false
            }
        });

        // Count how many targets support RDMA
        let rdma_targets = targets.iter().filter(|t| t.supports_rdma()).count();
        let all_targets_rdma = rdma_targets == targets.len();
        let most_targets_rdma = rdma_targets >= (targets.len() * 2 / 3); // >66%

        debug!(
            "Strategy selection: block_size={}, targets={}, rdma_targets={}, has_rdma_transport={}",
            block_size, targets.len(), rdma_targets, has_rdma_transport
        );

        // Decision tree based on OSU HiBD research
        if has_rdma_transport && all_targets_rdma {
            // All targets support RDMA - use best RDMA strategy
            if block_size >= self.large_block_threshold {
                // Large blocks (≥64MB) benefit most from overlapped strategy
                // Expected: 4.1x improvement
                info!(
                    "Selected Overlapped strategy: large block ({:.2} MB), all RDMA targets",
                    block_size as f64 / (1024.0 * 1024.0)
                );
                ReplicationStrategy::Overlapped
            } else {
                // Medium blocks (<64MB) use parallel strategy
                // Expected: 2.9x improvement
                info!(
                    "Selected Parallel strategy: medium block ({:.2} MB), all RDMA targets",
                    block_size as f64 / (1024.0 * 1024.0)
                );
                ReplicationStrategy::Parallel
            }
        } else if has_rdma_transport && most_targets_rdma {
            // Most targets support RDMA - still beneficial to use parallel
            if block_size >= self.large_block_threshold {
                info!(
                    "Selected Overlapped strategy: large block ({:.2} MB), most RDMA targets ({}/{})",
                    block_size as f64 / (1024.0 * 1024.0),
                    rdma_targets,
                    targets.len()
                );
                ReplicationStrategy::Overlapped
            } else {
                info!(
                    "Selected Parallel strategy: medium block ({:.2} MB), most RDMA targets ({}/{})",
                    block_size as f64 / (1024.0 * 1024.0),
                    rdma_targets,
                    targets.len()
                );
                ReplicationStrategy::Parallel
            }
        } else if targets.len() >= 3 {
            // Even with TCP, parallel replication provides benefits
            // TCP parallel: 1.3x improvement
            // TCP overlapped: 1.5x improvement
            if block_size >= self.large_block_threshold {
                info!(
                    "Selected Overlapped strategy: large block ({:.2} MB), TCP-only cluster",
                    block_size as f64 / (1024.0 * 1024.0)
                );
                ReplicationStrategy::Overlapped
            } else {
                info!(
                    "Selected Parallel strategy: {} targets, TCP-only cluster",
                    targets.len()
                );
                ReplicationStrategy::Parallel
            }
        } else {
            // Fallback to pipeline for small clusters or mixed scenarios
            info!(
                "Selected Pipeline strategy: fallback for {} targets (rdma={}/{})",
                targets.len(),
                rdma_targets,
                targets.len()
            );
            ReplicationStrategy::Pipeline
        }
    }

    /// Estimate expected throughput improvement for a strategy
    /// Returns a multiplier relative to baseline pipeline replication
    #[allow(dead_code)]
    pub fn estimate_improvement(
        &self,
        strategy: ReplicationStrategy,
        targets: &[ReplicationTarget],
        block_size: u64,
    ) -> f64 {
        let rdma_targets = targets.iter().filter(|t| t.supports_rdma()).count();
        let all_rdma = rdma_targets == targets.len();

        match strategy {
            ReplicationStrategy::Pipeline => 1.0, // Baseline
            ReplicationStrategy::Parallel => {
                if all_rdma {
                    2.9 // OSU HiBD: 2.9x with RDMA
                } else {
                    1.3 // OSU HiBD: 1.3x with TCP
                }
            }
            ReplicationStrategy::Overlapped => {
                if all_rdma && block_size >= self.large_block_threshold {
                    4.1 // OSU HiBD: 4.1x with RDMA for large blocks
                } else if all_rdma {
                    3.0 // Still good with RDMA for medium blocks
                } else if block_size >= self.large_block_threshold {
                    1.5 // OSU HiBD: 1.5x with TCP for large blocks
                } else {
                    1.2 // Minimal benefit for TCP with medium blocks
                }
            }
        }
    }
}

impl Default for StrategySelector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_rdma_target(uuid: &str) -> ReplicationTarget {
        ReplicationTarget {
            datanode_uuid: uuid.to_string(),
            hostname: format!("{}.example.com", uuid),
            ip_addr: "10.0.0.1".to_string(),
            xfer_port: 50010,
            rdma_port: 50011,
            supported_transports: vec!["tcp".to_string(), "rdma".to_string()],
        }
    }

    fn create_tcp_target(uuid: &str) -> ReplicationTarget {
        ReplicationTarget {
            datanode_uuid: uuid.to_string(),
            hostname: format!("{}.example.com", uuid),
            ip_addr: "10.0.0.2".to_string(),
            xfer_port: 50010,
            rdma_port: 0,
            supported_transports: vec!["tcp".to_string()],
        }
    }

    #[test]
    fn test_strategy_selector_creation() {
        let selector = StrategySelector::new();
        assert_eq!(selector.large_block_threshold, 64 * 1024 * 1024);
        assert_eq!(selector.min_replicas_for_parallel, 2);
    }

    #[test]
    fn test_single_target_uses_pipeline() {
        let selector = StrategySelector::new();
        let targets = vec![create_rdma_target("dn1")];
        let transports: Vec<Arc<dyn DataTransport>> = vec![];

        let strategy = selector.select_strategy(&targets, 1024 * 1024, &transports);
        assert_eq!(strategy, ReplicationStrategy::Pipeline);
    }

    #[test]
    fn test_small_block_tcp_only() {
        let selector = StrategySelector::new();
        let targets = vec![
            create_tcp_target("dn1"),
            create_tcp_target("dn2"),
            create_tcp_target("dn3"),
        ];
        let transports: Vec<Arc<dyn DataTransport>> = vec![];

        // Small block (1 MB) with 3 TCP targets -> Parallel
        let strategy = selector.select_strategy(&targets, 1024 * 1024, &transports);
        assert_eq!(strategy, ReplicationStrategy::Parallel);
    }

    #[test]
    fn test_large_block_tcp_only() {
        let selector = StrategySelector::new();
        let targets = vec![
            create_tcp_target("dn1"),
            create_tcp_target("dn2"),
            create_tcp_target("dn3"),
        ];
        let transports: Vec<Arc<dyn DataTransport>> = vec![];

        // Large block (100 MB) with 3 TCP targets -> Overlapped
        let strategy = selector.select_strategy(&targets, 100 * 1024 * 1024, &transports);
        assert_eq!(strategy, ReplicationStrategy::Overlapped);
    }

    #[test]
    fn test_improvement_estimation() {
        let selector = StrategySelector::new();

        // All RDMA targets
        let rdma_targets = vec![
            create_rdma_target("dn1"),
            create_rdma_target("dn2"),
            create_rdma_target("dn3"),
        ];

        // Parallel with RDMA: 2.9x
        let improvement = selector.estimate_improvement(
            ReplicationStrategy::Parallel,
            &rdma_targets,
            10 * 1024 * 1024,
        );
        assert_eq!(improvement, 2.9);

        // Overlapped with RDMA and large block: 4.1x
        let improvement = selector.estimate_improvement(
            ReplicationStrategy::Overlapped,
            &rdma_targets,
            100 * 1024 * 1024,
        );
        assert_eq!(improvement, 4.1);

        // Pipeline: 1.0x (baseline)
        let improvement = selector.estimate_improvement(
            ReplicationStrategy::Pipeline,
            &rdma_targets,
            10 * 1024 * 1024,
        );
        assert_eq!(improvement, 1.0);
    }

    #[test]
    fn test_tcp_improvement_estimation() {
        let selector = StrategySelector::new();

        // All TCP targets
        let tcp_targets = vec![
            create_tcp_target("dn1"),
            create_tcp_target("dn2"),
            create_tcp_target("dn3"),
        ];

        // Parallel with TCP: 1.3x
        let improvement = selector.estimate_improvement(
            ReplicationStrategy::Parallel,
            &tcp_targets,
            10 * 1024 * 1024,
        );
        assert_eq!(improvement, 1.3);

        // Overlapped with TCP and large block: 1.5x
        let improvement = selector.estimate_improvement(
            ReplicationStrategy::Overlapped,
            &tcp_targets,
            100 * 1024 * 1024,
        );
        assert_eq!(improvement, 1.5);
    }

    #[test]
    fn test_custom_thresholds() {
        // Use 32 MB threshold instead of 64 MB
        let selector = StrategySelector::with_thresholds(32 * 1024 * 1024, 2);

        let targets = vec![
            create_tcp_target("dn1"),
            create_tcp_target("dn2"),
            create_tcp_target("dn3"),
        ];
        let transports: Vec<Arc<dyn DataTransport>> = vec![];

        // 50 MB block (>32 MB threshold) with 3 targets -> Overlapped
        // Even with TCP-only, parallel strategies are used for ≥3 targets
        let strategy = selector.select_strategy(&targets, 50 * 1024 * 1024, &transports);
        assert_eq!(strategy, ReplicationStrategy::Overlapped);

        // 10 MB block (<32 MB threshold) with 3 targets -> Parallel
        let strategy = selector.select_strategy(&targets, 10 * 1024 * 1024, &transports);
        assert_eq!(strategy, ReplicationStrategy::Parallel);
    }
}

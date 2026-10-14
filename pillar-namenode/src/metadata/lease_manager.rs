//! Write leases (HDFS `LeaseManager`).
//!
//! A client holds one lease covering every file it has open for writing; it
//! keeps the lease alive with `renewLease` (the Hadoop client does so every
//! few seconds while any file is open). After the **soft limit** without a
//! renewal another client may take a file over (create with overwrite,
//! append, recoverLease); after the **hard limit** the NameNode recovers
//! the files itself.
//!
//! Leases are not persisted: which files are open, and by whom, is part of
//! the namespace (the edit log). After a restart every open file's holder
//! gets a fresh lease, as in HDFS, so a writer that is still alive carries
//! on and a dead one is recovered after the hard limit.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

/// HDFS `HdfsConstants.LEASE_SOFTLIMIT_PERIOD`.
pub const DEFAULT_SOFT_LIMIT: Duration = Duration::from_secs(60);
/// HDFS `dfs.namenode.lease-hard-limit-sec`.
pub const DEFAULT_HARD_LIMIT: Duration = Duration::from_secs(20 * 60);

struct Lease {
    renewed: Instant,
    /// Open files (inode ids): renames do not affect a lease.
    files: HashSet<u64>,
}

pub struct LeaseManager {
    leases: Mutex<HashMap<String, Lease>>,
    soft_limit: Duration,
    hard_limit: Duration,
}

impl Default for LeaseManager {
    fn default() -> Self {
        Self::new(DEFAULT_SOFT_LIMIT, DEFAULT_HARD_LIMIT)
    }
}

impl LeaseManager {
    pub fn new(soft_limit: Duration, hard_limit: Duration) -> Self {
        Self { leases: Mutex::new(HashMap::new()), soft_limit, hard_limit }
    }

    /// `holder` now has `inode` open (and its lease is renewed).
    pub fn add(&self, holder: &str, inode: u64) {
        let mut leases = self.leases.lock().unwrap();
        let lease = leases
            .entry(holder.to_string())
            .or_insert_with(|| Lease { renewed: Instant::now(), files: HashSet::new() });
        lease.renewed = Instant::now();
        lease.files.insert(inode);
    }

    /// `inode` is closed or deleted; a holder with no open files has no lease.
    pub fn remove(&self, holder: &str, inode: u64) {
        let mut leases = self.leases.lock().unwrap();
        if let Some(lease) = leases.get_mut(holder) {
            lease.files.remove(&inode);
            if lease.files.is_empty() {
                leases.remove(holder);
            }
        }
    }

    /// renewLease. Unknown holders (no open files) are ignored, as in HDFS.
    pub fn renew(&self, holder: &str) {
        if let Some(lease) = self.leases.lock().unwrap().get_mut(holder) {
            lease.renewed = Instant::now();
        }
    }

    /// Whether `holder`'s lease has gone unrenewed past the soft limit (or
    /// there is no lease at all), so another client may take its files.
    pub fn soft_expired(&self, holder: &str) -> bool {
        match self.leases.lock().unwrap().get(holder) {
            Some(lease) => lease.renewed.elapsed() >= self.soft_limit,
            None => true,
        }
    }

    /// Files of every lease past the hard limit: (holder, inode).
    pub fn hard_expired(&self) -> Vec<(String, u64)> {
        let leases = self.leases.lock().unwrap();
        leases
            .iter()
            .filter(|(_, l)| l.renewed.elapsed() >= self.hard_limit)
            .flat_map(|(h, l)| l.files.iter().map(move |f| (h.clone(), *f)))
            .collect()
    }

    /// Number of open files under leases.
    pub fn open_file_count(&self) -> usize {
        self.leases.lock().unwrap().values().map(|l| l.files.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn soft_and_hard_limits_follow_renewals() {
        let lm = LeaseManager::new(Duration::from_secs(60), Duration::from_secs(1200));
        assert!(lm.soft_expired("c1"), "no lease at all");
        lm.add("c1", 7);
        lm.add("c1", 8);
        assert!(!lm.soft_expired("c1"));
        tokio::time::advance(Duration::from_secs(61)).await;
        assert!(lm.soft_expired("c1"));
        lm.renew("c1");
        assert!(!lm.soft_expired("c1"));
        tokio::time::advance(Duration::from_secs(1201)).await;
        let mut expired = lm.hard_expired();
        expired.sort();
        assert_eq!(expired, vec![("c1".to_string(), 7), ("c1".to_string(), 8)]);
        lm.remove("c1", 7);
        lm.remove("c1", 8);
        assert_eq!(lm.open_file_count(), 0);
        assert!(lm.hard_expired().is_empty());
    }
}

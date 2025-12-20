// HDFS block checksum utilities

use sha2::{Sha256, Digest};

pub fn compute_checksum(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

pub fn verify_checksum(data: &[u8], expected: &[u8; 32]) -> bool {
    &compute_checksum(data) == expected
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_checksum_empty() {
        let data = b"";
        let checksum = compute_checksum(data);

        // SHA-256 of empty string
        assert_eq!(checksum.len(), 32);
    }

    #[test]
    fn test_compute_checksum_consistency() {
        let data = b"Hello, HDFS!";
        let checksum1 = compute_checksum(data);
        let checksum2 = compute_checksum(data);

        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn test_compute_checksum_different_data() {
        let data1 = b"Hello";
        let data2 = b"World";
        let checksum1 = compute_checksum(data1);
        let checksum2 = compute_checksum(data2);

        assert_ne!(checksum1, checksum2);
    }

    #[test]
    fn test_verify_checksum_valid() {
        let data = b"Test data for checksum verification";
        let checksum = compute_checksum(data);

        assert!(verify_checksum(data, &checksum));
    }

    #[test]
    fn test_verify_checksum_invalid() {
        let data1 = b"Original data";
        let data2 = b"Modified data";
        let checksum = compute_checksum(data1);

        assert!(!verify_checksum(data2, &checksum));
    }

    #[test]
    fn test_verify_checksum_empty_data() {
        let data = b"";
        let checksum = compute_checksum(data);

        assert!(verify_checksum(data, &checksum));
    }

    #[test]
    fn test_checksum_length() {
        let data = b"Any data";
        let checksum = compute_checksum(data);

        // SHA-256 always produces 32 bytes
        assert_eq!(checksum.len(), 32);
    }
}

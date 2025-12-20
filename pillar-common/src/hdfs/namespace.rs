// HDFS namespace utilities

pub fn normalize_path(path: &str) -> String {
    // Normalize HDFS path (remove trailing slash, collapse //, etc.)
    let mut normalized = path.replace("//", "/");
    if normalized.len() > 1 && normalized.ends_with('/') {
        normalized.pop();
    }
    if normalized.is_empty() {
        "/".to_string()
    } else {
        normalized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path() {
        assert_eq!(normalize_path("/"), "/");
        assert_eq!(normalize_path("/foo/"), "/foo");
        assert_eq!(normalize_path("//foo//bar"), "/foo/bar");
        assert_eq!(normalize_path(""), "/");
    }
}

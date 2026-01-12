use chrono::{DateTime, Utc};
use regex::Regex;
use serde::Serialize;
use std::{fmt, str::FromStr, sync::LazyLock};

use crate::error::FileMetaError;

/// Metadata for a parquet file in S3 storage
///
/// File naming convention:
/// - Original files: {prefix}.{timestamp_millis}.parquet
/// - Compacted files: {prefix}.c.{timestamp_millis}.parquet
///
/// Examples:
/// - sensor_data.1234567890123.parquet (original)
/// - sensor_data.c.1234567890123.parquet (compacted)
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FileMeta {
    /// Full S3 key (filename)
    pub key: String,
    /// File prefix (e.g., "sensor_data")
    pub prefix: String,
    /// Timestamp extracted from filename
    pub timestamp: DateTime<Utc>,
    /// File size in bytes
    pub size: usize,
    /// Whether this file has been compacted (has .c marker)
    #[serde(default)]
    pub compacted: bool,
}

static RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"([a-z,\d,_]+)(\.c)?\.(\d+)(\.parquet)?").unwrap());

impl FromStr for FileMeta {
    type Err = FileMetaError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let key = s.to_string();
        let cap = RE
            .captures(s)
            .ok_or_else(|| FileMetaError::Regex(key.clone()))?;
        let prefix = cap[1].to_owned();

        // Check for .c marker (group 2 is optional)
        let compacted = cap.get(2).is_some();

        // Timestamp is now in group 3 (after the optional .c)
        let timestamp_millis = i64::from_str(&cap[3])?;
        let timestamp = DateTime::from_timestamp_millis(timestamp_millis)
            .ok_or(FileMetaError::InvalidTimestamp(timestamp_millis))?;

        Ok(Self {
            key,
            prefix,
            timestamp,
            size: 0,
            compacted,
        })
    }
}

impl fmt::Display for FileMeta {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.key)
    }
}

impl AsRef<str> for FileMeta {
    fn as_ref(&self) -> &str {
        &self.key
    }
}

impl From<FileMeta> for String {
    fn from(v: FileMeta) -> Self {
        v.key
    }
}

impl<T: Into<String>> From<(T, DateTime<Utc>)> for FileMeta {
    fn from(value: (T, DateTime<Utc>)) -> Self {
        let (prefix, timestamp) = value;
        let prefix = prefix.into();
        Self {
            key: format!("{}.{}.parquet", &prefix, timestamp.timestamp_millis()),
            prefix,
            timestamp,
            size: 0,
            compacted: false,
        }
    }
}

impl TryFrom<&aws_sdk_s3::types::Object> for FileMeta {
    type Error = FileMetaError;

    fn try_from(value: &aws_sdk_s3::types::Object) -> std::result::Result<Self, Self::Error> {
        let size = value.size().unwrap_or_default() as usize;
        let key = value.key.as_ref().ok_or(FileMetaError::MissingFilename)?;
        let mut meta = Self::from_str(key)?;
        meta.size = size;
        Ok(meta)
    }
}

impl FileMeta {
    /// Check if a string matches the expected file naming pattern
    pub fn matches(str: &str) -> bool {
        RE.is_match(str)
    }

    /// Create a new FileMeta with the given prefix and current timestamp
    pub fn new(prefix: impl Into<String>) -> Self {
        Self::from((prefix, Utc::now()))
    }

    /// Create a compacted file metadata with .c marker
    ///
    /// Transforms: `{prefix}.{timestamp}.parquet` → `{prefix}.c.{timestamp}.parquet`
    ///
    /// # Example
    /// ```
    /// use prestige::FileMeta;
    /// use chrono::{DateTime, Utc};
    ///
    /// let timestamp = DateTime::from_timestamp(1234567890, 0).unwrap();
    /// let compacted = FileMeta::as_compacted("sensor_data".to_string(), timestamp);
    ///
    /// assert!(compacted.compacted);
    /// assert!(compacted.key.contains(".c."));
    /// ```
    pub fn as_compacted(prefix: String, timestamp: DateTime<Utc>) -> Self {
        let base = Self::from((prefix.clone(), timestamp));
        // Transform key: "prefix.123456789.parquet" -> "prefix.c.123456789.parquet"
        let key = base
            .key
            .replace(&format!("{prefix}."), &format!("{prefix}.c."));
        Self {
            key,
            prefix,
            timestamp,
            size: 0,
            compacted: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_meta_parsing() {
        let meta = FileMeta::from_str("sensor_data.1234567890123.parquet").unwrap();
        assert_eq!(meta.prefix, "sensor_data");
        assert_eq!(meta.timestamp.timestamp_millis(), 1234567890123);
        assert_eq!(meta.key, "sensor_data.1234567890123.parquet");
    }

    #[test]
    fn test_file_meta_parsing_without_extension() {
        let meta = FileMeta::from_str("sensor_data.1234567890123").unwrap();
        assert_eq!(meta.prefix, "sensor_data");
        assert_eq!(meta.timestamp.timestamp_millis(), 1234567890123);
    }

    #[test]
    fn test_file_meta_creation() {
        let timestamp = DateTime::from_timestamp(1234567890, 0).unwrap();
        let meta = FileMeta::from(("test_prefix", timestamp));
        assert_eq!(meta.prefix, "test_prefix");
        assert_eq!(meta.timestamp, timestamp);
        assert!(meta.key.contains("test_prefix"));
        assert!(meta.key.ends_with(".parquet"));
    }

    #[test]
    fn test_file_meta_matches() {
        assert!(FileMeta::matches("data.123456.parquet"));
        assert!(FileMeta::matches("my_data.999.parquet"));
        assert!(FileMeta::matches("data.123456"));
        assert!(!FileMeta::matches("invalid"));
        assert!(!FileMeta::matches("no_timestamp.parquet"));
    }

    #[test]
    fn test_file_meta_display() {
        let meta = FileMeta::from_str("test.123.parquet").unwrap();
        assert_eq!(format!("{}", meta), "test.123.parquet");
    }

    #[test]
    fn test_file_meta_as_ref() {
        let meta = FileMeta::from_str("test.123.parquet").unwrap();
        let s: &str = meta.as_ref();
        assert_eq!(s, "test.123.parquet");
    }

    #[test]
    fn test_file_meta_into_string() {
        let meta = FileMeta::from_str("test.123.parquet").unwrap();
        let s: String = meta.into();
        assert_eq!(s, "test.123.parquet");
    }

    #[test]
    fn test_file_meta_compacted_marker_parsing() {
        // Test parsing file with .c marker
        let compacted = FileMeta::from_str("sensor_data.c.1234567890123.parquet").unwrap();
        assert_eq!(compacted.prefix, "sensor_data");
        assert_eq!(compacted.timestamp.timestamp_millis(), 1234567890123);
        assert_eq!(compacted.key, "sensor_data.c.1234567890123.parquet");
        assert!(compacted.compacted);

        // Test parsing file without .c marker
        let original = FileMeta::from_str("sensor_data.1234567890123.parquet").unwrap();
        assert_eq!(original.prefix, "sensor_data");
        assert_eq!(original.timestamp.timestamp_millis(), 1234567890123);
        assert_eq!(original.key, "sensor_data.1234567890123.parquet");
        assert!(!original.compacted);
    }

    #[test]
    fn test_file_meta_compacted_marker_without_extension() {
        // Test parsing compacted file without .parquet extension
        let compacted = FileMeta::from_str("data.c.999").unwrap();
        assert_eq!(compacted.prefix, "data");
        assert!(compacted.compacted);

        // Test parsing original file without .parquet extension
        let original = FileMeta::from_str("data.999").unwrap();
        assert_eq!(original.prefix, "data");
        assert!(!original.compacted);
    }

    #[test]
    fn test_file_meta_as_compacted() {
        let timestamp = DateTime::from_timestamp(1234567890, 0).unwrap();
        let compacted = FileMeta::as_compacted("test_prefix".to_string(), timestamp);

        assert_eq!(compacted.prefix, "test_prefix");
        assert_eq!(compacted.timestamp, timestamp);
        assert!(compacted.compacted);
        assert!(compacted.key.contains(".c."));
        assert!(compacted.key.starts_with("test_prefix.c."));
        assert!(compacted.key.ends_with(".parquet"));
    }

    #[test]
    fn test_file_meta_matches_with_compacted() {
        // Original files
        assert!(FileMeta::matches("data.123456.parquet"));
        assert!(FileMeta::matches("my_data.999.parquet"));
        assert!(FileMeta::matches("data.123456"));

        // Compacted files
        assert!(FileMeta::matches("data.c.123456.parquet"));
        assert!(FileMeta::matches("my_data.c.999.parquet"));
        assert!(FileMeta::matches("data.c.123456"));

        // Invalid patterns
        assert!(!FileMeta::matches("invalid"));
        assert!(!FileMeta::matches("no_timestamp.parquet"));
        assert!(!FileMeta::matches("data.c.parquet"));
    }

    #[test]
    fn test_file_meta_from_tuple_not_compacted() {
        let timestamp = DateTime::from_timestamp(1234567890, 0).unwrap();
        let meta = FileMeta::from(("test", timestamp));
        assert!(!meta.compacted);
    }
}

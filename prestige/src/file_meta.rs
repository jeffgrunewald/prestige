use chrono::{DateTime, Utc};
use regex::Regex;
use serde::Serialize;
use std::{fmt, str::FromStr, sync::LazyLock};

use crate::error::FileMetaError;

/// Metadata for a parquet file in S3 storage
///
/// File naming convention: {prefix}.{timestamp_millis}.parquet
/// Example: sensor_data.1234567890123.parquet
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
}

static RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"([a-z,\d,_]+)\.(\d+)(\.parquet)?").unwrap()
});

impl FromStr for FileMeta {
    type Err = FileMetaError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let key = s.to_string();
        let cap = RE
            .captures(s)
            .ok_or_else(|| FileMetaError::Regex(key.clone()))?;
        let prefix = cap[1].to_owned();

        let timestamp_millis = i64::from_str(&cap[2])?;
        let timestamp = DateTime::from_timestamp_millis(timestamp_millis)
            .ok_or(FileMetaError::InvalidTimestamp(timestamp_millis))?;

        Ok(Self {
            key,
            prefix,
            timestamp,
            size: 0,
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
}

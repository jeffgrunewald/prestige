//! Internal telemetry compatibility abstraction for prestige metrics
//!
//! This module provides metric recording that works with either the `metrics` crate
//! or `opentelemetry`, depending on which feature is enabled. When neither feature
//! is enabled, a default no-op implementation is used.
//!
//! ## Feature flags
//!
//! - `metrics`: Use the metrics crate backend
//! - `opentelemetry`: Use the opentelemetry backend
//!
//! These features are mutually exclusive
//!
//! ## Note
//!
//! This module is internal to prestige. Users should configure their own metrics exporter and
//! use their preferred metrics library directly for application-level instrumentation. Prestige's
//! metrics will automatically flow through the same exporter.

// Compile-time check: ensure mutual exclusivity of the active metrics backend
#[cfg(all(feature = "metrics", feature = "opentelemetry"))]
compile_error!(
    "Features `metrics` and `opentelemetry` are mutually exclusive. \
    Please enable only one backend."
);

#[cfg(feature = "metrics")]
mod metrics;
#[cfg(feature = "metrics")]
pub(crate) use metrics::*;

#[cfg(feature = "opentelemetry")]
mod otel;
#[cfg(feature = "opentelemetry")]
pub(crate) use otel::*;

#[cfg(not(any(feature = "metrics", feature = "opentelemetry")))]
mod noop {
    //! No-op implementation when no telemetry feature is enabled
    use super::Label;

    #[inline]
    pub fn increment_counter(_name: &'static str, _value: u64, _labels: &[Label]) {}

    #[inline]
    pub fn record_histogram(_name: &'static str, _value: f64, _label: &[Label]) {}

    #[inline]
    pub fn set_gauge(_name: &'static str, _value: f64, _labels: &[Label]) {}
}
#[cfg(not(any(feature = "metrics", feature = "opentelemetry")))]
pub(crate) use noop::*;

// Metric name constants
//
// All metric names follow OpenTelemetry semantic conventions:
// - Use `.` as namespace separator
// - Use `_` within name segments
// - Format: `<namespace>.<component>.<metric_name>`

// ====================================================================================
// File Poller Metrics
// ====================================================================================

// Histogram: Latency between file creation and processing (milliseconds)
pub const FILE_POLLER_LATENCY_MS: &str = "prestige.file_poller.latency_ms";

// Gauge: Timestamp of the most recently processed file (milliseconds since epoch)
pub const FILE_POLLER_LATEST_TIMESTAMP_MS: &str = "prestige.file_poller.latest_timestamp_ms";

// Counter: Number of files processed by the poller
pub const FILE_POLLER_FILES_PROCESSED: &str = "prestige.file_poller.files_processed";

// ====================================================================================
// File Upload Metrics
// ====================================================================================

// Histogram: Duration of S3 file uploads (milliseconds)
pub const FILE_UPLOAD_DURATION_MS: &str = "prestige.file_upload.duration_ms";

// Counter: Number of files uploaded to S3
pub const FILE_UPLOAD_COUNT: &str = "prestige.file_upload.count";

// Histogram: Size of uploaded files (bytes)
pub const FILE_UPLOAD_SIZE_BYTES: &str = "prestige.file_upload.size_bytes";

// ====================================================================================
// File Source Metrics
// ====================================================================================

// Counter: Number of parquet files successfully opened/read
pub const FILE_SOURCE_FILES_READ: &str = "prestige.file_source.files_read";

// Counter: Number of rows read from parquet files
pub const FILE_SOURCE_ROWS_READ: &str = "prestige.file_source.rows_read";

// Counter: Number of file read errors
pub const FILE_SOURCE_READ_ERRORS: &str = "prestige.file_source.read_errors";

// Histogram: Time to open/download a file (milliseconds)
pub const FILE_SOURCE_READ_DURATION_MS: &str = "prestige.file_source.read_duration_ms";

// Histogram: Bytes downloaded from S3
pub const FILE_SOURCE_BYTES_DOWNLOADED: &str = "prestige.file_source.bytes_downloaded";

// ====================================================================================
// Parquet Sink Metrics
// ====================================================================================

// Counter: Number of records written to parquet files
pub const SINK_RECORDS_WRITTEN: &str = "prestige.file_sink.records_written";

// Counter: Number of write errors (channel closed, timeout)
pub const SINK_WRITE_ERRORS: &str = "prestige.file_sink.write_errors";

// Counter: Number of parquet files rotated
pub const SINK_FILES_ROTATED: &str = "prestige.file_sink.files_rotated";

// Histogram: Number of records per batch flush
pub const SINK_BATCH_SIZE: &str = "prestige.file_sink.batch_size";

// Shared types for telemetry abstraction
// A key-value pair for metrics labels/attributes
#[derive(Debug, Clone)]
#[cfg_attr(
    not(any(feature = "metrics", feature = "opentelemetry")),
    allow(dead_code)
)]
pub(crate) struct Label {
    pub key: &'static str,
    pub value: String,
}

impl Label {
    pub fn new(key: &'static str, value: impl Into<String>) -> Self {
        Self {
            key,
            value: value.into(),
        }
    }
}

// Internal convenience macro for creating labels
//
// This macro mirrors the label syntax supported by the `metrics` crate
// accepting the `"key" => value` pairs where values implement `Into<String>`.
macro_rules! telemetry_labels {
    () => {
        &[] as &[$crate::telemetry::Label]
    };
    ($($key:expr => $value:expr),+ $(,)?) => {
        &{
            let mut labels = Vec::new();
            $(
                if let Some(v) = $crate::telemetry::IntoOptionString::into_option($value) {
                    labels.push($crate::telemetry::Label::new($key, v));
                }
            )+
            labels
        }
    };
}

// Make the macro available within the crate
pub(crate) use telemetry_labels;

// Helper trait to for the labels macro to accept Into<String> and Option<Into<String>> equally
pub(crate) trait IntoOptionString {
    fn into_option(self) -> Option<String>;
}

impl<T: Into<String>> IntoOptionString for Option<T> {
    fn into_option(self) -> Option<String> {
        self.map(Into::into)
    }
}

impl IntoOptionString for &str {
    fn into_option(self) -> Option<String> {
        Some(self.to_string())
    }
}

impl IntoOptionString for String {
    fn into_option(self) -> Option<String> {
        Some(self)
    }
}

impl IntoOptionString for &String {
    fn into_option(self) -> Option<String> {
        Some(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_label_creation() {
        let label = Label::new("key", "value");
        assert_eq!(label.key, "key");
        assert_eq!(label.value, "value");
    }

    #[test]
    fn test_label_from_string() {
        let label = Label::new("key", "value".to_string());
        assert_eq!(label.value, "value");
    }

    // No panic when labels are empty
    #[test]
    fn test_metrics_with_empty_labels() {
        increment_counter("test.counter", 1, &[]);
        record_histogram("test.histogram", 1.0, &[]);
        set_gauge("test.gauge", 1.0, &[]);
    }

    // No panic when labels are non-empty
    #[test]
    fn test_metrics_with_non_empty_labels() {
        let labels = &[Label::new("key", "value")];
        increment_counter("test.counter", 1, labels);
        record_histogram("test.histogram", 1.0, labels);
        set_gauge("test.gauge", 1.0, labels);
    }
}

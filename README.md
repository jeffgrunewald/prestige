# Prestige

A high-performance Rust library for working with Parquet files and S3 storage, built on Apache Arrow. Prestige provides a complete toolkit for streaming data to/from Parquet format with automatic batching, file rotation, and S3 integration.

Side note: the name "Prestige" is a reference to the "PrestoDB" query engine (since rebranded "Trino") for providing a relational SQL interface to columnar data files, including Parquet, in S3-compatible block storage.

## Features

- **Type-safe Parquet I/O**: Derive macros for automatic schema generation and serialization
- **Streaming Architecture**: Process large datasets without loading everything into memory
- **Automatic File Rotation**: Configure rotation based on row count or time intervals
- **S3 Integration**: Native support for reading from and writing to S3
- **Crash Recovery**: Automatic recovery and cleanup of incomplete files
- **File Monitoring**: Poll S3 buckets for new files with configurable lookback
- **Batching & Buffering**: Configurable batch sizes for optimal performance
- **Metrics Support**: Built-in metrics using the `metrics` crate

## Architecture

Prestige is organized into several key components:

### ParquetSink

A managed actor that writes Rust types to Parquet files with automatic batching and rotation.

```rust
use prestige::ParquetSinkBuilder;

#[prestige::prestige_schema]
#[derive(Debug, Clone)]
struct SensorData {
    timestamp: u64,
    sensor_id: String,
    temperature: f32,
}

// Create a sink with file rotation
let (client, sink) = ParquetSinkBuilder::<SensorData>::new(
    "sensor_data",
    output_dir,
    file_upload,
    "sensor_metrics",
)
.batch_size(1000)           // Buffer 1000 records before writing
.max_rows(100_000)          // Rotate after 100k rows
.rotation_interval(3600)    // Or rotate every hour
.auto_commit(true)          // Auto-upload completed files
.create()
.await?;

// Write records
client.write(sensor_data, &[]).await?;

// Commit to finalize and get file manifest
let manifest = client.commit().await?;
```

### File Source

Stream Parquet files from local filesystem or S3 as Arrow RecordBatch.

```rust
use prestige::file_source;
use futures::StreamExt;

// Read from local files
let paths = vec!["data/file1.parquet", "data/file2.parquet"];
let mut stream = file_source::source(paths, None, None);

while let Some(batch) = stream.next().await {
    let batch = batch?;
    // Process RecordBatch
}

// Read from S3
let client = prestige::new_client(None, None, None, None).await;
let metas = prestige::list_files(&client, "my-bucket", "sensor_data/", None, None);
let mut stream = file_source::source_s3_files(&client, "my-bucket", metas, None, None);
```

### File Upload

Managed service for uploading files to S3 with automatic retries and metrics.

```rust
use prestige::FileUpload;

let client = prestige::new_client(None, None, None, None).await;
let (uploader, server) = FileUpload::new(client, "my-bucket".to_string()).await;

// Upload returns immediately, actual upload happens in background
uploader.upload(file_path).await?;
```

### File Poller

Monitor S3 buckets for new files with configurable polling intervals and state tracking.

```rust
use prestige::{FilePollerConfigBuilder, LookbackBehavior};
use chrono::Duration;

let config = FilePollerConfigBuilder::default()
    .bucket("my-bucket".to_string())
    .file_type("sensor_data/".to_string())
    .poll_interval(Duration::seconds(60))
    .lookback(LookbackBehavior::from_duration(Duration::hours(24)))
    .build()?;

let (poller, mut file_stream) = FilePollerServer::new(config, state_store).await?;

// Receive new files as they appear
while let Some(file_meta) = file_stream.recv().await {
    println!("New file: {}", file_meta.key);
}
```

### Schema Attribute Macro

The `#[prestige::prestige_schema]` attribute macro automatically generates all necessary schema and serialization code. It also auto-injects `Serialize` and `Deserialize` derives if not already present.

```rust
#[prestige::prestige_schema]
#[derive(Debug, Clone)]
struct MyData {
    id: u64,
    name: String,
    value: f64,
    optional_field: Option<i32>,
    raw_bytes: [u8; 16],             // Default: List(UInt8) — structural representation
    #[prestige(as_binary)]
    binary_data: [u8; 16],           // Opt-in: FixedSizeBinary(16)
    #[prestige(as_binary)]
    payload: Vec<u8>,                // Opt-in: Binary
}

// Generated methods:
// - arrow_schema() -> Schema
// - from_arrow_records() -> Result<Vec<Self>>
// - to_arrow_arrays() -> Result<(Vec<Arc<Array>>, Schema)>
// - from_arrow_reader() / write_arrow_file() / write_arrow_stream()
```

Individual derive macros are also available for advanced use cases:
- `#[derive(ArrowGroup)]` - Schema generation only
- `#[derive(ArrowReader)]` - Reading from Arrow/Parquet (requires `Deserialize`)
- `#[derive(ArrowWriter)]` - Writing to Arrow/Parquet (requires `Serialize`)

## Complete Example: Data Pipeline

```rust
use prestige::{ParquetSinkBuilder, FilePollerConfigBuilder};
use super_visor::ManagedProc;

#[prestige::prestige_schema]
#[derive(Debug, Clone)]
struct Event {
    timestamp: u64,
    event_type: String,
    user_id: String,
    payload: String,
}

#[tokio::main]
async fn main() -> prestige::Result<()> {
    // Setup S3 client
    let client = prestige::new_client(None, None, None, None).await;

    // Create file upload service
    let (uploader, upload_server) = prestige::FileUpload::new(
        client.clone(),
        "events-bucket".to_string(),
    ).await;

    // Create parquet sink
    let (sink_client, sink) = ParquetSinkBuilder::<Event>::new(
        "events",
        "./output",
        uploader,
        "event_metrics",
    )
    .batch_size(10_000)
    .max_rows(1_000_000)
    .rotation_interval(3600)
    .auto_commit(true)
    .create()
    .await?;

    // Write events
    for i in 0..100_000 {
        let event = Event {
            timestamp: chrono::Utc::now().timestamp() as u64,
            event_type: "click".to_string(),
            user_id: format!("user_{}", i % 1000),
            payload: format!("{{\"page\": \"home\", \"index\": {}}}", i),
        };

        sink_client.write(event, &[]).await?;
    }

    // Commit and get uploaded file paths
    let manifest = sink_client.commit().await?;
    println!("Uploaded {} files", manifest.len());

    Ok(())
}
```

## S3 Configuration

Prestige uses the AWS SDK for S3 operations. Configure credentials using standard AWS methods:

```bash
# Environment variables
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

# Or use AWS profiles
export AWS_PROFILE=my-profile
```

For local testing with LocalStack:

```rust
let client = prestige::new_client(
    Some("us-east-1".to_string()),
    Some("http://localhost:4566".to_string()),  // LocalStack endpoint
    Some("test".to_string()),
    Some("test".to_string()),
).await;
```

## Crash Recovery

ParquetSink includes automatic crash recovery:

- **Auto-commit enabled**: Incomplete files are moved to `.incomplete` directory on restart
- **Auto-commit disabled**: Incomplete files are deleted on restart
- **Completed files**: Automatically re-uploaded if found in output directory

This ensures data consistency even if your process crashes during file writing.

## Optional Features

Enable additional functionality via Cargo features:

```toml
[dependencies]
prestige = { version = "0.1", features = ["chrono", "decimal", "sqlx-postgres"] }
```

- `chrono` (default): Support for `chrono::DateTime` types
- `decimal`: Support for `rust_decimal::Decimal` types
- `sqlx`: Enable SQLx integration
- `sqlx-postgres`: PostgreSQL support via SQLx
- `metrics`: Instrument with performance metrics compatible with the `metrics` crate
- `opentelemetry`: Instrument with performance metrics compatible with the `opentelemetry` crate

## Metrics Support

Prestige supports optional metrics collection via two backends:

### Using metrics-rs
```toml
[dependencies]
prestige = { version = "0.1", features = ["metrics"] }
metrics-exporter-prometheus = "0.16" # or your preferred exporter library
```

### Using OpenTelemetry
```toml
[dependencies]
prestige = { version = "0.1", features = ["opentelemetry"] }
opentelemetry = { version = "0.31", features = ["metrics"] }
opentelemetry_sdk = { version = "0.31", features = ["rt-tokio", "metrics"] }
opentelemetry-otlp = { version = "0.31", features = ["metrics", "grpc-tonic"] }
```

Configure the meter provider in your application
\```rust
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{MeterProviderBuilder, PeriodicReader};

fn init_metrics() -> Result<(), Box<dyn std::error::Error>> {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint("http://localhost:4317")
        .build()?;

    let reader = PeriodicReader::builder(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_interval(std::time::Duration::from_secs(60))
        .build();

    let provider = MeterProviderBuilder::default()
        .with_reader(reader)
        .build();

    global::set_meter_provider(provider);
    Ok(())
}
\```

### Available Metrics

| Metric Name | Type | Description | Labels |
|-------------|------|-------------|--------|
| `prestige.file_poller.latency_ms` | Histogram | File processing latency | process_name, file_type |
| `prestige.file_poller.latest_timestamp_ms` | Gauge | Latest processed file timestamp | process_name, file_type |
| `prestige.file_poller.files_processed` | Counter | Files processed count | process_name, file_type, status |
| `prestige.file_upload.duration_ms` | Histogram | S3 upload duration | bucket |
| `prestige.file_upload.count` | Counter | Files uploaded count | bucket, status |
| `prestige.file_upload.size_bytes` | Histogram | Uploaded file sizes | bucket |
| `prestige.file_sink.records_written` | Counter | Records written to parquet | sink |
| `prestige.file_sink.files_created` | Counter | Parquet files created | sink |
| `prestige.file_sink.write_duration_ms` | Histogram | Batch write duration | sink |

### Disabling Metrics

To compile without any metrics overhead, simply don't enable either feature:

\```
[dependencies]
prestige = "0.1"  # No features = no-op metrics impl
\```

## Performance Considerations

- **Batch Size**: Larger batches reduce overhead but increase memory usage (default: 8192 for reading, configurable for writing)
- **File Rotation**: Balance between number of files and file size (default: no rotation)
- **Buffering**: File source reads up to 2 files concurrently by default
- **Parallel S3 Reads**: Use `source_s3_files_unordered()` for maximum throughput when order doesn't matter

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

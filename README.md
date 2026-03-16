# Prestige

A high-performance Rust library for working with Parquet files and S3 storage, built on Apache Arrow. Prestige provides a complete toolkit for streaming data to/from Parquet format with automatic batching, file rotation, and S3 integration — plus optional Apache Iceberg table format support for catalog-managed lakehouse workloads.

Side note: the name "Prestige" is a reference to the "PrestoDB" query engine (since rebranded "Trino") for providing a relational SQL interface to columnar data files, including Parquet, in S3-compatible block storage.

## Features

- **Type-safe Parquet I/O**: Derive macros for automatic schema generation and serialization
- **Streaming Architecture**: Process large datasets without loading everything into memory
- **Automatic File Rotation**: Configure rotation based on row count, byte size, or time intervals
- **S3 Integration**: Native support for reading from and writing to S3
- **Crash Recovery**: Automatic recovery and cleanup of incomplete files
- **File Monitoring**: Poll S3 buckets for new files with configurable lookback
- **Batching & Buffering**: Configurable batch sizes for optimal performance
- **Metrics Support**: Built-in metrics via `metrics` or `opentelemetry` crates
- **Apache Iceberg**: Catalog-managed tables with streaming writes, incremental reads, time travel, and automatic compaction

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

## Apache Iceberg

Enable with the `iceberg` feature:

```toml
[dependencies]
prestige = { version = "0.3", features = ["iceberg"] }
```

Prestige targets **Iceberg V2 and V3 table formats only**. V1 tables are not supported.

### Schema Definition with Iceberg Annotations

The `prestige_schema` macro supports iceberg-specific annotations for table name, namespace, partition spec, sort order, and identifier fields:

```rust
#[prestige::prestige_schema]
#[prestige(table = "sensor_readings", namespace = "telemetry")]
#[derive(Debug, Clone)]
struct SensorReading {
    #[prestige(identifier)]                    // Identifier field (used for dedup)
    sensor_id: String,

    #[prestige(partition(day), sort_key(order = 1))]  // Partition by day, sort first
    timestamp: i64,

    #[prestige(sort_key(order = 2))]           // Sort second (ascending by default)
    temperature: f64,

    humidity: Option<f64>,

    #[prestige(partition)]                     // Identity partition
    location: String,
}
```

**Partition transforms**: `identity` (default), `year`, `month`, `day`, `hour`, `bucket(n)`, `truncate(n)`

**Sort key options**: `sort_key` (ascending), `sort_key(desc)`, `sort_key(order = N)` for explicit ordering, `sort_key(desc, order = N)` for descending with order

### Catalog Connection

Connect to an Iceberg REST catalog:

```rust
use prestige::iceberg::{CatalogConfigBuilder, connect_catalog};

let config = CatalogConfigBuilder::default()
    .catalog_uri("http://localhost:8181")
    .catalog_name("my_catalog")
    .warehouse("s3://my-warehouse")
    .s3(S3Config {
        region: "us-east-1".into(),
        endpoint: Some("http://localhost:9000".into()),  // MinIO/LocalStack
        access_key: Some("minioadmin".into()),
        secret_key: Some("minioadmin".into()),
    })
    .build()?;

let catalog = connect_catalog(&config).await?;
```

### Streaming Writes (IcebergSink)

The iceberg sink provides pipelined writes to catalog-managed tables with crash recovery:

```rust
use prestige::iceberg::IcebergSinkBuilder;
use std::time::Duration;

let (client, sink) = IcebergSinkBuilder::<SensorReading>::for_type(
    catalog.clone(),
    "sensor_readings",
).await?
.max_rows(500_000)
.max_size_bytes(64 * 1024 * 1024)     // 64 MB
.roll_time(Duration::from_secs(60))    // Commit every 60s
.auto_commit(true)                     // Auto-commit on rotation
.manifest_dir("/tmp/prestige/manifests") // Enable crash recovery
.max_pending_commits(3)                // Pipeline up to 3 catalog commits
.create();

// Run the sink (typically via a supervisor)
tokio::spawn(sink.run(shutdown_signal));

// Write records — returns immediately, batching happens internally
client.write(reading).await?;

// Explicit commit returns file paths after all in-flight commits complete
let file_paths = client.commit().await?;
```

The sink pipelines S3 writes with catalog commits: while commit N is landing in the catalog, new data can be written to S3 for commit N+1. Per-commit manifest files on local disk ensure crash recovery — on restart, orphaned manifests are detected and re-committed.

### Incremental Reads (IcebergPoller)

Stream new data as it arrives via incremental snapshot scanning:

```rust
use prestige::iceberg::IcebergPollerConfigBuilder;
use std::sync::Arc;

let (mut receiver, poller) = IcebergPollerConfigBuilder::new(
    table,
    Arc::new(catalog) as Arc<dyn iceberg::Catalog>,
    "sensor-consumer",
)
.poll_interval(Duration::from_secs(10))  // Check every 10s
.channel_size(5)                          // Buffer up to 5 snapshots
.send_timeout(Duration::from_secs(30))   // Backpressure timeout
.start_after_snapshot(last_checkpoint)    // Resume from checkpoint
.create();

// Run the poller
tokio::spawn(poller.run(shutdown_signal));

// Consume incremental data
while let Some(file_stream) = receiver.recv().await {
    println!("snapshot {} from {}", file_stream.snapshot_id, file_stream.table_name);

    // Process all batches in the stream
    let mut stream = file_stream.batches;
    while let Some(batch) = stream.try_next().await? {
        // Process RecordBatch...
    }

    // Acknowledge after processing — poller won't advance until acked
    file_stream.ack();
}
```

The poller is compaction-aware: when the compactor rewrites files, the incremental scan correctly filters out `Replace` snapshots to avoid double-delivering data that was already consumed.

### Scan API — Windowed Reads and Time Travel

Prestige provides composable scan functions for arbitrary access patterns:

```rust
use prestige::iceberg::{
    scan_table, scan_snapshot, scan_since_snapshot, scan_snapshot_range,
    scan_at_timestamp, snapshot_at_timestamp, earliest_snapshot,
    scan_with_filter, scan_columns,
};

// Full table scan (current state)
let stream = scan_table(&table).await?;

// Point-in-time snapshot read
let stream = scan_snapshot(&table, snapshot_id).await?;

// Incremental: all data added after a checkpoint snapshot
let stream = scan_since_snapshot(&table, after_snapshot_id).await?;

// Windowed: data added between two arbitrary snapshots
let stream = scan_snapshot_range(&table, from_snapshot, Some(to_snapshot)).await?;

// Time travel: table state as of a timestamp (epoch millis)
let stream = scan_at_timestamp(&table, 1710547200000).await?;

// Resolve snapshots by timestamp
let snap_id = snapshot_at_timestamp(&table, timestamp_ms);

// Discover the earliest snapshot for full replay
let first = earliest_snapshot(&table);

// Predicate pushdown (partition pruning + row-group filtering)
use prestige::iceberg::{Reference, Datum};
let filter = Reference::new("temperature").greater_than(Datum::double(100.0));
let stream = scan_with_filter(&table, filter).await?;

// Column projection
let stream = scan_columns(&table, vec!["sensor_id", "temperature"]).await?;
```

### Compaction

The compactor consolidates small files into larger, sorted files:

```rust
use prestige::iceberg::IcebergCompactorConfigBuilder;

let result = IcebergCompactorConfigBuilder::default()
    .table(table.clone())
    .catalog(catalog.clone())
    .target_file_size_bytes(128 * 1024 * 1024)  // 128 MB target
    .min_files_to_compact(5_usize)               // Compact when >= 5 files/partition
    .deduplicate(true)                            // Remove duplicate rows by identifier fields
    .compression(Compression::ZSTD(Default::default()))
    .build()?
    .execute()
    .await?;

println!(
    "Compacted {} files into {}, eliminated {} duplicates",
    result.files_read, result.files_written, result.duplicates_eliminated
);
```

Compaction produces `Operation::Replace` snapshots that atomically swap old files for new ones. Output files are sorted according to the table's sort order for optimal query performance. The compactor handles concurrent writes via optimistic concurrency with automatic retry, and correctly applies delete files so that deleted rows are not resurrected in compacted output.

### Automatic Compaction Scheduling

For streaming workloads with frequent small commits, run compaction on a timer:

```rust
use prestige::iceberg::CompactionSchedulerBuilder;

let scheduler = CompactionSchedulerBuilder::new(
    table.clone(),
    catalog.clone(),
    "sensor_readings",
)
.interval(Duration::from_secs(300))       // Check every 5 minutes
.min_files_to_compact(5)                  // Threshold per partition
.target_file_size_bytes(128 * 1024 * 1024)
.deduplicate(true)
.build();

// Run as a managed process alongside sinks and pollers
tokio::spawn(scheduler.run(shutdown_signal));
```

The scheduler performs a lightweight file-count check per partition each cycle, only invoking the full compaction when thresholds are exceeded.

### Write-Audit-Publish (WAP)

For workflows requiring validation before data becomes visible:

```rust
let (client, sink) = IcebergSinkBuilder::<SensorReading>::for_type(catalog, "sensor_readings")
    .await?
    .wap_enabled("audit-branch")   // Write to audit branch
    .auto_publish(false)           // Don't auto-publish to main
    .create();

// Write data to audit branch
client.write(reading).await?;
client.commit().await?;

// Validate, then publish to main
client.publish().await?;
```

### Complete Iceberg Pipeline Example

```rust
use prestige::iceberg::{
    CatalogConfigBuilder, IcebergSinkBuilder, IcebergPollerConfigBuilder,
    CompactionSchedulerBuilder, connect_catalog,
};
use std::time::Duration;

#[prestige::prestige_schema]
#[prestige(table = "events", namespace = "analytics")]
#[derive(Debug, Clone)]
struct Event {
    #[prestige(identifier)]
    event_id: String,
    #[prestige(partition(day), sort_key(order = 1))]
    timestamp: i64,
    #[prestige(partition)]
    event_type: String,
    user_id: String,
    payload: String,
}

#[tokio::main]
async fn main() -> prestige::Result<()> {
    let catalog = connect_catalog(&config).await?;

    // --- Writer ---
    let (sink_client, sink) = IcebergSinkBuilder::<Event>::for_type(catalog.clone(), "events")
        .await?
        .auto_commit(true)
        .roll_time(Duration::from_secs(30))
        .manifest_dir("/tmp/prestige/manifests")
        .create();
    tokio::spawn(sink.run(shutdown.clone()));

    // --- Compaction scheduler ---
    let table = prestige::iceberg::load_table(&catalog, "analytics", "events").await?;
    let scheduler = CompactionSchedulerBuilder::new(table.clone(), catalog.clone(), "events")
        .interval(Duration::from_secs(120))
        .min_files_to_compact(3)
        .deduplicate(true)
        .build();
    tokio::spawn(scheduler.run(shutdown.clone()));

    // --- Consumer ---
    let (mut rx, poller) = IcebergPollerConfigBuilder::new(
        table,
        catalog.as_iceberg_catalog(),
        "events-consumer",
    )
    .poll_interval(Duration::from_secs(5))
    .create();
    tokio::spawn(poller.run(shutdown.clone()));

    // Write events
    sink_client.write(Event { /* ... */ }).await?;

    // Consume events
    while let Some(stream) = rx.recv().await {
        // Process stream.batches...
        stream.ack();
    }

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

IcebergSink uses per-commit manifest files for crash recovery:

- Data file paths are recorded to local disk before each catalog commit
- On restart, orphaned manifests are discovered and their data files re-committed
- Files already committed are detected and manifests cleaned up automatically

## Optional Features

Enable additional functionality via Cargo features:

```toml
[dependencies]
prestige = { version = "0.3", features = ["iceberg"] }
```

- `chrono` (default): Support for `chrono::DateTime` types
- `decimal`: Support for `rust_decimal::Decimal` types
- `iceberg`: Apache Iceberg table format support (REST catalog, streaming sink, poller, compactor, scanner)
- `iceberg-test-harness`: Test utilities for iceberg integration tests
- `sqlx`: Enable SQLx integration
- `sqlx-postgres`: PostgreSQL support via SQLx
- `metrics`: Instrument with performance metrics compatible with the `metrics` crate
- `opentelemetry`: Instrument with performance metrics compatible with the `opentelemetry` crate

## Metrics Support

Prestige supports optional metrics collection via two backends:

### Using metrics-rs
```toml
[dependencies]
prestige = { version = "0.3", features = ["metrics"] }
metrics-exporter-prometheus = "0.16" # or your preferred exporter library
```

### Using OpenTelemetry
```toml
[dependencies]
prestige = { version = "0.3", features = ["opentelemetry"] }
opentelemetry = { version = "0.31", features = ["metrics"] }
opentelemetry_sdk = { version = "0.31", features = ["rt-tokio", "metrics"] }
opentelemetry-otlp = { version = "0.31", features = ["metrics", "grpc-tonic"] }
```

### Disabling Metrics

To compile without any metrics overhead, simply don't enable either feature:

```toml
[dependencies]
prestige = "0.3"  # No features = no-op metrics impl
```

## Performance Considerations

- **Batch Size**: Larger batches reduce overhead but increase memory usage (default: 8192 for reading, configurable for writing)
- **File Rotation**: Balance between number of files and file size (default: no rotation)
- **Buffering**: File source reads up to 2 files concurrently by default
- **Parallel S3 Reads**: Use `source_s3_files_unordered()` for maximum throughput when order doesn't matter
- **Iceberg Commit Pipeline**: The sink pipelines S3 writes with catalog commits — tune `max_pending_commits` based on commit latency vs memory
- **Compaction Interval**: More frequent compaction keeps file counts low for query performance, but each compaction cycle has I/O cost

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

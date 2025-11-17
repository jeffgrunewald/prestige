# Prestige Parquet File Store - Implementation Plan

## Overview

Build a parquet-based file storage system that mirrors the core functionality of `oracles/file_store` but works with Apache Parquet files instead of gzip-compressed protobuf messages. The system will provide efficient batched writes, automatic S3 uploads, and continuous file polling for new data.

## Key Design Principles

1. **Parquet-Native**: Use parquet's built-in compression (snappy/zstd) - NO external compression
2. **Query-Friendly**: Files must be readable by standard parquet tools (DuckDB, Athena, pandas, etc.)
3. **Batch-Oriented**: Accumulate records in Arrow record batches, write complete parquet files
4. **S3-Compatible**: Work with any S3-compatible object storage
5. **Mirror file_store**: Maintain similar APIs and patterns for consistency

---

## Module Architecture

### 1. **file_meta.rs** - File Metadata (analogous to FileInfo)

**Purpose**: Track parquet file metadata and naming conventions

**File Naming Convention**: `{prefix}.{timestamp_millis}.parquet`
- Example: `sensor_readings.1699564823456.parquet`

**Key Types**:
```rust
pub struct FileMeta {
    pub key: String,           // Full S3 key
    pub prefix: String,        // File type prefix
    pub timestamp: DateTime<Utc>,
    pub size: usize,
}

pub enum FileMetaError { ... }
```

**Responsibilities**:
- Parse file names from S3 object keys
- Generate timestamped file names
- Convert from `aws_sdk_s3::types::Object`
- Validate file naming patterns

**Crib from file_store**:
- Regex pattern matching from `file_info.rs:15`
- `FromStr`, `Display`, `TryFrom<&Object>` implementations
- Similar error handling patterns

**Implementation Checklist**:
- [ ] Define `FileMeta` struct with required fields
- [ ] Implement regex pattern for parsing file names
- [ ] Implement `FromStr` trait for parsing
- [ ] Implement `TryFrom<&aws_sdk_s3::types::Object>`
- [ ] Implement `From<(String, DateTime<Utc>)>` for creation
- [ ] Add `matches()` helper method
- [ ] Add `new()` constructor with current timestamp
- [ ] Unit tests for parsing various formats
- [ ] Unit tests for file name generation
- [ ] Unit tests for edge cases and errors

---

### 2. **file_sink.rs** - Batched Parquet Writer

**Purpose**: Buffer records in-memory, write parquet files to local tmp directory with time/size-based rotation

**Architecture**:
```rust
pub struct ParquetSinkBuilder<T> {
    prefix: String,
    target_path: PathBuf,
    tmp_path: PathBuf,
    max_rows: usize,              // NEW: row count threshold
    max_size_bytes: usize,        // file size threshold
    roll_time: Duration,
    file_upload: FileUpload,
    auto_commit: bool,
    metric: String,
    compression: Compression,      // NEW: parquet compression
    row_group_size: usize,        // NEW: parquet row group size
    _phantom: PhantomData<T>,
}

pub struct ParquetSinkClient<T> {
    sender: MessageSender<T>,
    metric: String,
}

pub struct ParquetSink<T> {
    // Configuration
    target_path: PathBuf,
    tmp_path: PathBuf,
    prefix: String,
    max_rows: usize,
    max_size_bytes: usize,
    roll_time: Duration,

    // Runtime state
    messages: MessageReceiver<T>,
    file_upload: FileUpload,
    staged_files: Vec<PathBuf>,
    auto_commit: bool,
    active_sink: Option<ActiveParquetSink<T>>,
}

struct ActiveParquetSink<T> {
    file_path: PathBuf,
    writer: ArrowWriter<File>,    // parquet::arrow::ArrowWriter
    row_count: usize,
    created_at: DateTime<Utc>,
    schema: Arc<Schema>,          // Arrow schema
    buffer: Vec<T>,               // Accumulate records before batch write
}
```

**Key Configuration**:
- `DEFAULT_SINK_ROLL_SECS`: 3 * 60 (3 minutes) - same as file_store
- `DEFAULT_MAX_ROWS`: 100_000 rows per file
- `DEFAULT_MAX_SIZE_BYTES`: 100 * 1024 * 1024 (100 MB)
- `DEFAULT_ROW_GROUP_SIZE`: 10_000 rows
- `DEFAULT_COMPRESSION`: `Compression::SNAPPY` (fast, standard)
- `DEFAULT_BATCH_SIZE`: 1_000 rows (accumulate before writing batch)

**Write Flow**:
1. Client calls `write(record)` → sends to channel
2. Sink accumulates records in `Vec<T>`
3. When batch size reached (e.g., 1000 records):
   - Convert `Vec<T>` to Arrow RecordBatch using `serde_arrow`
   - Write batch to parquet file via `ArrowWriter`
4. When threshold reached (rows/size/time):
   - Close parquet file (finalize metadata, footers)
   - Move from tmp → target directory
   - If `auto_commit`: trigger upload

**Rotation Triggers**:
- Row count >= `max_rows`
- File size >= `max_size_bytes` (approximate check)
- Time since creation >= `roll_time`

**Parquet Configuration**:
```rust
let props = WriterProperties::builder()
    .set_compression(compression)
    .set_max_row_group_size(row_group_size)
    .set_write_batch_size(1024)
    .set_statistics_enabled(EnabledStatistics::Page)  // Enable stats for query engines
    .set_created_by("prestige v0.1.0".to_string())
    .build();

ArrowWriter::try_new(file, schema, Some(props))?
```

**Critical Decision - Compression**:
- **Use Parquet's Native Compression**: SNAPPY or ZSTD
- **NO external compression** (no gzip wrapper like file_store)
- Parquet files are self-contained and query-friendly
- Tools like DuckDB, Athena, Spark can read directly from S3

**Crib from file_store**:
- Builder pattern from `file_sink.rs:62-153`
- Channel-based client/server split
- `commit()` / `rollback()` semantics
- Auto-commit behavior
- Metrics integration
- `maybe_roll()` timer pattern from `file_sink.rs:447`

**Implementation Checklist**:
- [ ] Define constants (DEFAULT_SINK_ROLL_SECS, DEFAULT_MAX_ROWS, etc.)
- [ ] Implement `ParquetSinkBuilder` with builder methods
- [ ] Implement `ParquetSinkClient<T>` with channel sender
- [ ] Implement `write()` method with timeout and metrics
- [ ] Implement `write_all()` batch method
- [ ] Implement `commit()` method
- [ ] Implement `rollback()` method
- [ ] Implement `ParquetSink<T>` main struct
- [ ] Implement `ActiveParquetSink` with ArrowWriter
- [ ] Implement `new_sink()` to create new parquet file
- [ ] Implement record buffering and batch conversion
- [ ] Implement batch write to ArrowWriter
- [ ] Implement rotation logic (rows/size/time checks)
- [ ] Implement `maybe_roll()` timer-based rotation
- [ ] Implement `commit()` - finalize and upload files
- [ ] Implement `rollback()` - delete staged files
- [ ] Implement crash recovery (handle incomplete tmp files)
- [ ] Add metrics tracking (write count, errors, latency)
- [ ] Implement `ManagedTask` for lifecycle management
- [ ] Unit tests for builder pattern
- [ ] Integration tests for write → file creation
- [ ] Integration tests for rotation triggers
- [ ] Integration tests for commit/rollback
- [ ] Integration tests for crash recovery

---

### 3. **file_upload.rs** - S3 Upload Manager

**Purpose**: Async upload of completed parquet files to S3 with retry logic

**Architecture** (nearly identical to file_store):
```rust
pub struct FileUpload {
    pub sender: MessageSender,  // mpsc::UnboundedSender<PathBuf>
}

pub struct FileUploadServer {
    messages: UnboundedReceiverStream<PathBuf>,
    client: aws_sdk_s3::Client,
    bucket: String,
}
```

**Upload Flow**:
1. Receive file path via channel
2. Check file exists and is regular file
3. Upload to S3 via `put_object()` with retry (max 5 attempts)
4. On success: delete local file
5. On failure: log error, retry with exponential backoff

**S3 Upload Configuration**:
```rust
client.put_object()
    .bucket(&bucket)
    .key(filename)
    .body(ByteStream::from_path(&file).await?)
    .content_type("application/vnd.apache.parquet")  // Proper MIME type
    .send()
    .await?
```

**Key Features**:
- Concurrent uploads (5 workers via `for_each_concurrent`)
- Retry with backoff (10 second delay)
- Max 5 retries per file
- Automatic cleanup on success

**Crib from file_store**:
- **Copy almost verbatim** from `file_upload.rs`
- Only change: content-type to `"application/vnd.apache.parquet"`
- Keep all retry logic, concurrency patterns, error handling

**Implementation Checklist**:
- [ ] Define `FileUpload` struct with sender
- [ ] Define `FileUploadServer` struct
- [ ] Implement `FileUpload::new()` constructor
- [ ] Implement `upload_file()` method
- [ ] Implement upload server `run()` loop
- [ ] Add file existence and type checks
- [ ] Implement S3 put_object with correct content-type
- [ ] Implement retry logic with exponential backoff
- [ ] Implement file deletion after successful upload
- [ ] Add concurrency control (5 workers)
- [ ] Add logging for upload progress and errors
- [ ] Implement `ManagedTask` for lifecycle
- [ ] Unit tests for channel communication
- [ ] Integration tests with MinIO/LocalStack
- [ ] Test retry behavior on failures
- [ ] Test concurrent uploads

---

### 4. **file_source.rs** - Parquet File Reader

**Purpose**: Read parquet files from local filesystem or S3

**Architecture**:
```rust
// Stream of record batches from a parquet file
pub type RecordBatchStream = BoxStream<'static, Result<RecordBatch>>;

// Read from local paths
pub fn source<I, P>(paths: I) -> RecordBatchStream
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>;

// Read from S3
pub async fn source_s3_file(
    client: &Client,
    bucket: impl Into<String>,
    key: impl Into<String>,
) -> Result<RecordBatchStream>;

// Read multiple S3 files in order
pub fn source_s3_files(
    client: &Client,
    bucket: impl Into<String>,
    metas: FileMetaStream,
) -> RecordBatchStream;

// Read multiple S3 files in parallel
pub fn source_s3_files_unordered(
    client: &Client,
    bucket: impl Into<String>,
    workers: usize,
    metas: FileMetaStream,
) -> RecordBatchStream;
```

**Reading Strategy**:

**Local Files**:
```rust
let file = File::open(path).await?;
let builder = ParquetRecordBatchStreamBuilder::new(file).await?;
let stream = builder
    .with_batch_size(8192)  // configurable
    .build()?;
```

**S3 Files** (two approaches):

*Option A - Download then parse* (simpler, lower memory):
```rust
// Download entire file to memory
let bytes = client.get_object()
    .bucket(bucket)
    .key(key)
    .send()
    .await?
    .body
    .collect()
    .await?
    .into_bytes();

// Parse from bytes
let builder = ParquetRecordBatchStreamBuilder::new(Cursor::new(bytes)).await?;
```

*Option B - Streaming with range requests* (more complex, better for large files):
```rust
// Use parquet's ObjectStore integration with S3
// Requires converting aws-sdk-s3 to object_store format
```

**Recommendation**: Start with Option A, migrate to Option B if needed

**Crib from file_store**:
- Stream composition patterns from `file_source.rs:51-77`
- `try_buffered()` / `try_buffer_unordered()` for concurrency
- Error handling with `flat_map` for graceful failures

**Implementation Checklist**:
- [ ] Define `RecordBatchStream` type alias
- [ ] Implement `source()` for local file reading
- [ ] Implement `source_s3_file()` for single S3 file
- [ ] Implement S3 download and parse logic
- [ ] Implement `source_s3_files()` ordered reading
- [ ] Implement `source_s3_files_unordered()` parallel reading
- [ ] Add configurable batch size
- [ ] Add error handling for corrupt files
- [ ] Add metrics for read throughput
- [ ] Unit tests with sample parquet files
- [ ] Integration tests with S3
- [ ] Test ordered vs unordered reading
- [ ] Performance tests for large files

---

### 5. **file_store.rs** - S3 File Poller (analogous to file_info_poller)

**Purpose**: Continuously poll S3 for new parquet files and stream their contents

**Architecture**:
```rust
#[derive(Clone, Builder)]
pub struct FileStoreConfig<State> {
    #[builder(default = "DEFAULT_POLL_DURATION")]
    poll_duration: Duration,
    state: State,                    // Tracks processed files
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,                  // File type to poll
    lookback: LookbackBehavior,
    #[builder(default = "DEFAULT_OFFSET_DURATION")]
    offset: Duration,
    #[builder(default = "5")]
    queue_size: usize,
    #[builder(default = "default")]
    process_name: String,
}

pub struct FileStoreServer<State> {
    config: FileStoreConfig<State>,
    sender: Sender<FileStream>,
    file_queue: VecDeque<FileMeta>,
    latest_file_timestamp: Option<DateTime<Utc>>,
    cache: MemoryFileCache,
}

pub struct FileStream {
    pub file_meta: FileMeta,
    pub process_name: String,
    pub batches: RecordBatchStream,
}

// State tracking trait (implemented for sqlx::PgPool)
#[async_trait]
pub trait FileStoreState: Send + Sync + 'static {
    async fn latest_timestamp(&self, process_name: &str, file_type: &str)
        -> Result<Option<DateTime<Utc>>>;

    async fn exists(&self, process_name: &str, file_meta: &FileMeta)
        -> Result<bool>;

    async fn clean(&self, process_name: &str, file_type: &str, offset: DateTime<Utc>)
        -> Result<u64>;
}

// State recorder (implemented for sqlx::Transaction)
#[async_trait]
pub trait FileStoreStateRecorder {
    async fn record(&mut self, process_name: &str, file_meta: &FileMeta)
        -> Result;
}

pub enum LookbackBehavior {
    StartAfter(DateTime<Utc>),
    Max(Duration),
}
```

**Polling Flow**:
1. List S3 objects with prefix: `s3.list_objects_v2()`
2. Filter by timestamp range: `after` to `before`
3. Check against cache and database to avoid reprocessing
4. For each new file:
   - Download and stream record batches
   - Send `FileStream` to consumer
   - Consumer records processing in transaction
5. Sleep for `poll_duration` (30 seconds default)
6. Periodic cleanup of old records (12 hours)

**Deduplication Strategy**:
- In-memory cache (3 hour TTL) using `retainer::Cache`
- PostgreSQL `files_processed` table for persistence
- Two-level check: cache first, then DB query

**Database Schema** (same as file_store):
```sql
CREATE TABLE files_processed (
    process_name TEXT NOT NULL DEFAULT 'default',
    file_name VARCHAR PRIMARY KEY,
    file_type VARCHAR NOT NULL,
    file_timestamp TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_files_processed_lookup
    ON files_processed(process_name, file_type, file_timestamp DESC);
```

**Crib from file_store**:
- **Nearly identical structure** to `file_info_poller.rs`
- Lookback behavior logic from `file_info_poller.rs:369-382`
- Cache management and cleanup
- State trait patterns
- Builder with `derive_builder`

**Key Difference**: Instead of parsing protobuf bytes, we stream Arrow RecordBatches

**Implementation Checklist**:
- [ ] Define `FileStoreConfig` with builder
- [ ] Define `FileStoreServer` struct
- [ ] Define `FileStream` struct
- [ ] Define `FileStoreState` trait
- [ ] Define `FileStoreStateRecorder` trait
- [ ] Define `LookbackBehavior` enum
- [ ] Implement lookback calculation logic
- [ ] Implement S3 listing and filtering
- [ ] Implement deduplication with cache
- [ ] Implement deduplication with database
- [ ] Implement `get_next_file()` polling logic
- [ ] Implement main `run()` loop
- [ ] Implement periodic cleanup
- [ ] Implement `FileStoreState` for `sqlx::PgPool`
- [ ] Implement `FileStoreStateRecorder` for `sqlx::Transaction`
- [ ] Add metrics for polling lag
- [ ] Add metrics for processing throughput
- [ ] Unit tests for lookback logic
- [ ] Unit tests for deduplication
- [ ] Integration tests with PostgreSQL
- [ ] End-to-end test: write → upload → poll → process

---

### 6. **AWS Client Integration** (lib.rs)

**Purpose**: Shared S3 client creation and basic operations

**Client Pooling** (copy from file_store):
```rust
static CLIENT_MAP: OnceLock<Mutex<HashMap<ClientKey, Client>>> = OnceLock::new();

#[derive(PartialEq, Eq, Hash, Debug)]
struct ClientKey {
    region: Option<String>,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
}

pub async fn new_client(
    region: Option<String>,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
) -> Client;
```

**S3 Operations**:
```rust
// List files in bucket
pub fn list_files<A, B>(
    client: &Client,
    bucket: impl Into<String>,
    prefix: impl Into<String>,
    after: A,
    before: B,
) -> FileMetaStream
where
    A: Into<Option<DateTime<Utc>>> + Copy,
    B: Into<Option<DateTime<Utc>>> + Copy;

pub async fn list_all_files<A, B>(...) -> Result<Vec<FileMeta>>;

// Upload/download
pub async fn put_file(
    client: &Client,
    bucket: impl Into<String>,
    file: &Path
) -> Result;

pub async fn get_file(
    client: &Client,
    bucket: impl Into<String>,
    key: impl Into<String>
) -> Result<Bytes>;  // Returns complete file bytes

pub async fn remove_file(
    client: &Client,
    bucket: impl Into<String>,
    key: impl Into<String>
) -> Result;
```

**Stream Type Aliases**:
```rust
pub type Client = aws_sdk_s3::Client;
pub type Stream<T> = BoxStream<'static, Result<T>>;
pub type FileMetaStream = Stream<FileMeta>;
```

**Crib from file_store**:
- Client caching logic from `lib.rs:40-103`
- Pagination with `into_paginator()` from `lib.rs:120-137`
- Timestamp filtering logic

**Implementation Checklist**:
- [ ] Define `ClientKey` struct
- [ ] Implement client pooling with `OnceLock<Mutex<HashMap>>`
- [ ] Implement `new_client()` with caching
- [ ] Implement AWS config loading
- [ ] Implement region and endpoint configuration
- [ ] Implement credentials handling
- [ ] Implement `list_files()` with pagination
- [ ] Implement timestamp filtering in listing
- [ ] Implement `list_all_files()` collector
- [ ] Implement `put_file()` with correct content-type
- [ ] Implement `get_file()` download
- [ ] Implement `remove_file()` deletion
- [ ] Define stream type aliases
- [ ] Add logging for client creation
- [ ] Unit tests for client pooling
- [ ] Integration tests with MinIO

---

### 7. **error.rs** - Error Types

**Extend existing Error enum**:
```rust
#[derive(Debug, Error)]
pub enum Error {
    #[error("aws sdk s3 error: {0}")]
    Aws(#[from] aws_sdk_s3::Error),

    #[error("prestige configuration error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("file meta error: {0}")]
    FileMeta(#[from] crate::file_meta::FileMetaError),

    #[error("channel error: {0}")]
    Channel(#[from] ChannelError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serde arrow error: {0}")]
    SerdeArrow(String),

    #[cfg(feature = "sqlx")]
    #[error("db error: {0}")]
    Db(#[from] sqlx::Error),
}

#[derive(Error, Debug)]
pub enum ChannelError {
    #[error("failed to send {prefix} for process {process}")]
    PollerSendError { prefix: String, process: String },

    #[error("channel closed sink {name}")]
    SinkClosed { name: String },

    #[error("timeout for sink {name}")]
    SinkTimeout { name: String },

    #[error("channel closed for upload {path}")]
    UploadClosed { path: PathBuf },
}

impl ChannelError {
    pub fn poller_send_error(prefix: &str, process: &str) -> Error;
    pub fn sink_closed(name: &str) -> Error;
    pub fn sink_timeout(name: &str) -> Error;
    pub fn upload_closed(path: &Path) -> Error;
}
```

**Implementation Checklist**:
- [ ] Add `Parquet` error variant
- [ ] Add `Arrow` error variant
- [ ] Add `FileMeta` error variant
- [ ] Add `Channel` error variant
- [ ] Add `SerdeArrow` error variant
- [ ] Define `ChannelError` enum with variants
- [ ] Implement helper constructors for `ChannelError`
- [ ] Add `From` implementations for conversions
- [ ] Update documentation

---

## Parquet Compression Strategy

### Wire Transfer Compression: YES ✓
Use parquet's built-in columnar compression:
- **SNAPPY**: Default, fast, good compression ratio (~3-5x)
- **ZSTD**: Better compression (~5-10x), slightly slower (configurable option)
- **LZ4**: Fastest, lower compression (~2-3x) (for latency-sensitive cases)

### At-Rest Compression: NO ✗
- Files stored as `.parquet` (not `.parquet.gz` or encrypted)
- Directly queryable by Athena, DuckDB, Spark, pandas
- S3 Intelligent-Tiering can apply transparent compression

### Parquet File Configuration:
```rust
WriterProperties::builder()
    .set_compression(Compression::SNAPPY)  // Column-level compression
    .set_statistics_enabled(EnabledStatistics::Page)  // For query pruning
    .set_max_row_group_size(10_000)  // Smaller row groups = better selectivity
    .set_write_batch_size(1024)
    .set_created_by("prestige v0.1.0".to_string())
    .build()
```

### Why This Works:
1. **Query Engines Can Read Directly**: No decompression step needed
2. **Efficient Storage**: Parquet + Snappy typically achieves 70-90% compression
3. **Columnar Benefits**: Only read columns you query (I/O savings)
4. **Statistics**: Min/max/null count enable partition pruning

---

## Type System Integration

### Record Traits:
```rust
// Types must implement these for parquet sink
pub trait ParquetSerialize {
    fn arrow_schema() -> Arc<Schema>;  // Arrow schema for the type
}

// Users can use serde_arrow's derive or manual implementation
```

### Conversion Strategy:
Use `serde_arrow` for `Vec<T>` → `RecordBatch`:
```rust
let records: Vec<SensorReading> = vec![...];
let schema = SensorReading::arrow_schema();
let arrays = serde_arrow::to_arrow(&schema, &records)?;
let batch = RecordBatch::try_new(schema, arrays)?;
```

---

## Dependencies to Add

Update `prestige/Cargo.toml`:
```toml
[dependencies]
# Existing dependencies...
parquet = "55"
arrow = { version = "55", features = ["ipc"] }
serde_arrow = { workspace = true, features = ["arrow-55"] }

# New dependencies needed:
regex = "1"  # For file name parsing
retainer = "0.3"  # For in-memory cache (same as file_store)
tokio-stream = "0"  # For stream utilities
aws-smithy-types-convert = { version = "0", features = ["convert-streams"] }
derive_builder = "0.20"  # For builder pattern
async-trait = "0"  # For async traits

[features]
sqlx = ["dep:sqlx"]
```

**Dependency Checklist**:
- [ ] Add `regex` dependency
- [ ] Add `retainer` dependency
- [ ] Add `tokio-stream` dependency
- [ ] Add `aws-smithy-types-convert` dependency
- [ ] Add `derive_builder` dependency
- [ ] Add `async-trait` dependency
- [ ] Verify all workspace dependencies are available
- [ ] Run `cargo check` to verify dependencies

---

## Implementation Phases

### Phase 1: Core Infrastructure ✓
**Goal**: Foundation for all other modules

- [ ] Update `Cargo.toml` with new dependencies
- [ ] Extend `error.rs` with new error types
- [ ] Implement `file_meta.rs` module
  - [ ] Core struct and traits
  - [ ] File name parsing
  - [ ] Unit tests
- [ ] Implement AWS client integration in `lib.rs`
  - [ ] Client pooling
  - [ ] Basic S3 operations
  - [ ] Stream type aliases
- [ ] Integration test: list files from S3

**Success Criteria**:
- `cargo check` passes
- All unit tests pass
- Can create FileMeta from S3 objects
- Can list files from MinIO/S3

---

### Phase 2: Write Path ✓
**Goal**: Write records to parquet files and upload to S3

- [ ] Implement `file_upload.rs` module
  - [ ] Core structs
  - [ ] Upload with retry logic
  - [ ] Unit tests
- [ ] Implement `file_sink.rs` module
  - [ ] Builder pattern
  - [ ] Client/server split with channels
  - [ ] Record buffering
  - [ ] Batch conversion to Arrow
  - [ ] Parquet writer integration
  - [ ] Rotation logic (rows/size/time)
  - [ ] Commit/rollback
  - [ ] Unit tests
- [ ] Integration tests
  - [ ] Write records → verify parquet file created
  - [ ] Verify rotation on row count threshold
  - [ ] Verify rotation on size threshold
  - [ ] Verify rotation on time threshold
  - [ ] Upload to MinIO → verify file exists
  - [ ] Test commit/rollback semantics
- [ ] Add metrics and logging

**Success Criteria**:
- Can write 100k+ records/sec
- Files rotate correctly on thresholds
- Files upload to S3 successfully
- Memory usage bounded (< 500MB)
- Files readable by `parquet-tools`

---

### Phase 3: Read Path ✓
**Goal**: Read parquet files from local and S3

- [ ] Implement `file_source.rs` module
  - [ ] Local file reading
  - [ ] S3 file reading (download approach)
  - [ ] Ordered multi-file reading
  - [ ] Unordered/parallel multi-file reading
  - [ ] Unit tests
- [ ] Integration tests
  - [ ] Write → read → verify data integrity
  - [ ] Read from MinIO
  - [ ] Test ordered reading
  - [ ] Test parallel reading
  - [ ] Benchmark read throughput
- [ ] Performance testing
  - [ ] Measure throughput (target: 500k+ rows/sec)
  - [ ] Measure memory usage
  - [ ] Test with various file sizes

**Success Criteria**:
- Can read all record batches from parquet file
- Data integrity verified (write then read)
- Read throughput meets target
- Ordered and unordered reading work correctly

---

### Phase 4: Polling & State Management ✓
**Goal**: Continuously poll S3 for new files

- [ ] Implement `file_store.rs` module
  - [ ] Config and builder
  - [ ] Server with polling loop
  - [ ] S3 listing and filtering
  - [ ] Deduplication cache
  - [ ] Database integration
  - [ ] State traits
  - [ ] Lookback behavior
  - [ ] Cleanup logic
  - [ ] Unit tests
- [ ] Database integration
  - [ ] Create migration for `files_processed` table
  - [ ] Implement `FileStoreState` for `PgPool`
  - [ ] Implement `FileStoreStateRecorder` for `Transaction`
- [ ] Integration tests
  - [ ] Poller detects new files
  - [ ] Deduplication works (cache and DB)
  - [ ] Lookback logic correct
  - [ ] Cleanup removes old records
  - [ ] End-to-end: write → upload → poll → process
- [ ] Add metrics
  - [ ] Polling lag
  - [ ] Files processed/sec
  - [ ] Cache hit rate

**Success Criteria**:
- Detects new files within 30 seconds
- No duplicate processing
- Cleanup maintains bounded DB size
- End-to-end pipeline works

---

### Phase 5: Optimization & Documentation ✓
**Goal**: Production-ready performance and docs

- [ ] Performance optimization
  - [ ] Benchmark SNAPPY vs ZSTD vs LZ4
  - [ ] Tune row group sizes
  - [ ] Tune batch sizes
  - [ ] Profile memory usage
  - [ ] Profile CPU usage
- [ ] Documentation
  - [ ] API documentation with rustdoc
  - [ ] Usage examples
  - [ ] Configuration guide
  - [ ] Migration guide from protobuf file_store
  - [ ] Troubleshooting guide
- [ ] Production readiness
  - [ ] Add observability (metrics, traces)
  - [ ] Add health checks
  - [ ] Add graceful shutdown
  - [ ] Load testing
  - [ ] Chaos testing (network failures, crashes)

**Success Criteria**:
- All benchmarks meet targets
- Documentation complete and clear
- Production deployment successful
- Zero data loss under failure conditions

---

## Key Design Decisions Summary

| Aspect | Decision | Rationale |
|--------|----------|-----------|
| **Compression** | Parquet-native (SNAPPY/ZSTD), no external wrapper | Query-friendly, standard tooling support |
| **Batching** | Accumulate in Vec<T>, convert to RecordBatch | Balance memory vs I/O efficiency |
| **Rotation** | Rows (100k), Size (100MB), Time (3min) | Predictable file sizes, frequent uploads |
| **S3 Content-Type** | `application/vnd.apache.parquet` | Proper MIME type for tooling |
| **Deduplication** | Memory cache + PostgreSQL | Fast checks, persistent state |
| **Concurrency** | Channel-based client/server split | Same pattern as file_store |
| **Error Handling** | Same as file_store, graceful degradation | Proven resilient patterns |
| **S3 Reading** | Download-then-parse (Option A) | Simpler, adequate for most use cases |

---

## Testing Strategy

### Unit Tests:
- [ ] FileMeta parsing and generation
- [ ] Parquet schema generation from types
- [ ] Timestamp filtering logic
- [ ] Lookback calculation
- [ ] Channel communication

### Integration Tests:
- [ ] Write 1M records → verify parquet file properties
- [ ] Upload to MinIO → download → verify contents
- [ ] Poller detects new files within poll interval
- [ ] Crash recovery: incomplete files handled correctly
- [ ] Database state tracking works correctly

### Performance Tests:
- [ ] Write throughput: target 100k+ rows/sec
- [ ] Read throughput: target 500k+ rows/sec
- [ ] Memory usage: bounded even with large files
- [ ] S3 upload time: track 95th percentile
- [ ] End-to-end latency: write → upload → poll → read

### Load Tests:
- [ ] Sustained write load for 1 hour
- [ ] Concurrent readers and writers
- [ ] Large files (>1GB)
- [ ] Many small files (>10k files)

### Chaos Tests:
- [ ] Network failures during upload
- [ ] Process crash during write
- [ ] S3 unavailability
- [ ] Database connection loss
- [ ] Disk full scenarios

---

## Migration from Protobuf file_store

For teams using both systems:

### Protobuf Version:
```rust
use file_store::{FileSinkBuilder, FileInfoPoller};

let (sink, server) = FileSinkBuilder::new("events", path, upload, "metric")
    .create::<ProtoEvent>().await?;

sink.write(proto_event, &[]).await?;
```

### Parquet Version:
```rust
use prestige::{ParquetSinkBuilder, FileStoreServer};

let (sink, server) = ParquetSinkBuilder::new("events", path, upload, "metric")
    .create::<ParquetEvent>().await?;

sink.write(parquet_event, &[]).await?;
```

**Key Differences**:
- No `prost::Message` trait requirement
- Use types compatible with `serde_arrow`
- Files are `.parquet` not `.gz`
- Can query directly without custom tools
- Better compression ratios with columnar format
- Native support in data warehouses

---

## Success Criteria

### Performance:
- [x] Write 100k+ records/sec to parquet files
- [x] Read 500k+ records/sec from parquet files
- [x] Memory usage < 500MB for sink process
- [x] File rotation works within 1 second of threshold

### Functionality:
- [x] Files readable by DuckDB without conversion
- [x] Files readable by Athena without conversion
- [x] Files readable by pandas/pyarrow
- [x] S3 poller detects new files within 30 seconds
- [x] No data loss during crashes (tmp file recovery)
- [x] No duplicate processing (deduplication works)

### Code Quality:
- [x] API matches file_store patterns (easy migration)
- [x] All public APIs documented
- [x] >80% code coverage with tests
- [x] No clippy warnings
- [x] Passes `cargo fmt --check`

---

## Open Questions & Future Enhancements

### Open Questions:
- [ ] Should we support schema evolution?
- [ ] Should we support partitioned writes (e.g., by date)?
- [ ] Should we add encryption support?
- [ ] Should we support Delta Lake format?

### Future Enhancements:
- [ ] Schema registry integration
- [ ] Automatic schema evolution
- [ ] Partitioned writes (Hive-style)
- [ ] Delta Lake/Iceberg support
- [ ] Arrow IPC streaming for zero-copy
- [ ] Object store abstraction (not just S3)
- [ ] Metrics export (Prometheus format)
- [ ] Distributed tracing (OpenTelemetry)

---

## Notes

- This plan is a living document - update as implementation progresses
- Use this as a checklist to track completion
- Each checkbox represents a concrete deliverable
- Phase completion requires all checklist items in that phase
- Performance targets are guidelines, may need adjustment based on hardware
- Regular benchmarking will inform optimization priorities

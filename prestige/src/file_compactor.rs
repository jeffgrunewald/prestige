use arrow::array::RecordBatch;
use arrow_row::{RowConverter, SortField};
use arrow_select::filter::filter_record_batch;
use chrono::{DateTime, Utc};
use derive_builder::Builder;
use futures::{StreamExt, TryStreamExt};
use parquet::{
    arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder},
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    marker::PhantomData,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::{info, warn};

/// Default bounded concurrency for S3 batch operations (marker checks, deletions, etc.)
const S3_BATCH_CONCURRENCY: usize = 50;

use crate::{
    Client,
    error::{CompactionError, Error, Result},
    file_meta::FileMeta,
    file_sink::DEFAULT_MAX_SIZE_BYTES,
    list_files, put_file, remove_file,
    traits::ArrowSchema,
};
use aws_sdk_s3::primitives::ByteStream;

/// Run an async operation on each item with bounded concurrency and progress logging.
///
/// Returns a vec of `(key, result)` pairs preserving input order.
async fn run_batched<T, Fut>(
    items: Vec<String>,
    op: impl FnMut(String) -> Fut,
    label: &str,
    progress_interval: usize,
) -> Vec<(String, T)>
where
    Fut: Future<Output = (String, T)>,
{
    let total = items.len();
    let start = Instant::now();
    let mut results = Vec::with_capacity(total);
    let mut completed: usize = 0;

    let mut stream =
        futures::stream::iter(items.into_iter().map(op)).buffered(S3_BATCH_CONCURRENCY);

    while let Some(result) = stream.next().await {
        completed += 1;
        results.push(result);
        if completed.is_multiple_of(progress_interval) {
            info!(
                completed,
                total,
                elapsed_secs = start.elapsed().as_secs(),
                "{label} in progress"
            );
        }
    }

    info!(
        completed,
        total,
        elapsed_secs = start.elapsed().as_secs(),
        "{label} complete"
    );

    results
}

/// File Compactor Module
///
/// Consolidates multiple small parquet files from S3 into fewer, larger files using
/// a streaming architecture with idempotency protection against duplicate records.
///
/// # File Naming Convention
///
/// Original files: `{prefix}.{timestamp_millis}.parquet`
/// Compacted files: `{prefix}.{timestamp_millis}.c.parquet`
/// Processed markers: `{prefix}.{timestamp_millis}.parquet.processed`
///
/// Examples:
/// - `sensor_data.1234567890123.parquet` (original)
/// - `sensor_data.1234567890123.c.parquet` (compacted)
/// - `sensor_data.1234567890123.parquet.processed` (marker)
///
/// # Algorithm
///
/// The compactor uses a streaming approach with idempotency protection:
/// 1. List all uncompacted files (those without `.c` marker) before the specified timestamp
/// 2. Sort files by timestamp for deterministic processing
/// 3. For each file:
///    - Skip if processed marker exists (idempotency check)
///    - Download and deserialize records
///    - Accumulate in memory until size limit (100MB default)
///    - When limit reached, finalize current batch and upload
/// 4. After successful upload, create processed markers for all source files
/// 5. Delete original files and markers
///
/// This approach avoids memory limits by processing incrementally rather than loading
/// all files for a day into memory at once.
///
/// # Idempotency and Duplicate Prevention
///
/// The compactor protects against duplicate records through:
///
/// 1. **Processed Markers**: After successful upload, a `.processed` marker is created
///    for each source file. On subsequent runs, files with markers are skipped.
///
/// 2. **Deletion Failure Tracking**: The `CompactionResult` includes `deletion_failures`
///    listing source files that failed to delete. Callers should monitor this field and
///    alert on failures to prevent eventual reprocessing.
///
/// 3. **Checkpoint-Based Processing**: Use `after_timestamp` and `last_processed_timestamp`
///    to maintain progress between runs. The timestamp checkpoint should be persisted
///    by the caller.
///
/// ## Handling Deletion Failures
///
/// If source file deletion fails but upload succeeded:
/// - The compacted file exists in S3 with the records
/// - Processed markers exist for the source files
/// - Source files remain in S3 (tracked in `deletion_failures`)
/// - Next run will skip these files (marker check) preventing duplicates
/// - Manual cleanup may be needed to remove orphaned source files
///
/// ## Best Practices
///
/// ```rust,ignore
/// use prestige::file_compactor::FileCompactorConfigBuilder;
/// use chrono::Utc;
///
/// let result = FileCompactorConfigBuilder::default()
///     .client(client)
///     .bucket("my-bucket".to_string())
///     .prefix("data".to_string())
///     .until_timestamp(Utc::now())
///     .delete_originals(true)
///     .execute::<MyDataType>()
///     .await?;
///
/// // Check for deletion failures
/// if !result.deletion_failures.is_empty() {
///     eprintln!("Warning: {} files failed to delete", result.deletion_failures.len());
///     for key in &result.deletion_failures {
///         eprintln!("  - {}", key);
///     }
///     // Alert monitoring system, but continue operation
/// }
///
/// // Persist checkpoint for next run
/// if let Some(ts) = result.last_processed_timestamp {
///     save_checkpoint(ts).await?;
/// }
/// ```
///
/// # Reference Implementation: Scheduled Compaction with ManagedProc
///
/// This example shows how to implement a scheduled compaction job that runs periodically,
/// querying a database to determine the cutoff timestamp for compaction.
///
/// ```rust,ignore
/// use prestige::{Client, file_compactor::FileCompactorConfigBuilder, traits::ArrowSchema};
/// use chrono::{DateTime, Duration, Utc};
/// use serde::{Deserialize, Serialize};
/// use super_visor::{ManagedProc, ShutdownSignal};
/// use tokio::time::{interval, Duration as TokioDuration};
/// use tracing::{info, error};
///
/// // Your data type
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// struct SensorData {
///     sensor_id: String,
///     temperature: f64,
///     timestamp: i64,
/// }
///
/// impl ArrowSchema for SensorData {
///     fn arrow_schema() -> std::sync::Arc<arrow::datatypes::Schema> {
///         // Implementation details...
///         # unimplemented!()
///     }
/// }
///
/// // Settings for the compaction job
/// #[derive(Debug, Clone)]
/// struct CompactionJobSettings {
///     s3_client: Client,
///     bucket: String,
///     prefix: String,
///     db_pool: sqlx::PgPool,
///     /// How often to run compaction (e.g., every hour)
///     interval_secs: u64,
///     /// Compact files older than this many minutes
///     lookback_minutes: i64,
/// }
///
/// // The compaction job as a ManagedProc
/// struct FileCompactionJob {
///     settings: CompactionJobSettings,
/// }
///
/// impl FileCompactionJob {
///     fn new(settings: CompactionJobSettings) -> Self {
///         Self { settings }
///     }
///
///     /// Query database to get the cutoff timestamp for compaction
///     async fn get_compaction_timestamp(&self) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
///         // Option 1: Use current time minus lookback period
///         let cutoff = Utc::now() - Duration::minutes(self.settings.lookback_minutes);
///
///         // Option 2: Query database for latest processed file timestamp
///         // This ensures we only compact files that have been fully processed
///         let db_timestamp: Option<DateTime<Utc>> = sqlx::query_scalar(
///             r#"
///             SELECT MAX(file_timestamp)
///             FROM files_processed
///             WHERE file_type = $1
///             "#
///         )
///         .bind(&self.settings.prefix)
///         .fetch_one(&self.settings.db_pool)
///         .await?;
///
///         // Use the earlier of the two timestamps (more conservative)
///         let timestamp = match db_timestamp {
///             Some(ts) if ts < cutoff => ts,
///             _ => cutoff,
///         };
///
///         info!(
///             prefix = %self.settings.prefix,
///             timestamp = %timestamp,
///             "Determined compaction cutoff timestamp"
///         );
///
///         Ok(timestamp)
///     }
///
///     /// Run a single compaction cycle
///     async fn run_compaction(&self) -> Result<(), Box<dyn std::error::Error>> {
///         let until_timestamp = self.get_compaction_timestamp().await?;
///
///         info!(
///             prefix = %self.settings.prefix,
///             bucket = %self.settings.bucket,
///             until = %until_timestamp,
///             "Starting compaction cycle"
///         );
///
///         // Build and execute compaction
///         let result = FileCompactorConfigBuilder::default()
///             .client(self.settings.s3_client.clone())
///             .bucket(self.settings.bucket.clone())
///             .prefix(self.settings.prefix.clone())
///             .until_timestamp(until_timestamp)
///             .max_bytes_per_file(100 * 1024 * 1024) // 100MB, or use default
///             .compression(parquet::basic::Compression::SNAPPY)
///             .delete_originals(true)
///             .execute::<SensorData>()
///             .await?;
///
///         info!(
///             prefix = %self.settings.prefix,
///             files_processed = result.files_processed,
///             files_created = result.files_created,
///             records = result.records_consolidated,
///             bytes_saved = result.bytes_saved,
///             "Compaction cycle completed"
///         );
///
///         Ok(())
///     }
///
///     /// Main run loop with periodic execution
///     async fn run(self, mut shutdown: ShutdownSignal) -> Result<(), Box<dyn std::error::Error>> {
///         let mut tick = interval(TokioDuration::from_secs(self.settings.interval_secs));
///
///         info!(
///             prefix = %self.settings.prefix,
///             interval_secs = self.settings.interval_secs,
///             "Starting file compaction job"
///         );
///
///         loop {
///             tokio::select! {
///                 biased;
///                 _ = &mut shutdown => {
///                     info!(
///                         prefix = %self.settings.prefix,
///                         "Shutting down file compaction job"
///                     );
///                     break Ok(());
///                 }
///                 _ = tick.tick() => {
///                     if let Err(e) = self.run_compaction().await {
///                         error!(
///                             prefix = %self.settings.prefix,
///                             error = %e,
///                             "Compaction cycle failed"
///                         );
///                         // Continue running despite errors
///                     }
///                 }
///             }
///         }
///     }
/// }
///
/// impl ManagedProc for FileCompactionJob {
///     fn run_proc(self: Box<Self>, shutdown: ShutdownSignal) -> super_visor::ManagedFuture {
///         super_visor::spawn(self.run(shutdown))
///     }
/// }
///
/// // Usage example:
/// async fn start_compaction_supervisor() -> Result<(), Box<dyn std::error::Error>> {
///     let s3_client = /* create S3 client */
///     # Client::new();
///     let db_pool = /* create database pool */
///     # sqlx::PgPool::connect("").await?;
///
///     let settings = CompactionJobSettings {
///         s3_client,
///         bucket: "my-data-bucket".to_string(),
///         prefix: "sensor_data".to_string(),
///         db_pool,
///         interval_secs: 3600, // Run every hour
///         lookback_minutes: 120, // Compact files older than 2 hours
///     };
///
///     let job = FileCompactionJob::new(settings);
///
///     // Spawn as a supervised process
///     let supervisor = super_visor::Supervisor::new();
///     supervisor.supervised(job);
///     supervisor.await;
///
///     Ok(())
/// }
/// ```
///
/// # Database Schema
///
/// The reference implementation assumes a `files_processed` table:
///
/// ```sql
/// CREATE TABLE files_processed (
///     process_name TEXT NOT NULL DEFAULT 'default',
///     file_name VARCHAR PRIMARY KEY,
///     file_type VARCHAR NOT NULL,
///     file_timestamp TIMESTAMPTZ NOT NULL,
///     processed_at TIMESTAMPTZ NOT NULL
/// );
///
/// CREATE INDEX idx_files_processed_compaction
///     ON files_processed(file_type, file_timestamp DESC);
/// ```
///
/// Configuration for file compaction operations
#[derive(Debug, Clone, Builder)]
#[builder(pattern = "owned")]
pub struct FileCompactorConfig<T> {
    /// S3 client
    client: Client,

    /// S3 bucket name
    bucket: String,

    /// File prefix to compact (e.g., "sensor_data")
    prefix: String,

    /// Optional timestamp to start compaction after (for checkpoint-based processing)
    /// When None, compacts all files from the beginning
    /// When Some, only compacts files with timestamp > after_timestamp (exclusive)
    #[builder(default = "None")]
    after_timestamp: Option<DateTime<Utc>>,

    /// Compact files until this timestamp (inclusive)
    /// Files with timestamp <= until_timestamp will be compacted
    until_timestamp: DateTime<Utc>,

    /// Maximum bytes per output file (soft limit, default 100MB)
    #[builder(default = "DEFAULT_MAX_SIZE_BYTES")]
    max_bytes_per_file: usize,

    /// Compression for output files
    #[builder(default = "Compression::SNAPPY")]
    compression: Compression,

    /// Row group size for parquet
    #[builder(default = "10_000")]
    row_group_size: usize,

    /// Whether to delete original files after successful compaction
    #[builder(default = "true")]
    delete_originals: bool,

    /// Enable row-level deduplication during compaction
    #[builder(default = "false")]
    enable_deduplication: bool,

    /// PhantomData for type parameter
    #[builder(default)]
    _phantom: PhantomData<T>,
}

/// Result of a compaction operation with statistics
#[derive(Debug, Clone, Serialize)]
pub struct CompactionResult {
    /// Number of input files processed
    pub files_processed: usize,

    /// Number of output files created
    pub files_created: usize,

    /// Total records consolidated
    pub records_consolidated: usize,

    /// Approximate storage savings in bytes
    pub bytes_saved: usize,

    /// Number of duplicate records eliminated (only non-zero when deduplication enabled)
    #[serde(skip_serializing_if = "is_zero", default)]
    pub duplicate_records_eliminated: usize,

    /// Timestamp of the last source file successfully processed.
    /// Use this as `after_timestamp` for subsequent compaction runs to implement checkpointing.
    /// None if no files were processed.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_processed_timestamp: Option<DateTime<Utc>>,

    /// S3 keys of source files that failed to delete after successful compaction.
    /// These files remain in S3 and may be reprocessed in subsequent runs if
    /// checkpoint management is not handled properly.
    /// Empty if all deletions succeeded or delete_originals was false.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub deletion_failures: Vec<String>,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

impl CompactionResult {
    /// Create an empty result (no files to compact)
    pub fn empty() -> Self {
        Self {
            files_processed: 0,
            files_created: 0,
            records_consolidated: 0,
            bytes_saved: 0,
            duplicate_records_eliminated: 0,
            last_processed_timestamp: None,
            deletion_failures: Vec::new(),
        }
    }
}

/// Helper struct for finalization results
#[derive(Debug)]
struct FinalizeResult {
    records_count: usize,
    bytes_saved: usize,
    duplicate_count: usize,
    deletion_failures: Vec<String>,
}

/// Accumulator for deduplicating RecordBatches
struct DeduplicatingAccumulator {
    batches: Vec<RecordBatch>,
    seen_hashes: HashSet<u128>,
    total_records: usize,
    duplicate_count: usize,
}

impl DeduplicatingAccumulator {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
            seen_hashes: HashSet::new(),
            total_records: 0,
            duplicate_count: 0,
        }
    }

    fn add_batch(&mut self, batch: RecordBatch, deduplicate: bool) -> Result<()> {
        if !deduplicate {
            self.total_records += batch.num_rows();
            self.batches.push(batch);
            return Ok(());
        }

        // Compute row hashes for deduplication
        let hashes = compute_row_hashes(&batch)?;
        let mut keep_mask = vec![false; batch.num_rows()];

        for (idx, hash) in hashes.iter().enumerate() {
            if self.seen_hashes.insert(*hash) {
                keep_mask[idx] = true;
            } else {
                self.duplicate_count += 1;
            }
        }

        // Filter batch to only keep non-duplicate rows
        let filtered = filter_batch_by_mask(&batch, &keep_mask)?;
        if filtered.num_rows() > 0 {
            self.total_records += filtered.num_rows();
            self.batches.push(filtered);
        }

        Ok(())
    }

    fn estimated_size(&self) -> usize {
        self.batches.iter().map(|b| b.get_array_memory_size()).sum()
    }

    fn take_batches(&mut self) -> (Vec<RecordBatch>, usize) {
        let batches = std::mem::take(&mut self.batches);
        let duplicate_count = self.duplicate_count;
        self.total_records = 0;
        self.duplicate_count = 0;
        (batches, duplicate_count)
    }

    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    fn total_records(&self) -> usize {
        self.total_records
    }
}

/// Compute hash for each row in a RecordBatch using arrow_row for efficient row conversion
fn compute_row_hashes(batch: &RecordBatch) -> Result<Vec<u128>> {
    let schema = batch.schema();
    let sort_fields: Vec<SortField> = schema
        .fields()
        .iter()
        .map(|field| SortField::new(field.data_type().clone()))
        .collect();

    let converter = RowConverter::new(sort_fields)?;
    let rows = converter.convert_columns(batch.columns())?;

    let hashes = (0..batch.num_rows())
        .map(|idx| xxhash_rust::xxh3::xxh3_128(rows.row(idx).as_ref()))
        .collect();

    Ok(hashes)
}

/// Filter a RecordBatch by a boolean mask
fn filter_batch_by_mask(batch: &RecordBatch, keep: &[bool]) -> Result<RecordBatch> {
    use arrow::array::BooleanArray;
    let filter_array = BooleanArray::from(keep.to_vec());
    let filtered = filter_record_batch(batch, &filter_array)?;
    Ok(filtered)
}

/// Serialize batches to an in-memory buffer with the given writer properties
/// and return the total byte count. Used by plan mode to measure output size
/// without writing to disk or S3.
fn measure_output_size(
    batches: &[RecordBatch],
    schema: &Arc<arrow::datatypes::Schema>,
    compression: Compression,
    row_group_size: usize,
) -> Result<usize> {
    let mut buf = Vec::new();
    let props = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_size(row_group_size)
        .set_write_batch_size(1024)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_created_by(format!("prestige/{}", env!("CARGO_PKG_VERSION")))
        .build();

    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props))?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;

    Ok(buf.len())
}

/// Create a marker file in S3 indicating a source file has been compacted
/// Marker format: {source_key}.processed
async fn create_processed_marker(client: &Client, bucket: &str, source_key: &str) -> Result<()> {
    let marker_key = format!("{}.processed", source_key);

    client
        .put_object()
        .bucket(bucket)
        .key(marker_key)
        .body(ByteStream::from_static(b""))
        .content_type("text/plain")
        .send()
        .await
        .map(|_| ())
        .map_err(|e| Error::from(aws_sdk_s3::Error::from(e)))
}

/// Check if a processed marker exists for a source file
async fn has_processed_marker(client: &Client, bucket: &str, source_key: &str) -> bool {
    let marker_key = format!("{}.processed", source_key);

    client
        .head_object()
        .bucket(bucket)
        .key(marker_key)
        .send()
        .await
        .is_ok()
}

/// Delete a processed marker file
async fn delete_processed_marker(client: &Client, bucket: &str, source_key: &str) -> Result<()> {
    let marker_key = format!("{}.processed", source_key);
    remove_file(client, bucket, &marker_key).await
}

/// Delete original files and their markers from S3 after successful consolidation
/// Returns a vector of file keys that failed to delete
async fn delete_original_files(client: Client, bucket: String, files: Vec<String>) -> Vec<String> {
    let results = run_batched(
        files,
        |key| {
            let client = client.clone();
            let bucket = bucket.clone();
            async move {
                let result = remove_file(&client, &bucket, &key).await;
                let _ = delete_processed_marker(&client, &bucket, &key).await;
                (key, result)
            }
        },
        "Deletion",
        1_000,
    )
    .await;

    results
        .into_iter()
        .filter_map(|(key, result)| {
            if let Err(e) = result {
                warn!(key, error = ?e, "Failed to delete original file");
                Some(key)
            } else {
                None
            }
        })
        .collect()
}

impl FileCompactorConfigBuilder<()> {
    /// Execute compaction without type parameter using RecordBatch directly
    pub async fn execute_schema_agnostic(self) -> Result<CompactionResult> {
        let config = self
            .build()
            .map_err(|e| Error::SerdeArrow(format!("Config builder error: {}", e)))?;
        execute_compaction_schema_agnostic(config).await
    }

    /// Plan compaction without writing or deleting any files.
    /// Reads all source parquet files and performs the full accumulation and
    /// deduplication logic to produce accurate statistics matching what an
    /// actual compaction run would return.
    pub async fn plan_schema_agnostic(self) -> Result<CompactionResult> {
        let config = self
            .build()
            .map_err(|e| Error::SerdeArrow(format!("Config builder error: {}", e)))?;
        plan_compaction_schema_agnostic(config).await
    }
}

impl<T> FileCompactorConfigBuilder<T>
where
    T: ArrowSchema + Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
{
    /// Execute the file compaction operation
    ///
    /// This delegates to the schema-agnostic implementation for efficiency.
    /// The type parameter `T` is maintained for backward compatibility but is not used.
    pub async fn execute(self) -> Result<CompactionResult> {
        let config = self
            .build()
            .map_err(|e| Error::SerdeArrow(format!("Config builder error: {}", e)))?;

        // Delegate to schema-agnostic implementation
        let schema_agnostic_config = FileCompactorConfig {
            client: config.client,
            bucket: config.bucket,
            prefix: config.prefix,
            after_timestamp: config.after_timestamp,
            until_timestamp: config.until_timestamp,
            max_bytes_per_file: config.max_bytes_per_file,
            compression: config.compression,
            row_group_size: config.row_group_size,
            delete_originals: config.delete_originals,
            enable_deduplication: config.enable_deduplication,
            _phantom: PhantomData::<()>,
        };

        execute_compaction_schema_agnostic(schema_agnostic_config).await
    }
}

/// Finalize and upload accumulated RecordBatches to S3 (schema-agnostic version)
async fn finalize_and_upload_schema_agnostic(
    batches: Vec<RecordBatch>,
    duplicate_count: usize,
    source_files: Vec<FileMeta>,
    schema: &Arc<arrow::datatypes::Schema>,
    config: &FileCompactorConfig<()>,
    temp_dir: &Path,
) -> Result<FinalizeResult> {
    // 1. Determine output timestamp (latest from sources)
    let latest_timestamp = source_files
        .iter()
        .map(|f| f.timestamp)
        .max()
        .ok_or(CompactionError::NoSourceFiles)?;

    // 2. Create compacted file metadata
    let compacted_meta = FileMeta::as_compacted(config.prefix.clone(), latest_timestamp);

    let total_records: usize = batches.iter().map(|b| b.num_rows()).sum();
    info!(
        "Finalizing {} records from {} source files into {}",
        total_records,
        source_files.len(),
        compacted_meta.key
    );

    // 3. Write to local temp file (offloaded to blocking pool to avoid starving the async runtime)
    let local_path = temp_dir.join(&compacted_meta.key);
    let compression = config.compression;
    let row_group_size = config.row_group_size;
    let schema_clone = schema.clone();
    let write_path = local_path.clone();

    let compressed_bytes = tokio::task::spawn_blocking(move || -> Result<u64> {
        let std_file = std::fs::File::create(&write_path)?;
        let props = WriterProperties::builder()
            .set_compression(compression)
            .set_max_row_group_size(row_group_size)
            .set_write_batch_size(1024)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_created_by(format!("prestige/{}", env!("CARGO_PKG_VERSION")))
            .build();

        let mut writer = ArrowWriter::try_new(std_file, schema_clone, Some(props))?;
        for batch in &batches {
            writer.write(batch)?;
        }
        writer.close()?;

        Ok(write_path.metadata()?.len())
    })
    .await
    .map_err(|e| Error::Internal(format!("parquet write task panicked: {e}")))??;

    // 4. Upload to S3 with retry
    let uncompressed_bytes: usize = source_files.iter().map(|f| f.size).sum();
    info!(
        file_key = %compacted_meta.key,
        compressed_bytes,
        uncompressed_bytes,
        total_records,
        "Uploading compacted file to S3",
    );
    let upload_delays = [
        Some(Duration::from_secs(1)),
        Some(Duration::from_secs(2)),
        None,
    ];

    let mut last_upload_error = None;
    for (attempt, delay) in upload_delays.iter().enumerate() {
        match put_file(&config.client, &config.bucket, &local_path).await {
            Ok(()) => {
                last_upload_error = None;
                break;
            }
            Err(err) => {
                warn!(
                    file_key = %compacted_meta.key,
                    attempt = attempt + 1,
                    error = ?err,
                    "Compaction upload failed, {}",
                    if delay.is_some() { "retrying" } else { "giving up" }
                );
                last_upload_error = Some(err);
                if let Some(d) = delay {
                    tokio::time::sleep(*d).await;
                }
            }
        }
    }

    if let Some(err) = last_upload_error {
        return Err(CompactionError::UploadFailed {
            file_key: compacted_meta.key.clone(),
            source: Box::new(err),
        }
        .into());
    }

    info!("Uploaded {} successfully", compacted_meta.key);

    // 5. Create processed markers for source files with bounded concurrency
    let marker_keys: Vec<String> = source_files.iter().map(|f| f.key.clone()).collect();
    let marker_results = {
        let client = config.client.clone();
        let bucket = config.bucket.clone();
        run_batched(
            marker_keys,
            |key| {
                let client = client.clone();
                let bucket = bucket.clone();
                async move {
                    let result = create_processed_marker(&client, &bucket, &key).await;
                    (key, result)
                }
            },
            "Marker creation",
            1_000,
        )
        .await
    };

    for (key, result) in &marker_results {
        if let Err(e) = result {
            warn!(key, error = ?e, "Failed to create processed marker");
        }
    }

    // 6. Calculate savings
    let original_bytes: usize = source_files.iter().map(|f| f.size).sum();
    let compacted_bytes = local_path.metadata()?.len() as usize;
    let bytes_saved = original_bytes.saturating_sub(compacted_bytes);

    // 7. Delete source files if upload successful
    let deletion_failures = if config.delete_originals {
        let keys: Vec<String> = source_files.iter().map(|f| f.key.clone()).collect();
        delete_original_files(config.client.clone(), config.bucket.clone(), keys).await
    } else {
        Vec::new()
    };

    Ok(FinalizeResult {
        records_count: total_records,
        bytes_saved,
        duplicate_count,
        deletion_failures,
    })
}

/// Main execution function for schema-agnostic compaction
async fn execute_compaction_schema_agnostic(
    config: FileCompactorConfig<()>,
) -> Result<CompactionResult> {
    info!(
        "Starting schema-agnostic compaction for prefix '{}' in bucket '{}'",
        config.prefix, config.bucket
    );
    info!(
        "Time range: after {:?}, until {}",
        config.after_timestamp, config.until_timestamp
    );

    // 1. Stream files and filter out compacted ones
    let mut uncompacted_files = Vec::new();
    let mut total_files_seen: usize = 0;
    let listing_start = Instant::now();
    let mut file_stream = list_files(
        &config.client,
        &config.bucket,
        &config.prefix,
        config.after_timestamp,
        Some(config.until_timestamp),
    );

    while let Some(file) = file_stream.try_next().await? {
        total_files_seen += 1;
        if total_files_seen.is_multiple_of(10_000) {
            info!(
                total_files_seen,
                uncompacted = uncompacted_files.len(),
                elapsed_secs = listing_start.elapsed().as_secs(),
                "File listing in progress",
            );
        }
        if !file.compacted {
            uncompacted_files.push(file);
        }
    }

    info!(
        total_files_seen,
        uncompacted = uncompacted_files.len(),
        elapsed_secs = listing_start.elapsed().as_secs(),
        "File listing complete",
    );

    if uncompacted_files.is_empty() {
        info!("No uncompacted files found");
        return Ok(CompactionResult::empty());
    }

    // 2. Sort by timestamp for deterministic ordering
    uncompacted_files.sort_by_key(|f| f.timestamp);

    // 3. Batch marker checks with bounded concurrency to filter already-processed files
    let already_processed = {
        let marker_keys: Vec<String> = uncompacted_files.iter().map(|f| f.key.clone()).collect();
        let results = run_batched(
            marker_keys,
            |key| {
                let client = config.client.clone();
                let bucket = config.bucket.clone();
                async move {
                    let has_marker = has_processed_marker(&client, &bucket, &key).await;
                    (key, has_marker)
                }
            },
            "Marker check",
            10_000,
        )
        .await;

        results
            .into_iter()
            .filter_map(|(key, has_marker)| has_marker.then_some(key))
            .collect::<HashSet<_>>()
    };

    // 4. Create temp directory
    let temp_dir = tempfile::tempdir()?;
    info!("Using temporary directory: {}", temp_dir.path().display());

    // 5. Separate already-processed files from files to download
    let mut files_processed = already_processed.len();
    let mut last_processed_timestamp: Option<DateTime<Utc>> = None;

    let files_to_process: Vec<FileMeta> = uncompacted_files
        .into_iter()
        .filter(|f| {
            if already_processed.contains(&f.key) {
                last_processed_timestamp = Some(f.timestamp);
                false
            } else {
                true
            }
        })
        .collect();

    info!(
        files_to_download = files_to_process.len(),
        already_skipped = files_processed,
        "Starting compaction"
    );

    // 6. Download-accumulate-upload loop
    //
    // Downloads and uploads must not compete for bandwidth. The loop processes
    // files in download phases: a buffered download stream runs until the
    // accumulator hits the size limit, then the stream is dropped (releasing
    // all in-flight connections) before the upload begins. After the upload,
    // a new download stream is created for the remaining files.
    const DOWNLOAD_CONCURRENCY: usize = 10;

    let mut accumulator = DeduplicatingAccumulator::new();
    let mut source_files: Vec<FileMeta> = Vec::new();
    let mut schema: Option<Arc<arrow::datatypes::Schema>> = None;

    let mut files_created = 0;
    let mut records_consolidated = 0;
    let mut bytes_saved = 0;
    let mut duplicate_records_eliminated = 0;
    let mut deletion_failures: Vec<String> = Vec::new();

    let mut remaining_files: &[FileMeta] = &files_to_process;

    while !remaining_files.is_empty() {
        // Start a download phase -- collect keys to avoid lifetime issues with the slice
        let download_keys: Vec<(usize, String)> = remaining_files
            .iter()
            .enumerate()
            .map(|(idx, f)| (idx, f.key.clone()))
            .collect();

        let mut download_stream =
            futures::stream::iter(download_keys.into_iter().map(|(idx, key)| {
                let client = config.client.clone();
                let bucket = config.bucket.clone();
                async move {
                    let result = client.get_object().bucket(&bucket).key(&key).send().await;

                    match result {
                        Ok(content) => {
                            let bytes = content
                                .body
                                .collect()
                                .await
                                .map(|b| b.into_bytes())
                                .map_err(|e| Error::Io(std::io::Error::other(e)));
                            (idx, bytes)
                        }
                        Err(e) => (idx, Err(Error::from(aws_sdk_s3::Error::from(e)))),
                    }
                }
            }))
            .buffered(DOWNLOAD_CONCURRENCY);

        let mut need_upload = false;
        let mut files_consumed_this_phase = 0;

        while let Some((idx, download_result)) = download_stream.next().await {
            let file_meta = &remaining_files[idx];
            let bytes = download_result?;

            info!("Processing file: {}", file_meta.key);
            files_consumed_this_phase = idx + 1;

            // Handle empty or invalid files
            if bytes.is_empty() {
                info!("Skipping empty file: {}", file_meta.key);
                files_processed += 1;
                last_processed_timestamp = Some(file_meta.timestamp);
                source_files.push(file_meta.clone());
                continue;
            }

            // Create parquet reader
            let cursor = std::io::Cursor::new(bytes);
            let builder = match ParquetRecordBatchStreamBuilder::new(cursor).await {
                Ok(b) => b,
                Err(e) => {
                    warn!("Failed to read parquet file {}: {}", file_meta.key, e);
                    files_processed += 1;
                    last_processed_timestamp = Some(file_meta.timestamp);
                    continue;
                }
            };

            // Get schema from first file
            if let Some(ref current_schema) = schema {
                if current_schema != builder.schema() {
                    warn!("Schema mismatch in file {}, skipping", file_meta.key);
                    files_processed += 1;
                    last_processed_timestamp = Some(file_meta.timestamp);
                    continue;
                }
            } else {
                let new_schema = builder.schema().clone();
                info!("Detected schema with {} fields", new_schema.fields().len());
                schema = Some(new_schema);
            }

            let mut stream = builder.build()?;

            // Stream RecordBatches and accumulate
            let mut file_had_records = false;
            while let Some(batch_result) = stream.try_next().await? {
                if batch_result.num_rows() == 0 {
                    continue;
                }

                file_had_records = true;
                let batch_size = batch_result.get_array_memory_size();

                info!(
                    "Loaded batch with {} records ({} bytes) from {}",
                    batch_result.num_rows(),
                    batch_size,
                    file_meta.key
                );

                // Check if adding this batch would exceed limit (soft limit)
                if !accumulator.is_empty()
                    && (accumulator.estimated_size() + batch_size) > config.max_bytes_per_file
                {
                    info!(
                        "Size limit reached, finalizing current batch ({} bytes, {} records)",
                        accumulator.estimated_size(),
                        accumulator.total_records()
                    );
                    need_upload = true;
                    // Add the batch that triggered the limit before breaking
                    accumulator.add_batch(batch_result, config.enable_deduplication)?;
                    break;
                }

                // Add batch to accumulator (with optional deduplication)
                accumulator.add_batch(batch_result, config.enable_deduplication)?;
            }

            if !file_had_records {
                info!("Skipping file with no records: {}", file_meta.key);
            }

            source_files.push(file_meta.clone());
            files_processed += 1;
            last_processed_timestamp = Some(file_meta.timestamp);

            if need_upload {
                break;
            }
        }

        // Advance past consumed files
        remaining_files = &remaining_files[files_consumed_this_phase..];

        // Drop download stream to release all in-flight connections before uploading
        drop(download_stream);

        if need_upload {
            let (batches, dup_count) = accumulator.take_batches();
            let finalize_result = finalize_and_upload_schema_agnostic(
                batches,
                dup_count,
                source_files,
                schema.as_ref().ok_or(Error::Internal(
                    "schema not initialized during compaction".into(),
                ))?,
                &config,
                temp_dir.path(),
            )
            .await?;

            files_created += 1;
            records_consolidated += finalize_result.records_count;
            bytes_saved += finalize_result.bytes_saved;
            duplicate_records_eliminated += finalize_result.duplicate_count;
            deletion_failures.extend(finalize_result.deletion_failures);

            // Reset for next batch
            source_files = Vec::new();
        } else {
            // Download phase exhausted all remaining files without hitting size limit
            break;
        }
    }

    // Finalize any remaining records
    if !accumulator.is_empty() {
        info!(
            "Finalizing remaining batch ({} bytes, {} records)",
            accumulator.estimated_size(),
            accumulator.total_records()
        );

        let (batches, dup_count) = accumulator.take_batches();
        let finalize_result = finalize_and_upload_schema_agnostic(
            batches,
            dup_count,
            source_files,
            schema.as_ref().ok_or(CompactionError::NoSourceFiles)?,
            &config,
            temp_dir.path(),
        )
        .await?;

        files_created += 1;
        records_consolidated += finalize_result.records_count;
        bytes_saved += finalize_result.bytes_saved;
        duplicate_records_eliminated += finalize_result.duplicate_count;
        deletion_failures.extend(finalize_result.deletion_failures);
    }

    info!(
        "Schema-agnostic compaction complete: {} files -> {} files, {} records, {} duplicates eliminated, ~{} bytes saved",
        files_processed,
        files_created,
        records_consolidated,
        duplicate_records_eliminated,
        bytes_saved
    );

    Ok(CompactionResult {
        files_processed,
        files_created,
        records_consolidated,
        bytes_saved,
        duplicate_records_eliminated,
        last_processed_timestamp,
        deletion_failures,
    })
}

/// Read-only compaction plan that mirrors `execute_compaction_schema_agnostic`
/// without writing or deleting any files. Downloads and processes all source
/// parquet files to produce accurate record counts, deduplication counts,
/// output file counts, and byte savings estimates.
async fn plan_compaction_schema_agnostic(
    config: FileCompactorConfig<()>,
) -> Result<CompactionResult> {
    info!(
        "Planning schema-agnostic compaction for prefix '{}' in bucket '{}'",
        config.prefix, config.bucket
    );
    info!(
        "Time range: after {:?}, until {}",
        config.after_timestamp, config.until_timestamp
    );

    // 1. Stream files and filter out compacted ones
    let mut uncompacted_files = Vec::new();
    let mut file_stream = list_files(
        &config.client,
        &config.bucket,
        &config.prefix,
        config.after_timestamp,
        Some(config.until_timestamp),
    );

    while let Some(file) = file_stream.try_next().await? {
        if !file.compacted {
            uncompacted_files.push(file);
        }
    }

    if uncompacted_files.is_empty() {
        info!("No uncompacted files found");
        return Ok(CompactionResult::empty());
    }

    info!(
        "Found {} uncompacted files to process",
        uncompacted_files.len()
    );

    // 2. Sort by timestamp for deterministic ordering
    uncompacted_files.sort_by_key(|f| f.timestamp);

    // 3. Process files with streaming accumulation (read-only)
    let mut accumulator = DeduplicatingAccumulator::new();
    let mut source_files: Vec<FileMeta> = Vec::new();
    let mut schema: Option<Arc<arrow::datatypes::Schema>> = None;

    let mut files_processed = 0;
    let mut files_created = 0;
    let mut records_consolidated = 0;
    let mut bytes_saved = 0;
    let mut duplicate_records_eliminated = 0;
    let mut last_processed_timestamp: Option<DateTime<Utc>> = None;

    for file_meta in uncompacted_files {
        info!("Planning file: {}", file_meta.key);

        // Skip if already processed (idempotency check — same as execute path)
        if has_processed_marker(&config.client, &config.bucket, &file_meta.key).await {
            info!(
                "Skipping already-processed file: {} (marker exists)",
                file_meta.key
            );
            files_processed += 1;
            last_processed_timestamp = Some(file_meta.timestamp);
            continue;
        }

        // Download and stream RecordBatches from parquet
        let file_content = config
            .client
            .get_object()
            .bucket(&config.bucket)
            .key(&file_meta.key)
            .send()
            .await
            .map_err(|e| Error::from(aws_sdk_s3::Error::from(e)))?;

        let bytes = file_content
            .body
            .collect()
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
            .into_bytes();

        // Handle empty or invalid files
        if bytes.is_empty() {
            info!("Skipping empty file: {}", file_meta.key);
            files_processed += 1;
            last_processed_timestamp = Some(file_meta.timestamp);
            source_files.push(file_meta.clone());
            continue;
        }

        // Create parquet reader
        let cursor = std::io::Cursor::new(bytes);
        let builder = match ParquetRecordBatchStreamBuilder::new(cursor).await {
            Ok(b) => b,
            Err(e) => {
                warn!("Failed to read parquet file {}: {}", file_meta.key, e);
                files_processed += 1;
                last_processed_timestamp = Some(file_meta.timestamp);
                continue;
            }
        };

        // Get schema from first file
        if let Some(ref current_schema) = schema {
            if current_schema != builder.schema() {
                warn!("Schema mismatch in file {}, skipping", file_meta.key);
                files_processed += 1;
                last_processed_timestamp = Some(file_meta.timestamp);
                continue;
            }
        } else {
            let new_schema = builder.schema().clone();
            info!("Detected schema with {} fields", new_schema.fields().len());
            schema = Some(new_schema);
        }

        let mut stream = builder.build()?;

        // Stream RecordBatches and accumulate
        let mut file_had_records = false;
        while let Some(batch_result) = stream.try_next().await? {
            if batch_result.num_rows() == 0 {
                continue;
            }

            file_had_records = true;
            let batch_size = batch_result.get_array_memory_size();

            // Check if adding this batch would exceed limit (soft limit)
            if !accumulator.is_empty()
                && (accumulator.estimated_size() + batch_size) > config.max_bytes_per_file
            {
                info!(
                    "Size limit reached, measuring planned output ({} bytes, {} records)",
                    accumulator.estimated_size(),
                    accumulator.total_records()
                );

                let (batches, dup_count) = accumulator.take_batches();
                let original_bytes: usize = source_files.iter().map(|f| f.size).sum();
                let output_bytes = measure_output_size(
                    &batches,
                    schema.as_ref().ok_or(Error::Internal(
                        "schema not initialized during compaction".into(),
                    ))?,
                    config.compression,
                    config.row_group_size,
                )?;

                let record_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                files_created += 1;
                records_consolidated += record_count;
                bytes_saved += original_bytes.saturating_sub(output_bytes);
                duplicate_records_eliminated += dup_count;

                source_files = Vec::new();
            }

            // Add batch to accumulator (with optional deduplication)
            accumulator.add_batch(batch_result, config.enable_deduplication)?;
        }

        if !file_had_records {
            info!("Skipping file with no records: {}", file_meta.key);
        }

        source_files.push(file_meta.clone());
        files_processed += 1;
        last_processed_timestamp = Some(file_meta.timestamp);
    }

    // Finalize any remaining records
    if !accumulator.is_empty() {
        let (batches, dup_count) = accumulator.take_batches();
        let original_bytes: usize = source_files.iter().map(|f| f.size).sum();
        let output_bytes = measure_output_size(
            &batches,
            schema.as_ref().ok_or(CompactionError::NoSourceFiles)?,
            config.compression,
            config.row_group_size,
        )?;

        let record_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        files_created += 1;
        records_consolidated += record_count;
        bytes_saved += original_bytes.saturating_sub(output_bytes);
        duplicate_records_eliminated += dup_count;
    }

    info!(
        "Schema-agnostic compaction plan complete: {} files -> {} files, {} records, {} duplicates, ~{} bytes saved",
        files_processed,
        files_created,
        records_consolidated,
        duplicate_records_eliminated,
        bytes_saved
    );

    Ok(CompactionResult {
        files_processed,
        files_created,
        records_consolidated,
        bytes_saved,
        duplicate_records_eliminated,
        last_processed_timestamp,
        deletion_failures: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compaction_result_empty() {
        let result = CompactionResult::empty();
        assert_eq!(result.files_processed, 0);
        assert_eq!(result.files_created, 0);
        assert_eq!(result.records_consolidated, 0);
        assert_eq!(result.bytes_saved, 0);
        assert_eq!(result.duplicate_records_eliminated, 0);
        assert_eq!(result.last_processed_timestamp, None);
        assert!(result.deletion_failures.is_empty());
    }

    #[test]
    fn test_finalize_result() {
        let result = FinalizeResult {
            records_count: 100,
            bytes_saved: 1024,
            duplicate_count: 0,
            deletion_failures: Vec::new(),
        };
        assert_eq!(result.records_count, 100);
        assert_eq!(result.bytes_saved, 1024);
        assert_eq!(result.duplicate_count, 0);
        assert!(result.deletion_failures.is_empty());
    }
}

use arrow::array::RecordBatch;
use chrono::{DateTime, Utc};
use derive_builder::Builder;
use futures::{TryStreamExt, future};
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
};
use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, path::Path};
use tracing::{info, warn};

use crate::{
    Client,
    error::{CompactionError, Error, Result},
    file_meta::FileMeta,
    file_sink::DEFAULT_MAX_SIZE_BYTES,
    file_source::{deserialize_to_vec, source_s3_file},
    list_files, put_file, remove_file,
    traits::ArrowSchema,
};

/// File Compactor Module
///
/// Consolidates multiple small parquet files from S3 into fewer, larger files using
/// a streaming architecture.
///
/// # File Naming Convention
///
/// Original files: `{prefix}.{timestamp_millis}.parquet`
/// Compacted files: `{prefix}.c.{timestamp_millis}.parquet`
///
/// Examples:
/// - `sensor_data.1234567890123.parquet` (original)
/// - `sensor_data.c.1234567890123.parquet` (compacted)
///
/// # Algorithm
///
/// The compactor uses a streaming approach:
/// 1. List all uncompacted files (those without `.c` marker) before the specified timestamp
/// 2. Sort files by timestamp for deterministic processing
/// 3. For each file:
///    - Download and deserialize records
///    - Accumulate in memory until size limit (100MB default)
///    - When limit reached, finalize current batch and upload
/// 4. Delete original files after successful upload
///
/// This approach avoids memory limits by processing incrementally rather than loading
/// all files for a day into memory at once.
///
/// # Reference Implementation: Scheduled Compaction with ManagedProc
///
/// This example shows how to implement a scheduled compaction job that runs periodically,
/// querying a database to determine the cutoff timestamp for compaction.
///
/// ```rust,no_run
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
///         let before_timestamp = self.get_compaction_timestamp().await?;
///
///         info!(
///             prefix = %self.settings.prefix,
///             bucket = %self.settings.bucket,
///             before = %before_timestamp,
///             "Starting compaction cycle"
///         );
///
///         // Build and execute compaction
///         let result = FileCompactorConfigBuilder::default()
///             .client(self.settings.s3_client.clone())
///             .bucket(self.settings.bucket.clone())
///             .prefix(self.settings.prefix.clone())
///             .before_timestamp(before_timestamp)
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

    /// Compact files before this timestamp (exclusive)
    before_timestamp: DateTime<Utc>,

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
}

impl CompactionResult {
    /// Create an empty result (no files to compact)
    pub fn empty() -> Self {
        Self {
            files_processed: 0,
            files_created: 0,
            records_consolidated: 0,
            bytes_saved: 0,
        }
    }
}

/// Helper struct for finalization results
#[derive(Debug)]
struct FinalizeResult {
    records_count: usize,
    bytes_saved: usize,
}

/// Delete original files from S3 after successful consolidation
async fn delete_original_files(client: &Client, bucket: &str, files: &[FileMeta]) -> Result<()> {
    info!("Deleting {} original files", files.len());

    // Delete in parallel using join_all
    let delete_futures: Vec<_> = files
        .iter()
        .map(|file| async move {
            let key = &file.key;
            remove_file(client, bucket, key).await
        })
        .collect();

    let results = future::join_all(delete_futures).await;

    // Log warnings for failures but don't fail entire operation
    let mut failed_count = 0;
    for (idx, result) in results.iter().enumerate() {
        if let Err(e) = result {
            warn!("Failed to delete original file {}: {}", files[idx].key, e);
            failed_count += 1;
        }
    }

    if failed_count > 0 {
        warn!(
            "Failed to delete {} out of {} original files",
            failed_count,
            files.len()
        );
    } else {
        info!("Successfully deleted all {} original files", files.len());
    }

    Ok(())
}

impl<T> FileCompactorConfigBuilder<T>
where
    T: ArrowSchema + Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
{
    /// Execute the file compaction operation
    pub async fn execute(self) -> Result<CompactionResult> {
        let config = self
            .build()
            .map_err(|e| crate::error::Error::SerdeArrow(format!("Config builder error: {}", e)))?;

        execute_compaction(config).await
    }
}

/// Finalize and upload accumulated records to S3
async fn finalize_and_upload<T>(
    records: Vec<T>,
    source_files: Vec<FileMeta>,
    config: &FileCompactorConfig<T>,
    temp_dir: &Path,
) -> Result<FinalizeResult>
where
    T: ArrowSchema + Serialize + Clone,
{
    // 1. Determine output timestamp (latest from sources)
    let latest_timestamp = source_files
        .iter()
        .map(|f| f.timestamp)
        .max()
        .ok_or(CompactionError::NoSourceFiles)?;

    // 2. Create compacted file metadata
    let compacted_meta = FileMeta::as_compacted(config.prefix.clone(), latest_timestamp);

    info!(
        "Finalizing {} records from {} source files into {}",
        records.len(),
        source_files.len(),
        compacted_meta.key
    );

    // 3. Write to local temp file
    let local_path = temp_dir.join(&compacted_meta.key);

    let schema = T::arrow_schema();
    let arrays = serde_arrow::to_arrow(schema.fields(), &records)
        .map_err(|e| crate::error::Error::SerdeArrow(e.to_string()))?;
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    let std_file = std::fs::File::create(&local_path)?;
    let props = WriterProperties::builder()
        .set_compression(config.compression)
        .set_max_row_group_size(config.row_group_size)
        .set_write_batch_size(1024)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_created_by(format!("prestige/{}", env!("CARGO_PKG_VERSION")))
        .build();

    let mut writer = ArrowWriter::try_new(std_file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    // 4. Upload to S3
    info!("Uploading {} to S3", compacted_meta.key);
    put_file(&config.client, &config.bucket, &local_path)
        .await
        .map_err(|_| CompactionError::UploadFailed {
            file_key: compacted_meta.key.clone(),
        })?;

    // 5. Calculate savings
    let original_bytes: usize = source_files.iter().map(|f| f.size).sum();
    let compacted_bytes = local_path.metadata()?.len() as usize;
    let bytes_saved = original_bytes.saturating_sub(compacted_bytes);

    info!("Uploaded {} successfully", compacted_meta.key);

    // 6. Delete source files if upload successful
    if config.delete_originals {
        delete_original_files(&config.client, &config.bucket, &source_files).await?;
    }

    Ok(FinalizeResult {
        records_count: records.len(),
        bytes_saved,
    })
}

/// Process files with streaming accumulation
async fn process_streaming_compaction<T>(
    config: &FileCompactorConfig<T>,
    files: Vec<FileMeta>,
    temp_dir: &Path,
) -> Result<CompactionResult>
where
    T: ArrowSchema + Serialize + for<'de> Deserialize<'de> + Clone,
{
    let mut all_records: Vec<T> = Vec::new();
    let mut current_size_bytes: usize = 0;
    let mut source_files: Vec<FileMeta> = Vec::new();

    let mut files_processed = 0;
    let mut files_created = 0;
    let mut records_consolidated = 0;
    let mut bytes_saved = 0;

    for file_meta in files {
        info!("Processing file: {}", file_meta.key);

        // Download and deserialize this file
        let stream = match source_s3_file(
            &config.client,
            &config.bucket,
            &file_meta.key,
            None,
            None,
        )
        .await
        {
            Err(Error::Io(err)) => {
                info!(
                    %err,
                    "Skipping empty or insufficiently sized file: {}",
                    file_meta.key
                );
                files_processed += 1;
                source_files.push(file_meta.clone());
                continue;
            }
            other_result => other_result?,
        };

        let records: Vec<T> = deserialize_to_vec(stream).await?;

        // A truly empty file will fail `source_s3_file/5` above with an empty file IO error
        // but handle here in case a structurally valid parquet file with 0 records is such a thing
        if records.is_empty() {
            info!("Skipping empty file: {}", file_meta.key);
            files_processed += 1;
            source_files.push(file_meta.clone()); // ensure the empty file is still cleaned up
            continue;
        }

        info!("Loaded {} records from {}", records.len(), file_meta.key);

        // Convert to RecordBatch to measure size
        let schema = T::arrow_schema();
        let arrays = serde_arrow::to_arrow(schema.fields(), &records)
            .map_err(|e| crate::error::Error::SerdeArrow(e.to_string()))?;
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;
        let batch_size = batch.get_array_memory_size();

        info!(
            "Batch size: {} bytes (current accumulation: {} bytes)",
            batch_size, current_size_bytes
        );

        // Check if adding this batch would exceed limit (soft limit)
        if current_size_bytes > 0 && (current_size_bytes + batch_size) > config.max_bytes_per_file {
            info!(
                "Size limit reached, finalizing current batch ({} bytes)",
                current_size_bytes
            );

            // Finalize current accumulation
            let finalize_result =
                finalize_and_upload(all_records, source_files, config, temp_dir).await?;

            files_created += 1;
            records_consolidated += finalize_result.records_count;
            bytes_saved += finalize_result.bytes_saved;

            // Reset accumulators
            all_records = Vec::new();
            current_size_bytes = 0;
            source_files = Vec::new();
        }

        // Accumulate this batch
        all_records.extend(records);
        current_size_bytes += batch_size;
        source_files.push(file_meta.clone());
        files_processed += 1;
    }

    // Finalize any remaining records
    if !all_records.is_empty() {
        info!(
            "Finalizing remaining batch ({} bytes, {} records)",
            current_size_bytes,
            all_records.len()
        );

        let finalize_result =
            finalize_and_upload(all_records, source_files, config, temp_dir).await?;

        files_created += 1;
        records_consolidated += finalize_result.records_count;
        bytes_saved += finalize_result.bytes_saved;
    }

    Ok(CompactionResult {
        files_processed,
        files_created,
        records_consolidated,
        bytes_saved,
    })
}

/// Main execution function for file compaction
async fn execute_compaction<T>(config: FileCompactorConfig<T>) -> Result<CompactionResult>
where
    T: ArrowSchema + Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
{
    info!(
        "Starting compaction for prefix '{}' in bucket '{}'",
        config.prefix, config.bucket
    );
    info!("Time range: before {}", config.before_timestamp);

    // 1. Stream files and stop at first compacted file (optimization)
    // Since lexicographic ordering guarantees all original files (starting with digits)
    // come before all compacted files (starting with 'c'), we can stop listing early.
    let mut uncompacted_files = Vec::new();
    let mut file_stream = list_files(
        &config.client,
        &config.bucket,
        &config.prefix,
        None, // No after filter
        Some(config.before_timestamp),
    );

    while let Some(file) = file_stream.try_next().await? {
        if file.compacted {
            // Hit first compacted file - all remaining files are compacted
            info!(
                "Reached first compacted file at {}, stopping listing (optimization)",
                file.key
            );
            break;
        }
        uncompacted_files.push(file);
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

    // 3. Create temp directory
    let temp_dir = tempfile::tempdir()?;
    info!("Using temporary directory: {}", temp_dir.path().display());

    // 4. Process files with streaming accumulation
    let result = process_streaming_compaction(&config, uncompacted_files, temp_dir.path()).await?;

    info!(
        "Compaction complete: {} files -> {} files, {} records, ~{} bytes saved",
        result.files_processed,
        result.files_created,
        result.records_consolidated,
        result.bytes_saved
    );

    Ok(result)
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
    }

    #[test]
    fn test_finalize_result() {
        let result = FinalizeResult {
            records_count: 100,
            bytes_saved: 1024,
        };
        assert_eq!(result.records_count, 100);
        assert_eq!(result.bytes_saved, 1024);
    }
}

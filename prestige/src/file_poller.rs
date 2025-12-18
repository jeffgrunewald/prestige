use crate::{Client, FileMeta, RecordBatchStream, Result};
use chrono::{DateTime, Utc};
use derive_builder::Builder;
use retainer::Cache;
use std::{collections::VecDeque, sync::Arc, time::Duration};
use super_visor::ManagedProc;
use tokio::sync::mpsc::{Receiver, Sender};

const DEFAULT_POLL_DURATION_SECS: i64 = 30;
const DEFAULT_POLL_DURATION: Duration = Duration::from_secs(DEFAULT_POLL_DURATION_SECS as u64);
const DEFAULT_OFFSET_DURATION: Duration = Duration::from_secs(10 * 60);
const CLEAN_DURATION: Duration = Duration::from_secs(12 * 60 * 60);
const CACHE_TTL: Duration = Duration::from_secs(3 * 60 * 60);

type MemoryFileCache = Arc<Cache<String, bool>>;

/// State tracking trait for FilePoller
///
/// Implementations track which files have been processed to avoid reprocessing.
/// Typically implemented for sqlx::PgPool for PostgreSQL state persistence.
#[async_trait::async_trait]
pub trait FilePollerState: Send + Sync + 'static {
    /// Get the timestamp of the most recently processed file
    async fn latest_timestamp(
        &self,
        process_name: &str,
        file_type: &str,
    ) -> Result<Option<DateTime<Utc>>>;

    /// Check if a file has already been processed
    async fn exists(&self, process_name: &str, file_meta: &FileMeta) -> Result<bool>;

    /// Clean old processed file records
    /// Returns the number of records removed
    async fn clean(
        &self,
        process_name: &str,
        file_type: &str,
        offset: DateTime<Utc>,
    ) -> Result<u64>;
}

/// State recorder trait for recording processed files
///
/// Typically implemented for sqlx::Transaction to record file processing
/// within a transaction context.
#[async_trait::async_trait]
pub trait FilePollerStateRecorder {
    /// Record that a file has been processed
    async fn record(&mut self, process_name: &str, file_meta: &FileMeta) -> Result;
}

/// A stream of record batches from a parquet file with metadata
pub struct FileStream {
    pub file_meta: FileMeta,
    pub process_name: String,
    pub batches: RecordBatchStream,
}

impl FileStream {
    pub fn new(process_name: String, file_meta: FileMeta, batches: RecordBatchStream) -> Self {
        Self {
            file_meta,
            process_name,
            batches,
        }
    }

    /// Convert into the batch stream after recording the file as processed
    pub async fn into_stream(
        self,
        recorder: &mut impl FilePollerStateRecorder,
    ) -> Result<RecordBatchStream> {
        let latency = Utc::now() - self.file_meta.timestamp;
        metrics::gauge!(
            "file-processing-latency",
            "file-type" => self.file_meta.prefix.clone(),
            "process-name" => self.process_name.clone(),
        )
        .set(latency.num_seconds() as f64);

        metrics::gauge!(
            "file-processing-timestamp",
            "file-type" => self.file_meta.prefix.clone(),
            "process-name" => self.process_name.clone(),
        )
        .set(self.file_meta.timestamp.timestamp_millis() as f64);

        recorder.record(&self.process_name, &self.file_meta).await?;
        Ok(self.batches)
    }
}

/// Lookback behavior for polling
#[derive(Debug, Clone)]
pub enum LookbackBehavior {
    /// Start polling after this specific timestamp
    StartAfter(DateTime<Utc>),
    /// Look back at most this duration from now
    Max(Duration),
}

impl From<DateTime<Utc>> for LookbackBehavior {
    fn from(value: DateTime<Utc>) -> Self {
        LookbackBehavior::StartAfter(value)
    }
}

impl From<Duration> for LookbackBehavior {
    fn from(value: Duration) -> Self {
        LookbackBehavior::Max(value)
    }
}

/// Configuration for FilePoller
#[derive(Debug, Clone, Builder)]
#[builder(pattern = "owned")]
pub struct FilePollerConfig<State> {
    /// How often to poll S3 for new files
    #[builder(default = "DEFAULT_POLL_DURATION")]
    poll_duration: Duration,

    /// State tracker for processed files
    state: State,

    /// S3 client
    client: Client,

    /// S3 bucket name
    bucket: String,

    /// File prefix to poll for (e.g., "sensor_data")
    prefix: String,

    /// Lookback behavior for determining where to start polling
    lookback: LookbackBehavior,

    /// Offset duration to subtract from latest timestamp
    #[builder(default = "DEFAULT_OFFSET_DURATION")]
    offset: Duration,

    /// Size of the channel queue for file streams
    #[builder(default = "5")]
    queue_size: usize,

    /// Process name for tracking in database
    #[builder(default = r#""default".to_string()"#)]
    process_name: String,
}

impl<State> FilePollerConfigBuilder<State> {
    /// Set the lookback behavior to start after the given timestamp
    pub fn lookback_start_after(self, start_after: DateTime<Utc>) -> Self {
        self.lookback(LookbackBehavior::StartAfter(start_after))
    }

    /// Set the lookback behavior to the maximum lookback duration
    pub fn lookback_max(self, max_lookback: Duration) -> Self {
        self.lookback(LookbackBehavior::Max(max_lookback))
    }
}

impl<State> FilePollerConfigBuilder<State>
where
    State: FilePollerState,
{
    /// Create the FilePoller server and receiver
    pub async fn create(self) -> Result<(FileStreamReceiver, FilePollerServer<State>)> {
        let config = self
            .build()
            .map_err(|e| crate::Error::Config(config::ConfigError::Message(e.to_string())))?;
        let (sender, receiver) = tokio::sync::mpsc::channel(config.queue_size);
        let latest_file_timestamp = config
            .state
            .latest_timestamp(&config.process_name, &config.prefix)
            .await?;

        Ok((
            receiver,
            FilePollerServer {
                config,
                sender,
                file_queue: VecDeque::new(),
                latest_file_timestamp,
                cache: create_cache(),
            },
        ))
    }
}

/// Server that polls S3 for new parquet files
pub struct FilePollerServer<State> {
    config: FilePollerConfig<State>,
    sender: Sender<FileStream>,
    file_queue: VecDeque<FileMeta>,
    latest_file_timestamp: Option<DateTime<Utc>>,
    cache: MemoryFileCache,
}

pub type FileStreamReceiver = Receiver<FileStream>;

fn create_cache() -> MemoryFileCache {
    Arc::new(Cache::new())
}

impl<State> FilePollerServer<State>
where
    State: FilePollerState,
{
    fn poll_duration(&self) -> Duration {
        self.config.poll_duration
    }

    fn after(&self, latest_file_timestamp: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
        let offset = self.config.offset;
        match self.config.lookback {
            LookbackBehavior::StartAfter(start_after) => {
                let latest_with_offset = latest_file_timestamp
                    .map(|ts| ts - chrono::Duration::from_std(offset).unwrap());

                match (latest_with_offset, Some(start_after)) {
                    (Some(latest), Some(start)) if latest > start => Some(latest),
                    (Some(_), Some(start)) => Some(start),
                    (Some(latest), None) => Some(latest),
                    (None, Some(start)) => Some(start),
                    (None, None) => None,
                }
            }
            LookbackBehavior::Max(max_lookback) => {
                let max_lookback_time =
                    Utc::now() - chrono::Duration::from_std(max_lookback).unwrap();
                let latest_with_offset = latest_file_timestamp
                    .map(|ts| ts - chrono::Duration::from_std(offset).unwrap());

                match latest_with_offset {
                    Some(latest) if latest > max_lookback_time => Some(latest),
                    _ => Some(max_lookback_time),
                }
            }
        }
    }

    async fn is_already_processed(&mut self, file_meta: &FileMeta) -> Result<bool> {
        // Check memory cache first
        if self.cache.get(&file_meta.key).await.is_some() {
            return Ok(true);
        }

        // Check database
        let exists = self
            .config
            .state
            .exists(&self.config.process_name, file_meta)
            .await?;

        if exists {
            // Update cache
            self.cache
                .insert(file_meta.key.clone(), true, CACHE_TTL)
                .await;
        }

        Ok(exists)
    }

    async fn get_next_file(&mut self) -> Result<FileMeta> {
        loop {
            if let Some(file_meta) = self.file_queue.pop_front() {
                return Ok(file_meta);
            }

            let after = self.after(self.latest_file_timestamp);
            let before = Utc::now();
            let files = crate::list_all_files(
                &self.config.client,
                &self.config.bucket,
                &self.config.prefix,
                after,
                before,
            )
            .await?;

            for file in files {
                if !self.is_already_processed(&file).await? {
                    self.latest_file_timestamp = Some(file.timestamp);
                    self.file_queue.push_back(file);
                }
            }

            if self.file_queue.is_empty() {
                tokio::time::sleep(self.poll_duration()).await;
            }
        }
    }

    pub async fn run(mut self, mut shutdown: super_visor::ShutdownSignal) -> Result {
        let mut cleanup_trigger = tokio::time::interval(CLEAN_DURATION);
        let process_name = self.config.process_name.clone();
        let prefix = self.config.prefix.clone();

        tracing::info!(
            r#type = self.config.prefix,
            %process_name,
            "starting FilePoller",
        );

        let sender = self.sender.clone();
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    tracing::info!(
                        r#type = prefix,
                        %process_name,
                        "stopping FilePoller",
                    );
                    break Ok(());
                }
                _ = cleanup_trigger.tick() => {
                    let offset = Utc::now() - chrono::Duration::from_std(CLEAN_DURATION).unwrap();
                    match self.config.state.clean(&process_name, &prefix, offset).await {
                        Ok(count) => {
                            tracing::debug!(
                                r#type = prefix,
                                %process_name,
                                %count,
                                "cleaned old file records"
                            );
                        }
                        Err(err) => {
                            tracing::error!(
                                r#type = prefix,
                                %process_name,
                                ?err,
                                "failed to clean old file records"
                            );
                        }
                    }
                }
                file_result = self.get_next_file() => {
                    let file_meta = file_result?;

                    // Mark as processed in cache
                    self.cache.insert(file_meta.key.clone(), true, CACHE_TTL).await;

                    // Download and create stream
                    match crate::file_source::source_s3_file(
                        &self.config.client,
                        &self.config.bucket,
                        &file_meta.key,
                        None,
                        Some(format!("file_poller_{prefix}")),
                    )
                    .await {
                        Ok(batches) => {
                            let file_stream = FileStream::new(
                                process_name.clone(),
                                file_meta,
                                batches,
                            );

                            if sender.send(file_stream).await.is_err() {
                                tracing::warn!(
                                    r#type = prefix,
                                    %process_name,
                                    "file stream receiver dropped",
                                );
                                break Ok(());
                            }
                        }
                        Err(err) => {
                            metrics::counter!("invalid-parquet-file").increment(1);
                            tracing::error!(
                                r#type = prefix,
                                %process_name,
                                file_key = %file_meta.key,
                                file_size = ?file_meta.timestamp,
                                ?err,
                                "failed to process file",
                            );
                            // Continue processing subsequent files instead of crashing
                            continue;
                        }
                    }
                }
            }
        }
    }
}

impl<State> ManagedProc for FilePollerServer<State>
where
    State: FilePollerState,
{
    fn run_proc(
        self: Box<Self>,
        shutdown: super_visor::ShutdownSignal,
    ) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move {
            self.run(shutdown)
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))
        })
    }
}

/// Generic implementation of FilePollerState for Arc<T>
///
/// This allows any type implementing FilePollerState to be wrapped in Arc
/// while still implementing the trait. This is useful for sharing state
/// across multiple tasks or threads.
#[async_trait::async_trait]
impl<T> FilePollerState for Arc<T>
where
    T: FilePollerState,
{
    async fn latest_timestamp(
        &self,
        process_name: &str,
        file_type: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        (**self).latest_timestamp(process_name, file_type).await
    }

    async fn exists(&self, process_name: &str, file_meta: &FileMeta) -> Result<bool> {
        (**self).exists(process_name, file_meta).await
    }

    async fn clean(
        &self,
        process_name: &str,
        file_type: &str,
        offset: DateTime<Utc>,
    ) -> Result<u64> {
        (**self).clean(process_name, file_type, offset).await
    }
}

// PostgreSQL implementations for FilePollerState and FilePollerStateRecorder
#[cfg(feature = "sqlx")]
mod sqlx_impl {
    use super::*;

    /// Implementation of FilePollerStateRecorder for PostgreSQL transactions
    ///
    /// Records processed files in the `files_processed` table within a transaction.
    /// This ensures that file processing and state recording are atomic.
    #[async_trait::async_trait]
    impl FilePollerStateRecorder for sqlx::Transaction<'_, sqlx::Postgres> {
        async fn record(&mut self, process_name: &str, file_meta: &FileMeta) -> Result {
            sqlx::query(
                r#"
                INSERT INTO files_processed(process_name, file_name, file_type, file_timestamp, processed_at)
                VALUES($1, $2, $3, $4, $5)
                "#,
            )
            .bind(process_name)
            .bind(&file_meta.key)
            .bind(&file_meta.prefix)
            .bind(file_meta.timestamp)
            .bind(Utc::now())
            .execute(&mut **self)
            .await
            .map(|_| ())
            .map_err(crate::Error::from)
        }
    }

    /// Implementation of FilePollerState for PostgreSQL connection pool
    ///
    /// Tracks file processing state in the `files_processed` table:
    /// - `latest_timestamp`: Gets the most recent file timestamp for a process/file-type
    /// - `exists`: Checks if a file has already been processed
    /// - `clean`: Removes old file records to keep table size bounded
    ///
    /// # Database Schema
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
    /// CREATE INDEX idx_files_processed_lookup
    ///     ON files_processed(process_name, file_type, file_timestamp DESC);
    /// ```
    #[async_trait::async_trait]
    impl FilePollerState for sqlx::Pool<sqlx::Postgres> {
        async fn latest_timestamp(
            &self,
            process_name: &str,
            file_type: &str,
        ) -> Result<Option<DateTime<Utc>>> {
            sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
                r#"
                SELECT MAX(file_timestamp)
                FROM files_processed
                WHERE process_name = $1 AND file_type = $2
                "#,
            )
            .bind(process_name)
            .bind(file_type)
            .fetch_one(self)
            .await
            .map_err(crate::Error::from)
        }

        async fn exists(&self, process_name: &str, file_meta: &FileMeta) -> Result<bool> {
            sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM files_processed
                    WHERE process_name = $1 AND file_name = $2
                )
                "#,
            )
            .bind(process_name)
            .bind(&file_meta.key)
            .fetch_one(self)
            .await
            .map_err(crate::Error::from)
        }

        async fn clean(
            &self,
            process_name: &str,
            file_type: &str,
            offset: DateTime<Utc>,
        ) -> Result<u64> {
            sqlx::query(
                r#"
                DELETE FROM files_processed
                WHERE process_name = $1
                  AND file_type = $2
                  AND file_timestamp < $3
                "#,
            )
            .bind(process_name)
            .bind(file_type)
            .bind(offset)
            .execute(self)
            .await
            .map(|result| result.rows_affected())
            .map_err(crate::Error::from)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    type LatestState = Arc<Mutex<HashMap<(String, String), DateTime<Utc>>>>;

    // Mock state implementation for testing
    #[derive(Clone)]
    struct MockState {
        latest: LatestState,
        processed: Arc<Mutex<HashMap<String, bool>>>,
    }

    impl MockState {
        fn new() -> Self {
            Self {
                latest: Arc::new(Mutex::new(HashMap::new())),
                processed: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl FilePollerState for MockState {
        async fn latest_timestamp(
            &self,
            process_name: &str,
            file_type: &str,
        ) -> Result<Option<DateTime<Utc>>> {
            let latest = self.latest.lock().await;
            Ok(latest
                .get(&(process_name.to_string(), file_type.to_string()))
                .copied())
        }

        async fn exists(&self, _process_name: &str, file_meta: &FileMeta) -> Result<bool> {
            let processed = self.processed.lock().await;
            Ok(processed.get(&file_meta.key).copied().unwrap_or(false))
        }

        async fn clean(
            &self,
            _process_name: &str,
            _file_type: &str,
            _offset: DateTime<Utc>,
        ) -> Result<u64> {
            Ok(0)
        }
    }

    #[test]
    fn test_lookback_behavior_from_datetime() {
        let ts = Utc::now();
        let lookback: LookbackBehavior = ts.into();
        match lookback {
            LookbackBehavior::StartAfter(t) => assert_eq!(t, ts),
            _ => panic!("Expected StartAfter variant"),
        }
    }

    #[test]
    fn test_lookback_behavior_from_duration() {
        let duration = Duration::from_secs(3600);
        let lookback: LookbackBehavior = duration.into();
        match lookback {
            LookbackBehavior::Max(d) => assert_eq!(d, duration),
            _ => panic!("Expected Max variant"),
        }
    }

    #[tokio::test]
    async fn test_mock_state_latest_timestamp() {
        let state = MockState::new();

        // Should return None initially
        let result = state.latest_timestamp("test", "prefix").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_mock_state_exists() {
        let state = MockState::new();
        let file_meta = FileMeta::from(("test".to_string(), Utc::now()));

        // Should return false for non-existent file
        let exists = state.exists("test", &file_meta).await.unwrap();
        assert!(!exists);
    }

    #[tokio::test]
    async fn test_mock_state_clean() {
        let state = MockState::new();
        let offset = Utc::now() - chrono::Duration::hours(12);

        let count = state.clean("test", "prefix", offset).await.unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_POLL_DURATION_SECS, 30);
        assert_eq!(DEFAULT_POLL_DURATION, Duration::from_secs(30));
        assert_eq!(DEFAULT_OFFSET_DURATION, Duration::from_secs(10 * 60));
        assert_eq!(CLEAN_DURATION, Duration::from_secs(12 * 60 * 60));
        assert_eq!(CACHE_TTL, Duration::from_secs(3 * 60 * 60));
    }
}

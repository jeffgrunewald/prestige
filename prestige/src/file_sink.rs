use std::{
    fs::File,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use chrono::{DateTime, Utc};
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
};
use serde::Serialize;
use super_visor::{ManagedProc, ShutdownSignal};
use tokio::{
    fs,
    sync::{
        mpsc::{self, error::SendTimeoutError},
        oneshot,
    },
    time,
};
use tracing::{debug, error, info, warn};

use crate::{
    ArrowSchema, Error, FileMeta, FileUpload, Result,
    error::ChannelError,
    telemetry::{
        self, SINK_BATCH_SIZE, SINK_FILES_ROTATED, SINK_RECORDS_WRITTEN, SINK_WRITE_ERRORS,
        telemetry_labels,
    },
};

// Configuration constants
pub const DEFAULT_SINK_ROLL_SECS: u64 = 3 * 60; // 3 minutes
pub const DEFAULT_MAX_ROWS: usize = 100_000;
pub const DEFAULT_MAX_SIZE_BYTES: usize = 100 * 1024 * 1024; // 100 MB
pub const DEFAULT_ROW_GROUP_SIZE: usize = 10_000;
pub const DEFAULT_BATCH_SIZE: usize = 1_000; // Records to accumulate before writing batch

#[cfg(not(test))]
pub const SINK_CHECK_MILLIS: u64 = 60_000; // 1 minute
#[cfg(test)]
pub const SINK_CHECK_MILLIS: u64 = 50; // Fast for tests

// Metrics constants
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

pub type FileManifest = Vec<String>;

// Channel message types
pub enum Message<T> {
    Data(oneshot::Sender<Result>, T),
    Commit(oneshot::Sender<Result<FileManifest>>),
    Rollback(oneshot::Sender<Result<FileManifest>>),
}

pub type MessageSender<T> = mpsc::Sender<Message<T>>;
pub type MessageReceiver<T> = mpsc::Receiver<Message<T>>;

fn message_channel<T>(buffer: usize) -> (MessageSender<T>, MessageReceiver<T>) {
    mpsc::channel(buffer)
}

/// Builder for creating a ParquetSink
pub struct ParquetSinkBuilder<T> {
    prefix: String,
    target_path: PathBuf,
    tmp_path: PathBuf,
    max_rows: usize,
    max_size_bytes: usize,
    roll_time: Duration,
    file_upload: FileUpload,
    auto_commit: bool,
    label: String,
    compression: Compression,
    row_group_size: usize,
    batch_size: usize,
    _phantom: PhantomData<T>,
}

impl<T> ParquetSinkBuilder<T> {
    /// Create a new ParquetSinkBuilder
    ///
    /// # Arguments
    /// * `prefix` - File prefix for generated parquet files
    /// * `target_path` - Directory for completed files
    /// * `file_upload` - FileUpload handle for S3 uploads
    /// * `label` - Metric name for tracking writes
    pub fn new(
        prefix: impl ToString,
        target_path: &Path,
        file_upload: FileUpload,
        label: impl ToString,
    ) -> Self {
        let tmp_path = target_path.join("tmp");
        Self {
            prefix: prefix.to_string(),
            target_path: target_path.to_path_buf(),
            tmp_path,
            max_rows: DEFAULT_MAX_ROWS,
            max_size_bytes: DEFAULT_MAX_SIZE_BYTES,
            roll_time: Duration::from_secs(DEFAULT_SINK_ROLL_SECS),
            file_upload,
            auto_commit: false,
            label: label.to_string(),
            compression: Compression::SNAPPY,
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            batch_size: DEFAULT_BATCH_SIZE,
            _phantom: PhantomData,
        }
    }

    /// Set maximum rows per file before rotation
    pub fn max_rows(self, max_rows: usize) -> Self {
        Self { max_rows, ..self }
    }

    /// Set maximum file size in bytes before rotation
    pub fn max_size_bytes(self, max_size_bytes: usize) -> Self {
        Self {
            max_size_bytes,
            ..self
        }
    }

    /// Set target directory for completed files
    pub fn target_path(self, target_path: &Path) -> Self {
        Self {
            target_path: target_path.to_path_buf(),
            tmp_path: target_path.join("tmp"),
            ..self
        }
    }

    /// Set temporary directory for in-progress files
    pub fn tmp_path(self, path: &Path) -> Self {
        Self {
            tmp_path: path.to_path_buf(),
            ..self
        }
    }

    /// Enable auto-commit (automatically upload files on rotation)
    pub fn auto_commit(self, auto_commit: bool) -> Self {
        Self {
            auto_commit,
            ..self
        }
    }

    /// Set roll time duration
    pub fn roll_time(self, duration: Duration) -> Self {
        Self {
            roll_time: duration,
            ..self
        }
    }

    /// Set parquet compression algorithm
    pub fn compression(self, compression: Compression) -> Self {
        Self {
            compression,
            ..self
        }
    }

    /// Set row group size for parquet files
    pub fn row_group_size(self, row_group_size: usize) -> Self {
        Self {
            row_group_size,
            ..self
        }
    }

    /// Set batch size (records to accumulate before writing)
    pub fn batch_size(self, batch_size: usize) -> Self {
        Self { batch_size, ..self }
    }

    /// Create the ParquetSinkClient and ParquetSink pair
    ///
    /// Type T must implement ArrowSchema and serde::Serialize
    pub async fn create(self) -> Result<(ParquetSinkClient<T>, ParquetSink<T>)>
    where
        T: ArrowSchema + Serialize,
    {
        let (tx, rx) = message_channel(50);

        Ok((
            ParquetSinkClient::new(tx, self.label.clone()),
            ParquetSink {
                target_path: self.target_path,
                tmp_path: self.tmp_path,
                prefix: self.prefix,
                max_rows: self.max_rows,
                max_size_bytes: self.max_size_bytes,
                roll_time: self.roll_time,
                messages: rx,
                file_upload: self.file_upload,
                staged_files: Vec::new(),
                auto_commit: self.auto_commit,
                active_sink: None,
                compression: self.compression,
                row_group_size: self.row_group_size,
                batch_size: self.batch_size,
                schema: T::arrow_schema(),
            },
        ))
    }
}

/// Client for writing records to the parquet sink
#[derive(Debug, Clone)]
pub struct ParquetSinkClient<T> {
    pub sender: MessageSender<T>,
    pub label: String,
}

impl<T> ParquetSinkClient<T> {
    pub fn new(sender: MessageSender<T>, label: impl Into<String>) -> Self {
        Self {
            sender,
            label: label.into(),
        }
    }

    /// Write a single record to the sink with optional labels for metrics
    pub async fn write(&self, item: impl Into<T>) -> Result<oneshot::Receiver<Result>> {
        let (on_write_tx, on_write_rx) = oneshot::channel();

        match self
            .sender
            .send_timeout(Message::Data(on_write_tx, item.into()), SEND_TIMEOUT)
            .await
        {
            Ok(_) => {
                telemetry::increment_counter(
                    SINK_RECORDS_WRITTEN,
                    1,
                    telemetry_labels!("sink_name" => self.label.as_str()),
                );
                debug!("parquet_sink write succeeded for {:?}", self.label);
                Ok(on_write_rx)
            }
            Err(SendTimeoutError::Closed(_)) => {
                telemetry::increment_counter(
                    SINK_WRITE_ERRORS,
                    1,
                    telemetry_labels!("sink_name" => self.label.as_str(), "error_type" => "channel_closed"),
                );
                error!(
                    "parquet_sink write failed for {:?} channel closed",
                    self.label
                );
                Err(ChannelError::sink_closed(&self.label))
            }
            Err(SendTimeoutError::Timeout(_)) => {
                telemetry::increment_counter(
                    SINK_WRITE_ERRORS,
                    1,
                    telemetry_labels!("sink_name" => self.label.as_str(), "error_type" => "timeout"),
                );
                error!(
                    "parquet_sink write failed for {:?} due to send timeout",
                    self.label
                );
                Err(ChannelError::sink_timeout(&self.label))
            }
        }
    }

    /// Write multiple records in batch
    pub async fn write_all(
        &self,
        items: impl IntoIterator<Item = T>,
    ) -> Result<Vec<oneshot::Receiver<Result>>> {
        let mut receivers = Vec::new();
        for item in items {
            receivers.push(self.write(item).await?);
        }
        Ok(receivers)
    }

    /// Commit all staged files (finalize and trigger upload)
    pub async fn commit(&self) -> Result<oneshot::Receiver<Result<FileManifest>>> {
        let (on_commit_tx, on_commit_rx) = oneshot::channel();
        self.sender
            .send_timeout(Message::Commit(on_commit_tx), SEND_TIMEOUT)
            .await
            .map_err(|_| ChannelError::sink_timeout(self.label.as_str()))?;
        Ok(on_commit_rx)
    }

    /// Rollback and delete all staged files
    pub async fn rollback(&self) -> Result<oneshot::Receiver<Result<FileManifest>>> {
        let (on_rollback_tx, on_rollback_rx) = oneshot::channel();
        self.sender
            .send_timeout(Message::Rollback(on_rollback_tx), SEND_TIMEOUT)
            .await
            .map_err(|_| ChannelError::sink_timeout(self.label.as_str()))?;
        Ok(on_rollback_rx)
    }
}

/// Active parquet file being written
struct ActiveParquetSink<T> {
    file_path: PathBuf,
    writer: ArrowWriter<File>,
    row_count: usize,
    buffer: Vec<T>,
    created_at: DateTime<Utc>,
    approximate_size: usize,
}

/// Server that manages parquet file writing with rotation
pub struct ParquetSink<T> {
    target_path: PathBuf,
    tmp_path: PathBuf,
    prefix: String,
    max_rows: usize,
    max_size_bytes: usize,
    roll_time: Duration,
    messages: MessageReceiver<T>,
    file_upload: FileUpload,
    staged_files: Vec<PathBuf>,
    auto_commit: bool,
    active_sink: Option<Mutex<ActiveParquetSink<T>>>,
    compression: Compression,
    row_group_size: usize,
    batch_size: usize,
    schema: SchemaRef,
}

impl<T: Send + Sync + Serialize + 'static> ManagedProc for ParquetSink<T> {
    fn run_proc(self: Box<Self>, shutdown: ShutdownSignal) -> super_visor::ManagedFuture {
        super_visor::run(self.run(shutdown))
    }
}

impl<T> ParquetSink<T>
where
    T: Serialize,
{
    #[cfg(test)]
    pub async fn init(&mut self) -> Result {
        fs::create_dir_all(&self.target_path).await?;
        fs::create_dir_all(&self.tmp_path).await?;

        self.recover_from_crash().await?;
        Ok(())
    }

    #[cfg(not(test))]
    async fn init(&mut self) -> Result {
        fs::create_dir_all(&self.target_path).await?;
        fs::create_dir_all(&self.tmp_path).await?;

        self.recover_from_crash().await?;
        Ok(())
    }

    /// Handle crash recovery on startup
    ///
    /// This processes files left behind from a previous crash:
    /// 1. Re-upload any completed files in target_path
    /// 2. Handle incomplete files in tmp_path:
    ///    - If auto_commit: move to target and upload
    ///    - If not auto_commit: delete (incomplete data)
    async fn recover_from_crash(&mut self) -> Result {
        // Re-upload any existing completed files in target directory
        let mut target_dir = fs::read_dir(&self.target_path).await?;
        loop {
            match target_dir.next_entry().await {
                Ok(Some(entry)) => {
                    let file_name = entry.file_name();
                    let file_name_str = file_name.to_string_lossy();

                    // Only process files matching our prefix
                    if file_name_str.starts_with(&self.prefix)
                        && file_name_str.ends_with(".parquet")
                    {
                        info!(
                            "crash recovery: re-uploading completed file {}",
                            file_name_str
                        );
                        if let Err(err) = self.file_upload.upload_file(&entry.path()).await {
                            warn!("failed to upload recovered file {}: {}", file_name_str, err);
                        }
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    warn!("error reading target directory during recovery: {}", err);
                    continue;
                }
            }
        }

        // Handle incomplete files in tmp directory
        let mut tmp_dir = fs::read_dir(&self.tmp_path).await?;
        loop {
            match tmp_dir.next_entry().await {
                Ok(Some(entry)) => {
                    let file_name = entry.file_name();
                    let file_name_str = file_name.to_string_lossy();

                    // Only process files matching our prefix
                    if file_name_str.starts_with(&self.prefix)
                        && file_name_str.ends_with(".parquet")
                    {
                        let entry_path = entry.path();

                        if self.auto_commit {
                            // Move incomplete file to target and upload
                            info!(
                                "crash recovery: recovering incomplete file {}",
                                file_name_str
                            );
                            if let Err(err) = self.deposit_file(&entry_path).await {
                                warn!(
                                    "failed to deposit recovered file {}: {}",
                                    file_name_str, err
                                );
                            }
                        } else {
                            // Delete incomplete file
                            info!("crash recovery: deleting incomplete file {}", file_name_str);
                            if let Err(err) = fs::remove_file(&entry_path).await {
                                warn!(
                                    "failed to remove incomplete file {}: {}",
                                    file_name_str, err
                                );
                            }
                        }
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    warn!("error reading tmp directory during recovery: {}", err);
                    continue;
                }
            }
        }

        Ok(())
    }

    /// Move a file from tmp to target and queue for upload
    async fn deposit_file(&mut self, file_path: &Path) -> Result {
        if !file_path.exists() {
            return Ok(());
        }

        let file_name = file_path.file_name().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "expected file name")
        })?;

        let target_path = self.target_path.join(file_name);
        fs::rename(file_path, &target_path).await?;
        self.file_upload.upload_file(&target_path).await?;

        Ok(())
    }

    /// Run the sink server loop
    pub async fn run(mut self, mut shutdown: ShutdownSignal) -> Result {
        info!(
            "starting parquet sink {} in {}",
            self.prefix,
            self.target_path.display()
        );

        self.init().await?;

        let mut rollover_timer = time::interval(Duration::from_millis(SINK_CHECK_MILLIS));
        rollover_timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = rollover_timer.tick() => self.maybe_roll().await?,
                msg = self.messages.recv() => match msg {
                    Some(Message::Data(on_write_tx, item)) => {
                        let res = self.write(item).await;
                        let _ = on_write_tx.send(res);
                    }
                    Some(Message::Commit(on_commit_tx)) => {
                        let res = self.commit().await;
                        let _ = on_commit_tx.send(res);
                    }
                    Some(Message::Rollback(on_rollback_tx)) => {
                        let res = self.rollback().await;
                        let _ = on_rollback_tx.send(res);
                    }
                    None => break,
                },
                _ = &mut shutdown => break,
            }
        }

        // Clean shutdown - commit any remaining data
        if self.active_sink.is_some() {
            self.maybe_close_active_sink().await?;
        }

        info!("stopping parquet sink {}", self.prefix);
        Ok(())
    }

    #[cfg(test)]
    pub async fn write(&mut self, item: T) -> Result {
        // Initialize sink if needed
        if self.active_sink.is_none() {
            self.new_sink().await?;
        }

        // Add to buffer
        if let Some(sink_mutex) = &self.active_sink {
            let should_flush = {
                let mut sink = sink_mutex.lock().unwrap();
                sink.buffer.push(item);
                sink.buffer.len() >= self.batch_size
            };

            // Write batch if buffer is full
            if should_flush {
                self.flush_buffer().await?;
            }

            // Check rotation triggers
            if let Some(reason) = self.should_rotate()? {
                self.rotate(reason).await?;
            }
        }

        Ok(())
    }

    #[cfg(not(test))]
    async fn write(&mut self, item: T) -> Result {
        // Initialize sink if needed
        if self.active_sink.is_none() {
            self.new_sink().await?;
        }

        // Add to buffer
        if let Some(sink_mutex) = &self.active_sink {
            let should_flush = {
                let mut sink = sink_mutex.lock().unwrap();
                sink.buffer.push(item);
                sink.buffer.len() >= self.batch_size
            };

            // Write batch if buffer is full
            if should_flush {
                self.flush_buffer().await?;
            }

            // Check rotation triggers
            if let Some(reason) = self.should_rotate()? {
                self.rotate(reason).await?;
            }
        }

        Ok(())
    }

    /// Check if file should rotate based on thresholds
    fn should_rotate(&self) -> Result<Option<&'static str>> {
        if let Some(sink_mutex) = &self.active_sink {
            let sink = sink_mutex.lock().unwrap();
            // Row count threshold
            if sink.row_count >= self.max_rows {
                debug!("rotating on row count: {}", sink.row_count);
                return Ok(Some("row_count"));
            }

            // Size threshold (approximate)
            if sink.approximate_size >= self.max_size_bytes {
                debug!("rotating on size: {}", sink.approximate_size);
                return Ok(Some("size"));
            }

            // Time threshold
            let roll_duration = chrono::Duration::from_std(self.roll_time)
                .map_err(|e| Error::Io(std::io::Error::other(e)))?;
            if (sink.created_at + roll_duration) <= Utc::now() {
                debug!("rotating on time");
                return Ok(Some("time"));
            }
        }

        Ok(None)
    }

    /// Periodic rotation check
    async fn maybe_roll(&mut self) -> Result {
        if let Some(reason) = self.should_rotate()? {
            self.rotate(reason).await?;
        }
        Ok(())
    }

    /// Rotate to a new file
    async fn rotate(&mut self, reason: &str) -> Result {
        self.maybe_close_active_sink().await?;

        telemetry::increment_counter(
            SINK_FILES_ROTATED,
            1,
            telemetry_labels!("file_type" => self.prefix.as_str(), "reason" => reason),
        );
        if self.auto_commit {
            self.commit().await?;
        }
        Ok(())
    }

    /// Flush buffered records to parquet file
    async fn flush_buffer(&mut self) -> Result {
        if let Some(sink_mutex) = &mut self.active_sink {
            let sink = sink_mutex.get_mut().unwrap();
            if sink.buffer.is_empty() {
                return Ok(());
            }

            // Convert buffer to RecordBatch using serde_arrow
            let arrays = serde_arrow::to_arrow(self.schema.fields(), &sink.buffer)
                .map_err(|e| Error::SerdeArrow(e.to_string()))?;

            let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;

            let buffer_size = sink.buffer.len() as f64;

            // Write batch to parquet file
            sink.writer.write(&batch)?;

            telemetry::record_histogram(
                SINK_BATCH_SIZE,
                buffer_size,
                telemetry_labels!("file_type" => self.prefix.as_str()),
            );

            // Update counts
            sink.row_count += sink.buffer.len();
            sink.approximate_size += batch.get_array_memory_size();
            sink.buffer.clear();
        }

        Ok(())
    }

    /// Create a new parquet file sink
    async fn new_sink(&mut self) -> Result {
        let file_meta = FileMeta::new(&self.prefix);
        let file_path = self.tmp_path.join(&file_meta.key);

        let file = fs::File::create(&file_path).await?;
        let std_file = file.into_std().await;

        // Build writer properties
        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .set_max_row_group_size(self.row_group_size)
            .set_write_batch_size(1024)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_created_by(format!("prestige/{}", env!("CARGO_PKG_VERSION")))
            .build();

        // Create Arrow writer
        let writer = ArrowWriter::try_new(std_file, self.schema.clone(), Some(props))?;

        self.active_sink = Some(Mutex::new(ActiveParquetSink {
            file_path,
            writer,
            row_count: 0,
            buffer: Vec::with_capacity(self.batch_size),
            created_at: Utc::now(),
            approximate_size: 0,
        }));

        info!("created new parquet file: {}", file_meta.key);
        Ok(())
    }

    /// Close active sink and move to staged
    async fn maybe_close_active_sink(&mut self) -> Result {
        // Flush remaining buffer first
        if self.active_sink.is_some() {
            self.flush_buffer().await?;
        }

        if let Some(sink_mutex) = self.active_sink.take() {
            let sink = sink_mutex.into_inner().unwrap();
            // Close the writer
            sink.writer.close()?;

            // Move from tmp to target
            let target_file = self.target_path.join(
                sink.file_path
                    .file_name()
                    .ok_or_else(|| std::io::Error::other("no filename"))?,
            );

            fs::rename(&sink.file_path, &target_file).await?;
            self.staged_files.push(target_file);

            info!("closed parquet file with {} rows", sink.row_count);
        }

        Ok(())
    }

    /// Commit staged files and trigger uploads
    pub async fn commit(&mut self) -> Result<FileManifest> {
        self.maybe_close_active_sink().await?;

        let manifest = self
            .staged_files
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();

        // Trigger uploads
        for file_path in self.staged_files.drain(..) {
            self.file_upload.upload_file(&file_path).await?;
        }

        Ok(manifest)
    }

    /// Rollback and delete staged files
    pub async fn rollback(&mut self) -> Result<FileManifest> {
        self.maybe_close_active_sink().await?;

        let manifest = self
            .staged_files
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();

        // Delete staged files
        for file_path in self.staged_files.drain(..) {
            if let Err(err) = fs::remove_file(&file_path).await {
                warn!("failed to remove file {:?}: {}", file_path, err);
            }
        }

        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FileUploadServer, new_client};
    use arrow::datatypes::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::time::{Duration, sleep};

    // Test record type
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestRecord {
        id: i64,
        name: String,
        value: f64,
    }

    impl ArrowSchema for TestRecord {
        fn arrow_schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
            ]))
        }
    }

    fn create_test_records(count: usize) -> Vec<TestRecord> {
        (0..count)
            .map(|i| TestRecord {
                id: i as i64,
                name: format!("record_{}", i),
                value: i as f64 * 1.5,
            })
            .collect()
    }

    async fn create_test_file_upload() -> (FileUpload, FileUploadServer) {
        let client = new_client(None, None, None, None).await;
        FileUpload::new(client, "test-bucket".to_string()).await
    }

    #[tokio::test]
    async fn test_builder_defaults() {
        let temp_dir = TempDir::new().unwrap();
        let (file_upload, _server) = create_test_file_upload().await;

        let (_, sink) = ParquetSinkBuilder::<TestRecord>::new(
            "test",
            temp_dir.path(),
            file_upload,
            "test_metric",
        )
        .create()
        .await
        .unwrap();

        assert_eq!(sink.prefix, "test");
        assert_eq!(sink.max_rows, DEFAULT_MAX_ROWS);
        assert_eq!(sink.max_size_bytes, DEFAULT_MAX_SIZE_BYTES);
        assert_eq!(
            sink.roll_time,
            std::time::Duration::from_secs(DEFAULT_SINK_ROLL_SECS)
        );
        assert_eq!(sink.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(sink.compression, Compression::SNAPPY);
        assert_eq!(sink.row_group_size, DEFAULT_ROW_GROUP_SIZE);
    }

    #[tokio::test]
    async fn test_builder_custom_values() {
        let temp_dir = TempDir::new().unwrap();
        let (file_upload, _server) = create_test_file_upload().await;

        let (_, sink) = ParquetSinkBuilder::<TestRecord>::new(
            "custom",
            temp_dir.path(),
            file_upload,
            "custom_metric",
        )
        .max_rows(50_000)
        .max_size_bytes(50 * 1024 * 1024)
        .roll_time(std::time::Duration::from_secs(60))
        .batch_size(500)
        .compression(Compression::ZSTD(Default::default()))
        .row_group_size(5_000)
        .create()
        .await
        .unwrap();

        assert_eq!(sink.prefix, "custom");
        assert_eq!(sink.max_rows, 50_000);
        assert_eq!(sink.max_size_bytes, 50 * 1024 * 1024);
        assert_eq!(sink.roll_time, std::time::Duration::from_secs(60));
        assert_eq!(sink.batch_size, 500);
        assert!(matches!(sink.compression, Compression::ZSTD(_)));
        assert_eq!(sink.row_group_size, 5_000);
    }

    #[tokio::test]
    async fn test_write_and_commit_direct() {
        let temp_dir = TempDir::new().unwrap();
        let (file_upload, _server) = create_test_file_upload().await;

        let (_, mut sink) = ParquetSinkBuilder::<TestRecord>::new(
            "test",
            temp_dir.path(),
            file_upload,
            "test_metric",
        )
        .batch_size(10)
        .create()
        .await
        .unwrap();

        sink.init().await.unwrap();

        // Write some records directly
        let records = create_test_records(25);
        for record in records {
            sink.write(record).await.unwrap();
        }

        // Commit should close the file
        let manifest = sink.commit().await.unwrap();

        assert_eq!(manifest.len(), 1);
        assert!(manifest[0].contains("test."));
        assert!(manifest[0].ends_with(".parquet"));

        // Verify file exists in target location
        let file_path = PathBuf::from(&manifest[0]);
        assert!(file_path.exists());
    }

    #[tokio::test]
    async fn test_rotation_on_row_count() {
        let temp_dir = TempDir::new().unwrap();
        let (file_upload, _server) = create_test_file_upload().await;

        let (_, mut sink) = ParquetSinkBuilder::<TestRecord>::new(
            "test",
            temp_dir.path(),
            file_upload,
            "test_metric",
        )
        .max_rows(100)
        .batch_size(10)
        .create()
        .await
        .unwrap();

        sink.init().await.unwrap();

        // Write 150 records - should trigger rotation at 100
        let records = create_test_records(150);
        for record in records {
            sink.write(record).await.unwrap();
        }

        let manifest = sink.commit().await.unwrap();

        // Should have 2 files: one rotated (100 rows) and one final (50 rows)
        assert_eq!(manifest.len(), 2);
    }

    #[tokio::test]
    async fn test_rotation_on_time() {
        let temp_dir = TempDir::new().unwrap();
        let (file_upload, _server) = create_test_file_upload().await;

        let (_, mut sink) = ParquetSinkBuilder::<TestRecord>::new(
            "test",
            temp_dir.path(),
            file_upload,
            "test_metric",
        )
        .roll_time(std::time::Duration::from_millis(100))
        .batch_size(10)
        .create()
        .await
        .unwrap();

        sink.init().await.unwrap();

        // Write some records
        for _ in 0..5 {
            sink.write(create_test_records(1)[0].clone()).await.unwrap();
        }

        // Wait for rotation time to pass
        sleep(Duration::from_millis(150)).await;

        // Write more records - should trigger time-based rotation
        for _ in 0..5 {
            sink.write(create_test_records(1)[0].clone()).await.unwrap();
        }

        let manifest = sink.commit().await.unwrap();

        // Should have 2 files: one from time rotation, one from commit
        assert_eq!(manifest.len(), 2);
    }

    #[tokio::test]
    async fn test_rollback() {
        let temp_dir = TempDir::new().unwrap();
        let (file_upload, _server) = create_test_file_upload().await;

        let (_, mut sink) = ParquetSinkBuilder::<TestRecord>::new(
            "test",
            temp_dir.path(),
            file_upload,
            "test_metric",
        )
        .batch_size(10)
        .max_rows(50) // Set low threshold to trigger rotation
        .create()
        .await
        .unwrap();

        sink.init().await.unwrap();

        // Write records to trigger rotation (75 records with max_rows=50)
        let records = create_test_records(75);
        for record in records {
            sink.write(record).await.unwrap();
        }

        // At this point we should have 1 rotated file in staged_files
        assert_eq!(sink.staged_files.len(), 1);

        // Rollback should delete all staged files
        let manifest = sink.rollback().await.unwrap();

        // Manifest should contain the rotated file + the active file
        assert_eq!(manifest.len(), 2);

        // Verify files are deleted
        for path_str in &manifest {
            let path = PathBuf::from(path_str);
            assert!(!path.exists());
        }

        // staged_files should be empty
        assert_eq!(sink.staged_files.len(), 0);
    }

    #[tokio::test]
    async fn test_channel_communication() {
        let temp_dir = TempDir::new().unwrap();
        let (file_upload, _server) = create_test_file_upload().await;

        let (client, _sink) = ParquetSinkBuilder::<TestRecord>::new(
            "test",
            temp_dir.path(),
            file_upload,
            "test_metric",
        )
        .create()
        .await
        .unwrap();

        let record = create_test_records(1)[0].clone();

        // Should be able to send a record
        let result = client.write(record).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_closed_channel_error() {
        let temp_dir = TempDir::new().unwrap();
        let (file_upload, _server) = create_test_file_upload().await;

        let (client, sink) = ParquetSinkBuilder::<TestRecord>::new(
            "test",
            temp_dir.path(),
            file_upload,
            "test_metric",
        )
        .create()
        .await
        .unwrap();

        // Drop sink to close receiver
        drop(sink);

        let record = create_test_records(1)[0].clone();
        let result = client.write(record).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Channel(_)));
    }

    #[tokio::test]
    async fn test_write_parquet_file_format() {
        let temp_dir = TempDir::new().unwrap();
        let (file_upload, _server) = create_test_file_upload().await;

        let (_, mut sink) = ParquetSinkBuilder::<TestRecord>::new(
            "test",
            temp_dir.path(),
            file_upload,
            "test_metric",
        )
        .batch_size(5)
        .create()
        .await
        .unwrap();

        sink.init().await.unwrap();

        // Write records and commit
        let records = create_test_records(10);
        for record in records {
            sink.write(record).await.unwrap();
        }

        let manifest = sink.commit().await.unwrap();
        assert_eq!(manifest.len(), 1);

        // Verify we can read the parquet file back
        let file_path = PathBuf::from(&manifest[0]);
        let file = File::open(&file_path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        let mut total_rows = 0;
        for batch_result in reader {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();

            // Verify schema
            assert_eq!(batch.num_columns(), 3);
            assert_eq!(batch.schema().field(0).name(), "id");
            assert_eq!(batch.schema().field(1).name(), "name");
            assert_eq!(batch.schema().field(2).name(), "value");
        }

        assert_eq!(total_rows, 10);
    }

    #[tokio::test]
    async fn test_crash_recovery_reuploads_completed_files() {
        let temp_dir = TempDir::new().unwrap();
        let (file_upload, _server) = create_test_file_upload().await;

        // Create a completed parquet file in target directory (simulating previous run)
        let target_file = temp_dir.path().join("test.1234567890.parquet");
        std::fs::write(&target_file, b"fake parquet data").unwrap();

        let (_, mut sink) = ParquetSinkBuilder::<TestRecord>::new(
            "test",
            temp_dir.path(),
            file_upload,
            "test_metric",
        )
        .create()
        .await
        .unwrap();

        // Init should trigger crash recovery and attempt to re-upload the file
        sink.init().await.unwrap();

        // File should still exist in target directory
        assert!(target_file.exists());
    }

    #[tokio::test]
    async fn test_crash_recovery_deletes_incomplete_files_when_no_auto_commit() {
        let temp_dir = TempDir::new().unwrap();
        let (file_upload, _server) = create_test_file_upload().await;

        // Create an incomplete parquet file in tmp directory
        let tmp_dir = temp_dir.path().join("tmp");
        std::fs::create_dir_all(&tmp_dir).unwrap();
        let incomplete_file = tmp_dir.join("test.1234567890.parquet");
        std::fs::write(&incomplete_file, b"incomplete data").unwrap();

        let (_, mut sink) = ParquetSinkBuilder::<TestRecord>::new(
            "test",
            temp_dir.path(),
            file_upload,
            "test_metric",
        )
        .auto_commit(false) // No auto-commit means delete incomplete files
        .create()
        .await
        .unwrap();

        // Init should trigger crash recovery and delete the incomplete file
        sink.init().await.unwrap();

        // Incomplete file should be deleted
        assert!(!incomplete_file.exists());
    }

    #[tokio::test]
    async fn test_crash_recovery_moves_incomplete_files_when_auto_commit() {
        let temp_dir = TempDir::new().unwrap();
        let (file_upload, _server) = create_test_file_upload().await;

        // Create an incomplete parquet file in tmp directory
        let tmp_dir = temp_dir.path().join("tmp");
        std::fs::create_dir_all(&tmp_dir).unwrap();
        let incomplete_file = tmp_dir.join("test.1234567890.parquet");
        std::fs::write(&incomplete_file, b"incomplete data").unwrap();

        let (_, mut sink) = ParquetSinkBuilder::<TestRecord>::new(
            "test",
            temp_dir.path(),
            file_upload,
            "test_metric",
        )
        .auto_commit(true) // Auto-commit means recover and upload incomplete files
        .create()
        .await
        .unwrap();

        // Init should trigger crash recovery and move the file to target
        sink.init().await.unwrap();

        // Original file should be gone from tmp
        assert!(!incomplete_file.exists());

        // File should be in target directory
        let target_file = temp_dir.path().join("test.1234567890.parquet");
        assert!(target_file.exists());
    }

    #[tokio::test]
    async fn test_crash_recovery_ignores_non_matching_files() {
        let temp_dir = TempDir::new().unwrap();
        let (file_upload, _server) = create_test_file_upload().await;

        // Create files that don't match our prefix
        let tmp_dir = temp_dir.path().join("tmp");
        std::fs::create_dir_all(&tmp_dir).unwrap();
        let other_file = tmp_dir.join("other.1234567890.parquet");
        let txt_file = tmp_dir.join("test.1234567890.txt");
        std::fs::write(&other_file, b"other data").unwrap();
        std::fs::write(&txt_file, b"text data").unwrap();

        let (_, mut sink) = ParquetSinkBuilder::<TestRecord>::new(
            "test",
            temp_dir.path(),
            file_upload,
            "test_metric",
        )
        .auto_commit(false)
        .create()
        .await
        .unwrap();

        // Init should not delete non-matching files
        sink.init().await.unwrap();

        // Both files should still exist
        assert!(other_file.exists());
        assert!(txt_file.exists());
    }
}

use crate::{
    ArrowSchema, Error, Result,
    error::ChannelError,
    file_sink::{
        DEFAULT_BATCH_SIZE, DEFAULT_MAX_ROWS, DEFAULT_MAX_SIZE_BYTES, DEFAULT_SINK_ROLL_SECS,
    },
    telemetry::{
        self, SINK_BATCH_SIZE, SINK_FILES_ROTATED, SINK_RECORDS_WRITTEN, SINK_WRITE_ERRORS,
        telemetry_labels,
    },
};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use iceberg::table::Table;
use parquet::basic::Compression;
use serde::Serialize;
use std::{collections::HashMap, marker::PhantomData, path::PathBuf, sync::Arc, time::Duration};
use super_visor::{ManagedProc, ShutdownSignal};
use tokio::{
    sync::{mpsc, oneshot},
    time,
};
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::{
    catalog::Catalog,
    manifest::CommitManifest,
    schema::IcebergSchema,
    table::{IcebergTableConfigBuilder, ensure_table_for, ensure_table_for_with},
};

// ---------------------------------------------------------------------------
// DataWriter trait — generic write abstraction for iceberg sinks
// ---------------------------------------------------------------------------

/// Async trait for writing typed records to an iceberg table.
///
/// Decouples consumers from the concrete sink transport — implementations
/// may write via the channel-based `IcebergSinkClient`, a direct writer,
/// or a mock for testing.
#[async_trait]
pub trait DataWriter<T: Send + 'static>: Send + Sync {
    /// Write a single record.
    async fn write(&self, item: T) -> Result;

    /// Write a batch of records in a single operation.
    async fn write_all(&self, items: Vec<T>) -> Result;
}

/// Type-erased, shareable writer handle.
pub type BoxedDataWriter<T> = Arc<dyn DataWriter<T>>;

/// Blanket conversion into a `BoxedDataWriter<T>`.
pub trait IntoBoxedDataWriter<T: Send + 'static>: DataWriter<T> + Sized + 'static {
    fn boxed(self) -> BoxedDataWriter<T> {
        Arc::new(self)
    }
}

impl<T: Send + 'static, W: DataWriter<T> + 'static> IntoBoxedDataWriter<T> for W {}

const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const SINK_CHECK_MILLIS: u64 = 60_000;

pub type IcebergFileManifest = Vec<String>;

pub enum IcebergMessage<T> {
    Data(oneshot::Sender<Result>, T),
    DataBatch(oneshot::Sender<Result>, Vec<T>),
    Commit(oneshot::Sender<Result<IcebergFileManifest>>),
    Rollback(oneshot::Sender<Result<IcebergFileManifest>>),
    Publish(oneshot::Sender<Result>),
}

pub struct IcebergSinkBuilder<T> {
    table: Table,
    catalog: Catalog,
    max_rows: usize,
    max_size_bytes: usize,
    roll_time: Duration,
    auto_commit: bool,
    label: String,
    compression: Compression,
    batch_size: usize,
    snapshot_properties: HashMap<String, String>,
    wap_branch: Option<String>,
    auto_publish: bool,
    manifest_dir: Option<PathBuf>,
    _phantom: PhantomData<T>,
}

impl<T: IcebergSchema + Serialize> IcebergSinkBuilder<T> {
    /// Create a sink builder by automatically ensuring the table exists using
    /// the type's `IcebergSchema` metadata (table name, namespace, partition spec, sort order).
    ///
    /// `label` is used as fallback table name when `#[prestige(table = "...")]` is absent.
    pub async fn for_type(catalog: Catalog, label: &str) -> crate::Result<Self> {
        let table = ensure_table_for::<T>(&catalog, label).await?.into_table();
        Ok(Self::new(table, catalog, label))
    }

    /// Like `for_type`, but accepts a closure to override table config builder fields.
    pub async fn for_type_with(
        catalog: Catalog,
        label: &str,
        config_override: impl FnOnce(IcebergTableConfigBuilder) -> IcebergTableConfigBuilder,
    ) -> crate::Result<Self> {
        let table = ensure_table_for_with::<T>(&catalog, label, config_override)
            .await?
            .into_table();
        Ok(Self::new(table, catalog, label))
    }
}

impl<T> IcebergSinkBuilder<T> {
    pub fn new(table: Table, catalog: Catalog, label: impl ToString) -> Self {
        Self {
            table,
            catalog,
            max_rows: DEFAULT_MAX_ROWS,
            max_size_bytes: DEFAULT_MAX_SIZE_BYTES,
            roll_time: Duration::from_secs(DEFAULT_SINK_ROLL_SECS),
            auto_commit: false,
            label: label.to_string(),
            compression: Compression::SNAPPY,
            batch_size: DEFAULT_BATCH_SIZE,
            snapshot_properties: HashMap::new(),
            wap_branch: None,
            auto_publish: true,
            manifest_dir: None,
            _phantom: PhantomData,
        }
    }

    pub fn max_rows(self, max_rows: usize) -> Self {
        Self { max_rows, ..self }
    }

    pub fn max_size_bytes(self, max_size_bytes: usize) -> Self {
        Self {
            max_size_bytes,
            ..self
        }
    }

    pub fn roll_time(self, duration: Duration) -> Self {
        Self {
            roll_time: duration,
            ..self
        }
    }

    pub fn auto_commit(self, auto_commit: bool) -> Self {
        Self {
            auto_commit,
            ..self
        }
    }

    pub fn compression(self, compression: Compression) -> Self {
        Self {
            compression,
            ..self
        }
    }

    pub fn batch_size(self, batch_size: usize) -> Self {
        Self { batch_size, ..self }
    }

    pub fn snapshot_properties(self, properties: HashMap<String, String>) -> Self {
        Self {
            snapshot_properties: properties,
            ..self
        }
    }

    /// Enable Write-Audit-Publish (WAP) mode: commits write to a named audit
    /// branch instead of directly to main. By default the branch is
    /// auto-published after each commit; disable with `auto_publish(false)`.
    pub fn wap_enabled(self, branch_name: impl Into<String>) -> Self {
        Self {
            wap_branch: Some(branch_name.into()),
            ..self
        }
    }

    /// When WAP is enabled, control whether the audit branch is automatically
    /// published to main after each commit. Defaults to `true`.
    ///
    /// When `false`, use `IcebergSinkClient::publish()` to manually promote
    /// branch data to main after validation.
    pub fn auto_publish(self, auto_publish: bool) -> Self {
        Self {
            auto_publish,
            ..self
        }
    }

    /// Enable local write-ahead manifest for crash recovery. Data file paths
    /// are recorded to disk before catalog commit so they can be re-committed
    /// on restart if the process crashes mid-commit.
    pub fn manifest_dir(self, dir: impl Into<PathBuf>) -> Self {
        Self {
            manifest_dir: Some(dir.into()),
            ..self
        }
    }

    pub fn create(self) -> (IcebergSinkClient<T>, IcebergSink<T>)
    where
        T: ArrowSchema + Serialize,
    {
        let (tx, rx) = mpsc::channel(50);

        let manifest = self.manifest_dir.as_ref().and_then(|dir| {
            match CommitManifest::new(dir, &self.label) {
                Ok(m) => Some(m),
                Err(err) => {
                    warn!(
                        ?err,
                        label = self.label,
                        "failed to create commit manifest, running without crash recovery"
                    );
                    None
                }
            }
        });

        (
            IcebergSinkClient::new(tx, self.label.clone()),
            IcebergSink {
                table: self.table,
                catalog: self.catalog,
                max_rows: self.max_rows,
                max_size_bytes: self.max_size_bytes,
                roll_time: self.roll_time,
                messages: rx,
                auto_commit: self.auto_commit,
                label: self.label,
                compression: self.compression,
                batch_size: self.batch_size,
                schema: T::arrow_schema(),
                buffer: Vec::new(),
                row_count: 0,
                approximate_size: 0,
                created_at: None,
                staged_batches: Vec::new(),
                snapshot_properties: self.snapshot_properties,
                wap_branch: self.wap_branch,
                auto_publish: self.auto_publish,
                manifest,
                _phantom: PhantomData,
            },
        )
    }
}

#[derive(Debug, Clone)]
pub struct IcebergSinkClient<T> {
    pub sender: mpsc::Sender<IcebergMessage<T>>,
    pub label: String,
}

impl<T> IcebergSinkClient<T> {
    pub fn new(sender: mpsc::Sender<IcebergMessage<T>>, label: impl Into<String>) -> Self {
        Self {
            sender,
            label: label.into(),
        }
    }

    pub async fn write(&self, item: impl Into<T>) -> Result<oneshot::Receiver<Result>> {
        let (tx, rx) = oneshot::channel();
        match self
            .sender
            .send_timeout(IcebergMessage::Data(tx, item.into()), SEND_TIMEOUT)
            .await
        {
            Ok(_) => {
                telemetry::increment_counter(
                    SINK_RECORDS_WRITTEN,
                    1,
                    telemetry_labels!("sink_name" => self.label.as_str()),
                );
                Ok(rx)
            }
            Err(mpsc::error::SendTimeoutError::Closed(_)) => {
                telemetry::increment_counter(
                    SINK_WRITE_ERRORS,
                    1,
                    telemetry_labels!("sink_name" => self.label.as_str(), "error_type" => "channel_closed"),
                );
                Err(ChannelError::sink_closed(&self.label))
            }
            Err(mpsc::error::SendTimeoutError::Timeout(_)) => {
                telemetry::increment_counter(
                    SINK_WRITE_ERRORS,
                    1,
                    telemetry_labels!("sink_name" => self.label.as_str(), "error_type" => "timeout"),
                );
                Err(ChannelError::sink_timeout(&self.label))
            }
        }
    }

    pub async fn commit(&self) -> Result<oneshot::Receiver<Result<IcebergFileManifest>>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send_timeout(IcebergMessage::Commit(tx), SEND_TIMEOUT)
            .await
            .map_err(|_| ChannelError::sink_timeout(&self.label))?;
        Ok(rx)
    }

    pub async fn rollback(&self) -> Result<oneshot::Receiver<Result<IcebergFileManifest>>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send_timeout(IcebergMessage::Rollback(tx), SEND_TIMEOUT)
            .await
            .map_err(|_| ChannelError::sink_timeout(&self.label))?;
        Ok(rx)
    }

    /// Write multiple items in a single channel message, avoiding per-item
    /// channel overhead. The entire batch is acknowledged with one response.
    pub async fn write_all(&self, items: Vec<T>) -> Result<oneshot::Receiver<Result>> {
        let count = items.len();
        let (tx, rx) = oneshot::channel();
        match self
            .sender
            .send_timeout(IcebergMessage::DataBatch(tx, items), SEND_TIMEOUT)
            .await
        {
            Ok(_) => {
                telemetry::increment_counter(
                    SINK_RECORDS_WRITTEN,
                    count as u64,
                    telemetry_labels!("sink_name" => self.label.as_str()),
                );
                Ok(rx)
            }
            Err(mpsc::error::SendTimeoutError::Closed(_)) => {
                telemetry::increment_counter(
                    SINK_WRITE_ERRORS,
                    1,
                    telemetry_labels!("sink_name" => self.label.as_str(), "error_type" => "channel_closed"),
                );
                Err(ChannelError::sink_closed(&self.label))
            }
            Err(mpsc::error::SendTimeoutError::Timeout(_)) => {
                telemetry::increment_counter(
                    SINK_WRITE_ERRORS,
                    1,
                    telemetry_labels!("sink_name" => self.label.as_str(), "error_type" => "timeout"),
                );
                Err(ChannelError::sink_timeout(&self.label))
            }
        }
    }

    /// Publish the WAP audit branch to main. Only meaningful when the sink
    /// was created with `wap_enabled(...)` and `auto_publish(false)`.
    pub async fn publish(&self) -> Result<oneshot::Receiver<Result>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send_timeout(IcebergMessage::Publish(tx), SEND_TIMEOUT)
            .await
            .map_err(|_| ChannelError::sink_timeout(&self.label))?;
        Ok(rx)
    }
}

#[async_trait]
impl<T: Send + 'static> DataWriter<T> for IcebergSinkClient<T> {
    async fn write(&self, item: T) -> Result {
        let rx = IcebergSinkClient::write(self, item).await?;
        rx.await
            .map_err(|_| ChannelError::sink_closed(&self.label))?
    }

    async fn write_all(&self, items: Vec<T>) -> Result {
        let rx = IcebergSinkClient::write_all(self, items).await?;
        rx.await
            .map_err(|_| ChannelError::sink_closed(&self.label))?
    }
}

pub struct IcebergSink<T> {
    table: Table,
    catalog: Catalog,
    max_rows: usize,
    max_size_bytes: usize,
    roll_time: Duration,
    messages: mpsc::Receiver<IcebergMessage<T>>,
    auto_commit: bool,
    label: String,
    compression: Compression,
    batch_size: usize,
    schema: SchemaRef,

    buffer: Vec<T>,
    row_count: usize,
    approximate_size: usize,
    created_at: Option<DateTime<Utc>>,
    staged_batches: Vec<RecordBatch>,
    snapshot_properties: HashMap<String, String>,
    wap_branch: Option<String>,
    auto_publish: bool,
    manifest: Option<CommitManifest>,
    _phantom: PhantomData<T>,
}

impl<T: Send + Sync + Serialize + 'static> ManagedProc for IcebergSink<T> {
    fn run_proc(self: Box<Self>, shutdown: ShutdownSignal) -> super_visor::ManagedFuture {
        super_visor::run(self.run(shutdown))
    }
}

impl<T: Serialize> IcebergSink<T> {
    pub async fn run(mut self, mut shutdown: ShutdownSignal) -> Result {
        info!(label = self.label, "starting iceberg sink");

        // Recover any pending commit from a previous crash.
        if let Some(ref manifest) = self.manifest {
            match super::manifest::recover_pending_commit(
                manifest,
                &self.table,
                self.catalog.as_iceberg_catalog().as_ref(),
            )
            .await
            {
                Ok(Some(updated_table)) => {
                    self.table = updated_table;
                }
                Ok(None) => {}
                Err(err) => {
                    warn!(
                        label = self.label,
                        ?err,
                        "manifest recovery failed, continuing"
                    );
                }
            }
        }

        let mut rollover_timer = time::interval(Duration::from_millis(SINK_CHECK_MILLIS));
        rollover_timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = rollover_timer.tick() => self.maybe_roll().await?,
                msg = self.messages.recv() => match msg {
                    Some(IcebergMessage::Data(on_write_tx, item)) => {
                        let res = self.write_item(item).await;
                        let _ = on_write_tx.send(res);
                    }
                    Some(IcebergMessage::DataBatch(on_write_tx, items)) => {
                        let res = self.write_items(items).await;
                        let _ = on_write_tx.send(res);
                    }
                    Some(IcebergMessage::Commit(on_commit_tx)) => {
                        let res = self.commit().await;
                        let _ = on_commit_tx.send(res);
                    }
                    Some(IcebergMessage::Rollback(on_rollback_tx)) => {
                        let res = self.rollback().await;
                        let _ = on_rollback_tx.send(res);
                    }
                    Some(IcebergMessage::Publish(on_publish_tx)) => {
                        let res = self.publish_wap().await;
                        let _ = on_publish_tx.send(res);
                    }
                    None => break,
                },
                _ = &mut shutdown => break,
            }
        }

        // Flush remaining data on shutdown
        if (!self.buffer.is_empty() || !self.staged_batches.is_empty())
            && let Err(err) = self.commit().await
        {
            warn!(label = self.label, ?err, "failed to commit on shutdown");
        }

        info!(label = self.label, "stopping iceberg sink");
        Ok(())
    }

    async fn write_items(&mut self, items: Vec<T>) -> Result {
        if self.created_at.is_none() {
            self.created_at = Some(Utc::now());
        }

        self.buffer.extend(items);

        while self.buffer.len() >= self.batch_size {
            self.flush_buffer()?;
        }

        if let Some(reason) = self.should_rotate()? {
            self.rotate(reason).await?;
        }

        Ok(())
    }

    async fn write_item(&mut self, item: T) -> Result {
        if self.created_at.is_none() {
            self.created_at = Some(Utc::now());
        }

        self.buffer.push(item);

        if self.buffer.len() >= self.batch_size {
            self.flush_buffer()?;
        }

        if let Some(reason) = self.should_rotate()? {
            self.rotate(reason).await?;
        }

        Ok(())
    }

    fn flush_buffer(&mut self) -> Result {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let arrays = serde_arrow::to_arrow(self.schema.fields(), &self.buffer)
            .map_err(|e| Error::SerdeArrow(e.to_string()))?;

        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        let buffer_size = self.buffer.len() as f64;

        telemetry::record_histogram(
            SINK_BATCH_SIZE,
            buffer_size,
            telemetry_labels!("file_type" => self.label.as_str()),
        );

        self.row_count += self.buffer.len();
        self.approximate_size += batch.get_array_memory_size();
        self.staged_batches.push(batch);
        self.buffer.clear();

        Ok(())
    }

    fn should_rotate(&self) -> Result<Option<&'static str>> {
        if self.row_count >= self.max_rows {
            debug!("iceberg sink rotating on row count: {}", self.row_count);
            return Ok(Some("row_count"));
        }

        if self.approximate_size >= self.max_size_bytes {
            debug!("iceberg sink rotating on size: {}", self.approximate_size);
            return Ok(Some("size"));
        }

        if let Some(created_at) = self.created_at {
            let roll_duration = chrono::Duration::from_std(self.roll_time)
                .map_err(|e| Error::Io(std::io::Error::other(e)))?;
            if (created_at + roll_duration) <= Utc::now() {
                debug!("iceberg sink rotating on time");
                return Ok(Some("time"));
            }
        }

        Ok(None)
    }

    async fn maybe_roll(&mut self) -> Result {
        if let Some(reason) = self.should_rotate()? {
            self.rotate(reason).await?;
        }
        Ok(())
    }

    async fn rotate(&mut self, reason: &str) -> Result {
        telemetry::increment_counter(
            SINK_FILES_ROTATED,
            1,
            telemetry_labels!("file_type" => self.label.as_str(), "reason" => reason),
        );

        if self.auto_commit {
            self.commit().await?;
        }
        Ok(())
    }

    async fn commit(&mut self) -> Result<IcebergFileManifest> {
        self.flush_buffer()?;

        let batches = std::mem::take(&mut self.staged_batches);

        if batches.is_empty() {
            return Ok(Vec::new());
        }

        // Reload table metadata before writing to get current schema/partition info.
        if let Err(err) = self.reload_table().await {
            // Restore batches so data isn't lost.
            self.staged_batches = batches;
            return Err(err);
        }

        // Write data files to S3. RecordBatch clone is cheap (Arc column
        // refs), so we keep the originals to restore on failure.
        let data_files = match super::writer::write_data_files(
            &self.table,
            batches.clone(),
            Some(self.compression),
        )
        .await
        {
            Ok(files) => {
                // Drop the backup — data is durable in S3.
                drop(batches);
                files
            }
            Err(err) => {
                warn!(label = self.label, ?err, "failed to write data files to S3");
                self.staged_batches = batches;
                return Err(err);
            }
        };

        let file_paths: Vec<String> = data_files
            .iter()
            .map(|f| f.file_path().to_string())
            .collect();

        // Record to local manifest before catalog commit (crash recovery).
        // If this fails we still attempt the catalog commit — a successful
        // commit makes the data durable regardless. We only lose crash
        // recovery protection for this specific commit window.
        let manifest_recorded = if let Some(ref manifest) = self.manifest {
            let partition_spec_id = self.table.metadata().default_partition_spec_id();
            match manifest.record_files(&data_files, partition_spec_id) {
                Ok(()) => true,
                Err(err) => {
                    warn!(
                        label = self.label,
                        ?err,
                        "failed to write local manifest, crash recovery unavailable for this commit"
                    );
                    false
                }
            }
        } else {
            false
        };

        // Catalog commit (Transaction handles retry internally).
        let commit_result = if let Some(ref branch_name) = self.wap_branch {
            self.commit_wap(branch_name.clone(), data_files).await
        } else {
            self.commit_direct(data_files).await
        };

        match commit_result {
            Ok(()) => {
                // Clear manifest and reset counters only on success.
                if manifest_recorded
                    && let Some(ref manifest) = self.manifest
                    && let Err(err) = manifest.clear()
                {
                    warn!(label = self.label, ?err, "failed to clear local manifest");
                }
                self.row_count = 0;
                self.approximate_size = 0;
                self.created_at = None;

                info!(
                    label = self.label,
                    files = file_paths.len(),
                    "committed iceberg snapshot"
                );
                Ok(file_paths)
            }
            Err(err) => {
                // Data files are in S3. If the manifest was written, recovery
                // on restart will re-attempt the catalog commit. If the
                // manifest write also failed, the S3 files are orphaned —
                // this is logged above at warn level.
                self.row_count = 0;
                self.approximate_size = 0;
                self.created_at = None;
                Err(err)
            }
        }
    }

    /// Reload the table, reconnecting the catalog if the first attempt fails
    /// with an authentication error (expired internal OAuth2 token).
    async fn reload_table(&mut self) -> Result<()> {
        match self.catalog.load_table(self.table.identifier()).await {
            Ok(table) => {
                self.table = table;
                Ok(())
            }
            Err(first_err) => {
                debug!(
                    label = self.label,
                    err = %first_err,
                    "table reload failed, attempting catalog reconnect"
                );
                self.catalog.reconnect().await?;
                self.table = self.catalog.load_table(self.table.identifier()).await?;
                Ok(())
            }
        }
    }

    async fn commit_direct(&mut self, data_files: Vec<iceberg::spec::DataFile>) -> Result<()> {
        let snapshot_props = if self.snapshot_properties.is_empty() {
            None
        } else {
            Some(self.snapshot_properties.clone())
        };

        // Transaction::commit internally reloads the table, rebases on
        // conflict, and retries with exponential backoff — no outer retry
        // needed.
        match super::writer::commit_data_files(
            &self.table,
            self.catalog.as_iceberg_catalog().as_ref(),
            data_files,
            snapshot_props,
        )
        .await
        {
            Ok(updated_table) => {
                self.table = updated_table;
                Ok(())
            }
            Err(err) if super::manifest::is_already_committed(&err) => {
                info!(
                    label = self.label,
                    "data files already committed, treating as success"
                );
                // Refresh table to pick up the snapshot that contains our files.
                self.reload_table().await?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn commit_wap(
        &mut self,
        branch_name: String,
        data_files: Vec<iceberg::spec::DataFile>,
    ) -> Result<()> {
        if self
            .table
            .metadata()
            .snapshot_for_ref(&branch_name)
            .is_none()
        {
            super::branch::create_branch(&self.catalog, &self.table, &branch_name).await?;
            self.table = self.catalog.load_table(self.table.identifier()).await?;
        }

        let wap_id = Uuid::now_v7().to_string();
        match super::branch::commit_to_branch(
            &self.catalog,
            &self.table,
            &branch_name,
            data_files,
            &wap_id,
        )
        .await
        {
            Ok(()) => {}
            Err(err) if super::manifest::is_already_committed(&err) => {
                info!(
                    label = self.label,
                    branch = branch_name,
                    "WAP data files already committed, treating as success"
                );
            }
            Err(err) => return Err(err),
        }

        self.table = self.catalog.load_table(self.table.identifier()).await?;

        if self.auto_publish {
            super::branch::publish_branch(&self.catalog, &self.table, &branch_name).await?;
            self.table = self.catalog.load_table(self.table.identifier()).await?;
        }

        Ok(())
    }

    async fn publish_wap(&mut self) -> Result {
        let Some(ref branch_name) = self.wap_branch else {
            return Err(crate::Error::Branch(
                "cannot publish: WAP not enabled on this sink".into(),
            ));
        };

        super::branch::publish_branch(&self.catalog, &self.table, branch_name).await?;
        self.table = self.catalog.load_table(self.table.identifier()).await?;

        info!(
            label = self.label,
            branch = branch_name,
            "published WAP branch to main"
        );

        Ok(())
    }

    async fn rollback(&mut self) -> Result<IcebergFileManifest> {
        let batch_count = self.staged_batches.len();
        self.staged_batches.clear();
        self.buffer.clear();
        self.row_count = 0;
        self.approximate_size = 0;
        self.created_at = None;

        info!(
            label = self.label,
            batches_discarded = batch_count,
            "rolled back iceberg sink (uncommitted batches discarded)"
        );

        Ok(Vec::new())
    }
}

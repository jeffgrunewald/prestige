use crate::error::Result;
use arrow::array::RecordBatch;
use futures::stream::BoxStream;
use iceberg::table::Table;
use std::sync::Arc;
use std::time::Duration;
use super_visor::{ManagedProc, ShutdownSignal};
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, info, warn};

const DEFAULT_POLL_INTERVAL_SECS: u64 = 30;
const DEFAULT_CHANNEL_SIZE: usize = 10;

/// A stream of new data from an iceberg table snapshot.
pub struct IcebergFileStream {
    pub snapshot_id: i64,
    pub table_name: String,
    pub batches: BoxStream<'static, iceberg::Result<RecordBatch>>,
}

pub type IcebergStreamReceiver = mpsc::Receiver<IcebergFileStream>;

pub struct IcebergPollerConfig {
    table: Table,
    catalog: Arc<dyn iceberg::Catalog>,
    poll_interval: Duration,
    label: String,
}

pub struct IcebergPollerConfigBuilder {
    table: Table,
    catalog: Arc<dyn iceberg::Catalog>,
    poll_interval: Duration,
    channel_size: usize,
    label: String,
    start_after_snapshot: Option<i64>,
}

impl IcebergPollerConfigBuilder {
    pub fn new(table: Table, catalog: Arc<dyn iceberg::Catalog>, label: impl Into<String>) -> Self {
        Self {
            table,
            catalog,
            poll_interval: Duration::from_secs(DEFAULT_POLL_INTERVAL_SECS),
            channel_size: DEFAULT_CHANNEL_SIZE,
            label: label.into(),
            start_after_snapshot: None,
        }
    }

    pub fn poll_interval(self, interval: Duration) -> Self {
        Self {
            poll_interval: interval,
            ..self
        }
    }

    pub fn channel_size(self, size: usize) -> Self {
        Self {
            channel_size: size,
            ..self
        }
    }

    pub fn start_after_snapshot(self, snapshot_id: i64) -> Self {
        Self {
            start_after_snapshot: Some(snapshot_id),
            ..self
        }
    }

    pub fn create(self) -> (IcebergStreamReceiver, IcebergPollerServer) {
        let (tx, rx) = mpsc::channel(self.channel_size);
        let server = IcebergPollerServer {
            config: IcebergPollerConfig {
                table: self.table,
                catalog: self.catalog,
                poll_interval: self.poll_interval,
                label: self.label,
            },
            sender: tx,
            last_snapshot_id: self.start_after_snapshot,
        };
        (rx, server)
    }
}

pub struct IcebergPollerServer {
    config: IcebergPollerConfig,
    sender: mpsc::Sender<IcebergFileStream>,
    last_snapshot_id: Option<i64>,
}

impl ManagedProc for IcebergPollerServer {
    fn run_proc(self: Box<Self>, shutdown: ShutdownSignal) -> super_visor::ManagedFuture {
        super_visor::spawn(self.run(shutdown))
    }
}

impl IcebergPollerServer {
    /// Returns the last successfully processed snapshot ID (for checkpointing).
    pub fn last_snapshot_id(&self) -> Option<i64> {
        self.last_snapshot_id
    }

    pub async fn run(mut self, mut shutdown: ShutdownSignal) -> Result {
        info!(label = self.config.label, "starting iceberg poller");

        let mut poll_timer = time::interval(self.config.poll_interval);
        poll_timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = poll_timer.tick() => {
                    if let Err(err) = self.poll_once().await {
                        warn!(
                            label = self.config.label,
                            ?err,
                            "iceberg poll iteration failed"
                        );
                    }
                }
            }
        }

        info!(label = self.config.label, "stopping iceberg poller");
        Ok(())
    }

    async fn poll_once(&mut self) -> Result {
        // Reload table metadata to see new snapshots
        let table = self
            .config
            .catalog
            .load_table(self.config.table.identifier())
            .await?;
        self.config.table = table;

        let current_snapshot = self.config.table.metadata().current_snapshot();
        let current_id = current_snapshot.map(|s| s.snapshot_id());

        // No snapshot at all (empty table)
        let Some(current_id) = current_id else {
            debug!(label = self.config.label, "no snapshot found");
            return Ok(());
        };

        // Already processed this snapshot
        if self.last_snapshot_id == Some(current_id) {
            debug!(
                label = self.config.label,
                snapshot_id = current_id,
                "no new snapshot"
            );
            return Ok(());
        }

        info!(
            label = self.config.label,
            snapshot_id = current_id,
            previous = ?self.last_snapshot_id,
            "new iceberg snapshot detected"
        );

        // Build scan — if we have a previous snapshot, scan only the data
        // added since that snapshot (incremental). Otherwise do a full scan.
        let stream = if let Some(after_id) = self.last_snapshot_id {
            super::scanner::scan_since_snapshot(&self.config.table, after_id).await?
        } else {
            super::scanner::scan_snapshot(&self.config.table, current_id).await?
        };

        let table_name = self.config.table.identifier().to_string();

        let file_stream = IcebergFileStream {
            snapshot_id: current_id,
            table_name,
            batches: stream,
        };

        match self.sender.send(file_stream).await {
            Ok(()) => {
                self.last_snapshot_id = Some(current_id);
            }
            Err(_) => {
                warn!(
                    label = self.config.label,
                    "iceberg poller channel closed, consumer may be gone"
                );
            }
        }

        Ok(())
    }
}

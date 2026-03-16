use std::{sync::Arc, time::Duration};

use arrow::array::RecordBatch;
use futures::stream::BoxStream;
use iceberg::table::Table;
use super_visor::{ManagedProc, ShutdownSignal};
use tokio::{
    sync::{mpsc, oneshot},
    time,
};
use tracing::{debug, error, info, warn};

use crate::error::Result;

const DEFAULT_POLL_INTERVAL_SECS: u64 = 30;
const DEFAULT_CHANNEL_SIZE: usize = 10;
const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(30);

enum PollOutcome {
    Continue,
    ConsumerGone,
}

/// A stream of new data from an iceberg table snapshot.
///
/// Includes an acknowledgment sender — the consumer must call `ack()` after
/// fully processing the stream. The poller will not advance its checkpoint
/// past this snapshot until acknowledged.
pub struct IcebergFileStream {
    pub snapshot_id: i64,
    pub table_name: String,
    pub batches: BoxStream<'static, iceberg::Result<RecordBatch>>,
    ack_tx: Option<oneshot::Sender<()>>,
}

impl IcebergFileStream {
    /// Acknowledge that this stream has been fully consumed and processed.
    /// The poller advances its checkpoint only after receiving this signal.
    ///
    /// If dropped without calling `ack()`, the poller will re-send this
    /// snapshot's data on the next poll cycle.
    pub fn ack(mut self) {
        if let Some(tx) = self.ack_tx.take() {
            let _ = tx.send(());
        }
    }
}

pub type IcebergStreamReceiver = mpsc::Receiver<IcebergFileStream>;

pub struct IcebergPollerConfig {
    table: Table,
    catalog: Arc<dyn iceberg::Catalog>,
    poll_interval: Duration,
    send_timeout: Duration,
    label: String,
}

pub struct IcebergPollerConfigBuilder {
    table: Table,
    catalog: Arc<dyn iceberg::Catalog>,
    poll_interval: Duration,
    send_timeout: Duration,
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
            send_timeout: DEFAULT_SEND_TIMEOUT,
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

    /// Maximum time to wait when the consumer channel is full before
    /// skipping a snapshot delivery. Defaults to 30 seconds.
    pub fn send_timeout(self, timeout: Duration) -> Self {
        Self {
            send_timeout: timeout,
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
                send_timeout: self.send_timeout,
                label: self.label,
            },
            sender: tx,
            last_snapshot_id: self.start_after_snapshot,
            pending_ack: None,
        };
        (rx, server)
    }
}

pub struct IcebergPollerServer {
    config: IcebergPollerConfig,
    sender: mpsc::Sender<IcebergFileStream>,
    last_snapshot_id: Option<i64>,
    /// Snapshot sent to consumer but not yet acknowledged. Holds the oneshot
    /// receiver that resolves when the consumer calls `IcebergFileStream::ack()`.
    pending_ack: Option<(i64, oneshot::Receiver<()>)>,
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
                    match self.poll_once().await {
                        Ok(PollOutcome::Continue) => {}
                        Ok(PollOutcome::ConsumerGone) => {
                            info!(label = self.config.label, "consumer gone, exiting poller");
                            break;
                        }
                        Err(err) => {
                            warn!(
                                label = self.config.label,
                                ?err,
                                "iceberg poll iteration failed"
                            );
                        }
                    }
                }
            }
        }

        info!(label = self.config.label, "stopping iceberg poller");
        Ok(())
    }

    async fn poll_once(&mut self) -> Result<PollOutcome> {
        // Check if a previously sent snapshot has been acknowledged.
        if let Some((acked_id, ref mut ack_rx)) = self.pending_ack {
            match ack_rx.try_recv() {
                Ok(()) => {
                    debug!(
                        label = self.config.label,
                        snapshot_id = acked_id,
                        "consumer acknowledged snapshot"
                    );
                    self.last_snapshot_id = Some(acked_id);
                    self.pending_ack = None;
                }
                Err(oneshot::error::TryRecvError::Empty) => {
                    // Consumer is still processing — don't send more data.
                    debug!(
                        label = self.config.label,
                        snapshot_id = acked_id,
                        "waiting for consumer to acknowledge previous snapshot"
                    );
                    return Ok(PollOutcome::Continue);
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    // Consumer dropped the stream without acking — don't advance.
                    warn!(
                        label = self.config.label,
                        snapshot_id = acked_id,
                        "consumer dropped stream without acknowledging, will re-send"
                    );
                    self.pending_ack = None;
                }
            }
        }

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
            return Ok(PollOutcome::Continue);
        };

        // Already processed this snapshot
        if self.last_snapshot_id == Some(current_id) {
            debug!(
                label = self.config.label,
                snapshot_id = current_id,
                "no new snapshot"
            );
            return Ok(PollOutcome::Continue);
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
        let (ack_tx, ack_rx) = oneshot::channel();

        let file_stream = IcebergFileStream {
            snapshot_id: current_id,
            table_name,
            batches: stream,
            ack_tx: Some(ack_tx),
        };

        match self
            .sender
            .send_timeout(file_stream, self.config.send_timeout)
            .await
        {
            Ok(()) => {
                self.pending_ack = Some((current_id, ack_rx));
            }
            Err(mpsc::error::SendTimeoutError::Closed(_)) => {
                error!(
                    label = self.config.label,
                    "iceberg poller channel closed, consumer is gone"
                );
                return Ok(PollOutcome::ConsumerGone);
            }
            Err(mpsc::error::SendTimeoutError::Timeout(_)) => {
                warn!(
                    label = self.config.label,
                    timeout_secs = self.config.send_timeout.as_secs(),
                    "iceberg poller backpressure: consumer not draining fast enough, skipping snapshot"
                );
            }
        }

        Ok(PollOutcome::Continue)
    }
}

use crate::error::Result;
use arrow::array::RecordBatch;
use iceberg::table::Table;
use parquet::basic::Compression;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::branch;
use super::catalog::Catalog;

// ---------------------------------------------------------------------------
// WAP state detection
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WapState {
    NotStarted,
    StaleBranch,
    WrittenNotPublished,
    AlreadyPublished,
}

fn detect_wap_state(wap_id_found: bool, branch_exists: bool) -> WapState {
    match (wap_id_found, branch_exists) {
        (false, false) => WapState::NotStarted,
        (false, true) => WapState::StaleBranch,
        (true, true) => WapState::WrittenNotPublished,
        (true, false) => WapState::AlreadyPublished,
    }
}

fn has_wap_id(table: &Table, wap_id: &str) -> Result<bool> {
    let meta = table.metadata();

    let wap_enabled = meta
        .properties()
        .get(branch::WAP_ENABLED_PROPERTY)
        .map(|x| x.as_str());

    if wap_enabled != Some("true") {
        return Err(crate::Error::Branch(format!(
            "WAP not enabled for table {}. Set write.wap.enabled=true in table properties.",
            table.identifier()
        )));
    }

    Ok(meta.snapshots().any(|snapshot| {
        snapshot
            .summary()
            .additional_properties
            .get(branch::WAP_ID_KEY)
            .is_some_and(|v| v == wap_id)
    }))
}

async fn reload_table(catalog: &Catalog, table: &RwLock<Table>) -> Result<()> {
    let identifier = table.read().await.identifier().clone();
    let new_table = catalog.load_table(&identifier).await?;
    *table.write().await = new_table;
    Ok(())
}

// ---------------------------------------------------------------------------
// Branch transaction types
// ---------------------------------------------------------------------------

/// Write-Audit-Publish (WAP) transaction backed by real iceberg branches.
///
/// Lifecycle:
/// 1. **begin** — detects existing state and produces a `WapTransaction`
/// 2. **write** — writes data to an audit branch (not visible on main)
/// 3. **publish** — fast-forwards main to the branch and removes the branch
///
/// The `wap_id` is stored as a snapshot property (`wap.id`), enabling idempotent
/// detection: calling `begin()` with the same `wap_id` resumes from wherever
/// the previous attempt left off.
pub enum WapTransaction {
    /// Data can be written to the audit branch.
    Writer(Box<WapWriterState>),
    /// Data has been committed to the branch and is ready to publish.
    Publisher(Box<WapPublisherState>),
    /// Transaction is complete (already published or no-op).
    Complete,
}

pub struct WapWriterState {
    catalog: Catalog,
    table: Arc<RwLock<Table>>,
    branch_name: String,
    compression: Option<Compression>,
}

pub struct WapPublisherState {
    catalog: Catalog,
    table: Arc<RwLock<Table>>,
    branch_name: String,
}

impl WapTransaction {
    /// Begin a WAP transaction for the given `wap_id`.
    ///
    /// Reloads table metadata to detect the current WAP state:
    /// - `NotStarted` → creates a fresh audit branch, returns `Writer`
    /// - `StaleBranch` → deletes stale branch, creates fresh, returns `Writer`
    /// - `WrittenNotPublished` → returns `Publisher` (data already on branch)
    /// - `AlreadyPublished` → returns `Complete` (nothing to do)
    pub async fn begin(catalog: Catalog, table: Table, wap_id: &str) -> Result<Self> {
        let table = Arc::new(RwLock::new(table));

        reload_table(&catalog, &table).await?;

        let table_guard = table.read().await;
        let wap_id_found = has_wap_id(&table_guard, wap_id)?;
        let branch_exists = table_guard.metadata().snapshot_for_ref(wap_id).is_some();
        drop(table_guard);

        match detect_wap_state(wap_id_found, branch_exists) {
            WapState::NotStarted => {
                debug!(wap_id, "WAP not started, creating fresh branch");
                let table_guard = table.read().await;
                branch::create_branch(&catalog, &table_guard, wap_id).await?;
                drop(table_guard);
                Ok(Self::Writer(Box::new(WapWriterState {
                    catalog,
                    table,
                    branch_name: wap_id.to_string(),
                    compression: None,
                })))
            }
            WapState::StaleBranch => {
                debug!(wap_id, "stale branch detected, deleting and recreating");
                let table_guard = table.read().await;
                branch::delete_branch(&catalog, &table_guard, wap_id).await?;
                drop(table_guard);
                reload_table(&catalog, &table).await?;
                let table_guard = table.read().await;
                branch::create_branch(&catalog, &table_guard, wap_id).await?;
                drop(table_guard);
                Ok(Self::Writer(Box::new(WapWriterState {
                    catalog,
                    table,
                    branch_name: wap_id.to_string(),
                    compression: None,
                })))
            }
            WapState::WrittenNotPublished => {
                debug!(wap_id, "written but not published, returning publisher");
                Ok(Self::Publisher(Box::new(WapPublisherState {
                    catalog,
                    table,
                    branch_name: wap_id.to_string(),
                })))
            }
            WapState::AlreadyPublished => {
                debug!(wap_id, "already published, nothing to do");
                Ok(Self::Complete)
            }
        }
    }

    /// Set compression for writes (only applies to Writer state).
    pub fn with_compression(mut self, compression: Compression) -> Self {
        if let Self::Writer(ref mut state) = self {
            state.compression = Some(compression);
        }
        self
    }

    /// Write record batches to the audit branch.
    ///
    /// Transitions from `Writer` → `Publisher`. Data is committed to the
    /// branch but not yet visible on main.
    pub async fn write(&mut self, batches: Vec<RecordBatch>) -> Result<usize> {
        let prev = std::mem::replace(self, Self::Complete);
        match prev {
            Self::Writer(state) => {
                if batches.is_empty() {
                    info!("WAP write: no batches, transitioning to publisher (no-op publish)");
                    *self = Self::Publisher(Box::new(WapPublisherState {
                        catalog: state.catalog,
                        table: state.table,
                        branch_name: state.branch_name,
                    }));
                    return Ok(0);
                }

                reload_table(&state.catalog, &state.table).await?;
                let table_guard = state.table.read().await;
                let data_files =
                    super::writer::write_data_files(&table_guard, batches, state.compression)
                        .await?;
                let file_count = data_files.len();
                branch::commit_to_branch(
                    &state.catalog,
                    &table_guard,
                    &state.branch_name,
                    data_files,
                    &state.branch_name,
                )
                .await?;
                drop(table_guard);

                info!(
                    files = file_count,
                    branch = state.branch_name,
                    "WAP data committed to branch"
                );

                *self = Self::Publisher(Box::new(WapPublisherState {
                    catalog: state.catalog,
                    table: state.table,
                    branch_name: state.branch_name,
                }));
                Ok(file_count)
            }
            Self::Publisher(state) => {
                warn!("WAP write called after data already committed to branch");
                *self = Self::Publisher(state);
                Err(crate::Error::Branch(
                    "cannot write: data already committed to branch".into(),
                ))
            }
            Self::Complete => Err(crate::Error::Branch(
                "cannot write: transaction is complete".into(),
            )),
        }
    }

    /// Publish the audit branch to main.
    ///
    /// Fast-forwards main to the branch's snapshot and removes the branch ref.
    /// Consumes the transaction.
    pub async fn publish(self) -> Result<()> {
        match self {
            Self::Writer(_) => Err(crate::Error::Branch(
                "cannot publish: write data first".into(),
            )),
            Self::Publisher(state) => {
                reload_table(&state.catalog, &state.table).await?;
                let table_guard = state.table.read().await;
                branch::publish_branch(&state.catalog, &table_guard, &state.branch_name).await?;
                info!(branch = state.branch_name, "WAP branch published to main");
                Ok(())
            }
            Self::Complete => {
                debug!("WAP publish called on already-complete transaction (no-op)");
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_wap_state_not_started() {
        assert_eq!(detect_wap_state(false, false), WapState::NotStarted);
    }

    #[test]
    fn test_detect_wap_state_stale_branch() {
        assert_eq!(detect_wap_state(false, true), WapState::StaleBranch);
    }

    #[test]
    fn test_detect_wap_state_written_not_published() {
        assert_eq!(detect_wap_state(true, true), WapState::WrittenNotPublished);
    }

    #[test]
    fn test_detect_wap_state_already_published() {
        assert_eq!(detect_wap_state(true, false), WapState::AlreadyPublished);
    }
}

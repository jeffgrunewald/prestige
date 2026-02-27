use crate::error::Result;
use arrow::array::RecordBatch;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::Catalog;
use parquet::basic::Compression;
use std::collections::HashMap;
use tracing::{info, warn};

/// Write-Audit-Publish (WAP) transaction for iceberg tables.
///
/// Manages a lifecycle of:
/// 1. **Write** — accumulate data files on the table (uncommitted)
/// 2. **Publish** — commit all accumulated files atomically via fast_append
///
/// This provides a two-phase approach where data can be validated
/// before being made visible to readers.
pub struct WapTransaction {
    state: WapState,
}

enum WapState {
    Writing(Box<WritingState>),
    Published,
    Cancelled,
}

struct WritingState {
    table: Table,
    pending_files: Vec<DataFile>,
    compression: Option<Compression>,
    snapshot_properties: HashMap<String, String>,
}

impl WapTransaction {
    pub fn new(table: Table) -> Self {
        Self {
            state: WapState::Writing(Box::new(WritingState {
                table,
                pending_files: Vec::new(),
                compression: None,
                snapshot_properties: HashMap::new(),
            })),
        }
    }

    pub fn with_compression(mut self, compression: Compression) -> Self {
        if let WapState::Writing(ref mut state) = self.state {
            state.compression = Some(compression);
        }
        self
    }

    pub fn with_snapshot_properties(mut self, properties: HashMap<String, String>) -> Self {
        if let WapState::Writing(ref mut state) = self.state {
            state.snapshot_properties = properties;
        }
        self
    }

    /// Write record batches as data files. Files are staged but not yet committed.
    pub async fn write(&mut self, batches: Vec<RecordBatch>) -> Result<usize> {
        let state = match &mut self.state {
            WapState::Writing(s) => s,
            WapState::Published => {
                return Err(crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "cannot write to a published WAP transaction",
                )));
            }
            WapState::Cancelled => {
                return Err(crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "cannot write to a cancelled WAP transaction",
                )));
            }
        };

        let data_files = super::writer::write_data_files(
            &state.table,
            batches,
            state.compression,
        )
        .await?;

        let count = data_files.len();
        state.pending_files.extend(data_files);
        Ok(count)
    }

    /// Returns the number of pending (uncommitted) data files.
    pub fn pending_file_count(&self) -> usize {
        match &self.state {
            WapState::Writing(s) => s.pending_files.len(),
            _ => 0,
        }
    }

    /// Publish all pending data files atomically via fast_append.
    /// Consumes the transaction and returns the updated table.
    pub async fn publish(mut self, catalog: &dyn Catalog) -> Result<Table> {
        let state = std::mem::replace(&mut self.state, WapState::Published);
        let writing = match state {
            WapState::Writing(s) => s,
            WapState::Published => {
                return Err(crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "WAP transaction already published",
                )));
            }
            WapState::Cancelled => {
                return Err(crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "WAP transaction was cancelled",
                )));
            }
        };

        if writing.pending_files.is_empty() {
            info!("WAP publish: no pending files to commit");
            return Ok(writing.table);
        }

        let file_count = writing.pending_files.len();
        let snapshot_props = if writing.snapshot_properties.is_empty() {
            None
        } else {
            Some(writing.snapshot_properties)
        };

        let table = super::writer::commit_data_files(
            &writing.table,
            catalog,
            writing.pending_files,
            snapshot_props,
        )
        .await?;

        info!(files = file_count, "WAP transaction published");
        Ok(table)
    }

    /// Cancel the transaction. Pending data files in storage will remain
    /// but will not be referenced by any snapshot (orphan cleanup handles them).
    pub fn cancel(mut self) {
        let old_state = std::mem::replace(&mut self.state, WapState::Cancelled);
        if let WapState::Writing(state) = old_state
            && !state.pending_files.is_empty()
        {
            warn!(
                files = state.pending_files.len(),
                "WAP transaction cancelled with pending files (orphan cleanup will handle them)"
            );
        }
    }
}

use std::collections::HashSet;

use arrow::array::RecordBatch;
use futures::{TryStreamExt, stream::BoxStream};
use iceberg::{
    Runtime,
    arrow::ArrowReaderBuilder,
    expr::Predicate,
    spec::{ManifestStatus, Operation},
    table::Table,
};
use tracing::debug;

use crate::error::Result;

/// A stream of arrow RecordBatches from an iceberg table scan.
pub type IcebergRecordBatchStream = BoxStream<'static, iceberg::Result<RecordBatch>>;

/// Scan an entire iceberg table, returning all records as a RecordBatch stream.
pub async fn scan_table(table: &Table) -> Result<IcebergRecordBatchStream> {
    let scan = table.scan().build()?;
    let stream = scan.to_arrow().await?;
    Ok(stream)
}

/// Scan an iceberg table at a specific snapshot.
pub async fn scan_snapshot(table: &Table, snapshot_id: i64) -> Result<IcebergRecordBatchStream> {
    let scan = table.scan().snapshot_id(snapshot_id).build()?;
    let stream = scan.to_arrow().await?;
    Ok(stream)
}

/// Scan an iceberg table with column projection.
pub async fn scan_columns(table: &Table, columns: Vec<&str>) -> Result<IcebergRecordBatchStream> {
    let scan = table.scan().select(columns).build()?;
    let stream = scan.to_arrow().await?;
    Ok(stream)
}

/// Scan only the data files added after a given snapshot, up to the current
/// snapshot.
///
/// Equivalent to `scan_snapshot_range(table, after_snapshot_id, None)`.
/// See [`scan_snapshot_range`] for details.
pub async fn scan_since_snapshot(
    table: &Table,
    after_snapshot_id: i64,
) -> Result<IcebergRecordBatchStream> {
    scan_snapshot_range(table, after_snapshot_id, None).await
}

/// Scan data files added in a window between two snapshots.
///
/// Returns all data added **after** `after_snapshot_id` up to and including
/// `to_snapshot_id`. When `to_snapshot_id` is `None`, the current snapshot
/// is used as the upper bound.
///
/// Walks the upper-bound snapshot's manifest list, filters to manifest
/// entries whose sequence number falls within the window, and reads only
/// those files. Compaction (`Replace`) snapshots within the window are
/// excluded to avoid double-processing rewritten data.
///
/// For append-only workloads this returns exactly the new records in the
/// window. For tables with compaction it returns the compacted output files
/// (not the original un-compacted files).
pub async fn scan_snapshot_range(
    table: &Table,
    after_snapshot_id: i64,
    to_snapshot_id: Option<i64>,
) -> Result<IcebergRecordBatchStream> {
    let after_snapshot = table
        .metadata()
        .snapshot_by_id(after_snapshot_id)
        .ok_or_else(|| {
            iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!("checkpoint snapshot {after_snapshot_id} not found in table metadata"),
            )
        })?;
    let after_seq_num = after_snapshot.sequence_number();

    // Resolve the upper-bound snapshot.
    let to_snapshot = match to_snapshot_id {
        Some(id) => table.metadata().snapshot_by_id(id).ok_or_else(|| {
            iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!("upper-bound snapshot {id} not found in table metadata"),
            )
        })?,
        None => table.metadata().current_snapshot().ok_or_else(|| {
            iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "table has no current snapshot",
            )
        })?,
    };

    let to_id = to_snapshot.snapshot_id();
    let to_seq_num = to_snapshot.sequence_number();

    if to_seq_num <= after_seq_num {
        debug!(
            after_snapshot_id,
            after_seq_num,
            to_snapshot_id = to_id,
            to_seq_num,
            "upper-bound snapshot is not ahead of lower-bound, returning empty stream"
        );
        return Ok(Box::pin(futures::stream::empty()));
    }

    debug!(
        after_snapshot_id,
        after_seq_num,
        to_snapshot_id = to_id,
        to_seq_num,
        "scanning incremental data in snapshot range"
    );

    // Build set of sequence numbers for Replace (compaction) snapshots between
    // the two bounds. Manifests created by these snapshots contain rewritten
    // data files that would cause double-processing.
    let mut replace_seq_nums: HashSet<i64> = HashSet::new();
    {
        let mut snap_id = Some(to_id);
        while let Some(id) = snap_id {
            if id == after_snapshot_id {
                break;
            }
            if let Some(snapshot) = table.metadata().snapshot_by_id(id) {
                if snapshot.summary().operation == Operation::Replace {
                    replace_seq_nums.insert(snapshot.sequence_number());
                }
                snap_id = snapshot.parent_snapshot_id();
            } else {
                break;
            }
        }
    }

    // Walk the upper-bound snapshot's manifest list and collect paths of
    // files added within the window.
    let manifest_list = table.manifest_list_reader(to_snapshot).load().await?;

    let mut new_file_paths: HashSet<String> = HashSet::new();

    for manifest_file in manifest_list.entries() {
        // Skip manifests at or before the lower bound.
        if manifest_file.sequence_number <= after_seq_num {
            continue;
        }

        // Skip manifests beyond the upper bound.
        if manifest_file.sequence_number > to_seq_num {
            continue;
        }

        // Skip manifests created by compaction (Replace) snapshots.
        if replace_seq_nums.contains(&manifest_file.sequence_number) {
            debug!(
                seq = manifest_file.sequence_number,
                "skipping compaction manifest"
            );
            continue;
        }

        let manifest = manifest_file.load_manifest(table.file_io()).await?;

        for entry in manifest.entries() {
            if entry.status() != ManifestStatus::Added {
                continue;
            }

            // Double-check entry-level sequence number falls within the window.
            if let Some(entry_seq) = entry.sequence_number()
                && (entry_seq <= after_seq_num || entry_seq > to_seq_num)
            {
                continue;
            }

            new_file_paths.insert(entry.file_path().to_string());
        }
    }

    debug!(
        new_files = new_file_paths.len(),
        "identified data files in snapshot range"
    );

    if new_file_paths.is_empty() {
        return Ok(Box::pin(futures::stream::empty()));
    }

    // Scope the file plan to the upper-bound snapshot so we only see files
    // that are alive at that point — not files added after the window.
    let scan = table.scan().snapshot_id(to_id).build()?;
    let file_tasks = scan.plan_files().await?;
    let filtered_tasks = file_tasks.try_filter(move |task| {
        futures::future::ready(new_file_paths.contains(&task.data_file_path))
    });

    let reader = ArrowReaderBuilder::new(table.file_io().clone(), Runtime::try_current()?).build();
    let stream = reader.read(Box::pin(filtered_tasks))?.stream();
    Ok(stream)
}

/// Return the snapshot ID of the earliest snapshot in the table (lowest
/// sequence number). Returns `None` if the table has no snapshots.
///
/// Useful as the lower bound for `scan_snapshot_range` when replaying all
/// data from the beginning of the table's history.
pub fn earliest_snapshot(table: &Table) -> Option<i64> {
    table
        .metadata()
        .snapshots()
        .min_by_key(|s| s.sequence_number())
        .map(|s| s.snapshot_id())
}

/// Find the snapshot whose timestamp is the latest at or before `timestamp_ms`
/// (epoch milliseconds).
///
/// Walks all snapshots in table metadata and returns the one with the largest
/// `timestamp_ms` that does not exceed the given bound. Returns `None` if no
/// snapshot satisfies the constraint (e.g. all snapshots are newer than `T`,
/// or the table has no snapshots).
pub fn snapshot_at_timestamp(table: &Table, timestamp_ms: i64) -> Option<i64> {
    table
        .metadata()
        .snapshots()
        .filter(|s| s.timestamp_ms() <= timestamp_ms)
        .max_by_key(|s| s.timestamp_ms())
        .map(|s| s.snapshot_id())
}

/// Scan the full table state as of a point in time.
///
/// Resolves the latest snapshot at or before `timestamp_ms` (epoch
/// milliseconds), then reads all data visible at that snapshot. This is
/// iceberg's "time travel" — the table appears exactly as it did at time `T`.
///
/// Returns an error if no snapshot exists at or before the given timestamp.
pub async fn scan_at_timestamp(
    table: &Table,
    timestamp_ms: i64,
) -> Result<IcebergRecordBatchStream> {
    let snapshot_id = snapshot_at_timestamp(table, timestamp_ms).ok_or_else(|| {
        iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            format!("no snapshot found at or before timestamp {timestamp_ms}"),
        )
    })?;

    debug!(
        snapshot_id,
        timestamp_ms, "time-travel scan resolved to snapshot"
    );

    scan_snapshot(table, snapshot_id).await
}

/// Scan a table with a row filter expression for predicate pushdown.
///
/// The filter is pushed down to the iceberg scan builder, enabling
/// partition pruning and row-group filtering at the parquet level.
/// This is the programmatic Rust API for filtered queries — external
/// query engines handle their own predicate pushdown via the catalog.
pub async fn scan_with_filter(
    table: &Table,
    filter: Predicate,
) -> Result<IcebergRecordBatchStream> {
    let scan = table.scan().with_filter(filter).build()?;
    let stream = scan.to_arrow().await?;
    Ok(stream)
}

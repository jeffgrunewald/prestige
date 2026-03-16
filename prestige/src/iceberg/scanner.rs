use crate::error::Result;
use arrow::array::RecordBatch;
use futures::{TryStreamExt, stream::BoxStream};
use iceberg::{
    arrow::ArrowReaderBuilder,
    expr::Predicate,
    spec::{ManifestStatus, Operation},
    table::Table,
};
use std::collections::HashSet;
use tracing::debug;

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

/// Scan only the data files added after a given snapshot.
///
/// Walks the current snapshot's manifest list, filters to manifest entries
/// whose sequence number exceeds the checkpoint snapshot's sequence number,
/// and reads only those newly added files. This enables efficient incremental
/// streaming — on each poll, only new data since the last checkpoint is read.
///
/// For append-only workloads (typical streaming ingestion), this correctly
/// returns exactly the new records. For tables with compaction, it returns
/// the compacted output files (not the original un-compacted files).
pub async fn scan_since_snapshot(
    table: &Table,
    after_snapshot_id: i64,
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

    let current_snapshot = table.metadata().current_snapshot().ok_or_else(|| {
        iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            "table has no current snapshot",
        )
    })?;

    debug!(
        after_snapshot_id,
        after_seq_num,
        current_snapshot_id = current_snapshot.snapshot_id(),
        current_seq_num = current_snapshot.sequence_number(),
        "scanning incremental data since checkpoint"
    );

    // Build set of sequence numbers for Replace (compaction) snapshots between
    // the checkpoint and the current snapshot. Manifests created by these
    // snapshots contain rewritten data files that would cause double-processing.
    let mut replace_seq_nums: HashSet<i64> = HashSet::new();
    {
        let mut snap_id = Some(current_snapshot.snapshot_id());
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

    // Walk the current snapshot's manifest list and collect paths of newly added files.
    let manifest_list = current_snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await?;

    let mut new_file_paths: HashSet<String> = HashSet::new();

    for manifest_file in manifest_list.entries() {
        // Skip manifests that existed at or before the checkpoint.
        if manifest_file.sequence_number <= after_seq_num {
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

            // Double-check entry-level sequence number when available.
            if let Some(entry_seq) = entry.sequence_number()
                && entry_seq <= after_seq_num
            {
                continue;
            }

            new_file_paths.insert(entry.file_path().to_string());
        }
    }

    debug!(
        new_files = new_file_paths.len(),
        "identified new data files since checkpoint"
    );

    if new_file_paths.is_empty() {
        return Ok(Box::pin(futures::stream::empty()));
    }

    // Build a regular scan on the current snapshot, get file tasks,
    // filter to only the new file paths, then read with ArrowReader.
    let scan = table.scan().build()?;
    let file_tasks = scan.plan_files().await?;
    let filtered_tasks = file_tasks.try_filter(move |task| {
        futures::future::ready(new_file_paths.contains(&task.data_file_path))
    });

    let reader = ArrowReaderBuilder::new(table.file_io().clone()).build();
    let stream = reader.read(Box::pin(filtered_tasks))?;
    Ok(stream)
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

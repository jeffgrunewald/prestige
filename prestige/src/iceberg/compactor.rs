use crate::error::Result;
use arrow::array::RecordBatch;
use arrow_row::{RowConverter, SortField};
use arrow_select::filter::filter_record_batch;
use derive_builder::Builder;
use futures::TryStreamExt;
use iceberg::table::Table;
use iceberg::Catalog;
use parquet::basic::Compression;
use std::collections::HashSet;
use std::pin::pin;
use std::sync::Arc;
use tracing::info;

const DEFAULT_TARGET_FILE_SIZE_BYTES: usize = 100 * 1024 * 1024; // 100 MB
const DEFAULT_MIN_FILES_TO_COMPACT: usize = 5;

#[derive(Builder)]
#[builder(setter(into))]
pub struct IcebergCompactorConfig {
    table: Table,
    catalog: Arc<dyn Catalog>,
    #[builder(default = "DEFAULT_TARGET_FILE_SIZE_BYTES")]
    target_file_size_bytes: usize,
    #[builder(default = "DEFAULT_MIN_FILES_TO_COMPACT")]
    min_files_to_compact: usize,
    #[builder(default = "false")]
    deduplicate: bool,
    #[builder(default = "Compression::SNAPPY")]
    compression: Compression,
}

pub struct IcebergCompactionResult {
    pub files_read: usize,
    pub files_written: usize,
    pub records_consolidated: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub duplicates_eliminated: usize,
}

impl IcebergCompactorConfig {
    /// Execute compaction: scan the table, optionally deduplicate, write compacted
    /// files, and commit via fast_append.
    ///
    /// Note: iceberg-rust 0.8 only supports fast_append (no rewrite transaction).
    /// Compacted files are appended as a new snapshot. Old snapshots containing
    /// the original small files should be expired via table maintenance to reclaim
    /// storage.
    pub async fn execute(self) -> Result<IcebergCompactionResult> {
        let scan = self.table.scan().build()?;

        // Count data files via plan_files to decide if compaction is warranted
        let file_tasks: Vec<_> = {
            let stream = scan.plan_files().await?;
            let mut pinned = pin!(stream);
            let mut tasks = Vec::new();
            while let Some(task) = pinned.try_next().await? {
                tasks.push(task);
            }
            tasks
        };

        let files_read = file_tasks.len();
        let bytes_before: u64 = file_tasks.iter().map(|t| t.length).sum();

        if files_read < self.min_files_to_compact {
            info!(
                files = files_read,
                min = self.min_files_to_compact,
                "skipping compaction: not enough files"
            );
            return Ok(IcebergCompactionResult {
                files_read,
                files_written: 0,
                records_consolidated: 0,
                bytes_before,
                bytes_after: bytes_before,
                duplicates_eliminated: 0,
            });
        }

        // Read all data from the table
        let scan = self.table.scan().build()?;
        let stream = scan.to_arrow().await?;
        let mut pinned = pin!(stream);

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_records: usize = 0;
        let mut duplicates_eliminated: usize = 0;

        if self.deduplicate {
            let mut dedup = DeduplicatingAccumulator::new();
            while let Some(batch) = pinned.try_next().await? {
                total_records += batch.num_rows();
                let filtered = dedup.add_batch(&batch)?;
                if filtered.num_rows() > 0 {
                    batches.push(filtered);
                }
            }
            duplicates_eliminated = dedup.duplicates_eliminated;
        } else {
            while let Some(batch) = pinned.try_next().await? {
                total_records += batch.num_rows();
                batches.push(batch);
            }
        }

        if batches.is_empty() {
            return Ok(IcebergCompactionResult {
                files_read,
                files_written: 0,
                records_consolidated: 0,
                bytes_before,
                bytes_after: bytes_before,
                duplicates_eliminated,
            });
        }

        let data_files = super::writer::write_data_files_with_target_size(
            &self.table,
            batches,
            Some(self.compression),
            Some(self.target_file_size_bytes),
        )
        .await?;

        let files_written = data_files.len();
        let bytes_after: u64 = data_files.iter().map(|f| f.file_size_in_bytes()).sum();
        let records_consolidated = total_records - duplicates_eliminated;

        let mut snapshot_props = std::collections::HashMap::new();
        snapshot_props.insert("prestige.operation".to_string(), "compaction".to_string());

        super::writer::commit_data_files(
            &self.table,
            self.catalog.as_ref(),
            data_files,
            Some(snapshot_props),
        )
        .await?;

        info!(
            files_read,
            files_written,
            records_consolidated,
            duplicates_eliminated,
            "iceberg compaction complete"
        );

        Ok(IcebergCompactionResult {
            files_read,
            files_written,
            records_consolidated,
            bytes_before,
            bytes_after,
            duplicates_eliminated,
        })
    }
}

struct DeduplicatingAccumulator {
    seen_hashes: HashSet<u128>,
    duplicates_eliminated: usize,
}

impl DeduplicatingAccumulator {
    fn new() -> Self {
        Self {
            seen_hashes: HashSet::new(),
            duplicates_eliminated: 0,
        }
    }

    fn add_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let sort_fields: Vec<SortField> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect();

        let converter = RowConverter::new(sort_fields)?;
        let rows = converter.convert_columns(batch.columns())?;

        let mut keep = vec![true; batch.num_rows()];
        let mut dups_in_batch = 0usize;

        for (idx, flag) in keep.iter_mut().enumerate() {
            let hash = xxhash_rust::xxh3::xxh3_128(rows.row(idx).as_ref());
            if !self.seen_hashes.insert(hash) {
                *flag = false;
                dups_in_batch += 1;
            }
        }

        self.duplicates_eliminated += dups_in_batch;

        if dups_in_batch == 0 {
            return Ok(batch.clone());
        }

        let filter_array = arrow::array::BooleanArray::from(keep);
        let filtered = filter_record_batch(batch, &filter_array)?;
        Ok(filtered)
    }
}

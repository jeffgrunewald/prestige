use crate::error::Result;
use arrow::array::RecordBatch;
use arrow_row::{RowConverter, SortField};
use arrow_select::filter::filter_record_batch;
use derive_builder::Builder;
use futures::TryStreamExt;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::spec::{
    DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestListWriter,
    ManifestWriterBuilder, Operation, Snapshot, SnapshotReference, SnapshotRetention,
    SnapshotSummaryCollector, Struct, Summary,
};
use iceberg::table::Table;
use iceberg::{TableRequirement, TableUpdate};
use iceberg_catalog_rest::CommitTableRequest;
use parquet::basic::Compression;
use std::collections::{HashMap, HashSet};
use std::pin::pin;
use tracing::info;
use uuid::Uuid;

use super::catalog::Catalog;

const DEFAULT_TARGET_FILE_SIZE_BYTES: usize = 100 * 1024 * 1024; // 100 MB
const DEFAULT_MIN_FILES_TO_COMPACT: usize = 5;

#[derive(Builder)]
#[builder(setter(into))]
pub struct IcebergCompactorConfig {
    table: Table,
    catalog: Catalog,
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
    pub partitions_compacted: usize,
}

/// Collected metadata for an existing data file that will be deleted during compaction.
struct OldFileEntry {
    data_file: DataFile,
    sequence_number: i64,
    file_sequence_number: Option<i64>,
}

impl IcebergCompactorConfig {
    /// Execute compaction: scan the table, optionally deduplicate, write compacted
    /// files, and commit as an atomic rewrite (Operation::Replace).
    ///
    /// The commit marks old data files as DELETED and adds new compacted files as
    /// ADDED in a single snapshot, so readers never see duplicated data.
    pub async fn execute(self) -> Result<IcebergCompactionResult> {
        let metadata = self.table.metadata();
        let current_snapshot = match metadata.current_snapshot() {
            Some(snap) => snap,
            None => {
                return Ok(IcebergCompactionResult {
                    files_read: 0,
                    files_written: 0,
                    records_consolidated: 0,
                    bytes_before: 0,
                    bytes_after: 0,
                    duplicates_eliminated: 0,
                    partitions_compacted: 0,
                });
            }
        };

        // Collect all alive manifest entries from the current snapshot.
        // We need the full ManifestEntry (with sequence numbers) so we can
        // mark them as DELETED in the rewrite commit.
        let manifest_list = current_snapshot
            .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
            .await?;

        let mut old_entries: Vec<OldFileEntry> = Vec::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(self.table.file_io()).await?;
            for entry in manifest.entries() {
                if entry.is_alive() {
                    old_entries.push(OldFileEntry {
                        data_file: entry.data_file.clone(),
                        sequence_number: entry.sequence_number.unwrap_or(0),
                        file_sequence_number: entry.file_sequence_number,
                    });
                }
            }
        }

        // Group entries by partition value so each group is compacted independently.
        // For unpartitioned tables, all files share the same empty partition struct.
        let mut partition_groups: HashMap<Struct, Vec<OldFileEntry>> = HashMap::new();
        for entry in old_entries {
            partition_groups
                .entry(entry.data_file.partition().clone())
                .or_default()
                .push(entry);
        }

        let mut compact_entries: Vec<OldFileEntry> = Vec::new();
        let mut partitions_compacted: usize = 0;

        for (_, group) in partition_groups {
            if group.len() >= self.min_files_to_compact {
                partitions_compacted += 1;
                compact_entries.extend(group);
            }
        }

        let files_read = compact_entries.len();
        let bytes_before: u64 = compact_entries
            .iter()
            .map(|e| e.data_file.file_size_in_bytes())
            .sum();

        if compact_entries.is_empty() {
            info!(
                min = self.min_files_to_compact,
                "skipping compaction: no partition group has enough files"
            );
            return Ok(IcebergCompactionResult {
                files_read: 0,
                files_written: 0,
                records_consolidated: 0,
                bytes_before: 0,
                bytes_after: 0,
                duplicates_eliminated: 0,
                partitions_compacted: 0,
            });
        }

        // Build set of qualifying file paths for filtered scanning.
        let compact_paths: HashSet<String> = compact_entries
            .iter()
            .map(|e| e.data_file.file_path().to_string())
            .collect();

        // Read only the qualifying data files (not the entire table).
        let scan = self.table.scan().build()?;
        let file_tasks = scan.plan_files().await?;
        let filtered_tasks = file_tasks.try_filter(move |task| {
            futures::future::ready(compact_paths.contains(&task.data_file_path))
        });

        let reader = ArrowReaderBuilder::new(self.table.file_io().clone()).build();
        let stream = reader.read(Box::pin(filtered_tasks))?;
        let mut pinned = pin!(stream);

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut total_records: usize = 0;
        let mut duplicates_eliminated: usize = 0;

        if self.deduplicate {
            let id_columns = resolve_identifier_column_indices(&self.table);
            let mut dedup = DeduplicatingAccumulator::from_identifier_columns(id_columns);
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
                partitions_compacted,
            });
        }

        // Write new compacted data files (FanoutWriter handles partition routing).
        let new_data_files = super::writer::write_data_files_with_target_size(
            &self.table,
            batches,
            Some(self.compression),
            Some(self.target_file_size_bytes),
        )
        .await?;

        let files_written = new_data_files.len();
        let bytes_after: u64 = new_data_files.iter().map(|f| f.file_size_in_bytes()).sum();
        let records_consolidated = total_records - duplicates_eliminated;

        // Commit as an atomic rewrite: delete old files + add new files.
        // Only the qualifying partition groups are affected.
        self.commit_rewrite(
            current_snapshot.snapshot_id(),
            &compact_entries,
            new_data_files,
        )
        .await?;

        info!(
            files_read,
            files_written,
            records_consolidated,
            duplicates_eliminated,
            partitions_compacted,
            "iceberg compaction complete"
        );

        Ok(IcebergCompactionResult {
            files_read,
            files_written,
            records_consolidated,
            bytes_before,
            bytes_after,
            duplicates_eliminated,
            partitions_compacted,
        })
    }

    /// Build and commit a rewrite snapshot that atomically deletes old files
    /// and adds new compacted files.
    async fn commit_rewrite(
        &self,
        parent_snapshot_id: i64,
        old_entries: &[OldFileEntry],
        new_data_files: Vec<DataFile>,
    ) -> Result<()> {
        let metadata = self.table.metadata();
        let schema = metadata.current_schema().clone();
        let partition_spec = metadata.default_partition_spec();
        let next_seq_num = metadata.next_sequence_number();
        let commit_uuid = Uuid::now_v7();
        let snapshot_id = generate_unique_snapshot_id(&self.table);

        // Build summary — track both added and removed files
        let mut summary_collector = SnapshotSummaryCollector::default();
        for new_file in &new_data_files {
            summary_collector.add_file(new_file, schema.clone(), partition_spec.clone());
        }
        let mut additional_properties = summary_collector.build();
        additional_properties.insert("prestige.operation".to_string(), "compaction".to_string());
        let summary = Summary {
            operation: Operation::Replace,
            additional_properties,
        };

        // Write manifest for DELETED (old) files
        let delete_manifest_path = format!(
            "{}/metadata/{}-m-delete.{}",
            metadata.location(),
            commit_uuid,
            DataFileFormat::Avro
        );
        let delete_output = self.table.file_io().new_output(&delete_manifest_path)?;
        let delete_builder = ManifestWriterBuilder::new(
            delete_output,
            Some(snapshot_id),
            None,
            schema.clone(),
            partition_spec.as_ref().clone(),
        );
        let mut delete_writer = match metadata.format_version() {
            FormatVersion::V1 => delete_builder.build_v1(),
            FormatVersion::V2 => delete_builder.build_v2_data(),
            FormatVersion::V3 => delete_builder.build_v3_data(),
        };

        for entry in old_entries {
            delete_writer.add_delete_file(
                entry.data_file.clone(),
                entry.sequence_number,
                entry.file_sequence_number,
            )?;
        }
        let delete_manifest = delete_writer.write_manifest_file().await?;

        // Write manifest for ADDED (new compacted) files
        let add_manifest_path = format!(
            "{}/metadata/{}-m-add.{}",
            metadata.location(),
            commit_uuid,
            DataFileFormat::Avro
        );
        let add_output = self.table.file_io().new_output(&add_manifest_path)?;
        let add_builder = ManifestWriterBuilder::new(
            add_output,
            Some(snapshot_id),
            None,
            schema.clone(),
            partition_spec.as_ref().clone(),
        );
        let mut add_writer = match metadata.format_version() {
            FormatVersion::V1 => add_builder.build_v1(),
            FormatVersion::V2 => add_builder.build_v2_data(),
            FormatVersion::V3 => add_builder.build_v3_data(),
        };

        for data_file in new_data_files {
            add_writer.add_file(data_file, next_seq_num)?;
        }
        let add_manifest = add_writer.write_manifest_file().await?;

        // Write manifest list containing both manifests
        let manifest_list_path = format!(
            "{}/metadata/snap-{}-0-{}.{}",
            metadata.location(),
            snapshot_id,
            commit_uuid,
            DataFileFormat::Avro
        );
        let manifest_list_output = self.table.file_io().new_output(&manifest_list_path)?;
        let mut manifest_list_writer = match metadata.format_version() {
            FormatVersion::V1 => {
                ManifestListWriter::v1(manifest_list_output, snapshot_id, Some(parent_snapshot_id))
            }
            FormatVersion::V2 => ManifestListWriter::v2(
                manifest_list_output,
                snapshot_id,
                Some(parent_snapshot_id),
                next_seq_num,
            ),
            FormatVersion::V3 => ManifestListWriter::v3(
                manifest_list_output,
                snapshot_id,
                Some(parent_snapshot_id),
                next_seq_num,
                None,
            ),
        };
        manifest_list_writer.add_manifests([delete_manifest, add_manifest].into_iter())?;
        manifest_list_writer.close().await?;

        // Build snapshot
        let commit_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .map_err(|e| crate::Error::Branch(format!("failed to get system time: {e}")))?;

        let new_snapshot = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(Some(parent_snapshot_id))
            .with_sequence_number(next_seq_num)
            .with_timestamp_ms(commit_ts)
            .with_manifest_list(manifest_list_path)
            .with_summary(summary)
            .with_schema_id(metadata.current_schema_id())
            .build();

        // Commit via REST: add snapshot + advance main ref
        let updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ];

        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: Some(parent_snapshot_id),
            },
        ];

        let request = CommitTableRequest {
            identifier: Some(self.table.identifier().clone()),
            requirements,
            updates,
        };

        self.catalog.commit_table_request(&request).await
    }
}

fn generate_unique_snapshot_id(table: &Table) -> i64 {
    let generate_random_id = || -> i64 {
        let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
        let snapshot_id = (lhs ^ rhs) as i64;
        snapshot_id.abs()
    };

    let mut snapshot_id = generate_random_id();
    while table
        .metadata()
        .snapshots()
        .any(|s| s.snapshot_id() == snapshot_id)
    {
        snapshot_id = generate_random_id();
    }
    snapshot_id
}

/// Dedup strategy used by the compactor.
enum DeduplicationKey {
    /// Hash only the identifier columns (efficient, semantically correct for upsert-style dedup).
    IdentifierColumns(Vec<usize>),
    /// Hash all columns (fallback when no identifier fields are declared on the schema).
    AllColumns,
}

struct DeduplicatingAccumulator {
    seen_hashes: HashSet<u128>,
    duplicates_eliminated: usize,
    key: DeduplicationKey,
}

impl DeduplicatingAccumulator {
    fn from_identifier_columns(column_indices: Vec<usize>) -> Self {
        let key = if column_indices.is_empty() {
            DeduplicationKey::AllColumns
        } else {
            DeduplicationKey::IdentifierColumns(column_indices)
        };
        Self {
            seen_hashes: HashSet::new(),
            duplicates_eliminated: 0,
            key,
        }
    }

    fn add_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let (sort_fields, columns): (Vec<SortField>, Vec<arrow::array::ArrayRef>) = match &self.key
        {
            DeduplicationKey::IdentifierColumns(indices) => indices
                .iter()
                .map(|&i| {
                    (
                        SortField::new(schema.field(i).data_type().clone()),
                        batch.column(i).clone(),
                    )
                })
                .unzip(),
            DeduplicationKey::AllColumns => schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    (
                        SortField::new(field.data_type().clone()),
                        batch.column(i).clone(),
                    )
                })
                .unzip(),
        };

        let converter = RowConverter::new(sort_fields)?;
        let rows = converter.convert_columns(&columns)?;

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

/// Resolve identifier field IDs from the iceberg schema to column indices
/// within the Arrow record batches that the scan produces.
fn resolve_identifier_column_indices(table: &Table) -> Vec<usize> {
    let schema = table.metadata().current_schema();
    let identifier_ids: Vec<i32> = schema.identifier_field_ids().collect();
    if identifier_ids.is_empty() {
        return Vec::new();
    }

    // Map identifier field IDs → field names → Arrow column indices.
    // The Arrow scan output preserves field order from the iceberg schema.
    let field_names: Vec<&str> = identifier_ids
        .iter()
        .filter_map(|id| schema.field_by_id(*id).map(|f| f.name.as_str()))
        .collect();

    schema
        .as_struct()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| field_names.contains(&f.name.as_str()))
        .map(|(i, _)| i)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(ids: &[i32], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn dedup_no_duplicates() {
        let mut dedup = DeduplicatingAccumulator::from_identifier_columns(vec![]);
        let batch = make_batch(&[1, 2, 3], &["a", "b", "c"]);
        let result = dedup.add_batch(&batch).unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(dedup.duplicates_eliminated, 0);
    }

    #[test]
    fn dedup_within_single_batch() {
        let mut dedup = DeduplicatingAccumulator::from_identifier_columns(vec![]);
        let batch = make_batch(&[1, 2, 1], &["a", "b", "a"]);
        let result = dedup.add_batch(&batch).unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(dedup.duplicates_eliminated, 1);

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
    }

    #[test]
    fn dedup_across_batches() {
        let mut dedup = DeduplicatingAccumulator::from_identifier_columns(vec![]);

        let batch1 = make_batch(&[1, 2], &["a", "b"]);
        let result1 = dedup.add_batch(&batch1).unwrap();
        assert_eq!(result1.num_rows(), 2);

        // Second batch contains a duplicate of row from batch1
        let batch2 = make_batch(&[2, 3], &["b", "c"]);
        let result2 = dedup.add_batch(&batch2).unwrap();
        assert_eq!(result2.num_rows(), 1);

        assert_eq!(dedup.duplicates_eliminated, 1);

        let ids = result2
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 3);
    }

    #[test]
    fn dedup_all_duplicates() {
        let mut dedup = DeduplicatingAccumulator::from_identifier_columns(vec![]);

        let batch1 = make_batch(&[1, 2], &["a", "b"]);
        dedup.add_batch(&batch1).unwrap();

        // All rows are duplicates
        let batch2 = make_batch(&[1, 2], &["a", "b"]);
        let result2 = dedup.add_batch(&batch2).unwrap();
        assert_eq!(result2.num_rows(), 0);
        assert_eq!(dedup.duplicates_eliminated, 2);
    }

    #[test]
    fn dedup_all_columns_same_id_different_name_not_duplicate() {
        let mut dedup = DeduplicatingAccumulator::from_identifier_columns(vec![]);
        let batch = make_batch(&[1, 1], &["a", "b"]);
        let result = dedup.add_batch(&batch).unwrap();

        // All-columns mode: same id but different name → rows differ → not a duplicate
        assert_eq!(result.num_rows(), 2);
        assert_eq!(dedup.duplicates_eliminated, 0);
    }

    #[test]
    fn dedup_empty_batch() {
        let mut dedup = DeduplicatingAccumulator::from_identifier_columns(vec![]);
        let batch = make_batch(&[], &[]);
        let result = dedup.add_batch(&batch).unwrap();

        assert_eq!(result.num_rows(), 0);
        assert_eq!(dedup.duplicates_eliminated, 0);
    }

    #[test]
    fn dedup_accumulates_across_many_batches() {
        let mut dedup = DeduplicatingAccumulator::from_identifier_columns(vec![]);

        for i in 0..5 {
            let ids = vec![i, i + 1];
            let names: Vec<&str> = vec!["x", "y"];
            let batch = make_batch(&ids, &names);
            dedup.add_batch(&batch).unwrap();
        }

        // Batch 0: [0,x] [1,y] → 2 new
        // Batch 1: [1,x] [2,y] → 2 new (different name for id=1)
        // Batch 2: [2,x] [3,y] → 2 new (different name for id=2)
        // Batch 3: [3,x] [4,y] → 2 new (different name for id=3)
        // Batch 4: [4,x] [5,y] → 2 new (different name for id=4)
        // All rows are unique because (id, name) combinations differ
        assert_eq!(dedup.duplicates_eliminated, 0);
    }

    // --- Identifier-column-based dedup tests ---

    #[test]
    fn dedup_by_identifier_same_key_different_values() {
        // Dedup on column 0 (id) only — same id with different name IS a duplicate
        let mut dedup = DeduplicatingAccumulator::from_identifier_columns(vec![0]);
        let batch = make_batch(&[1, 1], &["a", "b"]);
        let result = dedup.add_batch(&batch).unwrap();

        assert_eq!(result.num_rows(), 1);
        assert_eq!(dedup.duplicates_eliminated, 1);

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
    }

    #[test]
    fn dedup_by_identifier_across_batches() {
        let mut dedup = DeduplicatingAccumulator::from_identifier_columns(vec![0]);

        let batch1 = make_batch(&[1, 2], &["a", "b"]);
        let result1 = dedup.add_batch(&batch1).unwrap();
        assert_eq!(result1.num_rows(), 2);

        // id=2 appears again with different name — still a duplicate by identifier
        let batch2 = make_batch(&[2, 3], &["updated_b", "c"]);
        let result2 = dedup.add_batch(&batch2).unwrap();
        assert_eq!(result2.num_rows(), 1);
        assert_eq!(dedup.duplicates_eliminated, 1);

        let ids = result2
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 3);
    }

    #[test]
    fn dedup_by_identifier_distinct_keys_no_duplicates() {
        let mut dedup = DeduplicatingAccumulator::from_identifier_columns(vec![0]);
        let batch = make_batch(&[1, 2, 3], &["a", "b", "c"]);
        let result = dedup.add_batch(&batch).unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(dedup.duplicates_eliminated, 0);
    }

    #[test]
    fn dedup_by_identifier_all_same_key() {
        let mut dedup = DeduplicatingAccumulator::from_identifier_columns(vec![0]);
        let batch = make_batch(&[1, 1, 1], &["a", "b", "c"]);
        let result = dedup.add_batch(&batch).unwrap();

        // Only the first row survives — all share id=1
        assert_eq!(result.num_rows(), 1);
        assert_eq!(dedup.duplicates_eliminated, 2);
    }

    #[test]
    fn dedup_by_composite_identifier() {
        // Dedup on both columns (id AND name) — same as all-columns for this schema
        let mut dedup = DeduplicatingAccumulator::from_identifier_columns(vec![0, 1]);
        let batch = make_batch(&[1, 1], &["a", "b"]);
        let result = dedup.add_batch(&batch).unwrap();

        // Different (id, name) pairs → no duplicates
        assert_eq!(result.num_rows(), 2);
        assert_eq!(dedup.duplicates_eliminated, 0);
    }
}

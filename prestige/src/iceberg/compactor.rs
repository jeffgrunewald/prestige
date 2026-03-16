use std::{
    collections::{HashMap, HashSet},
    pin::pin,
    sync::Arc,
    time::Duration,
};

use arrow::{
    array::RecordBatch,
    compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take},
    datatypes::SchemaRef,
};
use arrow_row::{RowConverter, SortField};
use arrow_select::filter::filter_record_batch;
use derive_builder::Builder;
use futures::TryStreamExt;
use iceberg::{
    TableRequirement, TableUpdate,
    arrow::ArrowReaderBuilder,
    arrow::schema_to_arrow_schema,
    spec::{
        DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestFile, ManifestListWriter,
        ManifestWriterBuilder, NullOrder, Operation, Snapshot, SnapshotReference,
        SnapshotRetention, SnapshotSummaryCollector, SortDirection, Struct, Summary, Transform,
    },
    table::Table,
};
use iceberg_catalog_rest::CommitTableRequest;
use parquet::basic::Compression;
use super_visor::{ManagedProc, ShutdownSignal};
use tokio::time;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::catalog::Catalog;
use crate::error::Result;

const DEFAULT_TARGET_FILE_SIZE_BYTES: usize = 100 * 1024 * 1024; // 100 MB
const DEFAULT_MIN_FILES_TO_COMPACT: usize = 5;
const MAX_COMMIT_RETRIES: usize = 3;

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
    pub async fn execute(mut self) -> Result<IcebergCompactionResult> {
        let current_snapshot_id = match self.table.metadata().current_snapshot() {
            Some(snap) => snap.snapshot_id(),
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

        let current_snapshot = self
            .table
            .metadata()
            .snapshot_by_id(current_snapshot_id)
            .expect("snapshot just resolved");

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
                if entry.is_alive() && entry.content_type() == iceberg::spec::DataContentType::Data
                {
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

        // Stream batches through a sorting writer instead of accumulating
        // everything in memory. Each output file is sorted according to the
        // table's sort order, and memory usage is bounded to roughly one
        // target-sized file per partition at a time.
        let mut writer = StreamingCompactionWriter::new(
            &self.table,
            Some(self.compression),
            self.target_file_size_bytes,
        )?;

        let mut total_records: usize = 0;
        let mut duplicates_eliminated: usize = 0;

        if self.deduplicate {
            let id_columns = resolve_identifier_column_indices(&self.table);
            let mut dedup = DeduplicatingAccumulator::from_identifier_columns(id_columns);
            while let Some(batch) = pinned.try_next().await? {
                total_records += batch.num_rows();
                let filtered = dedup.add_batch(&batch)?;
                if filtered.num_rows() > 0 {
                    writer.write(filtered).await?;
                }
            }
            duplicates_eliminated = dedup.duplicates_eliminated;
        } else {
            while let Some(batch) = pinned.try_next().await? {
                total_records += batch.num_rows();
                writer.write(batch).await?;
            }
        }

        let new_data_files = writer.close().await?;

        if new_data_files.is_empty() {
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

        let files_written = new_data_files.len();
        let bytes_after: u64 = new_data_files.iter().map(|f| f.file_size_in_bytes()).sum();
        let records_consolidated = total_records - duplicates_eliminated;

        // Commit as an atomic rewrite: delete old files + add new files.
        // Retry on commit conflict — a concurrent write may have advanced the
        // snapshot between our metadata read and commit. The compacted data is
        // still valid; only the parent snapshot reference needs updating.
        // Paths of files being compacted — used to verify presence on retry.
        let compacted_file_paths: HashSet<String> = compact_entries
            .iter()
            .map(|e| e.data_file.file_path().to_string())
            .collect();

        let mut last_err = None;
        for attempt in 0..=MAX_COMMIT_RETRIES {
            if attempt > 0 {
                // Reload table to get updated parent snapshot ID and fresh
                // sequence numbers for the entries we're deleting.
                let reloaded = self.catalog.load_table(self.table.identifier()).await?;
                self.table = reloaded;

                let Some(snap) = self.table.metadata().current_snapshot() else {
                    return Err(crate::Error::Branch(
                        "table has no current snapshot after reload during compaction retry".into(),
                    ));
                };

                // Re-collect entries from the reloaded table so sequence numbers
                // match the current manifest state.
                let ml = snap
                    .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
                    .await?;
                compact_entries.clear();
                for mf in ml.entries() {
                    let m = mf.load_manifest(self.table.file_io()).await?;
                    for entry in m.entries() {
                        if entry.is_alive()
                            && entry.content_type() == iceberg::spec::DataContentType::Data
                            && compacted_file_paths.contains(entry.data_file.file_path())
                        {
                            compact_entries.push(OldFileEntry {
                                data_file: entry.data_file.clone(),
                                sequence_number: entry.sequence_number.unwrap_or(0),
                                file_sequence_number: entry.file_sequence_number,
                            });
                        }
                    }
                }

                if compact_entries.len() != compacted_file_paths.len() {
                    return Err(crate::Error::Branch(
                        "compaction files no longer present after concurrent write, aborting"
                            .into(),
                    ));
                }
            }

            let parent_id = self
                .table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .unwrap_or(current_snapshot_id);

            match self
                .commit_rewrite(parent_id, &compact_entries, new_data_files.clone())
                .await
            {
                Ok(()) => {
                    last_err = None;
                    break;
                }
                Err(err) if is_commit_conflict(&err) && attempt < MAX_COMMIT_RETRIES => {
                    warn!(
                        attempt = attempt + 1,
                        max = MAX_COMMIT_RETRIES,
                        "compaction commit conflict, retrying with updated snapshot"
                    );
                    last_err = Some(err);
                }
                Err(err) => return Err(err),
            }
        }
        if let Some(err) = last_err {
            return Err(err);
        }

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

        // Carry forward surviving manifests from the parent snapshot that are
        // not affected by this compaction. Without this, non-compacted partitions
        // would disappear from the new snapshot's manifest list.
        //
        // For data manifests: drop if all alive entries are being compacted.
        // For delete manifests: drop if all referenced data files are being
        // compacted (the compacted output already has deletes applied via
        // ArrowReader, so carrying forward stale delete files would be
        // incorrect).
        let compacted_paths: HashSet<&str> = old_entries
            .iter()
            .map(|e| e.data_file.file_path())
            .collect();

        let parent_snapshot = metadata
            .snapshot_by_id(parent_snapshot_id)
            .expect("parent snapshot just resolved");
        let parent_manifest_list = parent_snapshot
            .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
            .await?;

        let mut surviving_manifests: Vec<ManifestFile> = Vec::new();
        for manifest_file in parent_manifest_list.entries() {
            let manifest = manifest_file.load_manifest(self.table.file_io()).await?;

            let dominated_by_compaction = match manifest_file.content {
                iceberg::spec::ManifestContentType::Data => {
                    // A data manifest is dominated if every alive entry is
                    // in the compacted set.
                    manifest.entries().iter().all(|entry| {
                        !entry.is_alive() || compacted_paths.contains(entry.data_file.file_path())
                    })
                }
                iceberg::spec::ManifestContentType::Deletes => {
                    // A delete manifest is dominated if every alive delete
                    // file only references data files that are being
                    // compacted. Since delete files in iceberg reference
                    // data files by path (position deletes) or by partition
                    // scope (equality deletes), we conservatively drop a
                    // delete manifest only when ALL alive entries' referenced
                    // data file paths are in the compacted set. For equality
                    // deletes (which don't reference specific file paths),
                    // we check whether the delete file's partition overlaps
                    // with any compacted partition — if it does, and ALL
                    // data files in that partition are compacted, the
                    // equality delete is subsumed.
                    //
                    // As a safe simplification: drop the delete manifest
                    // only if it has no alive entries, keeping it otherwise.
                    // The ArrowReader will simply find no matching rows and
                    // the delete files become no-ops. This avoids the risk
                    // of prematurely dropping equality deletes.
                    !manifest.entries().iter().any(|entry| entry.is_alive())
                }
            };

            if !dominated_by_compaction {
                surviving_manifests.push(manifest_file.clone());
            }
        }

        // Write manifest list: surviving manifests + delete manifest + add manifest
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
        manifest_list_writer.add_manifests(
            surviving_manifests
                .into_iter()
                .chain([delete_manifest, add_manifest]),
        )?;
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

// ---------------------------------------------------------------------------
// StreamingCompactionWriter — bounded-memory sorted file writer
// ---------------------------------------------------------------------------

/// Resolves the table's default sort order into Arrow column indices and
/// sort options. Returns `None` if the table has no sort order (order_id 0)
/// or if any sort field uses a non-identity transform.
fn resolve_sort_columns(table: &Table) -> Option<Vec<(usize, SortOptions)>> {
    let sort_order = table.metadata().default_sort_order();
    if sort_order.fields.is_empty() {
        return None;
    }

    let schema = table.metadata().current_schema();
    let mut columns = Vec::with_capacity(sort_order.fields.len());

    for field in &sort_order.fields {
        if field.transform != Transform::Identity {
            return None;
        }

        let iceberg_field = schema.field_by_id(field.source_id)?;
        let col_idx = schema
            .as_struct()
            .fields()
            .iter()
            .position(|f| f.name == iceberg_field.name)?;

        let options = SortOptions {
            descending: field.direction == SortDirection::Descending,
            nulls_first: field.null_order == NullOrder::First,
        };

        columns.push((col_idx, options));
    }

    Some(columns)
}

/// Sort a RecordBatch according to the given column indices and sort options.
fn sort_batch(
    batch: &RecordBatch,
    sort_columns: &[(usize, SortOptions)],
) -> crate::error::Result<RecordBatch> {
    let columns: Vec<SortColumn> = sort_columns
        .iter()
        .map(|(idx, options)| SortColumn {
            values: batch.column(*idx).clone(),
            options: Some(*options),
        })
        .collect();

    let indices = lexsort_to_indices(&columns, None)?;

    let sorted_columns: Vec<_> = batch
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None).map_err(crate::Error::from))
        .collect::<crate::error::Result<_>>()?;

    Ok(RecordBatch::try_new(batch.schema(), sorted_columns)?)
}

/// Streaming compaction writer that buffers batches per-partition up to the
/// target file size, sorts each buffer according to the table's sort order,
/// and writes sorted output files. Memory usage is bounded to approximately
/// one target-sized file per active partition at a time.
struct StreamingCompactionWriter {
    table: Table,
    compression: Option<Compression>,
    target_file_size_bytes: usize,
    arrow_schema: SchemaRef,
    sort_columns: Option<Vec<(usize, SortOptions)>>,
    is_partitioned: bool,
    /// Per-partition buffer: partition key → (batches, approximate byte size).
    partition_buffers: HashMap<Struct, (Vec<RecordBatch>, usize)>,
    /// Accumulated output data files from flushed buffers.
    data_files: Vec<DataFile>,
}

impl StreamingCompactionWriter {
    fn new(
        table: &Table,
        compression: Option<Compression>,
        target_file_size_bytes: usize,
    ) -> crate::error::Result<Self> {
        let metadata = table.metadata();
        let schema = metadata.current_schema();
        let arrow_schema = Arc::new(schema_to_arrow_schema(schema)?);
        let sort_columns = resolve_sort_columns(table);
        let is_partitioned = !metadata.default_partition_spec().is_unpartitioned();

        Ok(Self {
            table: table.clone(),
            compression,
            target_file_size_bytes,
            arrow_schema,
            sort_columns,
            is_partitioned,
            partition_buffers: HashMap::new(),
            data_files: Vec::new(),
        })
    }

    /// Add a batch to the writer. If any partition buffer exceeds the target
    /// file size, that partition is flushed (sorted + written) immediately.
    async fn write(&mut self, batch: RecordBatch) -> crate::error::Result<()> {
        if self.is_partitioned {
            let metadata = self.table.metadata();
            let schema = metadata.current_schema().clone();
            let partition_spec = metadata.default_partition_spec();
            let splitter =
                iceberg::arrow::RecordBatchPartitionSplitter::try_new_with_computed_values(
                    schema,
                    partition_spec.clone(),
                )?;

            let partitioned = splitter.split(&batch)?;
            for (key, partition_batch) in partitioned {
                self.buffer_batch(key.data().clone(), partition_batch)
                    .await?;
            }
        } else {
            self.buffer_batch(Struct::empty(), batch).await?;
        }

        Ok(())
    }

    async fn buffer_batch(
        &mut self,
        partition_key: Struct,
        batch: RecordBatch,
    ) -> crate::error::Result<()> {
        let size = batch.get_array_memory_size();
        let (buffers, buffered_size) = self
            .partition_buffers
            .entry(partition_key.clone())
            .or_insert_with(|| (Vec::new(), 0));
        *buffered_size += size;
        buffers.push(batch);

        if *buffered_size >= self.target_file_size_bytes {
            self.flush_partition(partition_key).await?;
        }

        Ok(())
    }

    /// Sort and write all buffered batches for a single partition.
    async fn flush_partition(&mut self, partition_key: Struct) -> crate::error::Result<()> {
        let Some((batches, _)) = self.partition_buffers.remove(&partition_key) else {
            return Ok(());
        };

        if batches.is_empty() {
            return Ok(());
        }

        let merged = concat_batches(&self.arrow_schema, &batches)?;
        drop(batches);

        let sorted = match &self.sort_columns {
            Some(cols) if !cols.is_empty() => sort_batch(&merged, cols)?,
            _ => merged,
        };

        let files = super::writer::write_data_files_with_target_size(
            &self.table,
            vec![sorted],
            self.compression,
            Some(self.target_file_size_bytes),
        )
        .await?;

        self.data_files.extend(files);
        Ok(())
    }

    /// Flush all remaining partition buffers and return the complete set of
    /// written data files.
    async fn close(mut self) -> crate::error::Result<Vec<DataFile>> {
        let keys: Vec<Struct> = self.partition_buffers.keys().cloned().collect();
        for key in keys {
            self.flush_partition(key).await?;
        }
        Ok(self.data_files)
    }
}

/// Returns true if the error indicates a commit conflict (optimistic concurrency
/// failure from `RefSnapshotIdMatch` or catalog-level conflict). These are
/// retryable — the compacted data files are still valid.
fn is_commit_conflict(err: &crate::Error) -> bool {
    match err {
        crate::Error::Iceberg(iceberg_err) => {
            matches!(
                iceberg_err.kind(),
                iceberg::ErrorKind::CatalogCommitConflicts
            )
        }
        crate::Error::CatalogHttp(msg) => msg.contains("commit conflict"),
        _ => false,
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

// ---------------------------------------------------------------------------
// CompactionScheduler — periodic compaction on a timer
// ---------------------------------------------------------------------------

const DEFAULT_COMPACTION_INTERVAL_SECS: u64 = 300; // 5 minutes

pub struct CompactionSchedulerBuilder {
    table: Table,
    catalog: Catalog,
    interval: Duration,
    target_file_size_bytes: usize,
    min_files_to_compact: usize,
    deduplicate: bool,
    compression: Compression,
    label: String,
}

impl CompactionSchedulerBuilder {
    pub fn new(table: Table, catalog: Catalog, label: impl Into<String>) -> Self {
        Self {
            table,
            catalog,
            interval: Duration::from_secs(DEFAULT_COMPACTION_INTERVAL_SECS),
            target_file_size_bytes: DEFAULT_TARGET_FILE_SIZE_BYTES,
            min_files_to_compact: DEFAULT_MIN_FILES_TO_COMPACT,
            deduplicate: false,
            compression: Compression::SNAPPY,
            label: label.into(),
        }
    }

    pub fn interval(self, interval: Duration) -> Self {
        Self { interval, ..self }
    }

    pub fn target_file_size_bytes(self, size: usize) -> Self {
        Self {
            target_file_size_bytes: size,
            ..self
        }
    }

    pub fn min_files_to_compact(self, min: usize) -> Self {
        Self {
            min_files_to_compact: min,
            ..self
        }
    }

    pub fn deduplicate(self, deduplicate: bool) -> Self {
        Self {
            deduplicate,
            ..self
        }
    }

    pub fn compression(self, compression: Compression) -> Self {
        Self {
            compression,
            ..self
        }
    }

    pub fn build(self) -> CompactionScheduler {
        CompactionScheduler {
            table: self.table,
            catalog: self.catalog,
            interval: self.interval,
            target_file_size_bytes: self.target_file_size_bytes,
            min_files_to_compact: self.min_files_to_compact,
            deduplicate: self.deduplicate,
            compression: self.compression,
            label: self.label,
        }
    }
}

/// Runs periodic compaction on a timer, checking whether any partitions
/// exceed the file count threshold and compacting them when they do.
///
/// Integrates with `ManagedProc` for lifecycle management alongside
/// sinks and pollers.
pub struct CompactionScheduler {
    table: Table,
    catalog: Catalog,
    interval: Duration,
    target_file_size_bytes: usize,
    min_files_to_compact: usize,
    deduplicate: bool,
    compression: Compression,
    label: String,
}

impl ManagedProc for CompactionScheduler {
    fn run_proc(self: Box<Self>, shutdown: ShutdownSignal) -> super_visor::ManagedFuture {
        super_visor::spawn(self.run(shutdown))
    }
}

impl CompactionScheduler {
    pub async fn run(mut self, mut shutdown: ShutdownSignal) -> Result<()> {
        info!(label = self.label, "starting compaction scheduler");

        let mut timer = time::interval(self.interval);
        timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = timer.tick() => {
                    if let Err(err) = self.compact_once().await {
                        warn!(
                            label = self.label,
                            ?err,
                            "scheduled compaction failed"
                        );
                    }
                }
            }
        }

        info!(label = self.label, "stopping compaction scheduler");
        Ok(())
    }

    async fn compact_once(&mut self) -> Result<()> {
        // Reload table to see latest state.
        self.table = self.catalog.load_table(self.table.identifier()).await?;

        if !self.needs_compaction().await? {
            debug!(
                label = self.label,
                min = self.min_files_to_compact,
                "no partition exceeds compaction threshold"
            );
            return Ok(());
        }

        let config = IcebergCompactorConfigBuilder::default()
            .table(self.table.clone())
            .catalog(self.catalog.clone())
            .target_file_size_bytes(self.target_file_size_bytes)
            .min_files_to_compact(self.min_files_to_compact)
            .deduplicate(self.deduplicate)
            .compression(self.compression)
            .build()
            .map_err(|e| crate::Error::Branch(e.to_string()))?;

        let result = config.execute().await?;

        if result.files_read > 0 {
            // Update our table handle so the next check sees the post-compaction state.
            self.table = self.catalog.load_table(self.table.identifier()).await?;

            info!(
                label = self.label,
                files_read = result.files_read,
                files_written = result.files_written,
                partitions = result.partitions_compacted,
                duplicates_eliminated = result.duplicates_eliminated,
                "scheduled compaction complete"
            );
        }

        Ok(())
    }

    /// Lightweight check: count files per partition and return true if any
    /// partition meets the compaction threshold. Avoids reading file data.
    async fn needs_compaction(&self) -> Result<bool> {
        let Some(snapshot) = self.table.metadata().current_snapshot() else {
            return Ok(false);
        };

        let manifest_list = snapshot
            .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
            .await?;

        let mut partition_file_counts: HashMap<Struct, usize> = HashMap::new();

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(self.table.file_io()).await?;
            for entry in manifest.entries() {
                if entry.is_alive() {
                    *partition_file_counts
                        .entry(entry.data_file.partition().clone())
                        .or_default() += 1;
                }
            }
        }

        Ok(partition_file_counts
            .values()
            .any(|&count| count >= self.min_files_to_compact))
    }
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

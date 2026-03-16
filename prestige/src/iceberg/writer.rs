use crate::error::Result;
use crate::{ArrowSchema, Error};
use arrow::array::RecordBatch;
use arrow::compute::cast;
use iceberg::arrow::{RecordBatchPartitionSplitter, schema_to_arrow_schema};
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::basic::Compression;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

/// Concrete builder type alias used throughout the writer pipeline.
type WriterBuilder =
    DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

/// Write record batches to iceberg data files via the table's writer stack.
///
/// Automatically uses partition-aware writing (FanoutWriter) when the table
/// has a non-trivial partition spec, and falls back to a single writer for
/// unpartitioned tables.
pub async fn write_data_files(
    table: &Table,
    batches: Vec<RecordBatch>,
    compression: Option<Compression>,
) -> Result<Vec<DataFile>> {
    write_data_files_with_target_size(table, batches, compression, None).await
}

/// Write record batches with an optional target file size.
///
/// When `target_file_size_bytes` is `None`, the table's default target size is
/// used (from `write.target-file-size-bytes` table property, or iceberg's default).
pub async fn write_data_files_with_target_size(
    table: &Table,
    batches: Vec<RecordBatch>,
    compression: Option<Compression>,
    target_file_size_bytes: Option<usize>,
) -> Result<Vec<DataFile>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let metadata = table.metadata();
    let schema = metadata.current_schema().clone();
    let partition_spec = metadata.default_partition_spec();
    let is_partitioned = !partition_spec.is_unpartitioned();

    // Rebatch with the table's Arrow schema so columns carry iceberg field ID
    // metadata (PARQUET:field_id). Without this, the writer can't match batch
    // columns to the iceberg schema fields.
    //
    // Iceberg's type system lacks unsigned integers, so the table schema may
    // use wider signed types (e.g. Int32) where the source data has unsigned
    // types (e.g. UInt8). Cast columns whose types diverge.
    let table_arrow_schema = Arc::new(schema_to_arrow_schema(&schema)?);
    let batches: Vec<RecordBatch> = batches
        .into_iter()
        .map(|b| {
            let columns: Vec<_> = b
                .columns()
                .iter()
                .zip(table_arrow_schema.fields())
                .map(|(col, target_field)| {
                    if col.data_type() == target_field.data_type() {
                        Ok(col.clone())
                    } else {
                        cast(col, target_field.data_type())
                    }
                })
                .collect::<std::result::Result<_, _>>()?;
            RecordBatch::try_new(table_arrow_schema.clone(), columns)
        })
        .collect::<std::result::Result<_, _>>()?;

    let file_io = table.file_io().clone();
    let location_gen = DefaultLocationGenerator::new(metadata.clone())?;
    let file_name_gen = DefaultFileNameGenerator::new(
        "data".to_string(),
        Some(uuid::Uuid::now_v7().as_simple().to_string()),
        DataFileFormat::Parquet,
    );

    let mut props_builder = parquet::file::properties::WriterProperties::builder();
    if let Some(compression) = compression {
        props_builder = props_builder.set_compression(compression);
    }

    let parquet_builder = ParquetWriterBuilder::new(props_builder.build(), schema.clone());

    let rolling_builder = match target_file_size_bytes {
        Some(size) => RollingFileWriterBuilder::new(
            parquet_builder,
            size,
            file_io,
            location_gen,
            file_name_gen,
        ),
        None => RollingFileWriterBuilder::new_with_default_file_size(
            parquet_builder,
            file_io,
            location_gen,
            file_name_gen,
        ),
    };

    let writer_builder = DataFileWriterBuilder::new(rolling_builder);

    if is_partitioned {
        write_partitioned(
            table,
            &batches,
            writer_builder,
            schema,
            partition_spec.clone(),
        )
        .await
    } else {
        write_unpartitioned(&batches, writer_builder).await
    }
}

/// Write batches through a FanoutWriter that routes records to per-partition files.
async fn write_partitioned(
    _table: &Table,
    batches: &[RecordBatch],
    writer_builder: WriterBuilder,
    schema: iceberg::spec::SchemaRef,
    partition_spec: iceberg::spec::PartitionSpecRef,
) -> Result<Vec<DataFile>> {
    let splitter =
        RecordBatchPartitionSplitter::try_new_with_computed_values(schema, partition_spec)?;

    let mut fanout_writer = FanoutWriter::new(writer_builder);

    for batch in batches {
        let partitioned_batches = splitter.split(batch)?;
        for (partition_key, partition_batch) in partitioned_batches {
            fanout_writer.write(partition_key, partition_batch).await?;
        }
    }

    let data_files = fanout_writer.close().await?;
    Ok(data_files)
}

/// Write batches through a single DataFileWriter (unpartitioned table).
async fn write_unpartitioned(
    batches: &[RecordBatch],
    writer_builder: WriterBuilder,
) -> Result<Vec<DataFile>> {
    let mut writer = writer_builder.build(None).await?;

    for batch in batches {
        writer.write(batch.clone()).await?;
    }

    let data_files = writer.close().await?;
    Ok(data_files)
}

/// Commit data files to an iceberg table via a fast_append transaction.
///
/// This atomically adds the data files to the table's current snapshot,
/// making them visible to readers.
pub async fn commit_data_files(
    table: &Table,
    catalog: &dyn iceberg::Catalog,
    data_files: Vec<DataFile>,
    snapshot_properties: Option<HashMap<String, String>>,
) -> Result<Table> {
    let tx = Transaction::new(table);
    let mut action = tx.fast_append().add_data_files(data_files);

    if let Some(props) = snapshot_properties {
        action = action.set_snapshot_properties(props);
    }

    let tx = action.apply(tx)?;
    let updated_table = tx.commit(catalog).await?;
    Ok(updated_table)
}

/// Serialize typed records, write data files, and commit in a single call.
///
/// This is a convenience function for batch / one-shot use cases where the
/// full streaming sink is unnecessary. Records are serialized to Arrow via
/// serde_arrow, written to partitioned (or unpartitioned) data files, and
/// atomically committed as a new snapshot.
pub async fn write_and_commit<T: ArrowSchema + Serialize>(
    table: &Table,
    catalog: &dyn iceberg::Catalog,
    records: &[T],
    compression: Option<Compression>,
    snapshot_properties: Option<HashMap<String, String>>,
) -> Result<Table> {
    if records.is_empty() {
        return Ok(table.clone());
    }

    let schema = T::arrow_schema();
    let arrays = serde_arrow::to_arrow(schema.fields(), records)
        .map_err(|e| Error::SerdeArrow(e.to_string()))?;
    let batch = RecordBatch::try_new(schema, arrays)?;

    let data_files = write_data_files(table, vec![batch], compression).await?;
    commit_data_files(table, catalog, data_files, snapshot_properties).await
}

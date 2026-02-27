use crate::error::Result;
use arrow::array::RecordBatch;
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::Catalog;
use parquet::basic::Compression;
use std::collections::HashMap;

/// Write record batches to iceberg data files via the table's writer stack.
///
/// Returns the list of data files written. These files exist in storage but
/// are not yet committed to the table's metadata — call `commit_data_files`
/// to make them visible.
pub async fn write_data_files(
    table: &Table,
    batches: Vec<RecordBatch>,
    compression: Option<Compression>,
) -> Result<Vec<DataFile>> {
    write_data_files_with_target_size(table, batches, compression, None).await
}

/// Write record batches to iceberg data files with an optional target file size.
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

    let file_io = table.file_io().clone();
    let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_gen =
        DefaultFileNameGenerator::new("data".to_string(), None, DataFileFormat::Parquet);

    let mut props_builder = parquet::file::properties::WriterProperties::builder();
    if let Some(compression) = compression {
        props_builder = props_builder.set_compression(compression);
    }

    let schema = table.metadata().current_schema().clone();
    let parquet_builder = ParquetWriterBuilder::new(props_builder.build(), schema);

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
    let mut writer = writer_builder.build(None).await?;

    for batch in &batches {
        writer.write(batch.clone()).await?;
    }

    let data_files = writer.close().await?;
    Ok(data_files)
}

/// Commit data files to an iceberg table via a fast_append transaction.
///
/// This atomically adds the data files to the table's current snapshot,
/// making them visible to readers. Iceberg handles retry logic and
/// conflict resolution automatically.
pub async fn commit_data_files(
    table: &Table,
    catalog: &dyn Catalog,
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

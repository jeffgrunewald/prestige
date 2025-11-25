use crate::{Client, FileMetaStream, Result};
use arrow::array::RecordBatch;
use futures::{
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use metrics::Label;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use std::{io::Cursor, path::Path, sync::Arc};
use tokio::fs::File;

/// Default batch size for reading parquet files
pub const DEFAULT_BATCH_SIZE: usize = 8192;

const OK_LABEL: Label = Label::from_static_parts("status", "ok");
const ERROR_LABEL: Label = Label::from_static_parts("status", "error");

/// Stream of Arrow RecordBatch from parquet files
pub type RecordBatchStream = BoxStream<'static, Result<RecordBatch>>;

/// Read parquet files from local filesystem
///
/// Opens multiple parquet files and streams their contents as Arrow RecordBatch.
/// Files are read sequentially with buffering.
///
/// # Arguments
/// * `paths` - Iterator of file paths to read
/// * `batch_size` - Optional batch size (defaults to DEFAULT_BATCH_SIZE)
/// * `metric` - Optional metric name for tracking read operations
///
/// # Example
/// ```no_run
/// use prestige::file_source;
///
/// let paths = vec!["data/file1.parquet", "data/file2.parquet"];
/// let stream = file_source::source(paths, None, None);
/// ```
pub fn source<I, P>(
    paths: I,
    batch_size: Option<usize>,
    metric: Option<String>,
) -> RecordBatchStream
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
{
    let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
    let paths: Vec<_> = paths
        .into_iter()
        .map(|p| p.as_ref().to_path_buf())
        .collect();

    let metric_clone = metric.clone();
    stream::iter(paths)
        .map(move |path| {
            let metric = metric_clone.clone();
            async move {
                let file = File::open(&path).await?;
                let builder = ParquetRecordBatchStreamBuilder::new(file).await?;
                let stream = builder.with_batch_size(batch_size).build()?;

                if let Some(ref m) = metric {
                    metrics::counter!(m.clone(), vec![OK_LABEL]).increment(1);
                }

                Ok(stream)
            }
        })
        .buffered(2) // Read up to 2 files concurrently
        .flat_map(move |result| {
            let metric = metric.clone();
            match result {
                Ok(stream) => stream
                    .inspect(move |result| {
                        if let Some(ref m) = metric {
                            match result {
                                Ok(batch) => {
                                    metrics::counter!(m.clone(), vec![OK_LABEL]).increment(batch.num_rows() as u64);
                                }
                                Err(_) => {
                                    metrics::counter!(m.clone(), vec![ERROR_LABEL]).increment(1);
                                }
                            }
                        }
                    })
                    .map_err(crate::Error::from)
                    .boxed(),
                Err(err) => {
                    if let Some(ref m) = metric {
                        metrics::counter!(m.clone(), vec![ERROR_LABEL]).increment(1);
                    }
                    stream::once(async { Err(err) }).boxed()
                }
            }
        })
        .boxed()
}

/// Read a single parquet file from S3
///
/// Downloads the entire file into memory and parses it as a parquet file.
/// Returns a stream of Arrow RecordBatch.
///
/// # Arguments
/// * `client` - AWS S3 client
/// * `bucket` - S3 bucket name
/// * `key` - S3 object key
/// * `batch_size` - Optional batch size (defaults to DEFAULT_BATCH_SIZE)
/// * `metric` - Optional metric name for tracking read operations
pub async fn source_s3_file(
    client: &Client,
    bucket: impl Into<String>,
    key: impl Into<String>,
    batch_size: Option<usize>,
    metric: Option<String>,
) -> Result<RecordBatchStream> {
    let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
    let key = key.into();

    // Download file from S3
    let bytes = crate::get_file(client, bucket, &key).await?;

    // Parse parquet from bytes
    let cursor = Cursor::new(bytes);
    let builder = ParquetRecordBatchStreamBuilder::new(cursor).await?;
    let stream = builder.with_batch_size(batch_size).build()?;

    if let Some(ref m) = metric {
        metrics::counter!(m.clone(), vec![OK_LABEL]).increment(1);
    }

    Ok(stream
        .inspect(move |result| {
            if let Some(ref m) = metric {
                match result {
                    Ok(batch) => {
                        metrics::counter!(m.clone(), vec![OK_LABEL]).increment(batch.num_rows() as u64);
                    }
                    Err(_) => {
                        metrics::counter!(m.clone(), vec![ERROR_LABEL]).increment(1);
                    }
                }
            }
        })
        .map_err(crate::Error::from)
        .boxed())
}

/// Read multiple S3 files in order (sequential)
///
/// Downloads and processes files one at a time in the order they appear
/// in the FileMeta stream.
///
/// # Arguments
/// * `client` - AWS S3 client
/// * `bucket` - S3 bucket name
/// * `metas` - Stream of FileMeta (from list_files)
/// * `batch_size` - Optional batch size (defaults to DEFAULT_BATCH_SIZE)
/// * `metric` - Optional metric name for tracking read operations
pub fn source_s3_files(
    client: &Client,
    bucket: impl Into<String>,
    metas: FileMetaStream,
    batch_size: Option<usize>,
    metric: Option<String>,
) -> RecordBatchStream {
    let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
    let bucket = bucket.into();
    let client = Arc::new(client.clone());

    metas
        .map(move |meta_result| {
            let client = Arc::clone(&client);
            let bucket = bucket.clone();
            let metric = metric.clone();

            async move {
                let meta = meta_result?;
                source_s3_file(&client, bucket, meta.key, Some(batch_size), metric).await
            }
        })
        .buffered(1) // Sequential - one file at a time
        .flat_map(|result| match result {
            Ok(stream) => stream.boxed(),
            Err(err) => stream::once(async { Err(err) }).boxed(),
        })
        .boxed()
}

/// Read multiple S3 files in parallel (unordered)
///
/// Downloads and processes multiple files concurrently.
/// Order of RecordBatch output is not guaranteed.
///
/// # Arguments
/// * `client` - AWS S3 client
/// * `bucket` - S3 bucket name
/// * `workers` - Number of concurrent downloads
/// * `metas` - Stream of FileMeta (from list_files)
/// * `batch_size` - Optional batch size (defaults to DEFAULT_BATCH_SIZE)
/// * `metric` - Optional metric name for tracking read operations
pub fn source_s3_files_unordered(
    client: &Client,
    bucket: impl Into<String>,
    workers: usize,
    metas: FileMetaStream,
    batch_size: Option<usize>,
    metric: Option<String>,
) -> RecordBatchStream {
    let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
    let bucket = bucket.into();
    let client = Arc::new(client.clone());

    metas
        .map(move |meta_result| {
            let client = Arc::clone(&client);
            let bucket = bucket.clone();
            let metric = metric.clone();

            async move {
                let meta = meta_result?;
                source_s3_file(&client, bucket, meta.key, Some(batch_size), metric).await
            }
        })
        .buffer_unordered(workers) // Parallel - up to N files at once
        .flat_map(|result| match result {
            Ok(stream) => stream.boxed(),
            Err(err) => stream::once(async { Err(err) }).boxed(),
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    use tempfile::{NamedTempFile, TempDir};

    /// Create a test parquet file with some sample data
    async fn create_test_parquet_file() -> NamedTempFile {
        let temp_file = NamedTempFile::new().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]);
        let value_array = Float64Array::from(vec![1.5, 2.5, 3.5, 4.5, 5.5]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value_array),
            ],
        )
        .unwrap();

        let file = std::fs::File::create(temp_file.path()).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        temp_file
    }

    /// Create multiple test parquet files in a directory
    async fn create_test_parquet_files(count: usize) -> (TempDir, Vec<std::path::PathBuf>) {
        let temp_dir = TempDir::new().unwrap();
        let mut paths = Vec::new();

        for i in 0..count {
            let file_path = temp_dir.path().join(format!("test_{}.parquet", i));
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("batch", DataType::Int64, false),
            ]));

            let id_array = Int64Array::from(vec![i as i64 * 10, i as i64 * 10 + 1, i as i64 * 10 + 2]);
            let batch_array = Int64Array::from(vec![i as i64, i as i64, i as i64]);

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(id_array), Arc::new(batch_array)],
            )
            .unwrap();

            let file = std::fs::File::create(&file_path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();

            paths.push(file_path);
        }

        (temp_dir, paths)
    }

    #[tokio::test]
    async fn test_source_local_single_file() {
        let temp_file = create_test_parquet_file().await;
        let paths = vec![temp_file.path()];
        let mut stream = source(paths, None, None);

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
        }

        assert_eq!(total_rows, 5);
    }

    #[tokio::test]
    async fn test_source_local_multiple_files() {
        let (_temp_dir, paths) = create_test_parquet_files(3).await;
        let mut stream = source(paths, None, None);

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
        }

        // 3 files × 3 rows each = 9 rows total
        assert_eq!(total_rows, 9);
    }

    #[tokio::test]
    async fn test_source_local_custom_batch_size() {
        let temp_file = create_test_parquet_file().await;
        let paths = vec![temp_file.path()];
        let mut stream = source(paths, Some(2), None);

        let mut batch_count = 0;
        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            batch_count += 1;
            total_rows += batch.num_rows();
        }

        assert_eq!(total_rows, 5);
        // With batch_size=2 and 5 rows, we should get 3 batches (2+2+1)
        assert!(batch_count >= 2);
    }

    #[tokio::test]
    async fn test_source_local_empty_paths() {
        let paths: Vec<&str> = vec![];
        let mut stream = source(paths, None, None);

        let result = stream.next().await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_source_local_nonexistent_file() {
        let paths = vec!["/tmp/nonexistent_file_12345.parquet"];
        let mut stream = source(paths, None, None);

        let result = stream.next().await;
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_source_local_verify_data_integrity() {
        let temp_file = create_test_parquet_file().await;
        let paths = vec![temp_file.path()];
        let mut stream = source(paths, None, None);

        let batch_result = stream.next().await;
        assert!(batch_result.is_some());

        let batch = batch_result.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 3);

        // Verify column names
        let schema = batch.schema();
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "value");

        // Verify first row data
        let id_col = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let name_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let value_col = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(id_col.value(0), 1);
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(value_col.value(0), 1.5);
    }

    #[tokio::test]
    async fn test_default_batch_size_constant() {
        assert_eq!(DEFAULT_BATCH_SIZE, 8192);
    }

    #[tokio::test]
    async fn test_source_with_metrics() {
        let temp_file = create_test_parquet_file().await;
        let paths = vec![temp_file.path()];
        let mut stream = source(paths, None, Some("test_read_metric".to_string()));

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
        }

        assert_eq!(total_rows, 5);
        // Metrics are tracked but we can't easily verify them in unit tests
        // Integration tests with a metrics recorder would validate the actual values
    }
}

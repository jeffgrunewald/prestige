use arrow::datatypes::DataType;
use futures::StreamExt;
use prestige::{FileUpload, file_sink::ParquetSinkBuilder, file_source};
use serde::{Deserialize, Serialize};
use super_visor::{ManagedProc, ShutdownSignal};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

/// Test struct representing sensor data with various field types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, prestige::PrestigeSchema)]
struct SensorData {
    #[prestige(identifier)]
    timestamp: u64,
    #[prestige(identifier)]
    sensor_id: String,
    temperature: f32,
    #[serde(with = "serde_bytes")]
    device_mac: [u8; 6],
}

/// Generate sample sensor data for testing
fn generate_sample_sensor_data(count: usize) -> Vec<SensorData> {
    (0..count)
        .map(|i| SensorData {
            timestamp: 1700000000 + (i as u64 * 60), // Increment by 1 minute
            sensor_id: format!("sensor-{:04}", i),
            temperature: 20.0 + (i as f32 * 0.5), // Temperature increases
            device_mac: [0xAA, 0xBB, 0xCC, (i / 256) as u8, (i % 256) as u8, 0xFF],
        })
        .collect()
}

/// Helper to create a test file upload server
async fn create_test_file_upload() -> (FileUpload, prestige::FileUploadServer) {
    let client = prestige::new_client(None, None, None, None).await;
    FileUpload::new(client, "test-bucket".to_string()).await
}

/// Helper function to write sensor data to a parquet file
async fn write_sensor_data_to_parquet(
    data: Vec<SensorData>,
    temp_dir: &TempDir,
) -> prestige::Result<Vec<std::path::PathBuf>> {
    let (file_upload, _upload_server) = create_test_file_upload().await;

    let (client, sink) = ParquetSinkBuilder::<SensorData>::new(
        "sensor_data",
        temp_dir.path(),
        file_upload,
        "test_sensor_metric",
    )
    .batch_size(100)
    .create()
    .await?;

    // Start the sink server as a background task
    let cancel_token = CancellationToken::new();
    let shutdown_signal = ShutdownSignal::new(cancel_token.clone());

    let sink_handle =
        tokio::task::spawn_local(async move { Box::new(sink).run_proc(shutdown_signal).await });

    // Write data using the client
    for record in data {
        let rx = client.write(record).await?;
        rx.await
            .map_err(|_| prestige::ChannelError::sink_closed("test_sensor_metric"))??;
    }

    // Commit to finalize the files
    let manifest_rx = client.commit().await?;
    let manifest = manifest_rx
        .await
        .map_err(|_| prestige::ChannelError::sink_closed("test_sensor_metric"))??;

    // Shutdown the sink server
    cancel_token.cancel();
    let _ = sink_handle.await;

    // Convert manifest strings to PathBufs
    Ok(manifest.into_iter().map(std::path::PathBuf::from).collect())
}

/// Helper function to read sensor data from parquet files
async fn read_sensor_data_from_parquet(
    file_paths: Vec<std::path::PathBuf>,
) -> prestige::Result<Vec<SensorData>> {
    let mut stream = file_source::source(file_paths, None, None);
    let mut all_data = Vec::new();

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;

        // Convert RecordBatch back to SensorData using serde_arrow
        let records: Vec<SensorData> = serde_arrow::from_arrow(
            batch.schema().fields(),
            &(0..batch.num_columns())
                .map(|i| batch.column(i).clone())
                .collect::<Vec<_>>(),
        )
        .map_err(|e| prestige::Error::SerdeArrow(e.to_string()))?;

        all_data.extend(records);
    }

    Ok(all_data)
}

#[tokio::test(flavor = "current_thread")]
async fn test_roundtrip_sensor_data_small() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let temp_dir = TempDir::new().unwrap();

            // Generate sample data
            let original_data = generate_sample_sensor_data(10);

            // Write to parquet file
            let file_paths = write_sensor_data_to_parquet(original_data.clone(), &temp_dir)
                .await
                .unwrap();

            assert_eq!(
                file_paths.len(),
                1,
                "Should produce exactly one parquet file"
            );

            // Verify file exists
            assert!(
                file_paths[0].exists(),
                "Parquet file should exist at {}",
                file_paths[0].display()
            );

            // Read back from parquet file
            let deserialized_data = read_sensor_data_from_parquet(file_paths).await.unwrap();

            // Validate data integrity
            assert_eq!(
                original_data.len(),
                deserialized_data.len(),
                "Should have same number of records"
            );

            for (original, deserialized) in original_data.iter().zip(deserialized_data.iter()) {
                assert_eq!(
                    original, deserialized,
                    "Record should match after round-trip"
                );
            }
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_roundtrip_sensor_data_large() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let temp_dir = TempDir::new().unwrap();

            // Generate larger dataset to test batching
            let original_data = generate_sample_sensor_data(1000);

            // Write to parquet file
            let file_paths = write_sensor_data_to_parquet(original_data.clone(), &temp_dir)
                .await
                .unwrap();

            assert!(!file_paths.is_empty(), "Should produce at least one file");

            // Read back from parquet file(s)
            let deserialized_data = read_sensor_data_from_parquet(file_paths).await.unwrap();

            // Validate data integrity
            assert_eq!(
                original_data.len(),
                deserialized_data.len(),
                "Should have same number of records"
            );

            for (i, (original, deserialized)) in original_data
                .iter()
                .zip(deserialized_data.iter())
                .enumerate()
            {
                assert_eq!(
                    original, deserialized,
                    "Record {} should match after round-trip",
                    i
                );
            }
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_roundtrip_sensor_data_with_rotation() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let temp_dir = TempDir::new().unwrap();
            let (file_upload, _upload_server) = create_test_file_upload().await;

            // Generate data that will trigger file rotation
            let original_data = generate_sample_sensor_data(250);

            let (client, sink) = ParquetSinkBuilder::<SensorData>::new(
                "sensor_data",
                temp_dir.path(),
                file_upload,
                "test_sensor_metric",
            )
            .batch_size(50)
            .max_rows(100) // Force rotation after 100 rows
            .auto_commit(false) // Disable auto-commit to collect all files in manifest
            .create()
            .await
            .unwrap();

            // Start the sink server as a background task
            let cancel_token = CancellationToken::new();
            let shutdown_signal = ShutdownSignal::new(cancel_token.clone());

            let sink_handle =
                tokio::task::spawn_local(
                    async move { Box::new(sink).run_proc(shutdown_signal).await },
                );

            // Write all data
            for record in original_data.clone() {
                let rx = client.write(record).await.unwrap();
                rx.await.unwrap().unwrap();
            }

            // Wait a bit to allow rotation timer to fire
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            // Commit to finalize
            let manifest_rx = client.commit().await.unwrap();
            let manifest = manifest_rx.await.unwrap().unwrap();

            // Shutdown the sink server
            cancel_token.cancel();
            let _ = sink_handle.await;

            // Should have multiple files due to rotation
            // With auto_commit disabled, all files should be in the manifest
            assert!(
                manifest.len() >= 2,
                "Should have multiple files due to rotation, got {}",
                manifest.len()
            );

            let file_paths: Vec<_> = manifest.into_iter().map(std::path::PathBuf::from).collect();

            // Read back all data
            let deserialized_data = read_sensor_data_from_parquet(file_paths).await.unwrap();

            // Validate all data is present
            assert_eq!(
                original_data.len(),
                deserialized_data.len(),
                "Should have all records across multiple files"
            );

            for (i, (original, deserialized)) in original_data
                .iter()
                .zip(deserialized_data.iter())
                .enumerate()
            {
                assert_eq!(
                    original, deserialized,
                    "Record {} should match after round-trip with rotation",
                    i
                );
            }
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_roundtrip_empty_dataset() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let temp_dir = TempDir::new().unwrap();
            let (file_upload, _upload_server) = create_test_file_upload().await;

            let (client, sink) = ParquetSinkBuilder::<SensorData>::new(
                "sensor_data",
                temp_dir.path(),
                file_upload,
                "test_sensor_metric",
            )
            .create()
            .await
            .unwrap();

            // Start the sink server
            let cancel_token = CancellationToken::new();
            let shutdown_signal = ShutdownSignal::new(cancel_token.clone());

            let sink_handle =
                tokio::task::spawn_local(
                    async move { Box::new(sink).run_proc(shutdown_signal).await },
                );

            // Don't write any data, just commit
            let manifest_rx = client.commit().await.unwrap();
            let manifest = manifest_rx.await.unwrap().unwrap();

            // Shutdown the server
            cancel_token.cancel();
            let _ = sink_handle.await;

            // Should produce no files for empty data
            assert_eq!(
                manifest.len(),
                0,
                "Should produce no files for empty dataset"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_roundtrip_single_record() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let temp_dir = TempDir::new().unwrap();

            // Single record
            let original_data = generate_sample_sensor_data(1);

            let file_paths = write_sensor_data_to_parquet(original_data.clone(), &temp_dir)
                .await
                .unwrap();

            assert_eq!(file_paths.len(), 1);

            let deserialized_data = read_sensor_data_from_parquet(file_paths).await.unwrap();

            assert_eq!(deserialized_data.len(), 1);
            assert_eq!(original_data[0], deserialized_data[0]);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_roundtrip_validates_fixed_size_binary() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let temp_dir = TempDir::new().unwrap();

            // Create data with specific MAC addresses to verify binary data handling
            let original_data = vec![
                SensorData {
                    timestamp: 1700000000,
                    sensor_id: "sensor-001".to_string(),
                    temperature: 25.5,
                    device_mac: [0x00, 0x11, 0x22, 0x33, 0x44, 0x55],
                },
                SensorData {
                    timestamp: 1700000060,
                    sensor_id: "sensor-002".to_string(),
                    temperature: 26.5,
                    device_mac: [0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA],
                },
            ];

            let file_paths = write_sensor_data_to_parquet(original_data.clone(), &temp_dir)
                .await
                .unwrap();

            let deserialized_data = read_sensor_data_from_parquet(file_paths).await.unwrap();

            // Verify MAC addresses are preserved exactly
            for (original, deserialized) in original_data.iter().zip(deserialized_data.iter()) {
                assert_eq!(
                    original.device_mac, deserialized.device_mac,
                    "MAC address should be preserved exactly"
                );
            }
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_roundtrip_parquet_file_format_verification() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let temp_dir = TempDir::new().unwrap();
            let original_data = generate_sample_sensor_data(50);

            let file_paths = write_sensor_data_to_parquet(original_data.clone(), &temp_dir)
                .await
                .unwrap();

            // Verify we can read the parquet file with parquet library directly
            let file = std::fs::File::open(&file_paths[0]).unwrap();
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                    .unwrap()
                    .build()
                    .unwrap();

            let mut total_rows = 0;
            for batch_result in reader {
                let batch = batch_result.unwrap();
                total_rows += batch.num_rows();

                // Verify schema structure
                assert_eq!(batch.num_columns(), 4);
                assert_eq!(batch.schema().field(0).name(), "timestamp");
                assert_eq!(batch.schema().field(1).name(), "sensor_id");
                assert_eq!(batch.schema().field(2).name(), "temperature");
                assert_eq!(batch.schema().field(3).name(), "device_mac");

                // Verify data types
                assert_eq!(
                    batch.schema().field(0).data_type(),
                    &DataType::UInt64,
                    "timestamp should be UInt64"
                );
                assert_eq!(
                    batch.schema().field(1).data_type(),
                    &DataType::Utf8,
                    "sensor_id should be Utf8"
                );
                assert_eq!(
                    batch.schema().field(2).data_type(),
                    &DataType::Float32,
                    "temperature should be Float32"
                );
                assert_eq!(
                    batch.schema().field(3).data_type(),
                    &DataType::FixedSizeBinary(6),
                    "device_mac should be FixedSizeBinary(6)"
                );
            }

            assert_eq!(total_rows, original_data.len());
        })
        .await;
}

#[test]
fn test_identifier_field_names() {
    let names = SensorData::identifier_field_names();
    assert_eq!(names, &["timestamp", "sensor_id"]);
}

/// Struct with no identifier fields should return an empty slice.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, prestige::PrestigeSchema)]
struct NoIdentifiers {
    x: i32,
    y: String,
}

#[test]
fn test_no_identifier_fields() {
    assert!(NoIdentifiers::identifier_field_names().is_empty());
}

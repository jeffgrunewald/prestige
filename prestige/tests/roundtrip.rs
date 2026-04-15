use arrow::datatypes::DataType;
use futures::StreamExt;
use prestige::{FileUpload, file_sink::ParquetSinkBuilder, file_source};
use serde::{Deserialize, Serialize};
use super_visor::{ManagedProc, ShutdownSignal};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

/// Test struct representing sensor data with various field types.
///
/// Uses `#[prestige_schema]` attribute macro which auto-injects `#[serde(with = "serde_bytes")]`
/// on `as_binary` fields and generates all Prestige trait implementations.
#[prestige::prestige_schema]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SensorData {
    #[prestige(identifier)]
    timestamp: u64,
    #[prestige(identifier)]
    sensor_id: String,
    temperature: f32,
    #[prestige(as_binary)]
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
#[prestige::prestige_schema]
#[derive(Debug, Clone, PartialEq)]
struct NoIdentifiers {
    x: i32,
    y: String,
}

#[test]
fn test_no_identifier_fields() {
    assert!(NoIdentifiers::identifier_field_names().is_empty());
}

/// Test whether serde_arrow can serialize a plain Vec<u8> (no serde_bytes)
/// into a Binary-typed Arrow field via the seq protocol.
mod serde_arrow_binary_compat {
    use arrow::array::RecordBatch;
    use arrow::datatypes::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    /// Plain Vec<u8> — serde serializes as a sequence of integers
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct PlainBytes {
        label: String,
        data: Vec<u8>,
    }

    /// Vec<u8> with serde_bytes — serde serializes as a byte buffer
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct AnnotatedBytes {
        label: String,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    }

    fn binary_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("label", DataType::Utf8, false),
            Field::new("data", DataType::Binary, false),
        ]))
    }

    fn list_u8_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("label", DataType::Utf8, false),
            Field::new(
                "data",
                DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
                false,
            ),
        ]))
    }

    fn sample_data() -> Vec<u8> {
        vec![0x00, 0x7F, 0x80, 0xFF, 0xDE, 0xAD]
    }

    #[test]
    fn plain_vec_u8_to_binary_schema() {
        let records = vec![PlainBytes {
            label: "test".into(),
            data: sample_data(),
        }];
        let schema = binary_schema();

        let result = serde_arrow::to_arrow(schema.fields(), &records);
        match &result {
            Ok(arrays) => {
                let batch = RecordBatch::try_new(schema.clone(), arrays.clone()).unwrap();
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(batch.schema().field(1).data_type(), &DataType::Binary);

                // Deserialize back
                let deserialized: Vec<PlainBytes> =
                    serde_arrow::from_arrow(schema.fields(), arrays).unwrap();
                assert_eq!(deserialized[0].data, sample_data());
                println!("PASS: plain Vec<u8> serializes to Binary schema via seq protocol");
            }
            Err(e) => {
                println!("FAIL: plain Vec<u8> cannot serialize to Binary schema: {e}");
                panic!("serde_arrow rejected plain Vec<u8> with Binary schema");
            }
        }
    }

    #[test]
    fn annotated_vec_u8_to_binary_schema() {
        let records = vec![AnnotatedBytes {
            label: "test".into(),
            data: sample_data(),
        }];
        let schema = binary_schema();

        let result = serde_arrow::to_arrow(schema.fields(), &records);
        match &result {
            Ok(arrays) => {
                let batch = RecordBatch::try_new(schema.clone(), arrays.clone()).unwrap();
                assert_eq!(batch.num_rows(), 1);
                assert_eq!(batch.schema().field(1).data_type(), &DataType::Binary);

                let deserialized: Vec<AnnotatedBytes> =
                    serde_arrow::from_arrow(schema.fields(), arrays).unwrap();
                assert_eq!(deserialized[0].data, sample_data());
                println!(
                    "PASS: serde_bytes Vec<u8> serializes to Binary schema via bytes protocol"
                );
            }
            Err(e) => {
                println!("FAIL: serde_bytes Vec<u8> cannot serialize to Binary schema: {e}");
                panic!("serde_arrow rejected serde_bytes Vec<u8> with Binary schema");
            }
        }
    }

    #[test]
    fn bench_binary_serialization_throughput() {
        // Simulate realistic workload: 10,000 records with variable-size binary blobs
        // similar to serialized Solana CompiledInstruction (~50-500 bytes each)
        let small_records: Vec<PlainBytes> = (0..10_000)
            .map(|i| PlainBytes {
                label: format!("tx-{i}"),
                data: vec![0xABu8; 64], // 64 bytes — small instruction
            })
            .collect();

        let large_records: Vec<PlainBytes> = (0..10_000)
            .map(|i| PlainBytes {
                label: format!("tx-{i}"),
                data: vec![0xCDu8; 1024], // 1KB — large instruction
            })
            .collect();

        let annotated_small: Vec<AnnotatedBytes> = small_records
            .iter()
            .map(|r| AnnotatedBytes {
                label: r.label.clone(),
                data: r.data.clone(),
            })
            .collect();

        let annotated_large: Vec<AnnotatedBytes> = large_records
            .iter()
            .map(|r| AnnotatedBytes {
                label: r.label.clone(),
                data: r.data.clone(),
            })
            .collect();

        let schema = binary_schema();
        let iterations = 20;

        // Warm up
        let _ = serde_arrow::to_arrow(schema.fields(), &small_records).unwrap();
        let _ = serde_arrow::to_arrow(schema.fields(), &annotated_small).unwrap();

        // --- Serialization: 64-byte blobs ---
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let _ = serde_arrow::to_arrow(schema.fields(), &small_records).unwrap();
        }
        let plain_small_ser = start.elapsed();

        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let _ = serde_arrow::to_arrow(schema.fields(), &annotated_small).unwrap();
        }
        let annotated_small_ser = start.elapsed();

        // --- Serialization: 1KB blobs ---
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let _ = serde_arrow::to_arrow(schema.fields(), &large_records).unwrap();
        }
        let plain_large_ser = start.elapsed();

        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let _ = serde_arrow::to_arrow(schema.fields(), &annotated_large).unwrap();
        }
        let annotated_large_ser = start.elapsed();

        // --- Deserialization ---
        let small_arrays = serde_arrow::to_arrow(schema.fields(), &small_records).unwrap();
        let large_arrays = serde_arrow::to_arrow(schema.fields(), &large_records).unwrap();

        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let _: Vec<PlainBytes> =
                serde_arrow::from_arrow(schema.fields(), &small_arrays).unwrap();
        }
        let plain_small_de = start.elapsed();

        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let _: Vec<AnnotatedBytes> =
                serde_arrow::from_arrow(schema.fields(), &small_arrays).unwrap();
        }
        let annotated_small_de = start.elapsed();

        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let _: Vec<PlainBytes> =
                serde_arrow::from_arrow(schema.fields(), &large_arrays).unwrap();
        }
        let plain_large_de = start.elapsed();

        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let _: Vec<AnnotatedBytes> =
                serde_arrow::from_arrow(schema.fields(), &large_arrays).unwrap();
        }
        let annotated_large_de = start.elapsed();

        println!(
            "\n=== serde_arrow Binary serialization: 10k records x {iterations} iterations ==="
        );
        println!("                        plain Vec<u8>    serde_bytes      ratio");
        println!(
            "Serialize  64B blobs:   {:>12.1?}    {:>12.1?}    {:.2}x",
            plain_small_ser,
            annotated_small_ser,
            plain_small_ser.as_secs_f64() / annotated_small_ser.as_secs_f64()
        );
        println!(
            "Serialize  1KB blobs:   {:>12.1?}    {:>12.1?}    {:.2}x",
            plain_large_ser,
            annotated_large_ser,
            plain_large_ser.as_secs_f64() / annotated_large_ser.as_secs_f64()
        );
        println!(
            "Deserialize 64B blobs:  {:>12.1?}    {:>12.1?}    {:.2}x",
            plain_small_de,
            annotated_small_de,
            plain_small_de.as_secs_f64() / annotated_small_de.as_secs_f64()
        );
        println!(
            "Deserialize 1KB blobs:  {:>12.1?}    {:>12.1?}    {:.2}x",
            plain_large_de,
            annotated_large_de,
            plain_large_de.as_secs_f64() / annotated_large_de.as_secs_f64()
        );
    }

    /// Compare serde_arrow vs arrow_json serialization approaches.
    /// Note: arrow_json cannot handle Binary fields (non-UTF-8 data fails),
    /// so this test uses only string/integer fields for a fair comparison.
    #[test]
    fn bench_serde_arrow_vs_arrow_json() {
        use arrow::json::ReaderBuilder;

        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct Record {
            signature: String,
            multisig: String,
            discriminator: String,
            account_pubkeys: Vec<String>,
            slot: i64,
            timestamp: i64,
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("signature", DataType::Utf8, false),
            Field::new("multisig", DataType::Utf8, false),
            Field::new("discriminator", DataType::Utf8, false),
            Field::new(
                "account_pubkeys",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
            Field::new("slot", DataType::Int64, false),
            Field::new("timestamp", DataType::Int64, false),
        ]));

        let records: Vec<Record> = (0..10_000)
            .map(|i| Record {
                signature: format!("sig_{i:06}"),
                multisig: format!("msig_{}", i % 100),
                discriminator: "proposal_create".into(),
                account_pubkeys: (0..5).map(|j| format!("pk_{i}_{j}")).collect(),
                slot: 200_000_000 + i,
                timestamp: 1_700_000_000 + i,
            })
            .collect();

        let iterations = 20;

        // Warm up
        let _ = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
        {
            let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder().unwrap();
            decoder.serialize(&records).unwrap();
            let _ = decoder.flush().unwrap();
        }

        // serde_arrow
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let arrays = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
            let _ = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        }
        let serde_arrow_time = start.elapsed();

        // arrow_json ReaderBuilder
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder().unwrap();
            decoder.serialize(&records).unwrap();
            let _ = decoder.flush().unwrap().unwrap();
        }
        let arrow_json_time = start.elapsed();

        println!("\n=== serde_arrow vs arrow_json: 10k records x {iterations} iters ===");
        println!("(strings + integers + list<string>, no binary)");
        println!("serde_arrow:  {:>10.1?}", serde_arrow_time,);
        println!("arrow_json:   {:>10.1?}", arrow_json_time,);
        println!(
            "ratio:        arrow_json is {:.2}x vs serde_arrow",
            arrow_json_time.as_secs_f64() / serde_arrow_time.as_secs_f64()
        );
        println!("\nNote: arrow_json CANNOT serialize Binary fields (fails with non-UTF-8 error)");
    }

    #[test]
    fn plain_vec_u8_to_list_u8_schema() {
        let records = vec![PlainBytes {
            label: "test".into(),
            data: sample_data(),
        }];
        let schema = list_u8_schema();

        let arrays = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), arrays.clone()).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let deserialized: Vec<PlainBytes> =
            serde_arrow::from_arrow(schema.fields(), &arrays).unwrap();
        assert_eq!(deserialized[0].data, sample_data());
        println!("PASS: plain Vec<u8> roundtrips through List<UInt8> schema (baseline)");
    }
}

/// Tests for the `#[prestige(as_binary)]` annotation and default byte-type behavior.
///
/// Validates that:
/// - `[u8; N]` defaults to `FixedSizeList(N, UInt8)` (structural representation)
/// - `Vec<u8>` defaults to `List(UInt8)` (structural representation)
/// - `#[prestige(as_binary)]` opts into `FixedSizeBinary(N)` / `Binary`
/// - `Option<[u8; N]>` and `Option<Vec<u8>>` work with both defaults and as_binary
/// - Edge cases: `[u8; 1]` and `vec![1u8]`
/// - Auto-injected serde_bytes enables round-trip through Parquet
mod as_binary_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, FieldRef};
    use std::sync::Arc;

    // --- Structs using the NEW attribute macro (#[prestige_schema]) ---

    /// as_binary on Vec<u8> → Binary + serde_bytes auto-injected
    #[prestige::prestige_schema]
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct BinaryVec {
        label: String,
        #[prestige(as_binary)]
        data: Vec<u8>,
    }

    /// as_binary on [u8; N] → FixedSizeBinary(N) + serde_bytes auto-injected
    #[prestige::prestige_schema]
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct BinaryFixed {
        label: String,
        #[prestige(as_binary)]
        hash: [u8; 32],
    }

    /// as_binary on Option<Vec<u8>> → nullable Binary
    #[prestige::prestige_schema]
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct BinaryOptionVec {
        label: String,
        #[prestige(as_binary)]
        data: Option<Vec<u8>>,
    }

    /// as_binary on Option<[u8; N]> → nullable FixedSizeBinary(N)
    #[prestige::prestige_schema]
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct BinaryOptionFixed {
        label: String,
        #[prestige(as_binary)]
        mac: Option<[u8; 6]>,
    }

    /// Default Vec<u8> (no as_binary) → List(UInt8)
    #[prestige::prestige_schema]
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct DefaultVec {
        label: String,
        data: Vec<u8>,
    }

    /// Default [u8; N] (no as_binary) → FixedSizeList(N, UInt8)
    #[prestige::prestige_schema]
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct DefaultFixed {
        label: String,
        hash: [u8; 4],
    }

    /// Edge case: [u8; 1] with as_binary → FixedSizeBinary(1)
    #[prestige::prestige_schema]
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct BinarySingleByte {
        label: String,
        #[prestige(as_binary)]
        flag: [u8; 1],
    }

    /// Default Option<[u8; N]> (no as_binary) → nullable List(UInt8)
    #[prestige::prestige_schema]
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct DefaultOptionFixed {
        label: String,
        mac: Option<[u8; 6]>,
    }

    /// Combined: both as_binary and default byte fields
    #[prestige::prestige_schema]
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct MixedBinaryFields {
        label: String,
        #[prestige(as_binary)]
        binary_blob: Vec<u8>,
        list_of_bytes: Vec<u8>,
        #[prestige(as_binary)]
        fixed_hash: [u8; 8],
        default_hash: [u8; 4],
    }

    // --- Schema assertion tests ---

    #[test]
    fn as_binary_vec_u8_schema_is_binary() {
        let schema = BinaryVec::arrow_schema();
        assert_eq!(
            schema.field_with_name("data").unwrap().data_type(),
            &DataType::Binary
        );
    }

    #[test]
    fn as_binary_fixed_schema_is_fixed_size_binary() {
        let schema = BinaryFixed::arrow_schema();
        assert_eq!(
            schema.field_with_name("hash").unwrap().data_type(),
            &DataType::FixedSizeBinary(32)
        );
    }

    #[test]
    fn as_binary_option_vec_schema_is_nullable_binary() {
        let schema = BinaryOptionVec::arrow_schema();
        let field = schema.field_with_name("data").unwrap();
        assert_eq!(field.data_type(), &DataType::Binary);
        assert!(field.is_nullable());
    }

    #[test]
    fn as_binary_option_fixed_schema_is_nullable_fixed_size_binary() {
        let schema = BinaryOptionFixed::arrow_schema();
        let field = schema.field_with_name("mac").unwrap();
        assert_eq!(field.data_type(), &DataType::FixedSizeBinary(6));
        assert!(field.is_nullable());
    }

    #[test]
    fn default_vec_u8_schema_is_list_uint8() {
        let schema = DefaultVec::arrow_schema();
        let expected = DataType::List(FieldRef::new(Field::new("item", DataType::UInt8, true)));
        assert_eq!(
            schema.field_with_name("data").unwrap().data_type(),
            &expected
        );
    }

    #[test]
    fn default_fixed_schema_is_list_uint8() {
        let schema = DefaultFixed::arrow_schema();
        let expected = DataType::List(FieldRef::new(Field::new("item", DataType::UInt8, true)));
        assert_eq!(
            schema.field_with_name("hash").unwrap().data_type(),
            &expected,
            "default [u8; N] maps to List(UInt8) — same as Vec<u8>"
        );
    }

    #[test]
    fn default_option_fixed_schema_is_nullable_list() {
        let schema = DefaultOptionFixed::arrow_schema();
        let field = schema.field_with_name("mac").unwrap();
        let expected = DataType::List(FieldRef::new(Field::new("item", DataType::UInt8, true)));
        assert_eq!(field.data_type(), &expected);
        assert!(field.is_nullable());
    }

    #[test]
    fn single_byte_as_binary_schema() {
        let schema = BinarySingleByte::arrow_schema();
        assert_eq!(
            schema.field_with_name("flag").unwrap().data_type(),
            &DataType::FixedSizeBinary(1)
        );
    }

    #[test]
    fn mixed_fields_schema() {
        let schema = MixedBinaryFields::arrow_schema();
        assert_eq!(
            schema.field_with_name("binary_blob").unwrap().data_type(),
            &DataType::Binary,
            "as_binary Vec<u8> → Binary"
        );
        assert_eq!(
            schema.field_with_name("list_of_bytes").unwrap().data_type(),
            &DataType::List(FieldRef::new(Field::new("item", DataType::UInt8, true))),
            "default Vec<u8> → List(UInt8)"
        );
        assert_eq!(
            schema.field_with_name("fixed_hash").unwrap().data_type(),
            &DataType::FixedSizeBinary(8),
            "as_binary [u8; 8] → FixedSizeBinary(8)"
        );
        assert_eq!(
            schema.field_with_name("default_hash").unwrap().data_type(),
            &DataType::List(FieldRef::new(Field::new("item", DataType::UInt8, true))),
            "default [u8; 4] → List(UInt8)"
        );
    }

    // --- Round-trip serialization tests ---

    #[test]
    fn as_binary_vec_roundtrip() {
        let records = vec![
            BinaryVec {
                label: "a".into(),
                data: vec![0x00, 0xFF, 0x80],
            },
            BinaryVec {
                label: "b".into(),
                data: vec![],
            },
            BinaryVec {
                label: "c".into(),
                data: vec![0x01],
            },
        ];
        let schema = Arc::new(BinaryVec::arrow_schema());
        let arrays = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
        let deserialized: Vec<BinaryVec> =
            serde_arrow::from_arrow(schema.fields(), &arrays).unwrap();
        assert_eq!(records, deserialized);
    }

    #[test]
    fn as_binary_fixed_roundtrip() {
        let records = vec![
            BinaryFixed {
                label: "x".into(),
                hash: [0xAB; 32],
            },
            BinaryFixed {
                label: "y".into(),
                hash: [0; 32],
            },
        ];
        let schema = Arc::new(BinaryFixed::arrow_schema());
        let arrays = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
        let deserialized: Vec<BinaryFixed> =
            serde_arrow::from_arrow(schema.fields(), &arrays).unwrap();
        assert_eq!(records, deserialized);
    }

    #[test]
    fn as_binary_option_vec_roundtrip() {
        let records = vec![
            BinaryOptionVec {
                label: "some".into(),
                data: Some(vec![1, 2, 3]),
            },
            BinaryOptionVec {
                label: "none".into(),
                data: None,
            },
        ];
        let schema = Arc::new(BinaryOptionVec::arrow_schema());
        let arrays = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
        let deserialized: Vec<BinaryOptionVec> =
            serde_arrow::from_arrow(schema.fields(), &arrays).unwrap();
        assert_eq!(records, deserialized);
    }

    #[test]
    fn as_binary_option_fixed_roundtrip() {
        let records = vec![
            BinaryOptionFixed {
                label: "has_mac".into(),
                mac: Some([0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]),
            },
            BinaryOptionFixed {
                label: "no_mac".into(),
                mac: None,
            },
        ];
        let schema = Arc::new(BinaryOptionFixed::arrow_schema());
        let arrays = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
        let deserialized: Vec<BinaryOptionFixed> =
            serde_arrow::from_arrow(schema.fields(), &arrays).unwrap();
        assert_eq!(records, deserialized);
    }

    #[test]
    fn default_vec_u8_roundtrip() {
        let records = vec![
            DefaultVec {
                label: "a".into(),
                data: vec![0, 127, 255],
            },
            DefaultVec {
                label: "b".into(),
                data: vec![],
            },
        ];
        let schema = Arc::new(DefaultVec::arrow_schema());
        let arrays = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
        let deserialized: Vec<DefaultVec> =
            serde_arrow::from_arrow(schema.fields(), &arrays).unwrap();
        assert_eq!(records, deserialized);
    }

    #[test]
    fn default_fixed_roundtrip() {
        let records = vec![
            DefaultFixed {
                label: "a".into(),
                hash: [1, 2, 3, 4],
            },
            DefaultFixed {
                label: "b".into(),
                hash: [0; 4],
            },
        ];
        let schema = Arc::new(DefaultFixed::arrow_schema());
        let arrays = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
        let deserialized: Vec<DefaultFixed> =
            serde_arrow::from_arrow(schema.fields(), &arrays).unwrap();
        assert_eq!(records, deserialized);
    }

    #[test]
    fn single_byte_roundtrip() {
        let records = vec![
            BinarySingleByte {
                label: "on".into(),
                flag: [1],
            },
            BinarySingleByte {
                label: "off".into(),
                flag: [0],
            },
        ];
        let schema = Arc::new(BinarySingleByte::arrow_schema());
        let arrays = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
        let deserialized: Vec<BinarySingleByte> =
            serde_arrow::from_arrow(schema.fields(), &arrays).unwrap();
        assert_eq!(records, deserialized);
    }

    #[test]
    fn default_option_fixed_roundtrip() {
        let records = vec![
            DefaultOptionFixed {
                label: "has".into(),
                mac: Some([0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]),
            },
            DefaultOptionFixed {
                label: "none".into(),
                mac: None,
            },
        ];
        let schema = Arc::new(DefaultOptionFixed::arrow_schema());
        let arrays = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
        let deserialized: Vec<DefaultOptionFixed> =
            serde_arrow::from_arrow(schema.fields(), &arrays).unwrap();
        assert_eq!(records, deserialized);
    }

    #[test]
    fn mixed_fields_roundtrip() {
        let records = vec![MixedBinaryFields {
            label: "test".into(),
            binary_blob: vec![0xDE, 0xAD, 0xBE, 0xEF],
            list_of_bytes: vec![1, 2, 3],
            fixed_hash: [0xFF; 8],
            default_hash: [10, 20, 30, 40],
        }];
        let schema = Arc::new(MixedBinaryFields::arrow_schema());
        let arrays = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
        let deserialized: Vec<MixedBinaryFields> =
            serde_arrow::from_arrow(schema.fields(), &arrays).unwrap();
        assert_eq!(records, deserialized);
    }
}

mod as_fixed_binary_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, FieldRef};
    use std::sync::Arc;

    /// A simple 32-byte address type that implements AsRef<[u8]> + From<[u8; 32]>.
    /// Simulates types like solana_sdk::pubkey::Pubkey.
    #[derive(Debug, Clone, PartialEq)]
    struct Address([u8; 32]);

    impl AsRef<[u8]> for Address {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }

    impl From<[u8; 32]> for Address {
        fn from(bytes: [u8; 32]) -> Self {
            Self(bytes)
        }
    }

    /// A simple 64-byte signature type.
    #[derive(Debug, Clone, PartialEq)]
    struct Signature([u8; 64]);

    impl AsRef<[u8]> for Signature {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }

    impl From<[u8; 64]> for Signature {
        fn from(bytes: [u8; 64]) -> Self {
            Self(bytes)
        }
    }

    // --- Struct definitions ---

    #[prestige::prestige_schema]
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct FixedBinaryScalar {
        label: String,
        #[prestige(as_fixed_binary(32))]
        address: Address,
        #[prestige(as_fixed_binary(64))]
        sig: Signature,
    }

    #[prestige::prestige_schema]
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct FixedBinaryOption {
        label: String,
        #[prestige(as_fixed_binary(32))]
        address: Option<Address>,
    }

    #[prestige::prestige_schema]
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct VecFixedBinary {
        label: String,
        #[prestige(as_vec_fixed_binary(32))]
        addresses: Vec<Address>,
    }

    // --- Schema tests ---

    #[test]
    fn as_fixed_binary_schema_is_fixed_size_binary() {
        let schema = FixedBinaryScalar::arrow_schema();
        assert_eq!(
            schema.field_with_name("address").unwrap().data_type(),
            &DataType::FixedSizeBinary(32)
        );
        assert_eq!(
            schema.field_with_name("sig").unwrap().data_type(),
            &DataType::FixedSizeBinary(64)
        );
    }

    #[test]
    fn as_fixed_binary_option_schema_is_nullable() {
        let schema = FixedBinaryOption::arrow_schema();
        let field = schema.field_with_name("address").unwrap();
        assert_eq!(field.data_type(), &DataType::FixedSizeBinary(32));
        assert!(field.is_nullable());
    }

    #[test]
    fn as_vec_fixed_binary_schema_is_list_fixed_size_binary() {
        let schema = VecFixedBinary::arrow_schema();
        let expected = DataType::List(FieldRef::new(Field::new(
            "item",
            DataType::FixedSizeBinary(32),
            true,
        )));
        assert_eq!(
            schema.field_with_name("addresses").unwrap().data_type(),
            &expected
        );
    }

    // --- Roundtrip tests ---

    #[test]
    fn as_fixed_binary_roundtrip() {
        let records = vec![
            FixedBinaryScalar {
                label: "a".into(),
                address: Address([1u8; 32]),
                sig: Signature([2u8; 64]),
            },
            FixedBinaryScalar {
                label: "b".into(),
                address: Address([0u8; 32]),
                sig: Signature([0xFF; 64]),
            },
        ];
        let schema = Arc::new(FixedBinaryScalar::arrow_schema());
        let arrays = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
        let deserialized: Vec<FixedBinaryScalar> =
            serde_arrow::from_arrow(schema.fields(), &arrays).unwrap();
        assert_eq!(records, deserialized);
    }

    #[test]
    fn as_fixed_binary_option_roundtrip() {
        let records = vec![
            FixedBinaryOption {
                label: "present".into(),
                address: Some(Address([42u8; 32])),
            },
            FixedBinaryOption {
                label: "absent".into(),
                address: None,
            },
        ];
        let schema = Arc::new(FixedBinaryOption::arrow_schema());
        let arrays = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
        let deserialized: Vec<FixedBinaryOption> =
            serde_arrow::from_arrow(schema.fields(), &arrays).unwrap();
        assert_eq!(records, deserialized);
    }

    #[test]
    fn as_vec_fixed_binary_roundtrip() {
        let records = vec![
            VecFixedBinary {
                label: "multi".into(),
                addresses: vec![Address([1u8; 32]), Address([2u8; 32]), Address([3u8; 32])],
            },
            VecFixedBinary {
                label: "empty".into(),
                addresses: vec![],
            },
        ];
        let schema = Arc::new(VecFixedBinary::arrow_schema());
        let arrays = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
        let deserialized: Vec<VecFixedBinary> =
            serde_arrow::from_arrow(schema.fields(), &arrays).unwrap();
        assert_eq!(records, deserialized);
    }
}

/// Tests using the real `solana_pubkey::Pubkey` type to validate the
/// `as_fixed_binary` and `as_vec_fixed_binary` annotations work with
/// actual Solana SDK types — the primary use case for these annotations.
mod solana_pubkey_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, FieldRef};
    use solana_pubkey::Pubkey;
    use std::sync::Arc;

    #[prestige::prestige_schema]
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Instruction {
        label: String,
        #[prestige(as_fixed_binary(32))]
        multisig: Pubkey,
        #[prestige(as_fixed_binary(32))]
        program_id: Pubkey,
        #[prestige(as_fixed_binary(32))]
        vault: Option<Pubkey>,
        #[prestige(as_vec_fixed_binary(32))]
        accounts: Vec<Pubkey>,
    }

    #[test]
    fn pubkey_scalar_schema() {
        let schema = Instruction::arrow_schema();
        assert_eq!(
            schema.field_with_name("multisig").unwrap().data_type(),
            &DataType::FixedSizeBinary(32)
        );
        assert_eq!(
            schema.field_with_name("program_id").unwrap().data_type(),
            &DataType::FixedSizeBinary(32)
        );
    }

    #[test]
    fn pubkey_option_schema() {
        let schema = Instruction::arrow_schema();
        let field = schema.field_with_name("vault").unwrap();
        assert_eq!(field.data_type(), &DataType::FixedSizeBinary(32));
        assert!(field.is_nullable());
    }

    #[test]
    fn pubkey_vec_schema() {
        let schema = Instruction::arrow_schema();
        let expected = DataType::List(FieldRef::new(Field::new(
            "item",
            DataType::FixedSizeBinary(32),
            true,
        )));
        assert_eq!(
            schema.field_with_name("accounts").unwrap().data_type(),
            &expected
        );
    }

    #[test]
    fn pubkey_roundtrip() {
        let pk1 = Pubkey::from([1u8; 32]);
        let pk2 = Pubkey::from([2u8; 32]);
        let pk3 = Pubkey::from([3u8; 32]);
        let program = Pubkey::from([0xAA; 32]);

        let records = vec![
            Instruction {
                label: "with_vault".into(),
                multisig: pk1,
                program_id: program,
                vault: Some(pk2),
                accounts: vec![pk1, pk2, pk3],
            },
            Instruction {
                label: "no_vault".into(),
                multisig: pk3,
                program_id: program,
                vault: None,
                accounts: vec![],
            },
        ];

        let schema = Arc::new(Instruction::arrow_schema());
        let arrays = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
        let deserialized: Vec<Instruction> =
            serde_arrow::from_arrow(schema.fields(), &arrays).unwrap();
        assert_eq!(records, deserialized);
    }

    #[test]
    fn pubkey_random_roundtrip() {
        let records: Vec<Instruction> = (0..10)
            .map(|i| Instruction {
                label: format!("row_{i}"),
                multisig: solana_pubkey::new_rand(),
                program_id: solana_pubkey::new_rand(),
                vault: if i % 2 == 0 {
                    Some(solana_pubkey::new_rand())
                } else {
                    None
                },
                accounts: (0..i).map(|_| solana_pubkey::new_rand()).collect(),
            })
            .collect();

        let schema = Arc::new(Instruction::arrow_schema());
        let arrays = serde_arrow::to_arrow(schema.fields(), &records).unwrap();
        let deserialized: Vec<Instruction> =
            serde_arrow::from_arrow(schema.fields(), &arrays).unwrap();
        assert_eq!(records, deserialized);
    }
}

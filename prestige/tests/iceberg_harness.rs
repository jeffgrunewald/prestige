//! Integration tests using the `IcebergTestHarness`.
//!
//! These tests require a running Polaris + MinIO stack. Start the infrastructure:
//!
//! ```sh
//! docker compose -f prestige/tests/iceberg-compose.yml up -d
//! ```
//!
//! Then run:
//!
//! ```sh
//! ICEBERG_TEST=1 cargo test --features iceberg-test-harness -p prestige --test iceberg_harness
//! ```
//!
//! Tests are skipped when `ICEBERG_TEST` is not set.

#![cfg(feature = "iceberg-test-harness")]

use futures::TryStreamExt;
use prestige::iceberg::{
    self, IcebergTableConfigBuilder, IcebergTestHarness, Reference, scan_since_snapshot,
    scan_table, scan_with_filter, write_and_commit,
};
use std::pin::pin;

/// Skip the test if ICEBERG_TEST env var is not set.
fn require_iceberg_env() -> bool {
    std::env::var("ICEBERG_TEST").is_ok()
}

// ---------------------------------------------------------------------------
// Test struct — SensorReading
// ---------------------------------------------------------------------------

#[prestige::prestige_schema]
#[derive(Debug, Clone, PartialEq)]
struct SensorReading {
    #[prestige(identifier)]
    sensor_id: String,

    #[prestige(identifier, sort_key)]
    timestamp: i64,

    temperature: f64,
    humidity: Option<f64>,
    location: String,
}

fn make_readings(count: usize) -> Vec<SensorReading> {
    (0..count)
        .map(|i| SensorReading {
            sensor_id: format!("sensor-{:04}", i % 10),
            timestamp: 1700000000 + (i as i64 * 60),
            temperature: 20.0 + (i as f64 * 0.1),
            humidity: if i % 3 == 0 {
                None
            } else {
                Some(45.0 + i as f64)
            },
            location: format!("zone-{}", i % 4),
        })
        .collect()
}

/// Collect all records from a scan stream.
async fn collect_readings(stream: iceberg::IcebergRecordBatchStream) -> Vec<SensorReading> {
    let mut pinned = pin!(stream);
    let mut all = Vec::new();

    while let Some(batch) = pinned.try_next().await.expect("scan batch") {
        let records: Vec<SensorReading> = serde_arrow::from_arrow(
            batch.schema().fields(),
            &(0..batch.num_columns())
                .map(|i| batch.column(i).clone())
                .collect::<Vec<_>>(),
        )
        .expect("deserialize SensorReading");
        all.extend(records);
    }

    all
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_harness_creates_isolated_catalog() {
    if !require_iceberg_env() {
        return;
    }

    let h1 = IcebergTestHarness::new().await.expect("harness 1");
    let h2 = IcebergTestHarness::new().await.expect("harness 2");

    assert_ne!(
        h1.catalog_name(),
        h2.catalog_name(),
        "each harness should get a unique catalog"
    );
    assert!(h1.catalog_name().starts_with("test_"));
    assert!(h2.catalog_name().starts_with("test_"));
}

#[tokio::test]
async fn test_ensure_table_creates_from_struct() {
    if !require_iceberg_env() {
        return;
    }

    let harness = IcebergTestHarness::new().await.expect("harness");

    let config = IcebergTableConfigBuilder::default()
        .namespace(vec!["default".into()])
        .name("sensor_readings".into())
        .build()
        .expect("config");

    let result = harness
        .ensure_table_with::<SensorReading>(&config, SensorReading::identifier_field_names())
        .await
        .expect("ensure_table");

    assert!(
        matches!(result, iceberg::EnsureTableResult::Created(_)),
        "expected table creation"
    );

    let table = result.into_table();
    let schema = table.metadata().current_schema();
    assert!(schema.field_by_name("sensor_id").is_some());
    assert!(schema.field_by_name("timestamp").is_some());
    assert!(schema.field_by_name("temperature").is_some());
    assert!(schema.field_by_name("humidity").is_some());
    assert!(schema.field_by_name("location").is_some());
}

#[tokio::test]
async fn test_ensure_table_idempotent() {
    if !require_iceberg_env() {
        return;
    }

    let harness = IcebergTestHarness::new().await.expect("harness");

    let config = IcebergTableConfigBuilder::default()
        .namespace(vec!["default".into()])
        .name("sensor_idempotent".into())
        .build()
        .expect("config");

    let ids = SensorReading::identifier_field_names();

    harness
        .ensure_table_with::<SensorReading>(&config, ids)
        .await
        .expect("first ensure");

    let result = harness
        .ensure_table_with::<SensorReading>(&config, ids)
        .await
        .expect("second ensure");

    assert!(
        matches!(result, iceberg::EnsureTableResult::UpToDate(_)),
        "second call should be UpToDate"
    );
}

#[tokio::test]
async fn test_direct_writer_roundtrip() {
    if !require_iceberg_env() {
        return;
    }

    let harness = IcebergTestHarness::new().await.expect("harness");

    let config = IcebergTableConfigBuilder::default()
        .namespace(vec!["default".into()])
        .name("writer_roundtrip".into())
        .build()
        .expect("config");

    let table = harness
        .ensure_table_with::<SensorReading>(&config, SensorReading::identifier_field_names())
        .await
        .expect("ensure")
        .into_table();

    let writer = harness.writer::<SensorReading>(&table);
    let readings = make_readings(10);
    writer.write_all(readings.clone()).await.expect("write_all");

    // Reload table to see new snapshot
    let table = iceberg::load_table(harness.catalog(), &["default".into()], "writer_roundtrip")
        .await
        .expect("reload");

    let scanned = collect_readings(scan_table(&table).await.expect("scan")).await;

    assert_eq!(scanned.len(), readings.len());
    for (original, scanned) in readings.iter().zip(scanned.iter()) {
        assert_eq!(original.sensor_id, scanned.sensor_id);
        assert_eq!(original.timestamp, scanned.timestamp);
        assert!((original.temperature - scanned.temperature).abs() < f64::EPSILON);
        assert_eq!(original.humidity, scanned.humidity);
        assert_eq!(original.location, scanned.location);
    }
}

#[tokio::test]
async fn test_write_single_via_trait() {
    if !require_iceberg_env() {
        return;
    }

    let harness = IcebergTestHarness::new().await.expect("harness");

    let config = IcebergTableConfigBuilder::default()
        .namespace(vec!["default".into()])
        .name("writer_single".into())
        .build()
        .expect("config");

    let table = harness
        .ensure_table_with::<SensorReading>(&config, SensorReading::identifier_field_names())
        .await
        .expect("ensure")
        .into_table();

    let writer = harness.writer::<SensorReading>(&table);

    let reading = SensorReading {
        sensor_id: "sensor-0001".into(),
        timestamp: 1700000000,
        temperature: 22.5,
        humidity: Some(55.0),
        location: "zone-a".into(),
    };

    // write() goes through the DataWriter trait
    writer.write(reading.clone()).await.expect("write single");

    let table = iceberg::load_table(harness.catalog(), &["default".into()], "writer_single")
        .await
        .expect("reload");

    let scanned = collect_readings(scan_table(&table).await.expect("scan")).await;
    assert_eq!(scanned.len(), 1);
    assert_eq!(scanned[0].sensor_id, reading.sensor_id);
    assert_eq!(scanned[0].timestamp, reading.timestamp);
}

#[tokio::test]
async fn test_write_and_commit_convenience() {
    if !require_iceberg_env() {
        return;
    }

    let harness = IcebergTestHarness::new().await.expect("harness");

    let config = IcebergTableConfigBuilder::default()
        .namespace(vec!["default".into()])
        .name("write_commit".into())
        .build()
        .expect("config");

    let table = harness
        .ensure_table_with::<SensorReading>(&config, SensorReading::identifier_field_names())
        .await
        .expect("ensure")
        .into_table();

    let readings = make_readings(25);

    let updated = write_and_commit(
        &table,
        harness.catalog().as_iceberg_catalog().as_ref(),
        &readings,
        None,
        None,
    )
    .await
    .expect("write_and_commit");

    let scanned = collect_readings(scan_table(&updated).await.expect("scan")).await;
    assert_eq!(scanned.len(), 25);
}

#[tokio::test]
async fn test_write_and_commit_empty_is_noop() {
    if !require_iceberg_env() {
        return;
    }

    let harness = IcebergTestHarness::new().await.expect("harness");

    let config = IcebergTableConfigBuilder::default()
        .namespace(vec!["default".into()])
        .name("write_commit_empty".into())
        .build()
        .expect("config");

    let table = harness
        .ensure_table_with::<SensorReading>(&config, SensorReading::identifier_field_names())
        .await
        .expect("ensure")
        .into_table();

    let empty: &[SensorReading] = &[];
    let updated = write_and_commit(
        &table,
        harness.catalog().as_iceberg_catalog().as_ref(),
        empty,
        None,
        None,
    )
    .await
    .expect("write_and_commit empty");

    // No snapshot should have been created
    assert!(
        updated.metadata().current_snapshot().is_none(),
        "empty write should not create a snapshot"
    );
}

#[tokio::test]
async fn test_incremental_scan() {
    if !require_iceberg_env() {
        return;
    }

    let harness = IcebergTestHarness::new().await.expect("harness");

    let config = IcebergTableConfigBuilder::default()
        .namespace(vec!["default".into()])
        .name("incremental".into())
        .build()
        .expect("config");

    let table = harness
        .ensure_table_with::<SensorReading>(&config, SensorReading::identifier_field_names())
        .await
        .expect("ensure")
        .into_table();

    let catalog = harness.catalog();
    let catalog_ref = catalog.as_iceberg_catalog();

    // Write batch 1
    let batch1 = make_readings(5);
    let table = write_and_commit(&table, catalog_ref.as_ref(), &batch1, None, None)
        .await
        .expect("batch 1");

    let snapshot1_id = table
        .metadata()
        .current_snapshot()
        .expect("snapshot after batch 1")
        .snapshot_id();

    // Write batch 2
    let batch2: Vec<SensorReading> = (100..108)
        .map(|i| SensorReading {
            sensor_id: format!("new-sensor-{i}"),
            timestamp: 1800000000 + i as i64,
            temperature: 30.0,
            humidity: Some(60.0),
            location: "zone-x".into(),
        })
        .collect();

    let table = write_and_commit(&table, catalog_ref.as_ref(), &batch2, None, None)
        .await
        .expect("batch 2");

    // Incremental scan since snapshot1 should return only batch2 records
    let incremental = collect_readings(
        scan_since_snapshot(&table, snapshot1_id)
            .await
            .expect("incremental"),
    )
    .await;

    assert_eq!(
        incremental.len(),
        batch2.len(),
        "incremental scan should return only new records"
    );

    for record in &incremental {
        assert!(
            record.sensor_id.starts_with("new-sensor-"),
            "incremental results should only contain batch2 records, got: {}",
            record.sensor_id
        );
    }

    // Full scan should return everything
    let full = collect_readings(scan_table(&table).await.expect("full scan")).await;
    assert_eq!(
        full.len(),
        batch1.len() + batch2.len(),
        "full scan should return all records"
    );
}

#[tokio::test]
async fn test_predicate_pushdown() {
    if !require_iceberg_env() {
        return;
    }

    let harness = IcebergTestHarness::new().await.expect("harness");

    let config = IcebergTableConfigBuilder::default()
        .namespace(vec!["default".into()])
        .name("predicate".into())
        .build()
        .expect("config");

    let table = harness
        .ensure_table_with::<SensorReading>(&config, SensorReading::identifier_field_names())
        .await
        .expect("ensure")
        .into_table();

    // Write a mix of locations
    let readings: Vec<SensorReading> = (0..20)
        .map(|i| SensorReading {
            sensor_id: format!("s-{i}"),
            timestamp: 1700000000 + i as i64,
            temperature: 20.0 + i as f64,
            humidity: Some(50.0),
            location: if i < 12 {
                "warehouse-a".into()
            } else {
                "warehouse-b".into()
            },
        })
        .collect();

    let table = write_and_commit(
        &table,
        harness.catalog().as_iceberg_catalog().as_ref(),
        &readings,
        None,
        None,
    )
    .await
    .expect("write");

    // Filter to location = "warehouse-b"
    let filter = Reference::new("location").equal_to(iceberg::Datum::string("warehouse-b"));
    let filtered = collect_readings(
        scan_with_filter(&table, filter)
            .await
            .expect("filtered scan"),
    )
    .await;

    assert_eq!(filtered.len(), 8, "should return only warehouse-b records");
    for record in &filtered {
        assert_eq!(record.location, "warehouse-b");
    }
}

#[tokio::test]
async fn test_nullable_field_roundtrip() {
    if !require_iceberg_env() {
        return;
    }

    let harness = IcebergTestHarness::new().await.expect("harness");

    let config = IcebergTableConfigBuilder::default()
        .namespace(vec!["default".into()])
        .name("nullable".into())
        .build()
        .expect("config");

    let table = harness
        .ensure_table_with::<SensorReading>(&config, SensorReading::identifier_field_names())
        .await
        .expect("ensure")
        .into_table();

    let readings = vec![
        SensorReading {
            sensor_id: "s-1".into(),
            timestamp: 1700000000,
            temperature: 22.0,
            humidity: Some(55.5),
            location: "lab".into(),
        },
        SensorReading {
            sensor_id: "s-2".into(),
            timestamp: 1700000060,
            temperature: 23.0,
            humidity: None, // null value
            location: "lab".into(),
        },
        SensorReading {
            sensor_id: "s-3".into(),
            timestamp: 1700000120,
            temperature: 21.5,
            humidity: Some(0.0), // zero is different from null
            location: "lab".into(),
        },
    ];

    let table = write_and_commit(
        &table,
        harness.catalog().as_iceberg_catalog().as_ref(),
        &readings,
        None,
        None,
    )
    .await
    .expect("write");

    let scanned = collect_readings(scan_table(&table).await.expect("scan")).await;
    assert_eq!(scanned.len(), 3);

    // Verify null handling: Some, None, and Some(0.0)
    let by_id: std::collections::HashMap<&str, &SensorReading> =
        scanned.iter().map(|r| (r.sensor_id.as_str(), r)).collect();

    assert_eq!(by_id["s-1"].humidity, Some(55.5));
    assert_eq!(by_id["s-2"].humidity, None);
    assert_eq!(by_id["s-3"].humidity, Some(0.0));
}

#[tokio::test]
async fn test_multiple_writes_accumulate() {
    if !require_iceberg_env() {
        return;
    }

    let harness = IcebergTestHarness::new().await.expect("harness");

    let config = IcebergTableConfigBuilder::default()
        .namespace(vec!["default".into()])
        .name("accumulate".into())
        .build()
        .expect("config");

    let table = harness
        .ensure_table_with::<SensorReading>(&config, SensorReading::identifier_field_names())
        .await
        .expect("ensure")
        .into_table();

    let writer = harness.writer::<SensorReading>(&table);

    // Three separate writes, each creating a new snapshot
    for batch_idx in 0..3 {
        let batch: Vec<SensorReading> = (0..5)
            .map(|i| SensorReading {
                sensor_id: format!("batch{batch_idx}-{i}"),
                timestamp: 1700000000 + (batch_idx * 1000 + i) as i64,
                temperature: 20.0 + batch_idx as f64,
                humidity: Some(50.0),
                location: "warehouse".into(),
            })
            .collect();
        writer.write_all(batch).await.expect("write batch");
    }

    let table = iceberg::load_table(harness.catalog(), &["default".into()], "accumulate")
        .await
        .expect("reload");

    let all = collect_readings(scan_table(&table).await.expect("scan")).await;
    assert_eq!(all.len(), 15, "3 batches * 5 records = 15 total");

    // Verify all three batches are present
    let batch0_count = all
        .iter()
        .filter(|r| r.sensor_id.starts_with("batch0"))
        .count();
    let batch1_count = all
        .iter()
        .filter(|r| r.sensor_id.starts_with("batch1"))
        .count();
    let batch2_count = all
        .iter()
        .filter(|r| r.sensor_id.starts_with("batch2"))
        .count();

    assert_eq!(batch0_count, 5);
    assert_eq!(batch1_count, 5);
    assert_eq!(batch2_count, 5);
}

#[tokio::test]
async fn test_compaction_via_harness() {
    if !require_iceberg_env() {
        return;
    }

    let harness = IcebergTestHarness::new().await.expect("harness");

    let config = IcebergTableConfigBuilder::default()
        .namespace(vec!["default".into()])
        .name("compact".into())
        .build()
        .expect("config");

    let table = harness
        .ensure_table_with::<SensorReading>(&config, SensorReading::identifier_field_names())
        .await
        .expect("ensure")
        .into_table();

    let writer = harness.writer::<SensorReading>(&table);

    // Write 6 separate snapshots to create 6 small files
    for i in 0..6 {
        let batch = vec![SensorReading {
            sensor_id: format!("s-{i}"),
            timestamp: 1700000000 + i as i64,
            temperature: 20.0 + i as f64,
            humidity: Some(50.0),
            location: "lab".into(),
        }];
        writer.write_all(batch).await.expect("write");
    }

    let table = iceberg::load_table(harness.catalog(), &["default".into()], "compact")
        .await
        .expect("reload");

    let compact_config = iceberg::IcebergCompactorConfigBuilder::default()
        .table(table)
        .catalog(harness.catalog().clone())
        .target_file_size_bytes(100 * 1024 * 1024_usize)
        .min_files_to_compact(5_usize)
        .deduplicate(false)
        .compression(parquet::basic::Compression::SNAPPY)
        .build()
        .expect("compactor config");

    let result = compact_config.execute().await.expect("compaction");

    assert_eq!(result.files_read, 6);
    assert_eq!(result.records_consolidated, 6);
    assert!(result.files_written > 0);
    assert_eq!(result.duplicates_eliminated, 0);
}

#[tokio::test]
async fn test_compaction_with_dedup_via_harness() {
    if !require_iceberg_env() {
        return;
    }

    let harness = IcebergTestHarness::new().await.expect("harness");

    let config = IcebergTableConfigBuilder::default()
        .namespace(vec!["default".into()])
        .name("compact_dedup".into())
        .build()
        .expect("config");

    let table = harness
        .ensure_table_with::<SensorReading>(&config, SensorReading::identifier_field_names())
        .await
        .expect("ensure")
        .into_table();

    let writer = harness.writer::<SensorReading>(&table);

    // Write the same record 6 times across separate snapshots
    let duplicate = SensorReading {
        sensor_id: "dup-sensor".into(),
        timestamp: 1700000000,
        temperature: 25.0,
        humidity: Some(60.0),
        location: "lab".into(),
    };

    for _ in 0..6 {
        writer.write(duplicate.clone()).await.expect("write dup");
    }

    let table = iceberg::load_table(harness.catalog(), &["default".into()], "compact_dedup")
        .await
        .expect("reload");

    let compact_config = iceberg::IcebergCompactorConfigBuilder::default()
        .table(table)
        .catalog(harness.catalog().clone())
        .target_file_size_bytes(100 * 1024 * 1024_usize)
        .min_files_to_compact(5_usize)
        .deduplicate(true)
        .compression(parquet::basic::Compression::SNAPPY)
        .build()
        .expect("compactor config");

    let result = compact_config.execute().await.expect("compaction");

    assert_eq!(result.files_read, 6);
    assert_eq!(result.records_consolidated, 1, "only 1 unique record");
    assert_eq!(result.duplicates_eliminated, 5);
}

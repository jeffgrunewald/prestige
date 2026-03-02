//! Integration tests for prestige's iceberg module.
//!
//! These tests require a running Polaris + MinIO stack. Start the infrastructure with:
//!
//! ```sh
//! docker compose -f prestige/tests/iceberg-compose.yml up -d
//! ```
//!
//! Then run:
//!
//! ```sh
//! ICEBERG_TEST=1 cargo test --features iceberg -p prestige --test iceberg_integration
//! ```
//!
//! Tests are skipped when `ICEBERG_TEST` is not set.

#![cfg(feature = "iceberg")]

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use futures::TryStreamExt;
use parquet::basic::Compression;
use std::pin::pin;
use std::sync::Arc;

/// Skip the test if ICEBERG_TEST env var is not set.
fn require_iceberg_env() -> bool {
    std::env::var("ICEBERG_TEST").is_ok()
}

/// Default test catalog URI (Polaris).
fn catalog_uri() -> String {
    std::env::var("ICEBERG_CATALOG_URI").unwrap_or_else(|_| "http://localhost:8181".to_string())
}

/// Default warehouse name.
fn warehouse() -> String {
    std::env::var("ICEBERG_WAREHOUSE").unwrap_or_else(|_| "iceberg-test".to_string())
}

fn s3_config() -> prestige::iceberg::S3Config {
    prestige::iceberg::S3Config {
        endpoint: Some(
            std::env::var("AWS_ENDPOINT_URL").unwrap_or_else(|_| "http://localhost:9000".into()),
        ),
        access_key_id: Some(
            std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "admin".into()),
        ),
        secret_access_key: Some(
            std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "password".into()),
        ),
        region: Some(std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".into())),
        path_style_access: Some(true),
    }
}

fn auth_config() -> prestige::iceberg::AuthConfig {
    prestige::iceberg::AuthConfig {
        token: None,
        credential: Some("root:s3cr3t".to_string()),
        oauth2_server_uri: None,
        scope: None,
        audience: None,
        resource: None,
    }
}

async fn connect() -> prestige::iceberg::Catalog {
    let config = prestige::iceberg::CatalogConfig::builder(catalog_uri(), "polaris".to_string())
        .warehouse(warehouse())
        .s3(s3_config())
        .auth(auth_config())
        .build();

    prestige::iceberg::connect_catalog(&config)
        .await
        .expect("failed to connect to catalog")
}

/// Generate a unique table name to avoid test collision.
fn unique_table_name(prefix: &str) -> String {
    let uuid = uuid::Uuid::new_v4();
    format!("{}_{}", prefix, uuid.simple())
}

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn make_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_catalog_connection() {
    if !require_iceberg_env() {
        eprintln!("ICEBERG_TEST not set, skipping");
        return;
    }

    let _catalog = connect().await;
}

#[tokio::test]
async fn test_create_table_and_scan() {
    if !require_iceberg_env() {
        eprintln!("ICEBERG_TEST not set, skipping");
        return;
    }

    let catalog = connect().await;
    let table_name = unique_table_name("scan");
    let iceberg_schema =
        prestige::iceberg::arrow_to_iceberg_schema(&test_schema()).expect("schema conversion");

    let table_config = prestige::iceberg::IcebergTableConfigBuilder::default()
        .namespace(vec!["default".to_string()])
        .name(table_name.clone())
        .build()
        .expect("table config");

    let table =
        prestige::iceberg::create_table_if_not_exists(&catalog, &table_config, iceberg_schema)
            .await
            .expect("create table");

    // Write data
    let batch = make_batch(&[1, 2, 3], &["alice", "bob", "carol"]);
    let data_files =
        prestige::iceberg::write_data_files(&table, vec![batch], Some(Compression::SNAPPY))
            .await
            .expect("write data files");

    assert!(!data_files.is_empty(), "should produce at least one data file");

    let updated_table = prestige::iceberg::commit_data_files(
        &table,
        catalog.as_iceberg_catalog().as_ref(),
        data_files,
        None,
    )
    .await
    .expect("commit");

    // Scan and verify
    let stream = prestige::iceberg::scan_table(&updated_table)
        .await
        .expect("scan");
    let mut pinned = pin!(stream);

    let mut total_rows = 0usize;
    while let Some(batch) = pinned.try_next().await.expect("next batch") {
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 3, "should read back 3 rows");
}

#[tokio::test]
async fn test_write_and_compact() {
    if !require_iceberg_env() {
        eprintln!("ICEBERG_TEST not set, skipping");
        return;
    }

    let catalog = connect().await;
    let table_name = unique_table_name("compact");
    let iceberg_schema =
        prestige::iceberg::arrow_to_iceberg_schema(&test_schema()).expect("schema conversion");

    let table_config = prestige::iceberg::IcebergTableConfigBuilder::default()
        .namespace(vec!["default".to_string()])
        .name(table_name.clone())
        .build()
        .expect("table config");

    let mut table =
        prestige::iceberg::create_table_if_not_exists(&catalog, &table_config, iceberg_schema)
            .await
            .expect("create table");

    // Write multiple small batches as separate snapshots to create many small files
    for i in 0..6 {
        let batch = make_batch(&[i * 10, i * 10 + 1], &["x", "y"]);
        let data_files =
            prestige::iceberg::write_data_files(&table, vec![batch], Some(Compression::SNAPPY))
                .await
                .expect("write");

        table = prestige::iceberg::commit_data_files(
            &table,
            catalog.as_iceberg_catalog().as_ref(),
            data_files,
            None,
        )
        .await
        .expect("commit");
    }

    // Compact (min_files=5, we have 6 files)
    let compact_config = prestige::iceberg::IcebergCompactorConfigBuilder::default()
        .table(table)
        .catalog(catalog.clone())
        .target_file_size_bytes(100 * 1024 * 1024usize)
        .min_files_to_compact(5usize)
        .deduplicate(false)
        .compression(Compression::SNAPPY)
        .build()
        .expect("compactor config");

    let result = compact_config.execute().await.expect("compaction");

    assert_eq!(result.files_read, 6, "should read 6 original files");
    assert!(result.files_written > 0, "should produce compacted files");
    assert_eq!(result.records_consolidated, 12, "12 total records");
    assert_eq!(result.duplicates_eliminated, 0, "no dedup requested");
}

#[tokio::test]
async fn test_compact_with_deduplication() {
    if !require_iceberg_env() {
        eprintln!("ICEBERG_TEST not set, skipping");
        return;
    }

    let catalog = connect().await;
    let table_name = unique_table_name("dedup");
    let iceberg_schema =
        prestige::iceberg::arrow_to_iceberg_schema(&test_schema()).expect("schema conversion");

    let table_config = prestige::iceberg::IcebergTableConfigBuilder::default()
        .namespace(vec!["default".to_string()])
        .name(table_name.clone())
        .build()
        .expect("table config");

    let mut table =
        prestige::iceberg::create_table_if_not_exists(&catalog, &table_config, iceberg_schema)
            .await
            .expect("create table");

    // Write same data 6 times to create duplicates
    for _ in 0..6 {
        let batch = make_batch(&[1, 2], &["alice", "bob"]);
        let data_files =
            prestige::iceberg::write_data_files(&table, vec![batch], Some(Compression::SNAPPY))
                .await
                .expect("write");

        table = prestige::iceberg::commit_data_files(
            &table,
            catalog.as_iceberg_catalog().as_ref(),
            data_files,
            None,
        )
        .await
        .expect("commit");
    }

    let compact_config = prestige::iceberg::IcebergCompactorConfigBuilder::default()
        .table(table.clone())
        .catalog(catalog.clone())
        .target_file_size_bytes(100 * 1024 * 1024usize)
        .min_files_to_compact(5usize)
        .deduplicate(true)
        .compression(Compression::SNAPPY)
        .build()
        .expect("compactor config");

    let result = compact_config.execute().await.expect("compaction");

    assert_eq!(result.files_read, 6);
    assert_eq!(result.records_consolidated, 2, "only 2 unique rows remain");
    assert_eq!(
        result.duplicates_eliminated, 10,
        "10 duplicate rows eliminated"
    );
}

#[tokio::test]
async fn test_wap_transaction_lifecycle() {
    if !require_iceberg_env() {
        eprintln!("ICEBERG_TEST not set, skipping");
        return;
    }

    let catalog = connect().await;
    let table_name = unique_table_name("wap");
    let iceberg_schema =
        prestige::iceberg::arrow_to_iceberg_schema(&test_schema()).expect("schema conversion");

    let table_config = prestige::iceberg::IcebergTableConfigBuilder::default()
        .namespace(vec!["default".to_string()])
        .name(table_name.clone())
        .wap_enabled(true)
        .build()
        .expect("table config");

    let table =
        prestige::iceberg::create_table_if_not_exists(&catalog, &table_config, iceberg_schema)
            .await
            .expect("create table");

    // Seed the table with an initial snapshot (WAP requires at least one snapshot for branching)
    let seed_batch = make_batch(&[0], &["seed"]);
    let data_files =
        prestige::iceberg::write_data_files(&table, vec![seed_batch], Some(Compression::SNAPPY))
            .await
            .expect("seed write");

    let table = prestige::iceberg::commit_data_files(
        &table,
        catalog.as_iceberg_catalog().as_ref(),
        data_files,
        None,
    )
    .await
    .expect("seed commit");

    // Begin WAP transaction
    let wap_id = format!("wap-{}", uuid::Uuid::new_v4().simple());
    let mut wap = prestige::iceberg::WapTransaction::begin(catalog.clone(), table, &wap_id)
        .await
        .expect("WAP begin");

    // Should be in Writer state
    let batch = make_batch(&[10, 20], &["hello", "world"]);
    let files_written = wap.write(vec![batch]).await.expect("WAP write");
    assert!(files_written > 0, "should write at least one file");

    // Now in Publisher state — publish to main
    wap.publish().await.expect("WAP publish");
}

#[tokio::test]
async fn test_wap_idempotent_resume() {
    if !require_iceberg_env() {
        eprintln!("ICEBERG_TEST not set, skipping");
        return;
    }

    let catalog = connect().await;
    let table_name = unique_table_name("wap_resume");
    let iceberg_schema =
        prestige::iceberg::arrow_to_iceberg_schema(&test_schema()).expect("schema conversion");

    let table_config = prestige::iceberg::IcebergTableConfigBuilder::default()
        .namespace(vec!["default".to_string()])
        .name(table_name.clone())
        .wap_enabled(true)
        .build()
        .expect("table config");

    let table =
        prestige::iceberg::create_table_if_not_exists(&catalog, &table_config, iceberg_schema)
            .await
            .expect("create table");

    // Seed table
    let seed_batch = make_batch(&[0], &["seed"]);
    let data_files =
        prestige::iceberg::write_data_files(&table, vec![seed_batch], Some(Compression::SNAPPY))
            .await
            .expect("seed write");

    let table = prestige::iceberg::commit_data_files(
        &table,
        catalog.as_iceberg_catalog().as_ref(),
        data_files,
        None,
    )
    .await
    .expect("seed commit");

    let wap_id = format!("wap-{}", uuid::Uuid::new_v4().simple());

    // First attempt: write but don't publish
    let mut wap = prestige::iceberg::WapTransaction::begin(catalog.clone(), table.clone(), &wap_id)
        .await
        .expect("WAP begin");

    let batch = make_batch(&[42], &["answer"]);
    wap.write(vec![batch]).await.expect("WAP write");
    // Drop without publishing — simulates crash

    // Resume with same wap_id — should detect WrittenNotPublished
    let wap2 = prestige::iceberg::WapTransaction::begin(catalog.clone(), table, &wap_id)
        .await
        .expect("WAP resume");

    // Should be in Publisher state (data already on branch)
    wap2.publish().await.expect("WAP publish on resume");

    // Calling begin again should detect AlreadyPublished
    let ns_parts: Vec<String> = vec!["default".to_string()];
    let reloaded = prestige::iceberg::load_table(&catalog, &ns_parts, &table_name)
        .await
        .expect("reload");

    let wap3 = prestige::iceberg::WapTransaction::begin(catalog.clone(), reloaded, &wap_id)
        .await
        .expect("WAP already published");

    assert!(
        matches!(wap3, prestige::iceberg::WapTransaction::Complete),
        "should be Complete after publish"
    );
}

// ---------------------------------------------------------------------------
// Schema reconciliation integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ensure_table_create() {
    if !require_iceberg_env() {
        eprintln!("ICEBERG_TEST not set, skipping");
        return;
    }

    let catalog = connect().await;
    let table_name = unique_table_name("ensure_create");

    let table_config = prestige::iceberg::IcebergTableConfigBuilder::default()
        .namespace(vec!["default".to_string()])
        .name(table_name.clone())
        .build()
        .expect("table config");

    let result = prestige::iceberg::ensure_table(&catalog, &table_config, &test_schema(), &["id"])
        .await
        .expect("ensure_table");

    assert!(
        matches!(result, prestige::iceberg::EnsureTableResult::Created(_)),
        "expected Created variant"
    );

    let table = result.into_table();
    let schema = table.metadata().current_schema();
    assert!(schema.field_by_name("id").is_some());
    assert!(schema.field_by_name("name").is_some());
}

#[tokio::test]
async fn test_ensure_table_up_to_date() {
    if !require_iceberg_env() {
        eprintln!("ICEBERG_TEST not set, skipping");
        return;
    }

    let catalog = connect().await;
    let table_name = unique_table_name("ensure_noop");

    let table_config = prestige::iceberg::IcebergTableConfigBuilder::default()
        .namespace(vec!["default".to_string()])
        .name(table_name.clone())
        .build()
        .expect("table config");

    // Create the table.
    prestige::iceberg::ensure_table(&catalog, &table_config, &test_schema(), &["id"])
        .await
        .expect("first ensure");

    // Calling again with the same schema should be a no-op.
    let result = prestige::iceberg::ensure_table(&catalog, &table_config, &test_schema(), &["id"])
        .await
        .expect("second ensure");

    assert!(
        matches!(result, prestige::iceberg::EnsureTableResult::UpToDate(_)),
        "expected UpToDate variant"
    );
}

#[tokio::test]
async fn test_ensure_table_evolve_schema() {
    if !require_iceberg_env() {
        eprintln!("ICEBERG_TEST not set, skipping");
        return;
    }

    let catalog = connect().await;
    let table_name = unique_table_name("ensure_evolve");

    let table_config = prestige::iceberg::IcebergTableConfigBuilder::default()
        .namespace(vec!["default".to_string()])
        .name(table_name.clone())
        .build()
        .expect("table config");

    // Create with original schema: (id, name).
    prestige::iceberg::ensure_table(&catalog, &table_config, &test_schema(), &["id"])
        .await
        .expect("initial create");

    // Evolve: drop "name", add "email" and "score".
    let evolved_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));

    let result = prestige::iceberg::ensure_table(&catalog, &table_config, &evolved_schema, &["id"])
        .await
        .expect("evolve");

    match result {
        prestige::iceberg::EnsureTableResult::Evolved {
            table,
            columns_added,
            columns_dropped,
        } => {
            assert_eq!(columns_added, vec!["email", "score"]);
            assert_eq!(columns_dropped, vec!["name"]);

            let schema = table.metadata().current_schema();
            assert!(schema.field_by_name("id").is_some(), "id should remain");
            assert!(schema.field_by_name("email").is_some(), "email should be added");
            assert!(schema.field_by_name("score").is_some(), "score should be added");
            assert!(schema.field_by_name("name").is_none(), "name should be dropped");
        }
        other => panic!("expected Evolved, got {other:?}"),
    }
}

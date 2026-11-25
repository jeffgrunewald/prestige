//! PostgreSQL `FilePollerState` tests, skipped unless `POSTGRES_TEST` is set.
//! Start: `docker compose -f prestige/tests/docker-compose.yml up -d --wait postgres`
//! Run: `POSTGRES_TEST=1 cargo test --features sqlx -p prestige --test postgres_state`
//! `DATABASE_URL` overrides the default URL. Each test uses its own schema.

#![cfg(feature = "sqlx")]

use prestige::{
    FileMeta, FilePollerState, FilePollerStateRecorder,
    jiff::{SignedDuration, Timestamp},
};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::str::FromStr;

const PROCESS: &str = "test-process";

fn require_postgres_env() -> bool {
    std::env::var("POSTGRES_TEST").is_ok()
}

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_string())
}

/// A pool whose connections resolve `files_processed` in a new, empty schema.
struct TestDb {
    pool: PgPool,
    admin: PgPool,
    schema: String,
}

impl TestDb {
    async fn new() -> Self {
        let schema = format!("prestige_test_{}", uuid::Uuid::new_v4().simple());
        let admin = PgPool::connect(&database_url()).await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();

        let options = PgConnectOptions::from_str(&database_url())
            .unwrap()
            .options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new().connect_with(options).await.unwrap();
        sqlx::query(
            r#"
            CREATE TABLE files_processed (
                process_name TEXT NOT NULL DEFAULT 'default',
                file_name VARCHAR PRIMARY KEY,
                file_type VARCHAR NOT NULL,
                file_timestamp TIMESTAMPTZ NOT NULL,
                processed_at TIMESTAMPTZ NOT NULL
            )
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        Self {
            pool,
            admin,
            schema,
        }
    }

    async fn record_all(&self, files: &[FileMeta]) {
        let mut tx = self.pool.begin().await.unwrap();
        for file in files {
            tx.record(PROCESS, file).await.unwrap();
        }
        tx.commit().await.unwrap();
    }

    async fn row_count(&self, file_type: &str) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM files_processed WHERE file_type = $1")
            .bind(file_type)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    async fn drop(self) {
        self.pool.close().await;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await
        .unwrap();
    }
}

fn base() -> Timestamp {
    Timestamp::from_millisecond(1_800_000_000_000).unwrap()
}

/// One file per second, starting at `base()`.
fn files(prefix: &str, count: i64) -> Vec<FileMeta> {
    (0..count)
        .map(|i| FileMeta::from((prefix, base() + SignedDuration::from_secs(i))))
        .collect()
}

#[tokio::test]
async fn recorded_files_set_latest_timestamp_and_exist() {
    if !require_postgres_env() {
        return;
    }
    let db = TestDb::new().await;
    let recorded = files("sensor", 3);

    db.record_all(&recorded).await;

    assert_eq!(
        db.pool.latest_timestamp(PROCESS, "sensor").await.unwrap(),
        Some(base() + SignedDuration::from_secs(2))
    );
    assert!(db.pool.exists(PROCESS, &recorded[0]).await.unwrap());
    let unrecorded = FileMeta::from(("sensor", base() - SignedDuration::from_secs(1)));
    assert!(!db.pool.exists(PROCESS, &unrecorded).await.unwrap());
    db.drop().await;
}

#[tokio::test]
async fn rolled_back_transaction_records_nothing() {
    if !require_postgres_env() {
        return;
    }
    let db = TestDb::new().await;
    let file = FileMeta::from(("sensor", base()));

    let mut tx = db.pool.begin().await.unwrap();
    tx.record(PROCESS, &file).await.unwrap();
    tx.rollback().await.unwrap();

    assert_eq!(
        db.pool.latest_timestamp(PROCESS, "sensor").await.unwrap(),
        None
    );
    assert!(!db.pool.exists(PROCESS, &file).await.unwrap());
    db.drop().await;
}

#[tokio::test]
async fn latest_timestamp_is_scoped_to_process_and_file_type() {
    if !require_postgres_env() {
        return;
    }
    let db = TestDb::new().await;

    db.record_all(&files("sensor", 2)).await;

    assert_eq!(
        db.pool.latest_timestamp(PROCESS, "other").await.unwrap(),
        None
    );
    assert_eq!(
        db.pool
            .latest_timestamp("other-process", "sensor")
            .await
            .unwrap(),
        None
    );
    db.drop().await;
}

#[tokio::test]
async fn rows_written_by_chrono_versions_decode_to_the_same_instant() {
    if !require_postgres_env() {
        return;
    }
    let db = TestDb::new().await;
    // prestige 0.4 bound chrono values; the column holds the same TIMESTAMPTZ.
    sqlx::query(
        "INSERT INTO files_processed VALUES ($1, 'legacy.1795623590428.parquet', 'legacy', \
         '2026-11-25 16:19:50.428+00', now())",
    )
    .bind(PROCESS)
    .execute(&db.pool)
    .await
    .unwrap();

    let latest = db.pool.latest_timestamp(PROCESS, "legacy").await.unwrap();

    assert_eq!(
        latest,
        Some(Timestamp::from_millisecond(1_795_623_590_428).unwrap())
    );
    db.drop().await;
}

#[tokio::test]
async fn sub_microsecond_timestamps_truncate_to_microseconds() {
    if !require_postgres_env() {
        return;
    }
    let db = TestDb::new().await;
    let precise = Timestamp::new(1_800_000_000, 123_456_789).unwrap();

    db.record_all(&[FileMeta::from(("sensor", precise))]).await;

    assert_eq!(
        db.pool.latest_timestamp(PROCESS, "sensor").await.unwrap(),
        Some(Timestamp::new(1_800_000_000, 123_456_000).unwrap())
    );
    db.drop().await;
}

#[tokio::test]
async fn clean_keeps_the_101_most_recent_files() {
    if !require_postgres_env() {
        return;
    }
    let db = TestDb::new().await;
    db.record_all(&files("sensor", 150)).await;

    let deleted = db
        .pool
        .clean(PROCESS, "sensor", base() + SignedDuration::from_hours(1))
        .await
        .unwrap();

    assert_eq!(deleted, 49);
    assert_eq!(db.row_count("sensor").await, 101);
    db.drop().await;
}

#[tokio::test]
async fn clean_keeps_files_inside_the_offset_window() {
    if !require_postgres_env() {
        return;
    }
    let db = TestDb::new().await;
    db.record_all(&files("sensor", 150)).await;

    let deleted = db
        .pool
        .clean(PROCESS, "sensor", base() + SignedDuration::from_secs(10))
        .await
        .unwrap();

    assert_eq!(deleted, 10);
    assert_eq!(db.row_count("sensor").await, 140);
    db.drop().await;
}

#[tokio::test]
async fn clean_deletes_nothing_with_101_files_or_fewer() {
    if !require_postgres_env() {
        return;
    }
    let db = TestDb::new().await;
    db.record_all(&files("sensor", 101)).await;

    let deleted = db
        .pool
        .clean(PROCESS, "sensor", base() + SignedDuration::from_hours(1))
        .await
        .unwrap();

    assert_eq!(deleted, 0);
    assert_eq!(db.row_count("sensor").await, 101);
    db.drop().await;
}

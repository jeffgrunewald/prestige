//! End-to-end checks that `#[prestige_schema]` structs with jiff fields write and read
//! Parquet, and produce the expected Iceberg schema and partition spec.

use futures::StreamExt;
use prestige::{
    ChannelError, FileUpload,
    arrow::datatypes::{DataType, TimeUnit},
    file_sink::ParquetSinkBuilder,
    file_source,
    jiff::{Timestamp, civil},
};
use serde::{Deserialize, Serialize};
use super_visor::{ManagedProc, ShutdownSignal};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

#[prestige::prestige_schema]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TemporalEvent {
    #[prestige(identifier)]
    event_id: String,
    #[prestige(partition(day), sort_key(order = 1))]
    occurred_at: Timestamp,
    observed_at: Option<Timestamp>,
    local_time: civil::DateTime,
    business_day: civil::Date,
    shift_start: civil::Time,
}

fn sample_events(count: i64) -> Vec<TemporalEvent> {
    (0..count)
        .map(|i| {
            // Microsecond precision: the Parquet column stores MICROS.
            let occurred_at = Timestamp::from_microsecond(1_700_000_000_000_000 + i * 1_234_567)
                .expect("timestamp in range");
            TemporalEvent {
                event_id: format!("event-{i:03}"),
                occurred_at,
                observed_at: (i % 2 == 0).then_some(occurred_at),
                // Millisecond precision: the Parquet column stores MILLIS.
                local_time: civil::date(2024, 2, 29).at(13, 45, 7, 123_000_000),
                business_day: civil::date(2024, 3, i8::try_from(1 + i).expect("small day")),
                shift_start: civil::time(8, 30, 0, 987_654_321),
            }
        })
        .collect()
}

async fn write_events(events: Vec<TemporalEvent>, dir: &TempDir) -> prestige::Result<Vec<String>> {
    let client = prestige::new_client(None, None, None, None).await;
    let (file_upload, _upload_server) = FileUpload::new(client, "test-bucket".to_string()).await;
    let (sink_client, sink) =
        ParquetSinkBuilder::<TemporalEvent>::new("temporal", dir.path(), file_upload, "temporal")
            .batch_size(100)
            .create()
            .await?;

    let cancel_token = CancellationToken::new();
    let shutdown_signal = ShutdownSignal::new(cancel_token.clone());
    let sink_handle =
        tokio::task::spawn_local(async move { Box::new(sink).run_proc(shutdown_signal).await });

    for event in events {
        sink_client
            .write(event)
            .await?
            .await
            .map_err(|_| ChannelError::sink_closed("temporal"))??;
    }
    let manifest = sink_client
        .commit()
        .await?
        .await
        .map_err(|_| ChannelError::sink_closed("temporal"))??;

    cancel_token.cancel();
    let _ = sink_handle.await;
    Ok(manifest)
}

async fn read_events(paths: Vec<String>) -> prestige::Result<Vec<TemporalEvent>> {
    let paths: Vec<std::path::PathBuf> = paths.into_iter().map(Into::into).collect();
    let mut stream = file_source::source(paths, None, None);
    let mut events = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let decoded: Vec<TemporalEvent> =
            serde_arrow::from_arrow(batch.schema().fields(), batch.columns())
                .map_err(|e| prestige::Error::SerdeArrow(e.to_string()))?;
        events.extend(decoded);
    }
    Ok(events)
}

#[test]
fn derived_arrow_schema_uses_temporal_types() {
    let schema = TemporalEvent::arrow_schema();

    let field_type = |name: &str| schema.field_with_name(name).unwrap().data_type().clone();

    assert_eq!(
        field_type("occurred_at"),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert!(schema.field_with_name("observed_at").unwrap().is_nullable());
    assert_eq!(
        field_type("local_time"),
        DataType::Timestamp(TimeUnit::Millisecond, None)
    );
    assert_eq!(field_type("business_day"), DataType::Date32);
    assert_eq!(
        field_type("shift_start"),
        DataType::Time64(TimeUnit::Nanosecond)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn jiff_fields_round_trip_through_parquet_sink() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = TempDir::new().unwrap();
            let original = sample_events(10);

            let paths = write_events(original.clone(), &dir).await.unwrap();
            let decoded = read_events(paths).await.unwrap();

            assert_eq!(decoded, original);
        })
        .await;
}

#[cfg(feature = "iceberg")]
mod iceberg {
    use super::*;
    use prestige::iceberg::{IcebergSchema, PrimitiveType, Transform, Type, build_partition_spec};

    // Iceberg needs microsecond timestamps, so it accepts only the jiff types that map to them.
    #[prestige::prestige_schema]
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct IcebergEvent {
        #[prestige(identifier)]
        event_id: String,
        #[prestige(partition(day), sort_key(order = 1))]
        occurred_at: Timestamp,
        observed_at: Option<Timestamp>,
        business_day: civil::Date,
    }

    #[test]
    fn derived_iceberg_schema_uses_temporal_types() {
        let schema = IcebergEvent::iceberg_schema();

        let field = |name: &str| schema.field_by_name(name).unwrap().clone();

        assert_eq!(
            *field("occurred_at").field_type,
            Type::Primitive(PrimitiveType::Timestamptz)
        );
        assert!(field("occurred_at").required);
        assert_eq!(
            *field("observed_at").field_type,
            Type::Primitive(PrimitiveType::Timestamptz)
        );
        assert!(!field("observed_at").required);
        assert_eq!(
            *field("business_day").field_type,
            Type::Primitive(PrimitiveType::Date)
        );
    }

    #[test]
    fn day_partition_binds_to_jiff_timestamp() {
        let schema = IcebergEvent::iceberg_schema();

        let spec =
            build_partition_spec(&schema, IcebergEvent::partition_field_definitions()).unwrap();

        let field = spec.fields().first().unwrap();
        assert_eq!(field.transform, Transform::Day);
        assert_eq!(
            field.source_id,
            schema.field_by_name("occurred_at").unwrap().id
        );
    }

    #[test]
    fn sort_order_binds_to_jiff_timestamp() {
        assert_eq!(
            IcebergEvent::sort_field_definitions()
                .iter()
                .map(|def| def.name)
                .collect::<Vec<_>>(),
            ["occurred_at"]
        );
    }
}

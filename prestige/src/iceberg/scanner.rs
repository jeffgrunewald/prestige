use crate::error::Result;
use arrow::array::RecordBatch;
use futures::stream::BoxStream;
use iceberg::table::Table;

/// A stream of arrow RecordBatches from an iceberg table scan.
pub type IcebergRecordBatchStream = BoxStream<'static, iceberg::Result<RecordBatch>>;

/// Scan an entire iceberg table, returning all records as a RecordBatch stream.
pub async fn scan_table(table: &Table) -> Result<IcebergRecordBatchStream> {
    let scan = table.scan().build()?;
    let stream = scan.to_arrow().await?;
    Ok(stream)
}

/// Scan an iceberg table at a specific snapshot.
pub async fn scan_snapshot(
    table: &Table,
    snapshot_id: i64,
) -> Result<IcebergRecordBatchStream> {
    let scan = table.scan().snapshot_id(snapshot_id).build()?;
    let stream = scan.to_arrow().await?;
    Ok(stream)
}

/// Scan an iceberg table with column projection.
pub async fn scan_columns(
    table: &Table,
    columns: Vec<&str>,
) -> Result<IcebergRecordBatchStream> {
    let scan = table.scan().select(columns).build()?;
    let stream = scan.to_arrow().await?;
    Ok(stream)
}

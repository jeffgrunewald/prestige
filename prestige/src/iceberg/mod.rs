pub(crate) mod branch;
mod catalog;
mod compactor;
mod manifest;
mod poller;
mod scanner;
mod schema;
mod sink;
mod table;
#[cfg(feature = "iceberg-test-harness")]
mod test_harness;
mod transaction;
mod writer;

pub use catalog::{
    AuthConfig, Catalog, CatalogConfig, CatalogConfigBuilder, S3Config, connect_catalog,
};
#[cfg(feature = "iceberg-s3tables-catalog")]
pub use catalog::{S3TablesCatalogConfig, S3TablesCatalogConfigBuilder};
pub use compactor::{
    CompactionScheduler, CompactionSchedulerBuilder, IcebergCompactionResult,
    IcebergCompactorConfig, IcebergCompactorConfigBuilder,
};
pub use poller::{
    IcebergFileStream, IcebergPollerConfigBuilder, IcebergPollerServer, IcebergStreamReceiver,
};
pub use scanner::{
    IcebergRecordBatchStream, earliest_snapshot, scan_at_timestamp, scan_columns,
    scan_since_snapshot, scan_snapshot, scan_snapshot_range, scan_table, scan_with_filter,
    snapshot_at_timestamp,
};
pub use schema::{
    IcebergSchema, PartitionFieldDef, SchemaReconciliation, SortFieldDef, arrow_to_iceberg_schema,
    arrow_to_iceberg_schema_with_identifiers, build_partition_spec, build_sort_order,
    reconcile_schema, resolve_identifier_field_ids,
};
pub use sink::{
    BoxedDataWriter, DataWriter, IcebergSink, IcebergSinkBuilder, IcebergSinkClient,
    IntoBoxedDataWriter,
};
pub use table::{
    EnsureTableResult, IcebergTableConfig, IcebergTableConfigBuilder, create_table,
    create_table_if_not_exists, ensure_table, ensure_table_for, ensure_table_for_with, load_table,
};
pub use transaction::{WapPublisherState, WapTransaction, WapWriterState};
pub use writer::{
    commit_data_files, write_and_commit, write_data_files, write_data_files_with_target_size,
};

// Re-export iceberg types that appear in prestige's public API, so users
// don't need a separate `iceberg` dependency to construct predicates,
// configure sort orders, or reference table handles.
pub use iceberg::expr::{Predicate, Reference};
pub use iceberg::spec::{
    Datum, NullOrder, PrimitiveType, Schema, SnapshotRef, SortDirection, SortOrder, Transform,
    Type, UnboundPartitionSpec,
};
pub use iceberg::table::Table;
pub use iceberg::{NamespaceIdent, TableCreation, TableIdent};

// Escape hatch for types not named above, so downstreams never need their own
// `iceberg` dependency (which could drift off the version prestige links).
pub use iceberg as upstream;

#[cfg(feature = "iceberg-test-harness")]
pub use test_harness::{
    DirectWriter, HarnessConfig, HarnessConfigBuilder, HasIdentifierFields, IcebergTestHarness,
    TestHost,
};

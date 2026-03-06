use crate::error::Result;
use arrow::datatypes::SchemaRef;
use derive_builder::Builder;
use iceberg::{
    NamespaceIdent, TableCreation, TableIdent, TableRequirement, TableUpdate,
    spec::{Schema, SortOrder, UnboundPartitionSpec},
    table::Table,
};
use iceberg_catalog_rest::CommitTableRequest;
use std::{collections::HashMap, time::Duration};
use tracing::info;

use super::{
    catalog::Catalog,
    schema::{IcebergSchema, SchemaReconciliation, reconcile_schema},
};

#[derive(Debug, Clone, Builder)]
#[builder(pattern = "owned")]
pub struct IcebergTableConfig {
    namespace: Vec<String>,
    name: String,

    #[builder(default)]
    location: Option<String>,

    #[builder(default)]
    properties: HashMap<String, String>,

    #[builder(default)]
    partition_spec: Option<UnboundPartitionSpec>,

    /// Enable Write-Audit-Publish mode on the table.
    #[builder(default)]
    wap_enabled: bool,

    /// Maximum age of snapshots before expiration.
    #[builder(default)]
    max_snapshot_age: Option<Duration>,

    /// Minimum number of snapshots to retain regardless of age.
    #[builder(default)]
    min_snapshots_to_keep: Option<u32>,

    /// Maximum age of branch/tag refs before expiration.
    #[builder(default)]
    max_ref_age: Option<Duration>,

    /// Parquet compression codec (e.g. "zstd", "snappy", "uncompressed").
    #[builder(default)]
    parquet_compression: Option<String>,

    /// Sort order for the table, controlling how data is ordered within files.
    #[builder(default)]
    sort_order: Option<SortOrder>,
}

pub async fn create_table(
    catalog: &Catalog,
    config: &IcebergTableConfig,
    schema: Schema,
) -> Result<Table> {
    let namespace = NamespaceIdent::from_strs(&config.namespace)?;
    catalog.create_namespace_if_not_exists(&namespace).await?;
    let creation = build_table_creation(config, schema);
    let table = catalog.create_table(&namespace, creation).await?;
    Ok(table)
}

pub async fn create_table_if_not_exists(
    catalog: &Catalog,
    config: &IcebergTableConfig,
    schema: Schema,
) -> Result<Table> {
    let namespace = NamespaceIdent::from_strs(&config.namespace)?;
    let table_ident = TableIdent::new(namespace.clone(), config.name.clone());

    if catalog.table_exists(&table_ident).await? {
        return load_table(catalog, &config.namespace, &config.name).await;
    }

    create_table(catalog, config, schema).await
}

pub async fn load_table(catalog: &Catalog, namespace: &[String], name: &str) -> Result<Table> {
    let namespace = NamespaceIdent::from_strs(namespace)?;
    let table_ident = TableIdent::new(namespace, name.to_string());
    let table = catalog.load_table(&table_ident).await?;
    Ok(table)
}

/// The outcome of an `ensure_table` call.
#[derive(Debug)]
pub enum EnsureTableResult {
    /// Table was created from scratch.
    Created(Table),
    /// Table already existed and its schema was up to date.
    UpToDate(Table),
    /// Table already existed and its schema was evolved.
    Evolved {
        table: Table,
        columns_added: Vec<String>,
        columns_dropped: Vec<String>,
    },
}

impl EnsureTableResult {
    /// Extract the table regardless of which variant was produced.
    pub fn into_table(self) -> Table {
        match self {
            Self::Created(t) | Self::UpToDate(t) | Self::Evolved { table: t, .. } => t,
        }
    }
}

impl IcebergTableConfig {
    /// Build an `IcebergTableConfigBuilder` pre-populated from a type's `IcebergSchema` impl.
    ///
    /// `label` is used as fallback table name when `#[prestige(table = "...")]` is absent.
    /// Returns the builder so callers can override any field before building.
    pub fn from_schema<T: IcebergSchema>(label: &str) -> IcebergTableConfigBuilder {
        let name = T::default_table_name()
            .map(String::from)
            .unwrap_or_else(|| label.to_string());
        let namespace = T::default_namespace()
            .map(|parts| parts.iter().map(|s| s.to_string()).collect())
            .unwrap_or_default();

        let mut builder = IcebergTableConfigBuilder::default();
        builder = builder.name(name).namespace(namespace);
        if let Some(spec) = T::table_partition_spec() {
            builder = builder.partition_spec(Some(spec));
        }
        if let Some(order) = T::table_sort_order() {
            builder = builder.sort_order(Some(order));
        }
        builder
    }
}

/// Ensure a table exists for a type implementing `IcebergSchema`, using its
/// derive-generated metadata for table name, namespace, partition spec, and sort order.
///
/// `label` is used as fallback table name when `#[prestige(table = "...")]` is absent.
pub async fn ensure_table_for<T: IcebergSchema>(
    catalog: &Catalog,
    label: &str,
) -> Result<EnsureTableResult> {
    ensure_table_for_with::<T>(catalog, label, |b| b).await
}

/// Like `ensure_table_for`, but accepts a closure to override builder fields
/// before the config is finalized.
pub async fn ensure_table_for_with<T: IcebergSchema>(
    catalog: &Catalog,
    label: &str,
    config_override: impl FnOnce(IcebergTableConfigBuilder) -> IcebergTableConfigBuilder,
) -> Result<EnsureTableResult> {
    let builder = IcebergTableConfig::from_schema::<T>(label);
    let config = config_override(builder)
        .build()
        .map_err(|e| crate::Error::Internal(e.to_string()))?;
    let arrow_schema = T::arrow_schema();
    let identifier_names = T::identifier_field_names();
    ensure_table(catalog, &config, &arrow_schema, identifier_names).await
}

/// Ensure a table exists and its schema matches the struct-derived Arrow schema.
///
/// Three cases:
/// 1. **Table does not exist** → create it from the Arrow schema.
/// 2. **Table exists, schemas match** → return it unchanged.
/// 3. **Table exists, schemas differ** → reconcile by adding/dropping columns
///    and commit the new schema to the catalog.
///
/// `identifier_field_names` declares the logical primary key columns (e.g. from
/// `#[prestige(identifier)]`). Pass an empty slice if there are none.
///
/// Column matching is by name. Added columns are always optional (iceberg
/// requires this for backward compatibility with existing data files).
pub async fn ensure_table(
    catalog: &Catalog,
    config: &IcebergTableConfig,
    arrow_schema: &SchemaRef,
    identifier_field_names: &[&str],
) -> Result<EnsureTableResult> {
    let namespace = NamespaceIdent::from_strs(&config.namespace)?;
    catalog.create_namespace_if_not_exists(&namespace).await?;

    let table_ident = TableIdent::new(namespace.clone(), config.name.clone());

    if !catalog.table_exists(&table_ident).await? {
        let iceberg_schema = super::schema::arrow_to_iceberg_schema_with_identifiers(
            arrow_schema,
            identifier_field_names,
        )?;
        let table = create_table(catalog, config, iceberg_schema).await?;
        info!(
            table = %config.name,
            columns = arrow_schema.fields().len(),
            identifiers = ?identifier_field_names,
            "created new iceberg table"
        );
        return Ok(EnsureTableResult::Created(table));
    }

    let table = catalog.load_table(&table_ident).await?;
    let catalog_schema = table.metadata().current_schema();

    match reconcile_schema(arrow_schema, catalog_schema, identifier_field_names)? {
        SchemaReconciliation::UpToDate => Ok(EnsureTableResult::UpToDate(table)),
        SchemaReconciliation::Evolved {
            schema,
            columns_added,
            columns_dropped,
        } => {
            info!(
                table = %config.name,
                added = ?columns_added,
                dropped = ?columns_dropped,
                "evolving iceberg table schema"
            );

            let request = CommitTableRequest {
                identifier: Some(table_ident.clone()),
                requirements: vec![TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: catalog_schema.schema_id(),
                }],
                updates: vec![
                    TableUpdate::AddSchema { schema: *schema },
                    TableUpdate::SetCurrentSchema { schema_id: -1 },
                ],
            };

            catalog.commit_table_request(&request).await?;

            // Reload to get the table with the new schema applied.
            let updated_table = catalog.load_table(&table_ident).await?;

            Ok(EnsureTableResult::Evolved {
                table: updated_table,
                columns_added,
                columns_dropped,
            })
        }
    }
}

fn build_table_creation(config: &IcebergTableConfig, schema: Schema) -> TableCreation {
    let mut properties = config.properties.clone();

    if config.wap_enabled {
        properties.insert(
            super::branch::WAP_ENABLED_PROPERTY.to_string(),
            "true".to_string(),
        );
    }

    if let Some(ref age) = config.max_snapshot_age {
        properties.insert(
            "history.expire.max-snapshot-age-ms".to_string(),
            age.as_millis().to_string(),
        );
    }

    if let Some(count) = config.min_snapshots_to_keep {
        properties.insert(
            "history.expire.min-snapshots-to-keep".to_string(),
            count.to_string(),
        );
    }

    if let Some(ref age) = config.max_ref_age {
        properties.insert(
            "history.expire.max-ref-age-ms".to_string(),
            age.as_millis().to_string(),
        );
    }

    if let Some(ref codec) = config.parquet_compression {
        properties.insert("write.parquet.compression-codec".to_string(), codec.clone());
    }

    TableCreation::builder()
        .name(config.name.clone())
        .location_opt(config.location.clone())
        .schema(schema)
        .partition_spec_opt(config.partition_spec.clone())
        .sort_order_opt(config.sort_order.clone())
        .properties(properties)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_table_creation_default() {
        let config = IcebergTableConfigBuilder::default()
            .namespace(vec!["db".to_string()])
            .name("test_table".to_string())
            .build()
            .unwrap();

        let schema = Schema::builder().with_fields(vec![]).build().unwrap();

        let creation = build_table_creation(&config, schema);
        assert_eq!(creation.name, "test_table");
    }

    #[test]
    fn test_build_table_creation_with_wap() {
        let config = IcebergTableConfigBuilder::default()
            .namespace(vec!["db".to_string()])
            .name("wap_table".to_string())
            .wap_enabled(true)
            .build()
            .unwrap();

        let schema = Schema::builder().with_fields(vec![]).build().unwrap();

        let creation = build_table_creation(&config, schema);
        assert_eq!(
            creation.properties.get("write.wap.enabled"),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn test_build_table_creation_with_sort_order() {
        use iceberg::spec::{
            NullOrder, SortDirection, SortField as IcebergSortField, SortOrder, Transform,
        };

        let sort_order = SortOrder::builder()
            .with_sort_field(
                IcebergSortField::builder()
                    .source_id(1)
                    .direction(SortDirection::Ascending)
                    .null_order(NullOrder::First)
                    .transform(Transform::Identity)
                    .build(),
            )
            .build_unbound()
            .unwrap();

        let config = IcebergTableConfigBuilder::default()
            .namespace(vec!["db".to_string()])
            .name("sorted_table".to_string())
            .sort_order(Some(sort_order.clone()))
            .build()
            .unwrap();

        let schema = Schema::builder().with_fields(vec![]).build().unwrap();

        let creation = build_table_creation(&config, schema);
        assert_eq!(creation.sort_order.as_ref(), Some(&sort_order));
    }

    #[test]
    fn test_build_table_creation_with_retention() {
        let config = IcebergTableConfigBuilder::default()
            .namespace(vec!["db".to_string()])
            .name("retention_table".to_string())
            .max_snapshot_age(Some(Duration::from_secs(86400)))
            .min_snapshots_to_keep(Some(5))
            .max_ref_age(Some(Duration::from_secs(3600)))
            .parquet_compression(Some("zstd".to_string()))
            .build()
            .unwrap();

        let schema = Schema::builder().with_fields(vec![]).build().unwrap();

        let creation = build_table_creation(&config, schema);
        assert_eq!(
            creation
                .properties
                .get("history.expire.max-snapshot-age-ms"),
            Some(&"86400000".to_string())
        );
        assert_eq!(
            creation
                .properties
                .get("history.expire.min-snapshots-to-keep"),
            Some(&"5".to_string())
        );
        assert_eq!(
            creation.properties.get("history.expire.max-ref-age-ms"),
            Some(&"3600000".to_string())
        );
        assert_eq!(
            creation.properties.get("write.parquet.compression-codec"),
            Some(&"zstd".to_string())
        );
    }
}

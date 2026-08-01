use std::{collections::HashMap, time::Duration};

use arrow::datatypes::SchemaRef;
use derive_builder::Builder;
use iceberg::{
    NamespaceIdent, TableCreation, TableIdent,
    spec::{Schema, SortOrder, UnboundPartitionSpec},
    table::Table,
    transaction::{AddColumn, ApplyTransactionAction, Transaction},
};
use tracing::{debug, info};

use super::{
    catalog::Catalog,
    schema::{IcebergSchema, SchemaReconciliation, reconcile_schema},
};
use crate::error::Result;

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
    debug!(namespace = ?namespace, "creating namespace if not exists");
    catalog.create_namespace_if_not_exists(&namespace).await?;

    let creation = build_table_creation(config, schema);
    debug!(
        table_name = %creation.name,
        location = ?creation.location,
        partition_spec = ?creation.partition_spec,
        sort_order = ?creation.sort_order,
        properties = ?creation.properties,
        "creating table with TableCreation"
    );

    // Serialize the equivalent CreateTableRequest to show the exact JSON that
    // will be sent by iceberg-catalog-rest.
    let simulated_request = serde_json::json!({
        "name": creation.name,
        "location": creation.location,
        "schema": serde_json::to_value(&creation.schema).ok(),
        "partition-spec": creation.partition_spec.as_ref().map(|s| serde_json::to_value(s).ok()),
        "write-order": creation.sort_order.as_ref().map(|s| serde_json::to_value(s).ok()),
        "stage-create": false,
        "properties": &creation.properties,
    });
    debug!(
        json = %serde_json::to_string_pretty(&simulated_request).unwrap_or_default(),
        "simulated CreateTableRequest JSON"
    );

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

    match create_table(catalog, config, schema).await {
        Ok(table) => Ok(table),
        Err(crate::Error::Iceberg(ref e)) if super::catalog::is_already_exists_error(e) => {
            load_table(catalog, &config.namespace, &config.name).await
        }
        Err(e) => Err(e),
    }
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
    debug!(
        ?namespace,
        table = %config.name,
        location = ?config.location,
        "ensure_table: starting"
    );

    debug!(?namespace, "ensure_table: creating namespace if not exists");
    catalog.create_namespace_if_not_exists(&namespace).await?;

    let table_ident = TableIdent::new(namespace.clone(), config.name.clone());

    debug!(table = %table_ident, "ensure_table: checking if table exists");
    if !catalog.table_exists(&table_ident).await? {
        debug!(table = %table_ident, "ensure_table: table does not exist, creating");
        let iceberg_schema = super::schema::arrow_to_iceberg_schema_with_identifiers(
            arrow_schema,
            identifier_field_names,
        )?;
        debug!(
            schema = ?iceberg_schema,
            "ensure_table: built iceberg schema from arrow"
        );
        match create_table(catalog, config, iceberg_schema).await {
            Ok(table) => {
                info!(
                    table = %config.name,
                    columns = arrow_schema.fields().len(),
                    identifiers = ?identifier_field_names,
                    "created new iceberg table",
                );
                return Ok(EnsureTableResult::Created(table));
            }
            Err(crate::Error::Iceberg(ref e)) if super::catalog::is_already_exists_error(e) => {}
            Err(e) => return Err(e),
        }
    }

    let table = catalog.load_table(&table_ident).await?;
    let catalog_schema = table.metadata().current_schema();

    match reconcile_schema(arrow_schema, catalog_schema, identifier_field_names)? {
        SchemaReconciliation::UpToDate => {
            info!(
                table = %config.name,
                "table schema is up to date, reusing existing table",
            );
            Ok(EnsureTableResult::UpToDate(table))
        }
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

            let updated_table = commit_schema_update(
                catalog,
                &table,
                &schema,
                &columns_added,
                &columns_dropped,
                identifier_field_names,
            )
            .await?;

            Ok(EnsureTableResult::Evolved {
                table: updated_table,
                columns_added,
                columns_dropped,
            })
        }
    }
}

async fn commit_schema_update(
    catalog: &Catalog,
    table: &Table,
    schema: &Schema,
    columns_added: &[String],
    columns_dropped: &[String],
    identifier_field_names: &[&str],
) -> Result<Table> {
    let current_identifier_names: Vec<&str> = table
        .metadata()
        .current_schema()
        .identifier_field_ids()
        .filter_map(|id| table.metadata().current_schema().field_by_id(id))
        .map(|field| field.name.as_str())
        .collect();

    if !identifier_field_names.is_empty()
        && identifier_field_names != current_identifier_names.as_slice()
    {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::FeatureUnsupported,
            "Iceberg 0.10 transactions cannot change identifier fields",
        )
        .into());
    }

    let action = columns_added.iter().try_fold(
        Transaction::new(table).update_schema(),
        |action, name| {
            let field = schema.field_by_name(name).ok_or_else(|| {
                iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    format!("reconciled schema is missing added column '{name}'"),
                )
            })?;
            Ok::<_, iceberg::Error>(
                action.add_column(AddColumn::optional(name, field.field_type.as_ref().clone())),
            )
        },
    )?;
    let action = columns_dropped
        .iter()
        .fold(action, |action, name| action.delete_column(name));
    let tx = action.apply(Transaction::new(table))?;

    tx.commit(catalog.as_iceberg_catalog().as_ref())
        .await
        .map_err(Into::into)
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
        assert_eq!(
            creation.location, None,
            "location must be None when not explicitly set (catalog resolves from warehouse)",
        );
    }

    #[test]
    fn test_build_table_creation_explicit_location_takes_precedence() {
        let config = IcebergTableConfigBuilder::default()
            .namespace(vec!["db".to_string()])
            .name("test_table".to_string())
            .location(Some("s3://my-bucket/custom/path".to_string()))
            .build()
            .unwrap();

        let schema = Schema::builder().with_fields(vec![]).build().unwrap();

        let creation = build_table_creation(&config, schema);
        assert_eq!(
            creation.location.as_deref(),
            Some("s3://my-bucket/custom/path"),
            "explicit location must not be overridden by derived default",
        );
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

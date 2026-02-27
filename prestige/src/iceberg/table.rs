use crate::error::Result;
use derive_builder::Builder;
use iceberg::spec::{Schema, UnboundPartitionSpec};
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use std::collections::HashMap;
use std::sync::Arc;

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
}

pub async fn create_table(
    catalog: &Arc<dyn Catalog>,
    config: &IcebergTableConfig,
    schema: Schema,
) -> Result<Table> {
    let namespace = NamespaceIdent::from_strs(&config.namespace)?;
    let creation = build_table_creation(config, schema);
    let table = catalog.create_table(&namespace, creation).await?;
    Ok(table)
}

pub async fn create_table_if_not_exists(
    catalog: &Arc<dyn Catalog>,
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

pub async fn load_table(
    catalog: &Arc<dyn Catalog>,
    namespace: &[String],
    name: &str,
) -> Result<Table> {
    let namespace = NamespaceIdent::from_strs(namespace)?;
    let table_ident = TableIdent::new(namespace, name.to_string());
    let table = catalog.load_table(&table_ident).await?;
    Ok(table)
}

fn build_table_creation(config: &IcebergTableConfig, schema: Schema) -> TableCreation {
    TableCreation::builder()
        .name(config.name.clone())
        .location_opt(config.location.clone())
        .schema(schema)
        .partition_spec_opt(config.partition_spec.clone())
        .properties(config.properties.clone())
        .build()
}

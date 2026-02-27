use crate::error::Result;
use derive_builder::Builder;
use iceberg::{Catalog, CatalogBuilder};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Builder)]
#[builder(pattern = "owned")]
pub struct CatalogConfig {
    name: String,
    uri: String,
    warehouse: String,

    #[builder(default)]
    s3_endpoint: Option<String>,
    #[builder(default)]
    s3_region: Option<String>,
    #[builder(default)]
    s3_access_key_id: Option<String>,
    #[builder(default)]
    s3_secret_access_key: Option<String>,

    #[builder(default)]
    extra_properties: HashMap<String, String>,
}

impl CatalogConfig {
    fn to_properties(&self) -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert("uri".to_string(), self.uri.clone());
        props.insert("warehouse".to_string(), self.warehouse.clone());

        if let Some(endpoint) = &self.s3_endpoint {
            props.insert("s3.endpoint".to_string(), endpoint.clone());
        }
        if let Some(region) = &self.s3_region {
            props.insert("s3.region".to_string(), region.clone());
        }
        if let Some(key_id) = &self.s3_access_key_id {
            props.insert("s3.access-key-id".to_string(), key_id.clone());
        }
        if let Some(secret) = &self.s3_secret_access_key {
            props.insert("s3.secret-access-key".to_string(), secret.clone());
        }

        props.extend(self.extra_properties.clone());
        props
    }
}

impl From<&crate::Settings> for CatalogConfigBuilder {
    fn from(settings: &crate::Settings) -> Self {
        let mut builder = CatalogConfigBuilder::default();

        if let Some(endpoint) = &settings.endpoint {
            builder = builder.s3_endpoint(Some(endpoint.clone()));
        }
        builder = builder.s3_region(Some(settings.region.clone()));

        if let Some(key_id) = &settings.access_key_id {
            builder = builder.s3_access_key_id(Some(key_id.clone()));
        }
        if let Some(secret) = &settings.secret_access_key {
            builder = builder.s3_secret_access_key(Some(secret.clone()));
        }

        builder
    }
}

pub async fn connect_catalog(config: &CatalogConfig) -> Result<Arc<dyn Catalog>> {
    let props = config.to_properties();
    let catalog = iceberg_catalog_rest::RestCatalogBuilder::default()
        .load(&config.name, props)
        .await?;
    Ok(Arc::new(catalog))
}

#[cfg(feature = "iceberg-sql-catalog")]
pub async fn connect_sql_catalog(config: &CatalogConfig) -> Result<Arc<dyn Catalog>> {
    let props = config.to_properties();
    let catalog = iceberg_catalog_sql::SqlCatalogBuilder::default()
        .load(&config.name, props)
        .await?;
    Ok(Arc::new(catalog))
}

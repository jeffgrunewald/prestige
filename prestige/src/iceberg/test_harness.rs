use crate::ArrowSchema;
use crate::error::Result;
use crate::iceberg::catalog::{AuthConfig, Catalog, CatalogConfig, S3Config};
use crate::iceberg::sink::{BoxedDataWriter, DataWriter, IntoBoxedDataWriter};
use async_trait::async_trait;
use derive_builder::Builder;
use iceberg::NamespaceIdent;
use iceberg::table::Table;
use serde::Serialize;
use tokio::sync::Mutex;
use tracing::info;
use uuid::Uuid;

const DEFAULT_NAMESPACE: &str = "default";

// ---------------------------------------------------------------------------
// Test harness errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
enum TestHarnessError {
    #[error("{description} request failed: {source:?}")]
    Request {
        description: &'static str,
        source: reqwest::Error,
    },

    #[error("{description} returned: {source:?}")]
    Response {
        description: &'static str,
        source: reqwest::Error,
    },

    #[error("failed to parse OAuth2 token: {0}")]
    TokenParse(reqwest::Error),
}

impl From<TestHarnessError> for crate::Error {
    fn from(error: TestHarnessError) -> Self {
        crate::Error::CatalogHttp(format!("{error}"))
    }
}

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

/// Integration test harness providing an isolated Polaris catalog per test.
///
/// Each instance provisions its own catalog against a running Polaris + S3/MinIO
/// stack, enabling fully isolated end-to-end iceberg tests. Tables are created
/// via prestige's `ensure_table`, and data is written via `write_and_commit`.
///
/// # Example
///
/// ```ignore
/// #[tokio::test]
/// async fn test_roundtrip() -> Result<()> {
///     let harness = IcebergTestHarness::new().await?;
///
///     let table_config = IcebergTableConfigBuilder::default()
///         .namespace(vec!["default".into()])
///         .name("readings".into())
///         .build()?;
///
///     let table = harness.ensure_table::<SensorReading>(&table_config).await?;
///     let writer = harness.writer::<SensorReading>(&table);
///     writer.write_all(vec![reading1, reading2]).await?;
///
///     let stream = scan_table(&table).await?;
///     // ... assert on results
///     Ok(())
/// }
/// ```
pub struct IcebergTestHarness {
    catalog_name: String,
    catalog: Catalog,
    namespace: String,
}

impl IcebergTestHarness {
    /// Create a new test harness with default configuration.
    ///
    /// Provisions a unique Polaris catalog (`test_{uuid}`), creates a `default`
    /// namespace, and connects an iceberg catalog.
    pub async fn new() -> Result<Self> {
        Self::with_config(HarnessConfig::default()).await
    }

    /// Create a new test harness with custom configuration.
    pub async fn with_config(config: HarnessConfig) -> Result<Self> {
        let catalog_name = format!("test_{}", Uuid::new_v4().as_simple());
        let namespace = DEFAULT_NAMESPACE.to_string();

        let http_client = reqwest::Client::new();

        let token = fetch_polaris_token(
            &http_client,
            &config.catalog_url(),
            &config.catalog_oauth2_credential,
            &config.catalog_oauth2_scope,
        )
        .await?;

        create_polaris_catalog(&http_client, &config, &token, &catalog_name).await?;

        let catalog = connect_catalog(&config, &catalog_name, &namespace).await?;

        info!(
            %catalog_name,
            %namespace,
            "test harness initialized with dedicated catalog"
        );

        Ok(Self {
            catalog_name,
            catalog,
            namespace,
        })
    }

    /// The unique catalog name for this test instance.
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    /// The default namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Access the underlying catalog for direct operations.
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Ensure a table exists with schema derived from type `T`.
    ///
    /// Uses `ensure_table` under the hood — creates if missing, validates
    /// schema if existing, and evolves if the struct has changed.
    pub async fn ensure_table<T: ArrowSchema + HasIdentifierFields>(
        &self,
        config: &super::table::IcebergTableConfig,
    ) -> Result<super::table::EnsureTableResult> {
        let schema = T::arrow_schema();
        let identifiers = T::identifier_field_names();
        super::table::ensure_table(&self.catalog, config, &schema, identifiers).await
    }

    /// Ensure a table exists with an explicit Arrow schema and identifier names.
    ///
    /// Use this when `T` doesn't implement `HasIdentifierFields` (e.g., in tests
    /// where you want to pass identifier names directly).
    pub async fn ensure_table_with<T: ArrowSchema>(
        &self,
        config: &super::table::IcebergTableConfig,
        identifier_field_names: &[&str],
    ) -> Result<super::table::EnsureTableResult> {
        let schema = T::arrow_schema();
        super::table::ensure_table(&self.catalog, config, &schema, identifier_field_names).await
    }

    /// Create a `DirectWriter<T>` for a loaded table.
    ///
    /// The writer commits immediately on each `write` / `write_all` call,
    /// making data visible for assertions right away.
    pub fn writer<T: ArrowSchema + Serialize + Send + Sync + 'static>(
        &self,
        table: &Table,
    ) -> BoxedDataWriter<T> {
        DirectWriter::new(self.catalog.clone(), table.clone()).boxed()
    }
}

/// Marker trait for types that expose `identifier_field_names()`.
///
/// Implemented automatically by `#[derive(PrestigeSchema)]` when any field
/// is annotated with `#[prestige(identifier)]`.
pub trait HasIdentifierFields {
    fn identifier_field_names() -> &'static [&'static str];
}

// ---------------------------------------------------------------------------
// DirectWriter — immediate-commit DataWriter for tests
// ---------------------------------------------------------------------------

/// A lightweight `DataWriter` that commits every write immediately.
///
/// Unlike the streaming `IcebergSink` (which buffers and batches), this writer
/// serializes, writes data files, and commits in a single operation per call.
/// This is ideal for integration tests where you want data to be visible
/// immediately after writing.
pub struct DirectWriter<T> {
    catalog: Catalog,
    table: Mutex<Table>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> DirectWriter<T> {
    pub fn new(catalog: Catalog, table: Table) -> Self {
        Self {
            catalog,
            table: Mutex::new(table),
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: ArrowSchema + Serialize + Send + Sync + 'static> DataWriter<T> for DirectWriter<T> {
    async fn write(&self, item: T) -> Result {
        self.write_all(vec![item]).await
    }

    async fn write_all(&self, items: Vec<T>) -> Result {
        if items.is_empty() {
            return Ok(());
        }

        let mut table = self.table.lock().await;
        let updated = super::writer::write_and_commit(
            &table,
            self.catalog.as_iceberg_catalog().as_ref(),
            &items,
            None,
            None,
        )
        .await?;
        *table = updated;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// HarnessConfig
// ---------------------------------------------------------------------------

/// Host + port pair for service endpoints.
#[derive(Debug, Clone, PartialEq)]
pub struct TestHost {
    pub host: String,
    pub port: u16,
}

impl TestHost {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}:{}{path}", self.host, self.port)
    }
}

/// Configuration for the integration test harness.
///
/// All fields have sensible defaults for a local Polaris + MinIO stack
/// (the common docker-compose dev environment). Override via builder or
/// environment variables.
#[derive(Debug, Clone, PartialEq, Builder)]
#[builder(pattern = "owned")]
pub struct HarnessConfig {
    /// S3 endpoint as seen by the test runner (host machine).
    #[builder(default = "env_defaults::s3_host()")]
    pub s3_host: TestHost,

    /// S3 endpoint as seen by the Polaris catalog server (inside Docker).
    ///
    /// Polaris uses this endpoint for server-side credential vending and
    /// storage validation. Defaults to `minio:9000` (the Docker network name).
    #[builder(default = "env_defaults::s3_warehouse_host()")]
    pub s3_warehouse_host: TestHost,

    #[builder(default = "env_defaults::s3_access_key()")]
    pub s3_access_key: String,

    #[builder(default = "env_defaults::s3_secret_key()")]
    pub s3_secret_key: String,

    #[builder(default = "env_defaults::s3_region()")]
    pub s3_region: String,

    #[builder(default = "env_defaults::catalog_host()")]
    pub catalog_host: TestHost,

    #[builder(default = "env_defaults::catalog_oauth2_credential()")]
    pub catalog_oauth2_credential: String,

    #[builder(default = "env_defaults::catalog_oauth2_scope()")]
    pub catalog_oauth2_scope: String,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            s3_host: env_defaults::s3_host(),
            s3_warehouse_host: env_defaults::s3_warehouse_host(),
            s3_access_key: env_defaults::s3_access_key(),
            s3_secret_key: env_defaults::s3_secret_key(),
            s3_region: env_defaults::s3_region(),
            catalog_host: env_defaults::catalog_host(),
            catalog_oauth2_credential: env_defaults::catalog_oauth2_credential(),
            catalog_oauth2_scope: env_defaults::catalog_oauth2_scope(),
        }
    }
}

impl HarnessConfig {
    pub fn builder() -> HarnessConfigBuilder {
        HarnessConfigBuilder::default()
    }

    fn s3_url(&self) -> String {
        self.s3_host.url("")
    }

    fn s3_warehouse_url(&self) -> String {
        self.s3_warehouse_host.url("")
    }

    fn catalog_url(&self) -> String {
        self.catalog_host.url("/api/catalog")
    }

    fn catalog_management_url(&self) -> String {
        self.catalog_host.url("/api/management/v1")
    }
}

// ---------------------------------------------------------------------------
// Polaris provisioning
// ---------------------------------------------------------------------------

/// Fetch an OAuth2 bearer token from the Polaris token endpoint.
async fn fetch_polaris_token(
    client: &reqwest::Client,
    catalog_url: &str,
    credential: &str,
    scope: &str,
) -> Result<String> {
    let (client_id, client_secret) = credential.split_once(':').unwrap_or((credential, ""));

    let token_url = format!("{catalog_url}/v1/oauth/tokens");
    let response = client
        .post(token_url)
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("scope", scope),
        ])
        .send()
        .await
        .map_polaris_err("OAuth2 token")?;

    #[derive(serde::Deserialize)]
    struct TokenResponse {
        access_token: String,
    }

    let token = response
        .json::<TokenResponse>()
        .await
        .map_err(TestHarnessError::TokenParse)?;

    Ok(token.access_token)
}

/// Create a Polaris catalog with S3 storage and admin permissions.
///
/// Steps:
/// 1. Create catalog with S3 storage config
/// 2. Create `admin` catalog role
/// 3. Grant `CATALOG_MANAGE_CONTENT` to admin role
/// 4. Assign admin role to `service_admin` principal role
async fn create_polaris_catalog(
    client: &reqwest::Client,
    config: &HarnessConfig,
    token: &str,
    catalog_name: &str,
) -> Result<()> {
    let auth_header = format!("Bearer {token}");
    let management_url = config.catalog_management_url();

    // 1. Create catalog
    let payload = serde_json::json!({
        "catalog": {
            "name": catalog_name,
            "type": "INTERNAL",
            "readOnly": false,
            "properties": {
                "default-base-location": format!("s3://iceberg-test/{catalog_name}")
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": [format!("s3://iceberg-test/{catalog_name}")],
                "endpoint": config.s3_warehouse_url(),
                "pathStyleAccess": true
            }
        }
    });

    client
        .post(format!("{management_url}/catalogs"))
        .header("Authorization", &auth_header)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await
        .map_polaris_err("create catalog")?;

    // 2. Create admin catalog role
    client
        .post(format!(
            "{management_url}/catalogs/{catalog_name}/catalog-roles"
        ))
        .header("Authorization", &auth_header)
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({"catalogRole": {"name": "admin"}}))
        .send()
        .await
        .map_polaris_err("create catalog role")?;

    // 3. Grant CATALOG_MANAGE_CONTENT to admin role
    client
        .put(format!(
            "{management_url}/catalogs/{catalog_name}/catalog-roles/admin/grants"
        ))
        .header("Authorization", &auth_header)
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}))
        .send()
        .await
        .map_polaris_err("grant catalog privilege")?;

    // 4. Assign admin role to service_admin principal role
    client
        .put(format!(
            "{management_url}/principal-roles/service_admin/catalog-roles/{catalog_name}"
        ))
        .header("Authorization", &auth_header)
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({"catalogRole": {"name": "admin"}}))
        .send()
        .await
        .map_polaris_err("assign catalog role")?;

    Ok(())
}

/// Connect to the provisioned catalog and create the default namespace.
async fn connect_catalog(
    config: &HarnessConfig,
    catalog_name: &str,
    namespace: &str,
) -> Result<Catalog> {
    let catalog_config = CatalogConfig::builder(config.catalog_url(), catalog_name.to_string())
        .warehouse(catalog_name.to_string())
        .auth(AuthConfig {
            credential: Some(config.catalog_oauth2_credential.clone()),
            scope: Some(config.catalog_oauth2_scope.clone()),
            ..Default::default()
        })
        .s3(S3Config {
            endpoint: Some(config.s3_url()),
            access_key_id: Some(config.s3_access_key.clone()),
            secret_access_key: Some(config.s3_secret_key.clone()),
            region: Some(config.s3_region.clone()),
            path_style_access: Some(true),
        })
        .build();

    let catalog = catalog_config.connect().await?;
    let ns = NamespaceIdent::from_strs([namespace])?;
    catalog.create_namespace_if_not_exists(&ns).await?;
    Ok(catalog)
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

trait PolarisErrorExt {
    fn map_polaris_err(self, description: &'static str) -> Result<reqwest::Response>;
}

impl PolarisErrorExt for reqwest::Result<reqwest::Response> {
    fn map_polaris_err(self, description: &'static str) -> Result<reqwest::Response> {
        let resp = self
            .map_err(|source| TestHarnessError::Request {
                description,
                source,
            })?
            .error_for_status()
            .map_err(|source| TestHarnessError::Response {
                description,
                source,
            })?;
        Ok(resp)
    }
}

// ---------------------------------------------------------------------------
// Environment variable defaults
// ---------------------------------------------------------------------------

mod env_defaults {
    use super::TestHost;

    const S3_HOST_ENV: &str = "PRESTIGE_TEST_S3_HOST";
    const S3_PORT_ENV: &str = "PRESTIGE_TEST_S3_PORT";
    const S3_WAREHOUSE_HOST_ENV: &str = "PRESTIGE_TEST_S3_WAREHOUSE_HOST";
    const S3_WAREHOUSE_PORT_ENV: &str = "PRESTIGE_TEST_S3_WAREHOUSE_PORT";
    const S3_ACCESS_KEY_ENV: &str = "PRESTIGE_TEST_S3_ACCESS_KEY";
    const S3_SECRET_KEY_ENV: &str = "PRESTIGE_TEST_S3_SECRET_KEY";
    const S3_REGION_ENV: &str = "PRESTIGE_TEST_S3_REGION";
    const S3_DEFAULT_PORT: u16 = 9000;

    const CATALOG_HOST_ENV: &str = "PRESTIGE_TEST_CATALOG_HOST";
    const CATALOG_PORT_ENV: &str = "PRESTIGE_TEST_CATALOG_PORT";
    const CATALOG_OAUTH2_CREDENTIAL_ENV: &str = "PRESTIGE_TEST_CATALOG_CREDENTIAL";
    const CATALOG_OAUTH2_SCOPE_ENV: &str = "PRESTIGE_TEST_CATALOG_SCOPE";
    const CATALOG_DEFAULT_PORT: u16 = 8181;

    pub fn s3_host() -> TestHost {
        TestHost::new(
            env_str(S3_HOST_ENV, "localhost"),
            env_port(S3_PORT_ENV, S3_DEFAULT_PORT),
        )
    }

    pub fn s3_warehouse_host() -> TestHost {
        TestHost::new(
            env_str(S3_WAREHOUSE_HOST_ENV, "minio"),
            env_port(S3_WAREHOUSE_PORT_ENV, S3_DEFAULT_PORT),
        )
    }

    pub fn s3_access_key() -> String {
        env_str(S3_ACCESS_KEY_ENV, "admin")
    }

    pub fn s3_secret_key() -> String {
        env_str(S3_SECRET_KEY_ENV, "password")
    }

    pub fn s3_region() -> String {
        env_str(S3_REGION_ENV, "us-east-1")
    }

    pub fn catalog_host() -> TestHost {
        TestHost::new(
            env_str(CATALOG_HOST_ENV, "localhost"),
            env_port(CATALOG_PORT_ENV, CATALOG_DEFAULT_PORT),
        )
    }

    pub fn catalog_oauth2_credential() -> String {
        env_str(CATALOG_OAUTH2_CREDENTIAL_ENV, "root:s3cr3t")
    }

    pub fn catalog_oauth2_scope() -> String {
        env_str(CATALOG_OAUTH2_SCOPE_ENV, "PRINCIPAL_ROLE:ALL")
    }

    fn env_str(var: &str, default: &str) -> String {
        std::env::var(var).unwrap_or_else(|_| default.to_string())
    }

    fn env_port(var: &str, default: u16) -> u16 {
        std::env::var(var)
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(default)
    }
}

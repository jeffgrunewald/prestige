use crate::error::Result;
use iceberg::{CatalogBuilder, NamespaceIdent, TableIdent, table::Table};
use iceberg_catalog_rest::{CommitTableRequest, RestCatalog, RestCatalogBuilder};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tracing::debug;

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

/// Authentication configuration for an Iceberg REST catalog.
///
/// Supports three strategies:
/// - **Bearer token**: static token passed directly (`token` field)
/// - **OAuth2 client credentials**: `credential` in `client_id:client_secret` format
/// - **None**: unauthenticated access (all fields `None`)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    /// Static bearer token.
    #[serde(skip_serializing)]
    pub token: Option<String>,
    /// OAuth2 client credential in `client_id:client_secret` format.
    #[serde(skip_serializing)]
    pub credential: Option<String>,
    /// Override the OAuth2 token endpoint (defaults to `{catalog_uri}/v1/oauth/tokens`).
    pub oauth2_server_uri: Option<String>,
    /// OAuth2 scope parameter.
    pub scope: Option<String>,
    /// OAuth2 audience parameter.
    pub audience: Option<String>,
    /// OAuth2 resource parameter.
    pub resource: Option<String>,
}

impl AuthConfig {
    /// Produce the properties HashMap expected by `iceberg-catalog-rest`.
    pub fn props(&self) -> HashMap<String, String> {
        let mut props = HashMap::new();
        if let Some(ref token) = self.token {
            props.insert("token".to_string(), token.clone());
        }
        if let Some(ref credential) = self.credential {
            props.insert("credential".to_string(), credential.clone());
        }
        if let Some(ref uri) = self.oauth2_server_uri {
            props.insert("oauth2-server-uri".to_string(), uri.clone());
        }
        if let Some(ref scope) = self.scope {
            props.insert("scope".to_string(), scope.clone());
        }
        if let Some(ref audience) = self.audience {
            props.insert("audience".to_string(), audience.clone());
        }
        if let Some(ref resource) = self.resource {
            props.insert("resource".to_string(), resource.clone());
        }
        props
    }
}

/// S3 storage configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct S3Config {
    pub endpoint: Option<String>,
    #[serde(skip_serializing)]
    pub access_key_id: Option<String>,
    #[serde(skip_serializing)]
    pub secret_access_key: Option<String>,
    pub region: Option<String>,
    pub path_style_access: Option<bool>,
}

impl S3Config {
    pub fn props(&self) -> HashMap<String, String> {
        let mut props = HashMap::new();
        if let Some(ref endpoint) = self.endpoint {
            props.insert("s3.endpoint".to_string(), endpoint.clone());
        }
        if let Some(ref access_key_id) = self.access_key_id {
            props.insert("s3.access-key-id".to_string(), access_key_id.clone());
        }
        if let Some(ref secret_access_key) = self.secret_access_key {
            props.insert(
                "s3.secret-access-key".to_string(),
                secret_access_key.clone(),
            );
        }
        if let Some(ref region) = self.region {
            props.insert("s3.region".to_string(), region.clone());
        }
        if let Some(path_style_access) = self.path_style_access {
            props.insert(
                "s3.path-style-access".to_string(),
                path_style_access.to_string(),
            );
        }
        props
    }
}

/// Full configuration for connecting to an Iceberg REST catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogConfig {
    pub catalog_uri: String,
    pub catalog_name: String,
    pub warehouse: Option<String>,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub s3: S3Config,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

impl CatalogConfig {
    pub fn builder(
        catalog_uri: impl Into<String>,
        catalog_name: impl Into<String>,
    ) -> CatalogConfigBuilder {
        CatalogConfigBuilder {
            config: CatalogConfig {
                catalog_uri: catalog_uri.into(),
                catalog_name: catalog_name.into(),
                warehouse: None,
                auth: AuthConfig::default(),
                s3: S3Config::default(),
                properties: HashMap::new(),
            },
        }
    }

    /// Connect to the catalog described by this config.
    pub async fn connect(&self) -> Result<Catalog> {
        Catalog::connect(self).await
    }
}

pub struct CatalogConfigBuilder {
    config: CatalogConfig,
}

impl CatalogConfigBuilder {
    pub fn warehouse(mut self, warehouse: impl Into<String>) -> Self {
        self.config.warehouse = Some(warehouse.into());
        self
    }

    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = auth;
        self
    }

    pub fn s3(mut self, s3: S3Config) -> Self {
        self.config.s3 = s3;
        self
    }

    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.properties.insert(key.into(), value.into());
        self
    }

    pub fn properties(mut self, props: HashMap<String, String>) -> Self {
        self.config.properties.extend(props);
        self
    }

    pub fn build(self) -> CatalogConfig {
        self.config
    }
}

impl From<&crate::Settings> for CatalogConfigBuilder {
    fn from(settings: &crate::Settings) -> Self {
        let mut builder = CatalogConfig::builder(&settings.region, "default").s3(S3Config {
            endpoint: settings.endpoint.clone(),
            access_key_id: settings.access_key_id.clone(),
            secret_access_key: settings.secret_access_key.clone(),
            region: Some(settings.region.clone()),
            path_style_access: None,
        });
        builder.config.warehouse = settings.endpoint.clone();
        builder
    }
}

// ---------------------------------------------------------------------------
// OAuth2 / endpoint auth
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct TokenResponse {
    access_token: String,
    /// Token lifetime in seconds, used for proactive refresh.
    expires_in: Option<u64>,
}

/// A cached OAuth2 token with its expiry instant.
#[derive(Clone)]
struct CachedToken {
    access_token: String,
    /// Proactive refresh deadline (75% of the original TTL).
    refresh_at: Option<tokio::time::Instant>,
}

impl CachedToken {
    fn from_response(resp: TokenResponse) -> Self {
        let refresh_at = resp.expires_in.map(|secs| {
            let refresh_secs = (secs as f64 * 0.75) as u64;
            tokio::time::Instant::now() + Duration::from_secs(refresh_secs.max(1))
        });
        Self {
            access_token: resp.access_token,
            refresh_at,
        }
    }

    fn is_expired(&self) -> bool {
        self.refresh_at
            .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
    }
}

#[derive(serde::Deserialize)]
struct CatalogConfigResponse {
    #[serde(default)]
    overrides: HashMap<String, String>,
    #[serde(default)]
    defaults: HashMap<String, String>,
}

/// Authentication strategy for direct REST API calls.
#[derive(Clone)]
enum EndpointAuth {
    None,
    Token(String),
    OAuth2 {
        token_endpoint: String,
        credential: String,
        extra_params: HashMap<String, String>,
        cached_token: Arc<Mutex<Option<CachedToken>>>,
    },
}

impl EndpointAuth {
    fn from_auth_config(auth: &AuthConfig, catalog_uri: &str) -> Self {
        if let Some(ref credential) = auth.credential {
            let token_endpoint = auth
                .oauth2_server_uri
                .clone()
                .unwrap_or_else(|| format!("{catalog_uri}/v1/oauth/tokens"));

            let mut extra_params = HashMap::new();
            if let Some(ref scope) = auth.scope {
                extra_params.insert("scope".to_string(), scope.clone());
            }
            if let Some(ref audience) = auth.audience {
                extra_params.insert("audience".to_string(), audience.clone());
            }
            if let Some(ref resource) = auth.resource {
                extra_params.insert("resource".to_string(), resource.clone());
            }

            Self::OAuth2 {
                token_endpoint,
                credential: credential.clone(),
                extra_params,
                cached_token: Arc::new(Mutex::new(None)),
            }
        } else if let Some(ref token) = auth.token {
            Self::Token(token.clone())
        } else {
            Self::None
        }
    }

    async fn fetch_token(
        client: &reqwest::Client,
        token_endpoint: &str,
        credential: &str,
        extra_params: &HashMap<String, String>,
    ) -> Result<CachedToken> {
        let (client_id, client_secret) = credential.split_once(':').unwrap_or((credential, ""));

        let mut params = vec![
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ];
        let extra: Vec<(&str, &str)> = extra_params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        params.extend(extra);

        let response = client
            .post(token_endpoint)
            .form(&params)
            .send()
            .await
            .map_err(|e| crate::Error::CatalogHttp(format!("OAuth2 token request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(crate::Error::CatalogHttp(format!(
                "OAuth2 token request returned {status}: {body}"
            )));
        }

        let token_response: TokenResponse = response.json().await.map_err(|e| {
            crate::Error::CatalogHttp(format!("failed to parse OAuth2 token response: {e}"))
        })?;

        debug!(
            expires_in = ?token_response.expires_in,
            "fetched OAuth2 token"
        );

        Ok(CachedToken::from_response(token_response))
    }

    async fn get_token(&self, client: &reqwest::Client) -> Result<Option<String>> {
        match self {
            Self::None => Ok(None),
            Self::Token(token) => Ok(Some(token.clone())),
            Self::OAuth2 {
                token_endpoint,
                credential,
                extra_params,
                cached_token,
            } => {
                let mut guard = cached_token.lock().await;
                if let Some(ref cached) = *guard {
                    if !cached.is_expired() {
                        return Ok(Some(cached.access_token.clone()));
                    }
                    debug!("OAuth2 token approaching expiry, proactively refreshing");
                }
                let cached =
                    Self::fetch_token(client, token_endpoint, credential, extra_params).await?;
                let access_token = cached.access_token.clone();
                *guard = Some(cached);
                Ok(Some(access_token))
            }
        }
    }

    async fn invalidate(&self) {
        if let Self::OAuth2 { cached_token, .. } = self {
            let mut guard = cached_token.lock().await;
            *guard = None;
        }
    }
}

// ---------------------------------------------------------------------------
// RestEndpoint — direct HTTP access for branch operations
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct RestEndpoint {
    uri: String,
    prefix: Option<String>,
    client: reqwest::Client,
    auth: EndpointAuth,
}

impl RestEndpoint {
    fn table_url(&self, table_ident: &TableIdent) -> String {
        let namespace = table_ident.namespace.to_url_string();
        let parts: Vec<&str> = [self.uri.as_str(), "v1"]
            .into_iter()
            .chain(self.prefix.as_deref())
            .chain(["namespaces", &namespace, "tables", &table_ident.name])
            .collect();
        parts.join("/")
    }

    async fn commit_table(&self, request: &CommitTableRequest) -> Result<()> {
        let url = request
            .identifier
            .as_ref()
            .map(|ident| self.table_url(ident))
            .ok_or_else(|| {
                crate::Error::CatalogHttp("table identifier required for commit".into())
            })?;

        let response = self.send_commit(&url, request).await?;

        match response.status().as_u16() {
            200 => return Ok(()),
            401 => {
                self.auth.invalidate().await;
                let retry_response = self.send_commit(&url, request).await?;
                return Self::handle_commit_response(retry_response).await;
            }
            _ => {}
        }

        Self::handle_commit_response(response).await
    }

    async fn send_commit(
        &self,
        url: &str,
        request: &CommitTableRequest,
    ) -> Result<reqwest::Response> {
        let mut http_request = self.client.post(url).json(request);

        if let Some(token) = self.auth.get_token(&self.client).await? {
            http_request = http_request.bearer_auth(token);
        }

        http_request
            .send()
            .await
            .map_err(|e| crate::Error::CatalogHttp(format!("commit request failed: {e}")))
    }

    async fn handle_commit_response(response: reqwest::Response) -> Result<()> {
        match response.status().as_u16() {
            200 => Ok(()),
            409 => Err(crate::Error::CatalogHttp(
                "commit conflict: one or more requirements failed".into(),
            )),
            404 => Err(crate::Error::CatalogHttp("table not found".into())),
            status => {
                let body = response.text().await.unwrap_or_default();
                Err(crate::Error::CatalogHttp(format!(
                    "unexpected status {status}: {body}"
                )))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Catalog — wrapper providing both iceberg trait delegation and direct HTTP
// ---------------------------------------------------------------------------

/// A shareable, authenticated connection to an Iceberg REST catalog.
///
/// Wraps `RestCatalog` in an `Arc` for sharing across async tasks, and also
/// holds a `RestEndpoint` for direct HTTP calls required by branch operations
/// (since `TableCommit`'s builder is `pub(crate)` in iceberg-rust 0.8).
#[derive(Clone)]
pub struct Catalog {
    inner: Arc<RestCatalog>,
    endpoint: RestEndpoint,
    config: Arc<CatalogConfig>,
}

impl Catalog {
    /// Connect to an Iceberg REST catalog.
    pub async fn connect(config: &CatalogConfig) -> Result<Self> {
        let mut props = HashMap::new();
        props.insert("uri".to_string(), config.catalog_uri.clone());
        if let Some(ref warehouse) = config.warehouse {
            props.insert("warehouse".to_string(), warehouse.clone());
        }
        props.extend(config.auth.props());
        props.extend(config.s3.props());
        props.extend(config.properties.clone());

        let catalog = RestCatalogBuilder::default()
            .load(&config.catalog_name, props)
            .await?;

        let endpoint = Self::resolve_endpoint(config).await?;

        Ok(Self {
            inner: Arc::new(catalog),
            endpoint,
            config: Arc::new(config.clone()),
        })
    }

    /// Rebuild the inner `RestCatalog` with a fresh OAuth2 token.
    ///
    /// `iceberg-catalog-rest` 0.8.0 caches the OAuth2 token internally with no
    /// automatic refresh. For indefinitely-running applications, call this when
    /// catalog operations start failing with authentication errors to obtain a
    /// new token without restarting the process.
    pub async fn reconnect(&mut self) -> Result<()> {
        debug!("catalog: reconnecting with fresh credentials");
        let fresh = Self::connect(&self.config).await?;
        self.inner = fresh.inner;
        self.endpoint = fresh.endpoint;
        debug!("catalog: reconnected successfully");
        Ok(())
    }

    async fn resolve_endpoint(config: &CatalogConfig) -> Result<RestEndpoint> {
        let client = reqwest::Client::new();
        let auth = EndpointAuth::from_auth_config(&config.auth, &config.catalog_uri);

        let mut config_url = format!("{}/v1/config", config.catalog_uri);
        if let Some(ref warehouse) = config.warehouse {
            config_url = format!("{config_url}?warehouse={warehouse}");
        }

        let mut request = client.get(&config_url);
        if let Some(token) = auth.get_token(&client).await? {
            request = request.bearer_auth(token);
        }

        let (uri, prefix) = match request.send().await {
            Ok(response) if response.status().is_success() => {
                let catalog_config: CatalogConfigResponse = response.json().await.map_err(|e| {
                    crate::Error::CatalogHttp(format!("failed to parse catalog config: {e}"))
                })?;

                let uri = catalog_config
                    .overrides
                    .get("uri")
                    .cloned()
                    .unwrap_or_else(|| config.catalog_uri.clone());

                let prefix = catalog_config
                    .overrides
                    .get("prefix")
                    .or_else(|| catalog_config.defaults.get("prefix"))
                    .cloned();

                (uri, prefix)
            }
            _ => (config.catalog_uri.clone(), None),
        };

        Ok(RestEndpoint {
            uri,
            prefix,
            client,
            auth,
        })
    }

    /// Check if a table exists.
    ///
    /// Uses `load_table` (GET) instead of the Iceberg REST `HEAD` endpoint because
    /// Polaris 1.3.0-incubating returns 400 for HEAD on existing tables.
    pub async fn table_exists(&self, table_ident: &TableIdent) -> Result<bool> {
        match iceberg::Catalog::load_table(&*self.inner, table_ident).await {
            Ok(_) => {
                debug!(table = %table_ident, exists = true, "catalog: table_exists result");
                Ok(true)
            }
            Err(e) if is_not_found_error(&e) => {
                debug!(table = %table_ident, exists = false, "catalog: table_exists result");
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Load a table from the catalog.
    pub async fn load_table(&self, table_ident: &TableIdent) -> Result<Table> {
        let table = iceberg::Catalog::load_table(&*self.inner, table_ident).await?;
        debug!(table = %table_ident, "catalog: table loaded");
        Ok(table)
    }

    /// Create a namespace if it doesn't exist.
    pub async fn create_namespace_if_not_exists(&self, namespace: &NamespaceIdent) -> Result<()> {
        match iceberg::Catalog::create_namespace(&*self.inner, namespace, HashMap::new()).await {
            Ok(_) => {
                debug!(namespace = ?namespace, "catalog: namespace created");
                Ok(())
            }
            Err(e) if is_already_exists_error(&e) => {
                debug!(namespace = ?namespace, "catalog: namespace already exists");
                Ok(())
            }
            Err(e) => {
                debug!(namespace = ?namespace, err = %e, "catalog: namespace creation failed");
                Err(e.into())
            }
        }
    }

    /// Create a table.
    pub async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: iceberg::TableCreation,
    ) -> Result<Table> {
        debug!(
            namespace = ?namespace,
            table = %creation.name,
            location = ?creation.location,
            has_partition_spec = creation.partition_spec.is_some(),
            has_sort_order = creation.sort_order.is_some(),
            "catalog: creating table"
        );
        match iceberg::Catalog::create_table(&*self.inner, namespace, creation).await {
            Ok(table) => {
                debug!(namespace = ?namespace, "catalog: table created successfully");
                Ok(table)
            }
            Err(e) => {
                debug!(namespace = ?namespace, err = %e, "catalog: table creation failed");
                Err(e.into())
            }
        }
    }

    /// Get an `Arc<dyn iceberg::Catalog>` for components that need the trait object
    /// (compactor, sink, poller).
    pub fn as_iceberg_catalog(&self) -> Arc<dyn iceberg::Catalog> {
        self.inner.clone()
    }

    /// Send a commit table request directly to the REST catalog API.
    ///
    /// Bypasses `TableCommit` (whose builder is `pub(crate)` in iceberg 0.8)
    /// by constructing and sending a `CommitTableRequest` via HTTP POST.
    pub(crate) async fn commit_table_request(&self, request: &CommitTableRequest) -> Result<()> {
        self.endpoint.commit_table(request).await
    }
}

/// Check whether an iceberg error indicates an "already exists conflict".
///
/// The iceberg-rust REST catalog (v0.8) maps HTTP 409 Conflict to
/// `ErrorKind::Unexpected` for `create_namespace` and `create_table` instead
/// of the semantically correct `NamespaceAlreadyExists` / `TableAlreadyExists`
/// variants. We match the proper kind first, then fall back to detecting the
/// `Unexpected` variant whose message contains `already exists`.
/// Check whether an iceberg error indicates a "not found" condition.
///
/// Polaris may return this when the catalog knows about a table but the
/// underlying metadata files in S3 are missing (e.g. after storage was wiped).
/// The `NotFoundException` message is buried in the error context, so we
/// check `to_string()` which includes the full context chain.
fn is_not_found_error(e: &iceberg::Error) -> bool {
    let msg = e.to_string();
    msg.contains("does not exist") || msg.contains("NotFoundException")
}

pub(crate) fn is_already_exists_error(e: &iceberg::Error) -> bool {
    matches!(
        e.kind(),
        iceberg::ErrorKind::NamespaceAlreadyExists | iceberg::ErrorKind::TableAlreadyExists
    ) || (e.kind() == iceberg::ErrorKind::Unexpected && e.to_string().contains("already exists"))
}

impl AsRef<RestCatalog> for Catalog {
    fn as_ref(&self) -> &RestCatalog {
        &self.inner
    }
}

// ---------------------------------------------------------------------------
// Backward-compatible free functions
// ---------------------------------------------------------------------------

/// Connect to a REST catalog using a `CatalogConfig`.
///
/// This is a convenience wrapper around `Catalog::connect`.
pub async fn connect_catalog(config: &CatalogConfig) -> Result<Catalog> {
    Catalog::connect(config).await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endpoint_auth_none() {
        let auth = AuthConfig::default();
        let endpoint_auth = EndpointAuth::from_auth_config(&auth, "http://localhost:8181");
        assert!(matches!(endpoint_auth, EndpointAuth::None));
    }

    #[test]
    fn test_endpoint_auth_token() {
        let auth = AuthConfig {
            token: Some("my-token".to_string()),
            ..Default::default()
        };
        let endpoint_auth = EndpointAuth::from_auth_config(&auth, "http://localhost:8181");
        assert!(matches!(endpoint_auth, EndpointAuth::Token(ref t) if t == "my-token"));
    }

    #[test]
    fn test_endpoint_auth_oauth2() {
        let auth = AuthConfig {
            credential: Some("client_id:client_secret".to_string()),
            scope: Some("PRINCIPAL_ROLE:ALL".to_string()),
            ..Default::default()
        };
        let endpoint_auth = EndpointAuth::from_auth_config(&auth, "http://localhost:8181");
        match endpoint_auth {
            EndpointAuth::OAuth2 {
                token_endpoint,
                credential,
                extra_params,
                ..
            } => {
                assert_eq!(token_endpoint, "http://localhost:8181/v1/oauth/tokens");
                assert_eq!(credential, "client_id:client_secret");
                assert_eq!(
                    extra_params.get("scope"),
                    Some(&"PRINCIPAL_ROLE:ALL".to_string())
                );
            }
            _ => panic!("expected OAuth2 variant"),
        }
    }

    #[test]
    fn test_endpoint_auth_oauth2_custom_uri() {
        let auth = AuthConfig {
            credential: Some("id:secret".to_string()),
            oauth2_server_uri: Some("http://auth.example.com/token".to_string()),
            ..Default::default()
        };
        let endpoint_auth = EndpointAuth::from_auth_config(&auth, "http://localhost:8181");
        match endpoint_auth {
            EndpointAuth::OAuth2 { token_endpoint, .. } => {
                assert_eq!(token_endpoint, "http://auth.example.com/token");
            }
            _ => panic!("expected OAuth2 variant"),
        }
    }

    #[test]
    fn test_table_url_without_prefix() {
        let endpoint = RestEndpoint {
            uri: "http://catalog.example.com".to_string(),
            prefix: None,
            client: reqwest::Client::new(),
            auth: EndpointAuth::None,
        };
        let table_ident = TableIdent::new(
            NamespaceIdent::new("mydb".to_string()),
            "mytable".to_string(),
        );
        let url = endpoint.table_url(&table_ident);
        assert_eq!(
            url,
            "http://catalog.example.com/v1/namespaces/mydb/tables/mytable"
        );
    }

    #[test]
    fn test_table_url_with_prefix() {
        let endpoint = RestEndpoint {
            uri: "http://catalog.example.com".to_string(),
            prefix: Some("warehouse123".to_string()),
            client: reqwest::Client::new(),
            auth: EndpointAuth::None,
        };
        let table_ident = TableIdent::new(
            NamespaceIdent::new("mydb".to_string()),
            "mytable".to_string(),
        );
        let url = endpoint.table_url(&table_ident);
        assert_eq!(
            url,
            "http://catalog.example.com/v1/warehouse123/namespaces/mydb/tables/mytable"
        );
    }

    #[test]
    fn test_auth_config_props() {
        let auth = AuthConfig {
            credential: Some("id:secret".to_string()),
            scope: Some("ADMIN".to_string()),
            ..Default::default()
        };
        let props = auth.props();
        assert_eq!(props.get("credential"), Some(&"id:secret".to_string()));
        assert_eq!(props.get("scope"), Some(&"ADMIN".to_string()));
        assert!(
            !props.contains_key("token"),
            "token=None should not be included in props"
        );
    }

    #[test]
    fn test_s3_config_props() {
        let s3 = S3Config {
            endpoint: Some("http://minio:9000".to_string()),
            access_key_id: Some("AKID".to_string()),
            secret_access_key: Some("SECRET".to_string()),
            region: Some("us-east-1".to_string()),
            path_style_access: Some(true),
        };
        let props = s3.props();
        assert_eq!(
            props.get("s3.endpoint"),
            Some(&"http://minio:9000".to_string())
        );
        assert_eq!(props.get("s3.access-key-id"), Some(&"AKID".to_string()));
        assert_eq!(props.get("s3.path-style-access"), Some(&"true".to_string()));
    }

    #[test]
    fn test_cached_token_not_expired_when_no_ttl() {
        let token = CachedToken::from_response(TokenResponse {
            access_token: "tok".into(),
            expires_in: None,
        });
        assert!(!token.is_expired());
    }

    #[test]
    fn test_cached_token_not_expired_within_ttl() {
        let token = CachedToken::from_response(TokenResponse {
            access_token: "tok".into(),
            expires_in: Some(3600),
        });
        // Just created — well within the 75% refresh window (2700s)
        assert!(!token.is_expired());
    }

    #[test]
    fn test_cached_token_expired_past_refresh_deadline() {
        let token = CachedToken {
            access_token: "tok".into(),
            refresh_at: Some(tokio::time::Instant::now() - Duration::from_secs(1)),
        };
        assert!(token.is_expired());
    }
}

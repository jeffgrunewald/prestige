use aws_config::BehaviorVersion;
use aws_sdk_s3::{config, primitives};
use aws_smithy_types_convert::stream::PaginationStreamExt;
use chrono::{DateTime, Utc};
use futures::{
    StreamExt, TryFutureExt, TryStreamExt, future,
    stream::{self, BoxStream},
};
use parquet::{
    basic::Repetition,
    schema::types::{Type, TypePtr},
};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::sync::Mutex;
use tracing::{debug, error, warn};

mod error;
pub mod file_compactor;
pub mod file_meta;
pub mod file_poller;
pub mod file_sink;
pub mod file_source;
pub mod file_upload;
mod settings;
pub(crate) mod telemetry;
pub mod traits;

pub use error::{AwsError, ChannelError, CompactionError, Error, FileMetaError, Result};
pub use file_compactor::{CompactionResult, FileCompactorConfig, FileCompactorConfigBuilder};
pub use file_meta::FileMeta;
pub use file_poller::{
    FilePollerConfig, FilePollerConfigBuilder, FilePollerServer, FilePollerState,
    FilePollerStateRecorder, FileStream, FileStreamReceiver, LookbackBehavior,
};
pub use file_sink::{ParquetSink, ParquetSinkBuilder, ParquetSinkClient};
pub use file_source::{RecordBatchStream, deserialize_stream, deserialize_to_vec};
pub use file_upload::{FileUpload, FileUploadServer};
pub use settings::Settings;
pub use traits::{ArrowSchema, ArrowSerialize, ParquetSerialize};

// Re-export derive macros from prestige-macros
pub use prestige_macros::{ArrowGroup, ArrowReader, ArrowWriter, PrestigeSchema};

/// Helper function to rebuild a parquet Type with OPTIONAL repetition and a new field name
/// This is used by the derive macros to properly handle Option<T> fields
pub fn rebuild_type_with_optional(base_type: Type, field_name: &str) -> Type {
    match base_type {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            type_length,
            scale,
            precision,
        } => {
            let mut builder = Type::primitive_type_builder(field_name, physical_type)
                .with_repetition(Repetition::OPTIONAL);

            if let Some(logical_type) = basic_info.logical_type() {
                builder = builder.with_logical_type(Some(logical_type.clone()));
            }

            if type_length >= 0 {
                builder = builder.with_length(type_length);
            }

            if scale >= 0 {
                builder = builder.with_scale(scale);
            }

            if precision >= 0 {
                builder = builder.with_precision(precision);
            }

            builder.build().expect("Failed to rebuild primitive type")
        }
        Type::GroupType { basic_info, fields } => {
            let mut builder =
                Type::group_type_builder(field_name).with_repetition(Repetition::OPTIONAL);

            if let Some(logical_type) = basic_info.logical_type() {
                builder = builder.with_logical_type(Some(logical_type.clone()));
            }

            let fields_vec: Vec<TypePtr> = fields.iter().map(Arc::clone).collect();
            builder = builder.with_fields(fields_vec);

            builder.build().expect("Failed to rebuild group type")
        }
    }
}

/// Helper function to rebuild a parquet Type with REQUIRED repetition and a new field name
/// This is used for map keys which must be non-nullable
pub fn rebuild_type_as_required(base_type: Type, field_name: &str) -> Type {
    match base_type {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            type_length,
            scale,
            precision,
        } => {
            let mut builder = Type::primitive_type_builder(field_name, physical_type)
                .with_repetition(Repetition::REQUIRED);

            if let Some(logical_type) = basic_info.logical_type() {
                builder = builder.with_logical_type(Some(logical_type.clone()));
            }

            if type_length >= 0 {
                builder = builder.with_length(type_length);
            }

            if scale >= 0 {
                builder = builder.with_scale(scale);
            }

            if precision >= 0 {
                builder = builder.with_precision(precision);
            }

            builder.build().expect("Failed to rebuild primitive type")
        }
        Type::GroupType { basic_info, fields } => {
            let mut builder =
                Type::group_type_builder(field_name).with_repetition(Repetition::REQUIRED);

            if let Some(logical_type) = basic_info.logical_type() {
                builder = builder.with_logical_type(Some(logical_type.clone()));
            }

            let fields_vec: Vec<TypePtr> = fields.iter().map(Arc::clone).collect();
            builder = builder.with_fields(fields_vec);

            builder.build().expect("Failed to rebuild group type")
        }
    }
}

pub type Client = aws_sdk_s3::Client;
pub type Stream<T> = BoxStream<'static, Result<T>>;
pub type FileMetaStream = Stream<FileMeta>;

static CLIENT_MAP: OnceLock<Mutex<HashMap<ClientKey, Client>>> = OnceLock::new();

#[derive(PartialEq, Eq, Hash, Debug)]
struct ClientKey {
    region: Option<String>,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
}

/// Create a new S3 client with caching
///
/// Clients are pooled based on region, endpoint, and credentials.
/// Subsequent calls with the same parameters will reuse existing clients.
pub async fn new_client(
    region: Option<String>,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
) -> Client {
    let mut client_map = CLIENT_MAP
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .await;

    let key = ClientKey {
        region: region.clone(),
        endpoint: endpoint.clone(),
        access_key_id: access_key_id.clone(),
        secret_access_key: secret_access_key.clone(),
    };

    if let Some(client) = client_map.get(&key) {
        debug!(params = ?key, "Using existing prestige s3 client");
        return client.clone();
    }

    let config = aws_config::defaults(BehaviorVersion::latest()).load().await;

    let mut s3_config = config::Builder::from(&config);

    if let Some(region_str) = region {
        s3_config = s3_config.region(aws_config::Region::new(region_str));
    }

    if let Some(endpoint) = endpoint {
        s3_config = s3_config.endpoint_url(endpoint);
        s3_config = s3_config.force_path_style(true);
    }

    if let Some((access_key_id, secret_access_key)) = access_key_id.zip(secret_access_key) {
        let creds = config::Credentials::builder()
            .provider_name("Static")
            .access_key_id(access_key_id)
            .secret_access_key(secret_access_key);

        s3_config = s3_config.credentials_provider(creds.build());
    }

    debug!(params = ?key, "Creating new prestige s3 client");
    let client = Client::from_conf(s3_config.build());
    client_map.insert(key, client.clone());
    client
}

/// List parquet files in an S3 bucket with optional timestamp filtering
///
/// Returns a stream of FileMeta objects for files matching the prefix
/// and within the specified timestamp range.
pub fn list_files<A, B>(
    client: &Client,
    bucket: impl Into<String>,
    prefix: impl Into<String>,
    after: A,
    before: B,
) -> FileMetaStream
where
    A: Into<Option<DateTime<Utc>>> + Copy,
    B: Into<Option<DateTime<Utc>>> + Copy,
{
    let file_type: String = prefix.into();
    let before = before.into();
    let after = after.into();

    client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(&file_type)
        .set_start_after(after.map(|dt| FileMeta::from((file_type.clone(), dt)).into()))
        .into_paginator()
        .send()
        .into_stream_03x()
        .map_ok(|page| stream::iter(page.contents.unwrap_or_default()).map(Ok))
        .map_err(AwsError::s3_error)
        .try_flatten()
        .try_filter_map(|file| {
            future::ready(FileMeta::try_from(&file).map(Some).map_err(Error::from))
        })
        .try_filter(move |meta| future::ready(after.is_none_or(|v| meta.timestamp > v)))
        .try_filter(move |meta| future::ready(before.is_none_or(|v| meta.timestamp <= v)))
        .boxed()
}

/// List all parquet files in an S3 bucket (collects stream into Vec)
pub async fn list_all_files<A, B>(
    client: &Client,
    bucket: impl Into<String>,
    prefix: impl Into<String>,
    after: A,
    before: B,
) -> Result<Vec<FileMeta>>
where
    A: Into<Option<DateTime<Utc>>> + Copy,
    B: Into<Option<DateTime<Utc>>> + Copy,
{
    list_files(client, bucket, prefix, after, before)
        .try_collect()
        .await
}

/// Upload a parquet file to S3
pub async fn put_file(client: &Client, bucket: impl Into<String>, file: &Path) -> Result {
    let byte_stream = primitives::ByteStream::from_path(&file).await?;

    client
        .put_object()
        .bucket(bucket)
        .key(file.file_name().map(|name| name.to_string_lossy()).unwrap())
        .body(byte_stream)
        .content_type("application/vnd.apache.parquet")
        .send()
        .map_ok(|_| ())
        .map_err(AwsError::s3_error)
        .await
}

/// Remove a file from S3
///
/// Retries up to 3 times with backoff (0.5s, 1s) on failure.
pub async fn remove_file(
    client: &Client,
    bucket: impl Into<String>,
    key: impl Into<String>,
) -> Result {
    let bucket = bucket.into();
    let key = key.into();
    let delays = [
        Some(Duration::from_millis(500)),
        Some(Duration::from_millis(1000)),
        None,
    ];

    let mut last_error = None;

    for (attempt, delay) in delays.iter().enumerate() {
        match client
            .delete_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(_) => return Ok(()),
            Err(err) => {
                last_error = Some(err);
                if let Some(d) = delay {
                    warn!(
                        %bucket,
                        %key,
                        attempt = attempt + 1,
                        "Failed to delete S3 object, retrying"
                    );
                    tokio::time::sleep(*d).await;
                }
            }
        }
    }

    let err = last_error.expect("last_error must be set after 3 failed attempts");
    error!(
        %bucket,
        %key,
        "Failed to delete S3 object after 3 attempts"
    );
    Err(AwsError::s3_error(err))
}

/// Download a file from S3 as bytes
pub async fn get_file(
    client: &Client,
    bucket: impl Into<String>,
    key: impl Into<String>,
) -> Result<bytes::Bytes> {
    let output = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .map_err(AwsError::s3_error)
        .await?;

    output
        .body
        .collect()
        .await
        .map(|data| data.into_bytes())
        .map_err(Error::from)
}

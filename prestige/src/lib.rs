use aws_config::BehaviorVersion;
use aws_sdk_s3::{config, primitives, types};
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
use tokio::{
    fs::{File, metadata},
    sync::Mutex,
};
use tracing::{debug, error, info, warn};

mod error;
pub mod file_compactor;
pub mod file_meta;
pub mod file_poller;
pub mod file_sink;
pub mod file_source;
pub mod file_upload;
#[cfg(feature = "iceberg")]
pub mod iceberg;
pub mod serde_u8_array;
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

// Re-export serde_bytes so users of #[prestige(as_binary)] don't need a direct dep.
pub use serde_bytes;

// Re-export derive macros from prestige-macros
pub use prestige_macros::{ArrowGroup, ArrowReader, ArrowWriter};

// Re-export the attribute macro that auto-injects serde_bytes on as_binary fields.
pub use prestige_macros::prestige_schema;

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

            if let Some(logical_type) = basic_info.logical_type_ref() {
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

            if let Some(logical_type) = basic_info.logical_type_ref() {
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

            if let Some(logical_type) = basic_info.logical_type_ref() {
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

            if let Some(logical_type) = basic_info.logical_type_ref() {
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

const PARQUET_CONTENT_TYPE: &str = "application/vnd.apache.parquet";

/// Multipart upload threshold/minimum file size (S3 minimum part size): 5 MB
const MULTIPART_PART_SIZE: usize = 5 * 1024 * 1024;

/// Upload a parquet file to S3
///
/// Uses multipart upload for files >= 5 MB to avoid idle socket timeouts
/// on large uploads. Files under the threshold use a single PUT.
pub async fn put_file(client: &Client, bucket: impl Into<String>, file: &Path) -> Result {
    let bucket = bucket.into();
    let key = file
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .ok_or_else(|| Error::Internal(format!("path has no file name: {}", file.display())))?;

    let file_size = metadata(file).await.map_err(Error::Io)?.len() as usize;

    if file_size < MULTIPART_PART_SIZE {
        let byte_stream = primitives::ByteStream::from_path(file).await?;

        client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(byte_stream)
            .content_type(PARQUET_CONTENT_TYPE)
            .send()
            .map_ok(|_| ())
            .map_err(AwsError::s3_error)
            .await
    } else {
        put_file_multipart(client, &bucket, &key, file, file_size).await
    }
}

/// Upload a file to S3 using multipart upload with 5 MB parts
async fn put_file_multipart(
    client: &Client,
    bucket: &str,
    key: &str,
    file: &Path,
    file_size: usize,
) -> Result {
    let total_parts = file_size.div_ceil(MULTIPART_PART_SIZE);
    info!(key, file_size, total_parts, "Starting multipart upload",);

    let create_resp = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .content_type(PARQUET_CONTENT_TYPE)
        .send()
        .await
        .map_err(AwsError::s3_error)?;

    let upload_id = create_resp
        .upload_id()
        .ok_or_else(|| Error::Internal("multipart upload response missing upload_id".into()))?
        .to_string();

    let result = upload_parts(client, bucket, key, &upload_id, file, total_parts).await;

    match result {
        Ok(completed_parts) => {
            let completed_upload = types::CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build();

            client
                .complete_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .multipart_upload(completed_upload)
                .send()
                .await
                .map_err(AwsError::s3_error)?;

            info!(key, "Multipart upload completed");
            Ok(())
        }
        Err(e) => {
            warn!(key, upload_id, err = %e, "Multipart upload failed, aborting");
            if let Err(abort_err) = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
                .await
            {
                warn!(key, upload_id, err = %abort_err, "Failed to abort multipart upload");
            }
            Err(e)
        }
    }
}

/// Read file in chunks and upload each part sequentially
async fn upload_parts(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    file: &Path,
    total_parts: usize,
) -> Result<Vec<types::CompletedPart>> {
    use tokio::io::AsyncReadExt;

    let mut file_handle = File::open(file).await.map_err(Error::Io)?;

    let mut completed_parts = Vec::with_capacity(total_parts);
    let mut part_number: i32 = 1;
    let mut buf = vec![0u8; MULTIPART_PART_SIZE];

    loop {
        let mut bytes_read = 0;
        // Fill the buffer completely (or until EOF)
        while bytes_read < MULTIPART_PART_SIZE {
            match file_handle.read(&mut buf[bytes_read..]).await {
                Ok(0) => break,
                Ok(n) => bytes_read += n,
                Err(e) => return Err(Error::Io(e)),
            }
        }

        if bytes_read == 0 {
            break;
        }

        let body = primitives::ByteStream::from(buf[..bytes_read].to_vec());

        let upload_resp = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(body)
            .send()
            .await
            .map_err(AwsError::s3_error)?;

        let e_tag = upload_resp.e_tag().map(|s| s.to_string());

        debug!(
            key,
            part_number,
            bytes = bytes_read,
            "Uploaded part {}/{}",
            part_number,
            total_parts,
        );

        completed_parts.push(
            types::CompletedPart::builder()
                .set_e_tag(e_tag)
                .part_number(part_number)
                .build(),
        );

        part_number += 1;
    }

    Ok(completed_parts)
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

    let err = last_error
        .ok_or_else(|| Error::Internal("retry loop exited without capturing an error".into()))?;
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

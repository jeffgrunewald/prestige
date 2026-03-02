use std::path::{Path, PathBuf};
use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("aws error: {0}")]
    Aws(#[from] AwsError),

    #[error("prestige configuration error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("file meta error: {0}")]
    FileMeta(#[from] FileMetaError),

    #[error("channel error: {0}")]
    Channel(#[from] ChannelError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serde arrow error: {0}")]
    SerdeArrow(String),

    #[cfg(feature = "iceberg")]
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    #[cfg(feature = "iceberg")]
    #[error("catalog http error: {0}")]
    CatalogHttp(String),

    #[cfg(feature = "iceberg")]
    #[error("branch error: {0}")]
    Branch(String),

    #[cfg(feature = "sqlx")]
    #[error("db error: {0}")]
    Db(#[from] sqlx::Error),

    #[error("compaction error: {0}")]
    Compaction(#[from] CompactionError),

    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(thiserror::Error, Debug)]
pub enum AwsError {
    #[error("s3: {0}")]
    S3(Box<aws_sdk_s3::Error>),

    #[error("streaming: {0}")]
    Streaming(#[from] aws_sdk_s3::primitives::ByteStreamError),
}

impl AwsError {
    pub fn s3_error<T>(err: T) -> Error
    where
        T: Into<aws_sdk_s3::Error>,
    {
        Error::from(err.into())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum FileMetaError {
    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(i64),

    #[error("invalid timestamp string: {0}")]
    TimestampStr(#[from] std::num::ParseIntError),

    #[error("filename did not match regex: {0}")]
    Regex(String),

    #[error("no file name found")]
    MissingFilename,

    #[error("IO: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Error, Debug)]
pub enum ChannelError {
    #[error("failed to send {prefix} for process {process}")]
    PollerSendError { prefix: String, process: String },

    #[error("channel closed sink {name}")]
    SinkClosed { name: String },

    #[error("timeout for sink {name}")]
    SinkTimeout { name: String },

    #[error("channel closed for upload {path}")]
    UploadClosed { path: PathBuf },
}

impl ChannelError {
    pub fn poller_send_error(prefix: &str, process: &str) -> Error {
        Error::Channel(Self::PollerSendError {
            prefix: prefix.to_owned(),
            process: process.to_owned(),
        })
    }

    pub fn sink_closed(name: &str) -> Error {
        Error::Channel(Self::SinkClosed {
            name: name.to_owned(),
        })
    }

    pub fn sink_timeout(name: &str) -> Error {
        Error::Channel(Self::SinkTimeout {
            name: name.to_owned(),
        })
    }

    pub fn upload_closed(path: &Path) -> Error {
        Error::Channel(Self::UploadClosed {
            path: path.to_owned(),
        })
    }
}

#[derive(Error, Debug)]
pub enum CompactionError {
    #[error("upload failed for {file_key}")]
    UploadFailed { file_key: String },

    #[error("no source files provided for compaction")]
    NoSourceFiles,
}

impl From<aws_sdk_s3::Error> for Error {
    fn from(err: aws_sdk_s3::Error) -> Self {
        Self::Aws(AwsError::S3(Box::new(err)))
    }
}

impl From<aws_sdk_s3::primitives::ByteStreamError> for Error {
    fn from(err: aws_sdk_s3::primitives::ByteStreamError) -> Self {
        Self::from(AwsError::Streaming(err))
    }
}

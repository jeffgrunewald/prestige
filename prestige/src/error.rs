use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("aws sdk s3 error: {0}")]
    Aws(#[from] aws_sdk_s3::Error),
    #[error("prestige configuration error: {0}")]
    Config(#[from] config::ConfigError),
    #[cfg(feature = "sqlx")]
    #[error("db error: {0}")]
    Db(#[from] sqlx::Error),
}

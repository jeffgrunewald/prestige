use crate::{Error, Result};
use config::{Config, File};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Settings {
    /// Bucket name for the store. Required
    pub bucket: String,
    /// API endpoint for the bucket. Optional
    pub endpoint: Option<String>,
    /// Region for the endpoint. Default: us-west-2
    #[serde(default = "default_region")]
    pub region: String,

    /// API credentials. Optional; instance-based authentication
    /// from within the cloud environment should be preferred for
    /// security whenever possible.
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

fn default_region() -> String {
    "us-west-2".to_string()
}

impl Settings {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Config::builder()
            .add_source(File::with_name(&path.as_ref().to_string_lossy()))
            .build()
            .and_then(|config| config.try_deserialize())
            .map_err(Error::from)
    }
}
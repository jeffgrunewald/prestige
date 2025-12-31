use crate::{
    Client, Result,
    error::ChannelError,
    put_file,
    telemetry::{
        self, FILE_UPLOAD_COUNT, FILE_UPLOAD_DURATION_MS, FILE_UPLOAD_SIZE_BYTES, telemetry_labels,
    },
};
use futures::{StreamExt, future::LocalBoxFuture};
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use super_visor::{ManagedProc, ShutdownSignal};
use tokio::{fs, sync::mpsc, time::sleep};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error, info, warn};

pub type MessageSender = mpsc::UnboundedSender<PathBuf>;
pub type MessageReceiver = mpsc::UnboundedReceiver<PathBuf>;

pub fn message_channel() -> (MessageSender, MessageReceiver) {
    mpsc::unbounded_channel()
}

/// Client handle for uploading files to S3
#[derive(Debug, Clone)]
pub struct FileUpload {
    pub sender: MessageSender,
}

/// Server that handles async file uploads to S3 with retry logic
pub struct FileUploadServer {
    messages: UnboundedReceiverStream<PathBuf>,
    client: Client,
    bucket: String,
}

impl FileUpload {
    /// Create a new FileUpload client and server pair
    pub async fn new(client: Client, bucket: String) -> (Self, FileUploadServer) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (
            Self { sender },
            FileUploadServer {
                messages: UnboundedReceiverStream::new(receiver),
                client,
                bucket,
            },
        )
    }

    /// Queue a file for upload to S3
    ///
    /// The file will be uploaded asynchronously and deleted locally on success.
    pub async fn upload_file(&self, file: &Path) -> Result {
        self.sender
            .send(file.to_path_buf())
            .map_err(|_| ChannelError::upload_closed(file))
    }
}

impl ManagedProc for FileUploadServer {
    fn run_proc(
        self: Box<Self>,
        shutdown: ShutdownSignal,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move {
            self.run(shutdown)
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))
        })
    }
}

impl FileUploadServer {
    /// Run the upload server loop
    ///
    /// Processes upload requests concurrently with retry logic.
    /// Automatically deletes files after successful upload.
    pub async fn run(self, shutdown: ShutdownSignal) -> Result {
        info!("starting file uploader {}", self.bucket);

        let client = &self.client;
        let bucket = &self.bucket;

        let uploads = self.messages.for_each_concurrent(5, |path| async move {
            let path_str = path.display();
            if !path.exists() {
                warn!("ignoring absent file {path_str}");
                return;
            }
            if !path.is_file() {
                warn!("ignoring non file {path_str}");
                return;
            }

            // Get file size before upload to emit metrics
            let file_size = match fs::metadata(&path).await {
                Ok(meta) => Some(meta.len()),
                Err(err) => {
                    warn!("failed to get file size for {path_str}: {err:?}");
                    None
                }
            };

            let mut retry = 0;
            const MAX_RETRIES: u8 = 5;
            const RETRY_WAIT: Duration = Duration::from_secs(10);

            while retry <= MAX_RETRIES {
                debug!("storing {path_str} in {bucket} retry {retry}");

                // Start timing the upload attempt
                let upload_start = Instant::now();

                match put_file(client, bucket, &path).await {
                    Ok(()) => {
                        let duration_ms = upload_start.elapsed().as_secs_f64() * 1000.0;

                        // Record successful upload duration
                        telemetry::record_histogram(
                            FILE_UPLOAD_DURATION_MS,
                            duration_ms,
                            telemetry_labels!("bucket" => bucket.as_str()),
                        );
                        // Record upload count increment
                        telemetry::increment_counter(
                            FILE_UPLOAD_COUNT,
                            1,
                            telemetry_labels!(
                                "bucket" => bucket.as_str(),
                                "status" => "success",
                            ),
                        );
                        // Record file size on successful upload
                        if let Some(size) = file_size {
                            telemetry::record_histogram(
                                FILE_UPLOAD_SIZE_BYTES,
                                size as f64,
                                telemetry_labels!("bucket" => bucket.as_str()),
                            );
                        }

                        match fs::remove_file(&path).await {
                            Ok(()) => info!("stored {path_str} in {bucket}"),
                            Err(err) => {
                                error!("failed to remove uploaded file {path_str}: {err:?}")
                            }
                        }
                        return;
                    }
                    Err(err) => {
                        let duration_ms = upload_start.elapsed().as_secs_f64() * 1000.0;

                        // Record failed upload duration (useful for identifying timeout patterns)
                        telemetry::record_histogram(
                            FILE_UPLOAD_DURATION_MS,
                            duration_ms,
                            telemetry_labels!("bucket" => bucket.as_str()),
                        );

                        if retry == MAX_RETRIES {
                            telemetry::increment_counter(
                                FILE_UPLOAD_COUNT,
                                1,
                                telemetry_labels!("bucket" => bucket.as_str(), "status" => "error"),
                            );
                        }

                        error!("failed to store {path_str} in {bucket} retry: {retry}: {err:?}");
                        retry += 1;
                        sleep(RETRY_WAIT).await;
                    }
                }
            }
        });

        tokio::select! {
            _ = uploads => (),
            _ = shutdown => (),
        }

        info!("stopping file uploader {}", self.bucket);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_upload_channel_communication() {
        let (sender, mut receiver) = message_channel();
        let path = PathBuf::from("/tmp/test.parquet");

        sender.send(path.clone()).unwrap();
        let received = receiver.recv().await.unwrap();

        assert_eq!(received, path);
    }

    #[tokio::test]
    async fn test_upload_closed_channel_error() {
        let (upload, _server) = FileUpload::new(
            crate::new_client(None, None, None, None).await,
            "test-bucket".to_string(),
        )
        .await;

        drop(_server); // Close receiver

        let temp_file = NamedTempFile::new().unwrap();
        let result = upload.upload_file(temp_file.path()).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), crate::Error::Channel(_)));
    }
}

use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::instrument;

use crate::error::{BlobStorageError, Result};
use crate::types::BlobMeta;

impl super::S3BlobStore {
    /// Upload a single blob using S3 Multipart Upload.
    ///
    /// Reads data from `reader` in [`self.part_size`] chunks, uploads each
    /// part via `UploadPart`, then completes the multipart upload.
    /// If any part fails, the multipart upload is aborted.
    /// Memory usage per part is bounded by `self.part_size`.
    #[instrument(skip(self, reader))]
    pub(super) async fn upload_multipart(
        &self,
        key: &str,
        reader: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<BlobMeta> {
        let upload_id = self.start_multipart(key).await?;
        let (completed_parts, total_size) = self.upload_parts(key, &upload_id, reader).await?;
        let parts_count = completed_parts.len();
        let result = self
            .complete_multipart(key, &upload_id, completed_parts, total_size)
            .await;
        tracing::debug!(
            key,
            total_size,
            parts_count,
            "Completed multipart upload to S3"
        );
        result
    }

    /// Phase 1: Initiate the multipart upload and return the upload ID.
    #[instrument(skip(self))]
    async fn start_multipart(&self, key: &str) -> Result<String> {
        let upload_id = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                if self.is_misconfigured(&e) {
                    BlobStorageError::BackendMisconfigured(format!(
                        "S3 bucket '{}' does not exist or is not accessible",
                        self.bucket
                    ))
                } else {
                    BlobStorageError::Storage {
                        message: format!("S3 CreateMultipartUpload failed for key '{key}'"),
                        source: Some(Box::new(e)),
                    }
                }
            })?
            .upload_id()
            .ok_or_else(|| BlobStorageError::Storage {
                message: format!(
                    "Missing upload_id in CreateMultipartUpload response for key '{key}'"
                ),
                source: None,
            })
            .map(|id| id.to_string())?;
        tracing::debug!(key, upload_id, "Started multipart upload to S3");
        Ok(upload_id)
    }

    /// Phase 2: Read data in chunks and upload each part.
    ///
    /// Each part is read up to `part_size` bytes via `reader.take(part_size)`,
    /// then uploaded to S3 as a [`ByteStream`].
    /// Memory usage is bounded by the configured `part_size`.
    #[instrument(skip(self, reader))]
    async fn upload_parts(
        &self,
        key: &str,
        upload_id: &str,
        reader: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<(Vec<aws_sdk_s3::types::CompletedPart>, u64)> {
        let part_size = self.part_size;
        let mut completed_parts = Vec::new();
        let mut part_number = 1i32;
        let mut total_size: u64 = 0;

        loop {
            let mut part_reader = reader.take(part_size);

            // Read one chunk from the stream — SDK ByteStream handles buffering
            let mut buffer = Vec::new();
            let n = match part_reader.read_to_end(&mut buffer).await {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!(
                        key,
                        upload_id,
                        part_number,
                        "Failed to read blob data; aborting multipart upload"
                    );
                    self.abort_upload(key, upload_id).await;
                    return Err(BlobStorageError::Storage {
                        message: format!("failed to read blob data for key '{key}'"),
                        source: Some(Box::new(e)),
                    });
                }
            };

            if n == 0 {
                break; // EOF — no more data
            }

            total_size += n as u64;

            // Upload this part — stream directly from buffer (SDK creates ByteStream)
            let result = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(aws_sdk_s3::primitives::ByteStream::from(buffer))
                .send()
                .await;

            match result {
                Ok(resp) => {
                    let etag = resp.e_tag.unwrap_or_default();
                    tracing::debug!(key, upload_id, part_number, "Uploaded part to S3");
                    completed_parts.push(
                        aws_sdk_s3::types::CompletedPart::builder()
                            .e_tag(&etag)
                            .part_number(part_number)
                            .build(),
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        key,
                        upload_id,
                        part_number,
                        "S3 UploadPart failed; aborting multipart upload"
                    );
                    self.abort_upload(key, upload_id).await;
                    return Err(if self.is_misconfigured(&e) {
                        BlobStorageError::BackendMisconfigured(format!(
                            "S3 bucket '{}' does not exist or is not accessible",
                            self.bucket
                        ))
                    } else {
                        BlobStorageError::Storage {
                            message: format!(
                                "S3 UploadPart failed for key '{key}' (part {part_number}, upload_id {upload_id})"
                            ),
                            source: Some(Box::new(e)),
                        }
                    });
                }
            }

            part_number += 1;
        }

        Ok((completed_parts, total_size))
    }

    /// Phase 3: Finalise the multipart upload.
    #[instrument(skip(self))]
    async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        completed_parts: Vec<aws_sdk_s3::types::CompletedPart>,
        total_size: u64,
    ) -> Result<BlobMeta> {
        let completed_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        let resp = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await
            .map_err(|e| {
                if self.is_misconfigured(&e) {
                    BlobStorageError::BackendMisconfigured(format!(
                        "S3 bucket '{}' does not exist or is not accessible",
                        self.bucket
                    ))
                } else {
                    BlobStorageError::Storage {
                        message: format!("S3 CompleteMultipartUpload failed for key '{key}'"),
                        source: Some(Box::new(e)),
                    }
                }
            })?;

        Ok(BlobMeta {
            key: key.to_string(),
            stored_size: total_size,
            modified_at: chrono::Utc::now(),
            etag: resp.e_tag.map(|s| s.to_string()),
        })
    }

    /// Abort a multipart upload (called on error during upload_parts).
    #[instrument(skip(self))]
    async fn abort_upload(&self, key: &str, upload_id: &str) {
        tracing::warn!(key, upload_id, "Aborting multipart upload to S3");
        let _ = self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
    }
}

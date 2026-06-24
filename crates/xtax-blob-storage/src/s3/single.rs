use tracing::instrument;

use crate::error::{BlobStorageError, Result};
use crate::types::BlobMeta;

impl super::S3BlobStore {
    /// Upload a single blob using S3 PutObject.
    ///
    /// Used for blobs smaller than the minimum multipart part size (5 MiB).
    /// Larger blobs use [`upload_multipart`](Self::upload_multipart) instead.
    #[instrument(skip(self))]
    pub(super) async fn put_object(&self, key: &str, data: bytes::Bytes) -> Result<BlobMeta> {
        let size = data.len() as u64;
        let resp = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from(data))
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
                        message: format!("S3 PutObject failed for key '{key}'"),
                        source: Some(Box::new(e)),
                    }
                }
            })?;

        Ok(BlobMeta {
            key: key.to_string(),
            stored_size: size,
            modified_at: chrono::Utc::now(),
            etag: resp.e_tag.map(|s| s.to_string()),
        })
    }
}

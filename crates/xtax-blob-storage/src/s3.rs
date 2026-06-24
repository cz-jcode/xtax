use aws_sdk_s3::Client;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;

use crate::error::{BlobStorageError, Result};

pub(crate) mod multipart;
pub(crate) mod single;
pub(crate) mod store;

/// Default size of each part in a multipart upload (50 MiB).
/// Must be at least 5 MiB (AWS minimum).
pub const DEFAULT_MULTIPART_PART_SIZE: u64 = 50 * 1024 * 1024; // 50 MiB

/// Minimum allowed multipart part size (5 MiB, AWS requirement).
pub const MIN_MULTIPART_PART_SIZE: u64 = 5 * 1024 * 1024; // 5 MiB

/// S3-compatible blob store (works with Garage, MinIO, AWS S3, etc.).
///
/// # Example
///
/// ```rust,no_run
/// use aws_sdk_s3::Client;
/// use xtax_blob_storage::{BlobStore, BlobInput, BlobStoreBuilder};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::new(&aws_config::load_from_env().await);
/// let store = BlobStoreBuilder::new()
///     .with_s3(client, "my-bucket")
///     .build()
///     .await?;
///
/// store.put(vec![BlobInput::new("hello.txt", b"data".as_slice())]).await?;
/// # Ok(())
/// # }
/// ```
///
/// Requires `s3` feature.
pub struct S3BlobStore {
    pub(crate) client: Client,
    pub(crate) bucket: String,
    /// Size (in bytes) of each part in a multipart upload.
    /// Must be at least 5 MiB (AWS requirement). Defaults to 50 MiB.
    /// Memory usage per part is bounded by this value.
    pub(crate) part_size: u64,
}

impl S3BlobStore {
    /// Create a new S3 blob store backed by an S3-compatible service.
    ///
    /// Works with AWS S3, Garage, MinIO, and any S3-compatible service.
    /// Blobs smaller than `part_size` use a single PutObject;
    /// larger blobs use multipart upload.
    pub fn new(client: Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            part_size: DEFAULT_MULTIPART_PART_SIZE,
        }
    }

    /// Set the size (in bytes) of each part in a multipart upload.
    ///
    /// Must be at least 5 MiB (AWS minimum). Defaults to 50 MiB.
    /// Memory usage per part is bounded by this value.
    /// Multipart is automatically used for blobs ≥ 5 MiB (S3 minimum).
    pub fn with_multipart_part_size(mut self, size: u64) -> Self {
        self.part_size = size.max(MIN_MULTIPART_PART_SIZE);
        self
    }

    /// Get the current multipart part size.
    pub fn multipart_part_size(&self) -> u64 {
        self.part_size
    }

    /// Helper to perform an S3 GetObject request and handle errors uniformly.
    pub(crate) async fn get_object_output(
        &self,
        key: &str,
    ) -> Result<aws_sdk_s3::operation::get_object::GetObjectOutput> {
        self.client
            .get_object()
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
                } else if self.is_not_found(&e) {
                    BlobStorageError::NotFound(key.to_string())
                } else {
                    BlobStorageError::Storage {
                        message: format!("S3 get failed for key '{key}'"),
                        source: Some(Box::new(e)),
                    }
                }
            })
    }

    /// Check if an S3 SDK error represents a "not found" condition for the
    /// requested object key (NOT the bucket).  Bucket-not-found is a
    /// configuration problem and is handled by
    /// [`is_misconfigured`](Self::is_misconfigured).
    ///
    /// Uses the AWS SDK's typed error code system rather than string matching on
    /// error messages (which may change between SDK versions or be localized).
    pub(crate) fn is_not_found<E>(&self, err: &aws_sdk_s3::error::SdkError<E>) -> bool
    where
        E: std::error::Error + ProvideErrorMetadata + 'static,
    {
        err.as_service_error()
            .and_then(|e| e.code())
            .is_some_and(|code| matches!(code, "NoSuchKey" | "NotFound"))
    }

    /// Check if an S3 SDK error represents a misconfigured backend — the
    /// bucket does not exist or is not accessible.
    pub(crate) fn is_misconfigured<E>(&self, err: &aws_sdk_s3::error::SdkError<E>) -> bool
    where
        E: std::error::Error + ProvideErrorMetadata + 'static,
    {
        err.as_service_error()
            .and_then(|e| e.code())
            .is_some_and(|code| code == "NoSuchBucket")
    }
}

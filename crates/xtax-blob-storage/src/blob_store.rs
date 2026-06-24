use async_trait::async_trait;
use chrono::Utc;
use tokio::io::AsyncRead;

use crate::error::Result;
use crate::list_filter::ListFilter;
use crate::types::{BlobInput, BlobMeta, PutResult};
use crate::visitor::BlobVisitor;

/// Generic blob storage abstraction.
///
/// Every backend and every manipulation layer implements this trait,
/// making them transparent and composable.
///
/// # Implementations
///
/// | Type | Description |
/// |------|-------------|
/// | [`FsBlobStore`](crate::fs::FsBlobStore) | Filesystem backend (feature `fs`) |
/// | [`S3BlobStore`](crate::s3::S3BlobStore) | S3-compatible backend (feature `s3`) |
/// | `PrefixBlobStore` | Transparent key prefixing (via builder) |
/// | [`EncryptedBlobStore`](crate::encrypt::store::EncryptedBlobStore) | Envelope encryption |
/// | [`BlobCleanup`](crate::cleanup::BlobCleanup) | Lifecycle cleanup |
///
/// For a step-by-step tutorial see the
/// [Getting Started guide](https://github.com/cz-jcode/xtax/blob/main/crates/xtax-blob-storage/docs/guide.md).
/// For architecture details see the
/// [Architecture guide](https://github.com/cz-jcode/xtax/blob/main/crates/xtax-blob-storage/docs/architecture.md).
/// For custom layers see the
/// [Custom layers guide](https://github.com/cz-jcode/xtax/blob/main/crates/xtax-blob-storage/docs/layers.md).
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Store one or more blobs.
    async fn put(&self, blobs: Vec<BlobInput>) -> Result<PutResult>;

    /// Retrieve a blob by key.
    async fn get(&self, key: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>>;

    /// Delete one or more blobs by key.
    ///
    /// ## Best-effort batch semantics
    ///
    /// Every key is always processed. If one or more keys fail, the method
    /// returns `Err(BlobStorageError::Batch(BatchError {...}))` containing
    /// the list of succeeded and failed keys with per-key error details:
    ///
    /// | `PerKeyError` variant | Meaning |
    /// |---|---|
    /// | `PermissionDenied(msg)` | Insufficient permissions |
    /// | `Unknown { message }` | Other backend error (I/O, timeout, …) |
    ///
    /// Keys that do **not** exist are **not** considered errors — delete is
    /// idempotent, and `NotFound` on a delete is silently ignored.
    ///
    /// ## Validation
    ///
    /// All keys are validated via [`validate_blob_key()`](crate::validate::validate_blob_key) before any backend
    /// operation. Invalid keys cause an immediate `Err(InvalidInput)` abort.
    async fn delete(&self, keys: &[&str]) -> Result<()>;

    /// List blob keys matching the given filter.
    ///
    /// ## Contract
    ///
    /// - All blobs stored via [`put`](BlobStore::put) MUST be visible to `list`
    ///   (assuming they haven't been deleted).
    /// - Keys are returned as strings — backends MUST use `/` as the path
    ///   separator (e.g. `a/b/c.txt`), regardless of the underlying storage
    ///   platform.
    /// - The caller-supplied [`ListFilter`] is applied to each key. Only keys
    ///   that pass the filter are returned.
    /// - Results are sorted alphabetically.
    /// - Empty directories or prefixes are NOT included in the result.
    ///
    /// ## Implementation notes
    ///
    /// - **FS backend**: walks the root directory recursively (breadth-first).
    ///   Keys containing `/` are mapped to nested directories — identical
    ///   behaviour to S3.
    /// - **S3 backend**: uses `ListObjectsV2` with pagination (continuation tokens).
    /// - **Prefix layer**: strips the prefix from keys before returning them.
    /// - **Encryption layer**: filters out header blobs (suffix `.enc-header`).
    async fn list(&self, filter: &dyn ListFilter) -> Result<Vec<String>>;

    /// Check whether a blob exists.
    async fn exists(&self, key: &str) -> Result<bool>;

    /// Retrieve a blob with its metadata.
    ///
    /// The default implementation calls `get()` and returns minimal metadata.
    /// Backends SHOULD override this to provide accurate metadata (e.g. `modified_at`)
    /// when available without an extra round-trip.
    async fn get_with_metadata(
        &self,
        key: &str,
    ) -> Result<(BlobMeta, Box<dyn AsyncRead + Send + Unpin>)> {
        let reader = self.get(key).await?;
        let meta = BlobMeta {
            key: key.to_string(),
            stored_size: 0,
            modified_at: Utc::now(),
            etag: None,
        };
        Ok((meta, reader))
    }

    /// Visit blobs matching the given filter, streaming results to a visitor.
    ///
    /// This is an alternative to [`list()`](BlobStore::list) /
    /// [`list_with_metadata()`](BlobStore::list_with_metadata) that yields
    /// results as they become available, without buffering everything in
    /// memory first. Useful for operations like rekey and cleanup that
    /// process each blob individually.
    ///
    /// The visitor receives each matching key and, when available without
    /// extra cost, the blob's metadata. Return `false` from the visitor to
    /// stop iteration early.
    ///
    /// ## Default implementation
    ///
    /// Falls back to [`list()`](BlobStore::list) and calls the visitor for
    /// each key. Backends SHOULD override this for true streaming.
    async fn visit(&self, filter: &dyn ListFilter, visitor: &mut dyn BlobVisitor) -> Result<()> {
        let keys = self.list(filter).await?;
        for key in &keys {
            if !visitor.visit(key, None).await? {
                break;
            }
        }
        Ok(())
    }

    /// List blob metadata matching the given filter.
    ///
    /// The default implementation calls `list()` and returns entries without timestamps.
    /// Backends SHOULD override this to provide accurate metadata (e.g. `modified_at`)
    /// when available without an extra round-trip.
    async fn list_with_metadata(&self, filter: &dyn ListFilter) -> Result<Vec<BlobMeta>> {
        let keys = self.list(filter).await?;
        let now = Utc::now();
        Ok(keys
            .into_iter()
            .map(|key| BlobMeta {
                key,
                stored_size: 0,
                modified_at: now,
                etag: None,
            })
            .collect())
    }
}

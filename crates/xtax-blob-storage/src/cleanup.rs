use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::AsyncRead;
use tracing::instrument;

use crate::blob_store::BlobStore;
use crate::cleanup::visitor::CleanupVisitor;
use crate::error::Result;
use crate::list_filter::{ListFilter, PrefixFilter};
use crate::types::{BlobInput, BlobMeta, CleanupResult, PutResult};

pub(crate) mod visitor;

/// Predicate for deciding which blobs to delete during cleanup.
///
/// Return `true` to delete the blob.
pub type CleanupPredicate = Box<dyn Fn(&str, &BlobMeta) -> bool + Send + Sync>;

/// Layer that provides cleanup functionality over a [`BlobStore`].
///
/// `BlobCleanup` is itself a [`BlobStore`] — it delegates all operations
/// to the inner store transparently, but also exposes a `cleanup()` method
/// that calls a predicate to decide which blobs to delete.
///
/// For full documentation see the
/// [Cleanup guide](https://github.com/cz-jcode/xtax/blob/main/crates/xtax-blob-storage/docs/cleanup.md).
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use xtax_blob_storage::{BlobInput, BlobStore, BlobStoreBuilder, BlobCleanup, CleanupPredicate};
///
/// # #[cfg(feature = "fs")]
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # #[cfg(feature = "fs")]
/// # {
/// let inner = BlobStoreBuilder::new()
///     .with_fs("/tmp/data")
///     .build()
///     .await?;
///
/// // Delete blobs whose key starts with "tmp-"
/// let predicate: xtax_blob_storage::CleanupPredicate =
///     Box::new(|key, _meta| key.starts_with("tmp-"));
///
/// let store = BlobCleanup::new(inner, predicate);
///
/// // Regular blob operations work transparently:
/// store.put(vec![BlobInput::new("hello.txt", b"data".as_slice())]).await?;
///
/// // Cleanup deletes blobs matching the predicate:
/// let result = store.cleanup().await?;
/// println!("deleted {} blobs", result.deleted_count);
/// # Ok(())
/// # }
/// # }
/// # #[cfg(not(feature = "fs"))]
/// # fn main() {}
/// ```
pub struct BlobCleanup {
    inner: Arc<dyn BlobStore>,
    predicate: CleanupPredicate,
    /// Maximum number of keys to accumulate before issuing a batch delete.
    batch_size: usize,
}

impl BlobCleanup {
    /// Default batch size for delete operations.
    const DEFAULT_BATCH_SIZE: usize = 1000;

    /// Create a new cleanup wrapper around an inner blob store.
    ///
    /// The `predicate` is called for each blob during `cleanup()`.
    /// Return `true` to delete the blob.
    ///
    /// Uses the default batch size of 1000 for delete operations.
    pub fn new(inner: Arc<dyn BlobStore>, predicate: CleanupPredicate) -> Self {
        Self {
            inner,
            predicate,
            batch_size: Self::DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the batch size for delete operations.
    ///
    /// Keys are accumulated until `batch_size` is reached, then deleted
    /// in a single batch. This reduces the number of round-trips to the
    /// backend while keeping memory usage bounded.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Run cleanup: visit all blobs and delete those matching the predicate.
    ///
    /// Uses streaming `visit()` to process blobs as they become available,
    /// deleting in batches of [`batch_size`](Self::with_batch_size).
    ///
    /// Uses an empty prefix filter to list everything. For scoped cleanup,
    /// wrap the store with a `PrefixBlobStore` (via the builder) first.
    #[instrument(skip(self))]
    pub async fn cleanup(&self) -> Result<CleanupResult> {
        let mut visitor = CleanupVisitor {
            store: &*self.inner,
            predicate: &self.predicate,
            batch: Vec::with_capacity(self.batch_size),
            batch_size: self.batch_size,
            deleted_count: 0u64,
        };
        tracing::debug!(batch_size = %self.batch_size, "Starting cleanup");
        self.inner
            .visit(&PrefixFilter::new(""), &mut visitor)
            .await?;
        // Flush remaining keys
        visitor.flush().await?;
        tracing::debug!(deleted_count = %visitor.deleted_count, "Cleanup completed");
        Ok(CleanupResult {
            deleted_count: visitor.deleted_count,
        })
    }
}

#[async_trait]
impl BlobStore for BlobCleanup {
    #[instrument(skip(self, blobs))]
    async fn put(&self, blobs: Vec<BlobInput>) -> Result<PutResult> {
        tracing::debug!(count = %blobs.len(), "Put via cleanup layer");
        self.inner.put(blobs).await
    }

    #[instrument(skip(self))]
    async fn get(&self, key: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        tracing::debug!(key, "Get via cleanup layer");
        self.inner.get(key).await
    }

    #[instrument(skip(self))]
    async fn delete(&self, keys: &[&str]) -> Result<()> {
        tracing::debug!(count = %keys.len(), "Delete via cleanup layer");
        self.inner.delete(keys).await
    }

    #[instrument(skip(self, filter))]
    async fn list(&self, filter: &dyn ListFilter) -> Result<Vec<String>> {
        self.inner.list(filter).await
    }

    #[instrument(skip(self))]
    async fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key).await
    }

    #[instrument(skip(self))]
    async fn get_with_metadata(
        &self,
        key: &str,
    ) -> Result<(BlobMeta, Box<dyn AsyncRead + Send + Unpin>)> {
        self.inner.get_with_metadata(key).await
    }

    #[instrument(skip(self, filter))]
    async fn list_with_metadata(&self, filter: &dyn ListFilter) -> Result<Vec<BlobMeta>> {
        self.inner.list_with_metadata(filter).await
    }
}

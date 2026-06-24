use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::AsyncRead;
use tracing::instrument;

use crate::blob_store::BlobStore;
use crate::error::Result;
use crate::list_filter::ListFilter;
use crate::types::{BlobInput, BlobMeta, PutResult};
use crate::visitor::BlobVisitor;

/// Combines the store's own prefix with the caller's filter prefix hint
/// to create an even more specific scope for the inner store.
///
/// This is the key optimisation: instead of listing all keys under the
/// store's prefix and then filtering, we pass a combined prefix hint to
/// the inner store so it can narrow the filesystem/S3 walk.
///
/// ## Directory filter vs file filter
///
/// When the combined hint ends with `KEY_SEPARATOR` (`/`), it is a
/// **directory filter** — the inner store walks only that subdirectory.
/// This is the most efficient case (e.g. `"my-app/users/"`).
///
/// When the combined hint does NOT end with `KEY_SEPARATOR`, it is a
/// **file filter** — the inner store walks the parent directory and
/// applies `starts_with` filtering. This is less efficient but still
/// correct (e.g. `"my-app/users"` walks `"my-app/"` and checks
/// `key.starts_with("my-app/users")`).
///
/// For example:
/// - Store prefix: `my-app/`
/// - Caller filter: `PrefixFilter("users/")` → hint `"users/"`
/// - Combined hint: `"my-app/users/"` → **directory filter** (walks only
///   `my-app/users/` subtree)
///
/// - Store prefix: `my-app/`
/// - Caller filter: `PrefixFilter("users")` → hint `"users"`
/// - Combined hint: `"my-app/users"` → **file filter** (walks `my-app/`
///   and checks `key.starts_with("my-app/users")`)
struct CombinedPrefixFilter {
    /// The full prefix to pass as `prefix_hint()` to the inner store.
    full: String,
}

impl CombinedPrefixFilter {
    fn new(store_prefix: &str, filter: &dyn ListFilter) -> Self {
        let hint = filter.prefix_hint().unwrap_or("");
        let full = if hint.is_empty() {
            store_prefix.to_string()
        } else {
            // If store_prefix already ends with `/`, just append the hint
            // Otherwise, treat it as a combined path prefix
            format!("{}{}", store_prefix, hint)
        };
        Self { full }
    }
}

impl ListFilter for CombinedPrefixFilter {
    fn matches(&self, key: &str, _meta: Option<&BlobMeta>) -> bool {
        key.starts_with(&self.full)
    }

    fn prefix_hint(&self) -> Option<&str> {
        Some(&self.full)
    }
}

/// Manipulation layer that prepends a prefix to every key.
///
/// All operations are forwarded to the inner store with the key prefixed.
/// The prefix is stripped from list results.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use xtax_blob_storage::{BlobStore, BlobInput, BlobStoreBuilder};
///
/// # #[cfg(feature = "fs")]
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # #[cfg(feature = "fs")]
/// # {
/// let store = BlobStoreBuilder::new()
///     .with_fs("/tmp/data")
///     .with_prefix("my-app/")
///     .build()
///     .await?;
///
/// // Keys are transparently prefixed:
/// store.put(vec![BlobInput::new("config.json", br#"{"key":"value"}"#.as_slice())]).await?;
///
/// // The prefix is stripped from list results:
/// let keys = store.list(&xtax_blob_storage::SuffixFilter::new("")).await?;
/// assert_eq!(keys, vec!["config.json"]);
/// # Ok(())
/// # }
/// # }
/// # #[cfg(not(feature = "fs"))]
/// # fn main() {}
/// ```
pub struct PrefixBlobStore {
    inner: Arc<dyn BlobStore>,
    prefix: String,
}

impl PrefixBlobStore {
    /// Create a new prefix wrapper around an inner blob store.
    ///
    /// All keys will be transparently prefixed with `prefix`.
    /// The prefix is stripped from list results.
    pub fn new(inner: Arc<dyn BlobStore>, prefix: impl Into<String>) -> Self {
        Self {
            inner,
            prefix: prefix.into(),
        }
    }

    fn prefixed(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }

    fn strip_prefix(&self, key: &str) -> Option<String> {
        if key.starts_with(&self.prefix) {
            Some(key[self.prefix.len()..].to_string())
        } else {
            None
        }
    }

    /// Map an error from the inner store back to the caller's logical key.
    ///
    /// This ensures that errors like `NotFound` never leak the prefixed key.
    fn map_error(
        &self,
        logical_key: &str,
        err: crate::error::BlobStorageError,
    ) -> crate::error::BlobStorageError {
        match err {
            crate::error::BlobStorageError::NotFound(_) => {
                crate::error::BlobStorageError::NotFound(logical_key.to_string())
            }
            other => other,
        }
    }

    /// Map a batch error from the inner store back to the caller's logical keys.
    ///
    /// Both `succeeded` and `errors` keys are mapped via `strip_prefix`.
    fn map_batch_error(
        &self,
        _logical_keys: &[&str],
        err: crate::error::BlobStorageError,
    ) -> crate::error::BlobStorageError {
        match err {
            crate::error::BlobStorageError::Batch(batch) => {
                use crate::error::{BatchError, KeyError};
                let succeeded: Vec<String> = batch
                    .succeeded
                    .into_iter()
                    .filter_map(|k| self.strip_prefix(&k))
                    .collect();
                let errors: Vec<KeyError> = batch
                    .errors
                    .into_iter()
                    .map(|mut ke| {
                        if let Some(stripped) = self.strip_prefix(&ke.key) {
                            ke.key = stripped;
                        }
                        ke
                    })
                    .collect();
                crate::error::BlobStorageError::Batch(BatchError { succeeded, errors })
            }
            other => other,
        }
    }
}

/// Internal visitor that strips the prefix and applies the caller's filter
/// before forwarding to the outer visitor.
///
/// Ensures that both the key and metadata passed to the outer visitor contain
/// logical (unprefixed) keys.
struct PrefixVisitor<'a, 'b> {
    inner: &'a mut dyn BlobVisitor,
    prefix: &'b str,
    filter: &'b dyn ListFilter,
}

#[async_trait]
impl BlobVisitor for PrefixVisitor<'_, '_> {
    async fn visit(&mut self, key: &str, meta: Option<&BlobMeta>) -> Result<bool> {
        if let Some(stripped) = key.strip_prefix(self.prefix) {
            // Strip prefix from metadata as well, so the outer visitor only sees logical keys.
            let stripped_meta = meta.map(|m| BlobMeta {
                key: m
                    .key
                    .strip_prefix(self.prefix)
                    .unwrap_or_default()
                    .to_string(),
                stored_size: m.stored_size,
                modified_at: m.modified_at,
                etag: m.etag.clone(),
            });
            if self.filter.matches(stripped, stripped_meta.as_ref()) {
                return self.inner.visit(stripped, stripped_meta.as_ref()).await;
            }
        }
        Ok(true) // continue iteration
    }
}

#[async_trait]
impl BlobStore for PrefixBlobStore {
    #[instrument(skip(self, blobs))]
    async fn put(&self, blobs: Vec<BlobInput>) -> Result<PutResult> {
        let prefixed: Vec<BlobInput> = blobs
            .into_iter()
            .map(|b| BlobInput {
                key: self.prefixed(&b.key),
                data: b.data,
                size_hint: b.size_hint,
            })
            .collect();
        let count = prefixed.len();
        tracing::debug!(prefix = %self.prefix, count, "Storing blobs via prefix layer");
        let mut result = self.inner.put(prefixed).await?;
        // Strip prefix from metadata keys so the caller sees logical keys.
        for meta in &mut result.blobs {
            if let Some(stripped) = self.strip_prefix(&meta.key) {
                meta.key = stripped;
            }
        }
        Ok(result)
    }

    #[instrument(skip(self))]
    async fn get(&self, key: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        let prefixed = self.prefixed(key);
        tracing::debug!(key, prefixed, "Retrieving blob via prefix layer");
        self.inner
            .get(&prefixed)
            .await
            .map_err(|e| self.map_error(key, e))
    }

    #[instrument(skip(self))]
    async fn delete(&self, keys: &[&str]) -> Result<()> {
        let prefixed: Vec<String> = keys.iter().map(|k| self.prefixed(k)).collect();
        let refs: Vec<&str> = prefixed.iter().map(|s| s.as_str()).collect();
        tracing::debug!(prefix = %self.prefix, count = %keys.len(), "Deleting blobs via prefix layer");
        self.inner
            .delete(&refs)
            .await
            .map_err(|e| self.map_batch_error(keys, e))
    }

    #[instrument(skip(self, filter))]
    async fn list(&self, filter: &dyn ListFilter) -> Result<Vec<String>> {
        // Combine the store's prefix with the caller's filter prefix hint
        // to scope the inner listing even more tightly.
        let inner_filter = CombinedPrefixFilter::new(&self.prefix, filter);
        let keys = self.inner.list(&inner_filter).await?;
        // Apply the caller's logical filter after stripping the prefix.
        let stripped: Vec<String> = keys
            .into_iter()
            .filter_map(|k| self.strip_prefix(&k))
            .filter(|k| filter.matches(k, Some(&BlobMeta::for_key(k))))
            .collect();
        tracing::debug!(prefix = %self.prefix, count = %stripped.len(), "Listed blobs via prefix layer");
        Ok(stripped)
    }

    #[instrument(skip(self, filter, visitor))]
    async fn visit(&self, filter: &dyn ListFilter, visitor: &mut dyn BlobVisitor) -> Result<()> {
        // Combine the store's prefix with the caller's filter prefix hint.
        let inner_filter = CombinedPrefixFilter::new(&self.prefix, filter);
        let prefix = self.prefix.clone();
        let mut prefix_visitor = PrefixVisitor {
            inner: visitor,
            prefix: &prefix,
            filter,
        };
        tracing::debug!(prefix = %self.prefix, "Visiting blobs via prefix layer");
        self.inner.visit(&inner_filter, &mut prefix_visitor).await
    }

    #[instrument(skip(self))]
    async fn exists(&self, key: &str) -> Result<bool> {
        let prefixed = self.prefixed(key);
        tracing::debug!(key, prefixed, "Checking blob existence via prefix layer");
        self.inner
            .exists(&prefixed)
            .await
            .map_err(|e| self.map_error(key, e))
    }

    #[instrument(skip(self))]
    async fn get_with_metadata(
        &self,
        key: &str,
    ) -> Result<(BlobMeta, Box<dyn AsyncRead + Send + Unpin>)> {
        let prefixed = self.prefixed(key);
        tracing::debug!(
            key,
            prefixed,
            "Retrieving blob with metadata via prefix layer"
        );
        let (mut meta, reader) = self
            .inner
            .get_with_metadata(&prefixed)
            .await
            .map_err(|e| self.map_error(key, e))?;
        // Strip prefix from the key in returned metadata
        if let Some(stripped) = self.strip_prefix(&meta.key) {
            meta.key = stripped;
        }
        Ok((meta, reader))
    }

    #[instrument(skip(self, filter))]
    async fn list_with_metadata(&self, filter: &dyn ListFilter) -> Result<Vec<BlobMeta>> {
        // Combine the store's prefix with the caller's filter prefix hint.
        let inner_filter = CombinedPrefixFilter::new(&self.prefix, filter);
        let metas = self.inner.list_with_metadata(&inner_filter).await?;
        let stripped: Vec<BlobMeta> = metas
            .into_iter()
            .filter_map(|mut meta| {
                self.strip_prefix(&meta.key).and_then(|stripped| {
                    let passes = filter.matches(&stripped, Some(&meta));
                    meta.key = stripped;
                    if passes { Some(meta) } else { None }
                })
            })
            .collect();
        tracing::debug!(prefix = %self.prefix, count = %stripped.len(), "Listed blobs with metadata via prefix layer");
        Ok(stripped)
    }
}

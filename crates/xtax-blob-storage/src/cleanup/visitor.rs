use async_trait::async_trait;

use crate::blob_store::BlobStore;
use crate::error::Result;
use crate::types::BlobMeta;
use crate::visitor::BlobVisitor;

/// Internal visitor that accumulates keys matching the predicate and
/// deletes them in batches.
pub(crate) struct CleanupVisitor<'a> {
    pub(crate) store: &'a dyn BlobStore,
    pub(crate) predicate: &'a crate::cleanup::CleanupPredicate,
    pub(crate) batch: Vec<String>,
    pub(crate) batch_size: usize,
    pub(crate) deleted_count: u64,
}

impl CleanupVisitor<'_> {
    /// Flush the current batch of keys to the store for deletion.
    pub(crate) async fn flush(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let refs: Vec<&str> = self.batch.iter().map(|s| s.as_str()).collect();
        self.store.delete(&refs).await?;
        self.deleted_count += self.batch.len() as u64;
        self.batch.clear();
        Ok(())
    }
}

#[async_trait]
impl BlobVisitor for CleanupVisitor<'_> {
    async fn visit(&mut self, key: &str, meta: Option<&BlobMeta>) -> Result<bool> {
        // Use a placeholder BlobMeta if meta is None (fallback for backends
        // that don't provide metadata during visit).
        let fallback = BlobMeta::for_key(key);
        let meta_ref = meta.unwrap_or(&fallback);
        if (self.predicate)(key, meta_ref) {
            self.batch.push(key.to_string());
            if self.batch.len() >= self.batch_size {
                self.flush().await?;
            }
        }
        Ok(true) // continue iteration
    }
}

use async_trait::async_trait;

use crate::error::Result;
use crate::list_filter::ListFilter;
use crate::types::BlobMeta;
use crate::visitor::BlobVisitor;

use crate::blob_store::BlobStore;

/// Visitor that filters out header blobs and orphan data blobs,
/// then applies the caller's filter before forwarding to the outer visitor.
pub(crate) struct EncryptVisitor<'a, 'b> {
    pub(crate) inner: &'a mut dyn BlobVisitor,
    pub(crate) header_suffix: &'b str,
    pub(crate) filter: &'b dyn ListFilter,
    pub(crate) store: &'a dyn BlobStore,
}

#[async_trait]
impl BlobVisitor for EncryptVisitor<'_, '_> {
    async fn visit(&mut self, key: &str, meta: Option<&BlobMeta>) -> Result<bool> {
        // Skip header blobs
        if key.ends_with(self.header_suffix) {
            return Ok(true);
        }
        // Skip orphan data blobs without a header
        let header_key = format!("{}{}", key, self.header_suffix);
        if !self.store.exists(&header_key).await? {
            return Ok(true);
        }
        // Apply the caller's filter
        if !self.filter.matches(key, meta) {
            return Ok(true);
        }
        self.inner.visit(key, meta).await
    }
}

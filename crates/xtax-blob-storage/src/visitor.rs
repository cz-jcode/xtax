use async_trait::async_trait;

use crate::error::Result;
use crate::types::BlobMeta;

/// Pluggable visitor for streaming `visit()` operations.
///
/// The visitor is called for each blob that matches the filter during
/// a [`BlobStore::visit()`](crate::BlobStore::visit) call. Return
/// `true` to continue iteration or `false` to stop early.
///
/// # Metadata
///
/// The `meta` parameter is `Some` when the backend can provide metadata
/// without extra cost (e.g. FS via `read_dir`, S3 via `ListObjectsV2`).
/// It is `None` when metadata would require an additional round-trip.
///
/// For usage examples see the
/// [Cleanup guide](https://github.com/cz-jcode/xtax/blob/main/crates/xtax-blob-storage/docs/cleanup.md#visitor-pattern).
#[async_trait]
pub trait BlobVisitor: Send {
    /// Called for each matching blob.
    ///
    /// Return `true` to continue iteration, `false` to stop early.
    async fn visit(&mut self, key: &str, meta: Option<&BlobMeta>) -> Result<bool>;
}

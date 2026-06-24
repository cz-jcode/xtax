use chrono::{DateTime, Utc};
use tokio::io::AsyncRead;

/// Separator used between key components.
///
/// All backends use `/` as the path separator, which gives identical
/// key-nesting behaviour as S3.  A key like `a/b/c.txt` is stored as
/// `{root}/a/b/c.txt` on the filesystem and as `a/b/c.txt` in S3.
pub const KEY_SEPARATOR: char = '/';

/// A single blob to store — key + data stream.
pub struct BlobInput {
    /// Storage key.
    pub key: String,
    /// Data stream.
    pub data: Box<dyn AsyncRead + Send + Unpin + 'static>,
    /// Optional size hint for optimizations.
    pub size_hint: Option<u64>,
}

impl BlobInput {
    /// Create a new blob input without a size hint.
    pub fn new(key: impl Into<String>, data: impl AsyncRead + Send + Unpin + 'static) -> Self {
        Self {
            key: key.into(),
            data: Box::new(data),
            size_hint: None,
        }
    }

    /// Create a new blob input with an explicit size hint.
    ///
    /// Backends may use the size hint for pre-allocation or optimisations.
    pub fn with_size(
        key: impl Into<String>,
        data: impl AsyncRead + Send + Unpin + 'static,
        size: u64,
    ) -> Self {
        Self {
            key: key.into(),
            data: Box::new(data),
            size_hint: Some(size),
        }
    }
}

/// Metadata about a stored blob.
///
/// # Best-effort semantics
///
/// Both `stored_size` and `modified_at` are **best-effort** values:
///
/// * `stored_size` — the size of the blob **as stored** on the backend,
///   which may differ from the original (decompressed / decrypted) size
///   due to compression, encryption, or other transformations. Suitable
///   for rough capacity estimates, not exact accounting.
///
/// * `modified_at` — the point in time when the backend last recorded the blob.
///   The resolution and accuracy depend on the underlying storage (e.g.
///   filesystem mtime, S3 `LastModified`). This is **not** a true creation
///   timestamp — it reflects the last modification time known to the backend.
///   Suitable for approximate recency ordering, not precise time-based decisions.
#[derive(Debug, Clone)]
pub struct BlobMeta {
    /// Storage key.
    pub key: String,
    /// Best-effort stored size in bytes (see struct docs).
    pub stored_size: u64,
    /// Best-effort modification timestamp (see struct docs).
    pub modified_at: DateTime<Utc>,
    /// ETag or content hash, if available.
    pub etag: Option<String>,
}

impl BlobMeta {
    /// Create a placeholder metadata for a given key (used internally for
    /// filter evaluation where only the key matters).
    pub(crate) fn for_key(key: &str) -> Self {
        Self {
            key: key.to_string(),
            stored_size: 0,
            modified_at: Utc::now(),
            etag: None,
        }
    }
}

/// Result of a put operation.
#[derive(Debug, Clone)]
pub struct PutResult {
    /// Metadata for all stored blobs.
    pub blobs: Vec<BlobMeta>,
}

impl PutResult {
    /// Create a `PutResult` from a single blob meta.
    pub fn single(meta: BlobMeta) -> Self {
        Self { blobs: vec![meta] }
    }

    /// Create a `PutResult` from multiple blob metas.
    pub fn multiple(blobs: Vec<BlobMeta>) -> Self {
        Self { blobs }
    }
}

/// Result of a cleanup operation.
#[derive(Debug, Clone)]
pub struct CleanupResult {
    /// Number of blobs deleted.
    pub deleted_count: u64,
}

/// Result of a rekey operation.
#[derive(Debug, Clone)]
pub struct RekeyResult {
    /// Number of encryption headers that were rekeyed.
    pub rekeyed_count: u64,
}

use std::path::PathBuf;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use tokio::fs;

use crate::error::{BlobStorageError, Result};
use crate::types::KEY_SEPARATOR;
use crate::validate::validate_blob_key;

pub(crate) mod store;

/// Convert a `SystemTime` to `DateTime<Utc>`.
fn system_time_to_utc(t: SystemTime) -> DateTime<Utc> {
    let duration = t.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    DateTime::from_timestamp(duration.as_secs() as i64, duration.subsec_nanos()).unwrap_or_default()
}

/// Filesystem-backed blob store.
///
/// Blobs are stored as individual files under a root directory.
///
/// ## Key nesting — identical behaviour to S3
///
/// Keys containing `/` are mapped to nested directories. For example,
/// key `a/b/c.txt` is stored as `{root}/a/b/c.txt`. This is equivalent
/// to how S3 interprets `/` as a path separator — the FS backend and
/// the S3 backend are interchangeable in this regard.
///
/// ## Root path normalisation
///
/// The root path is **always** normalised to end with `/` (the
/// `KEY_SEPARATOR`). This ensures that the `prefix_subdir` helper
/// can correctly split prefix hints into directory + basename
/// components. For example:
///
/// - `"/tmp/my-blobs"` → `"/tmp/my-blobs/"` (trailing `/` added)
/// - `"/tmp/my-blobs/"` → `"/tmp/my-blobs/"` (unchanged)
///
/// Without this normalisation, a prefix hint like `"a/b"` would be
/// joined with the root to produce `"/tmp/my-blobs/a/b"` which does
/// **not** end with `/`, so `prefix_subdir` would strip the last
/// component and walk `"/tmp/my-blobs/"` instead — which is correct
/// for the basename case.
///
/// ## Key validation
///
/// All keys are validated by [`validate_blob_key`] before any storage
/// operation:
///
/// | Pattern | Behaviour |
/// |---------|----------|
/// | `".."` or `"."` component | Rejected — `Err(InvalidInput)` |
/// | Leading `"/"` | Rejected — would resolve to absolute path |
/// | Empty key | Rejected — no backend can store it |
/// | Contains `"\"` (backslash) | Rejected — ambiguous cross-platform |
/// | Contains empty component `"//"` | Rejected — ambiguous across backends |
/// | Trailing `"/"` | Rejected — ambiguous file vs directory semantics |
/// | Valid key like `a/b/c.txt` | Allowed — stored as `{root}/a/b/c.txt` |
///
/// ## Security note
///
/// Path traversal via `..` and `.` components is prevented by key validation.
/// However, **symlink attacks within the root directory are not mitigated** —
/// if an untrusted party can create a symlink inside the root directory or
/// replace a file with a symlink, a TOCTOU attack could cause data to be
/// read from or written to an unexpected location.
///
/// The root directory **must be trusted**. Do not use a root directory where
/// untrusted users can create files or symlinks.
///
/// No `canonicalize()` is performed — the path is computed as
/// `root.join(component).join(component)…` without resolving symlinks.
/// This avoids race conditions inherent to TOCTOU-style `canonicalize` calls
/// but means symlink-based attacks within the root are the caller's
/// responsibility.
///
/// ## Listing (`list`, `list_with_metadata`)
///
/// Both methods walk the root directory **recursively** (breadth-first).
/// Every file found is included; empty directories are ignored.
///
/// - The relative path from root is used as the blob key, with the
///   platform path separator (`/` on Linux, `\` on Windows) replaced
///   by `/` to produce a consistent, platform-independent key.
/// - The caller-supplied [`ListFilter`](crate::ListFilter) is applied to each key. Only
///   files whose keys pass the filter are returned.
/// - Results are sorted alphabetically by key.
///
/// ## Metadata (`get_with_metadata`, `list_with_metadata`)
///
/// - `stored_size` — populated from `fs::metadata::len()` (exact byte count).
/// - `modified_at` — populated from `fs::metadata::modified()` (filesystem
///   mtime, best-effort — see [`BlobMeta`](crate::BlobMeta) docs).
/// - `etag` — always `None` (filesystems have no native ETag).
///
/// # Example
///
/// ```rust,no_run
/// use xtax_blob_storage::{BlobStore, BlobInput, BlobStoreBuilder};
///
/// # #[cfg(feature = "fs")]
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # #[cfg(feature = "fs")]
/// # {
/// let store = BlobStoreBuilder::new()
///     .with_fs("/tmp/my-blobs")
///     .build()
///     .await?;
///
/// store.put(vec![BlobInput::new("greeting.txt", b"hello".as_slice())]).await?;
///
/// use tokio::io::AsyncReadExt;
/// let mut reader = store.get("greeting.txt").await?;
/// let mut buf = Vec::new();
/// reader.read_to_end(&mut buf).await?;
/// assert_eq!(buf, b"hello");
/// # Ok(())
/// # }
/// # }
/// # #[cfg(not(feature = "fs"))]
/// # fn main() {}
/// ```
///
/// Requires `fs` feature (enabled by default).
pub struct FsBlobStore {
    root: PathBuf,
}

impl FsBlobStore {
    /// Create a new filesystem blob store rooted at `root`.
    ///
    /// The directory is created if it does not exist.  The root path is
    /// normalised to always end with `KEY_SEPARATOR` so that the
    /// `prefix_subdir` helper can correctly split prefix hints into
    /// directory + basename components.
    pub async fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let mut root: PathBuf = root.into();
        // Normalise: ensure the root always ends with '/' so that
        // prefix_subdir can correctly split directory + basename.
        if !root.to_string_lossy().ends_with(KEY_SEPARATOR) {
            root.push("");
        }
        fs::create_dir_all(&root)
            .await
            .map_err(|e| BlobStorageError::Storage {
                message: format!("cannot create root '{:?}'", root),
                source: Some(Box::new(e)),
            })?;
        Ok(Self { root })
    }

    /// Convert a blob key to a filesystem path.
    ///
    /// 1. Validates the key via [`validate_blob_key`] — absolute paths, `..`,
    ///    `.`, empty components, and suspicious separators are rejected.
    /// 2. Joins each `/`-separated component under `self.root`.
    ///
    /// The resulting path is relative to the trusted root directory.
    /// No `canonicalize()` is performed — symlink attacks within the root
    /// are the caller's responsibility.  See the [module-level security
    /// note](self#security-note).
    pub fn key_to_path(&self, key: &str) -> Result<PathBuf> {
        validate_blob_key(key)?;
        let mut path = self.root.clone();
        for component in key.split(KEY_SEPARATOR) {
            path = path.join(component);
        }
        Ok(path)
    }

    /// Verify that the root directory exists.
    ///
    /// Returns `BlobStorageError::BackendMisconfigured` if the root
    /// directory has been deleted — this matches S3's `NoSuchBucket`
    /// behaviour (missing backend is a configuration problem).
    pub(crate) async fn ensure_root(&self) -> Result<()> {
        match fs::metadata(&self.root).await {
            Ok(m) if m.is_dir() => Ok(()),
            Ok(_) => Err(BlobStorageError::BackendMisconfigured(format!(
                "FS root '{:?}' is not a directory",
                self.root
            ))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(BlobStorageError::BackendMisconfigured(format!(
                    "FS root directory '{:?}' does not exist",
                    self.root
                )))
            }
            Err(e) => Err(BlobStorageError::Storage {
                message: format!("cannot stat FS root '{:?}'", self.root),
                source: Some(Box::new(e)),
            }),
        }
    }

    /// Build `BlobMeta` from file metadata at the given path.
    ///
    /// Returns `BlobStorageError::NotFound` if the file does not exist,
    /// consistent with [`get()`](Self::get).
    async fn file_meta(&self, key: &str) -> Result<crate::types::BlobMeta> {
        let path = self.key_to_path(key)?;
        let meta = match fs::metadata(&path).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(BlobStorageError::NotFound(key.to_string()));
            }
            Err(e) => {
                return Err(BlobStorageError::Storage {
                    message: format!("stat failed for '{key}'"),
                    source: Some(Box::new(e)),
                });
            }
        };
        Ok(crate::types::BlobMeta {
            key: key.to_string(),
            stored_size: meta.len(),
            modified_at: meta
                .modified()
                .ok()
                .map(system_time_to_utc)
                .unwrap_or_default(),
            etag: None,
        })
    }
}

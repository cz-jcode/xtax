use std::collections::VecDeque;

use async_trait::async_trait;
use tokio::fs;
use tokio::io::{AsyncRead, AsyncWriteExt};
use tracing::instrument;

use crate::blob_store::BlobStore;
use crate::error::{BatchError, BlobStorageError, KeyError, PerKeyError, Result};
use crate::fs::{FsBlobStore, system_time_to_utc};
use crate::list_filter::ListFilter;
use crate::types::{BlobInput, BlobMeta, KEY_SEPARATOR, PutResult};
use crate::visitor::BlobVisitor;

/// Map a `std::io::Error` to `PerKeyError` for batch error reporting.
fn map_io_error_to_per_key(e: std::io::Error) -> PerKeyError {
    match e.kind() {
        std::io::ErrorKind::PermissionDenied => PerKeyError::PermissionDenied(e.to_string()),
        _ => PerKeyError::Unknown {
            message: e.to_string(),
        },
    }
}

/// Validate that a prefix hint is safe to use as a filesystem subdirectory hint.
///
/// Rejects:
/// - Absolute paths (starting with `/`)
/// - Paths containing `..` or `.` components (traversal)
/// - Paths containing backslash `\` (platform ambiguity)
///
/// Returns `Ok(())` if the hint is safe.
fn validate_prefix_hint(hint: &str) -> Result<()> {
    if hint.starts_with('/') {
        return Err(BlobStorageError::InvalidInput(format!(
            "prefix hint must not be absolute: '{hint}'"
        )));
    }
    if hint.contains('\\') {
        return Err(BlobStorageError::InvalidInput(format!(
            "prefix hint must not contain backslash: '{hint}'"
        )));
    }
    for component in hint.split(KEY_SEPARATOR) {
        if component == ".." || component == "." {
            return Err(BlobStorageError::InvalidInput(format!(
                "prefix hint must not contain '..' or '.' components: '{hint}'",
            )));
        }
    }
    Ok(())
}

/// Check if a directory exists. Returns `Ok(false)` if the path doesn't
/// exist (matching S3 behaviour: listing a missing prefix returns empty).
async fn dir_exists(path: &std::path::Path) -> Result<bool> {
    match fs::metadata(path).await {
        Ok(m) => Ok(m.is_dir()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Returns the subdirectory root based on a prefix hint.
///
/// This function is used by `list`, `list_with_metadata`, and `visit`
/// to narrow the filesystem walk to a subset of the root directory.
///
/// **When the prefix ends with `KEY_SEPARATOR`:** the full prefix is a
/// directory path — walk that subtree directly (e.g. `"myprefix/lama/"`).
/// This is the **directory filter** case: the prefix is a complete
/// directory path, so we walk only that directory.
///
/// **When the prefix does NOT end with `KEY_SEPARATOR`:** the last
/// component is a basename — walk the parent directory and rely on
/// the caller's `ListFilter::matches()` for `starts_with` filtering
/// (e.g. `PrefixFilter("myprefix/lama")` walks `myprefix/` and
/// checks `key.starts_with("myprefix/lama")`).
/// This is the **file filter** case: the prefix is a partial key path,
/// so we walk the parent directory and filter by `starts_with`.
///
/// **When no hint is given:** walk the full root.
///
/// **Security note:** The prefix hint is validated by [`validate_prefix_hint`]
/// before being used as a filesystem path. This prevents directory traversal
/// attacks via hints like `../` or `/tmp`.
fn prefix_subdir(root: &std::path::Path, filter: &dyn ListFilter) -> Result<std::path::PathBuf> {
    match filter.prefix_hint() {
        Some(prefix) => {
            validate_prefix_hint(prefix)?;
            // Root always ends with '/' (normalised in FsBlobStore::new).
            // Join root + prefix to get the full path.
            let mut full = root.join(prefix);
            if !full.to_string_lossy().ends_with(KEY_SEPARATOR)
                && let Some(parent) = full.parent()
            {
                full = parent.to_path_buf();
            }
            Ok(full)
        }
        _ => Ok(root.to_path_buf()),
    }
}

#[async_trait]
impl BlobStore for FsBlobStore {
    #[instrument(skip(self, blobs))]
    async fn put(&self, blobs: Vec<BlobInput>) -> Result<PutResult> {
        self.ensure_root().await?;
        let mut metas = Vec::with_capacity(blobs.len());
        for blob in blobs {
            let path = self.key_to_path(&blob.key)?;
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).await?;
            }
            let file = fs::File::create(&path).await?;
            let mut writer = tokio::io::BufWriter::new(file);
            let mut reader = blob.data;
            tokio::io::copy(&mut reader, &mut writer).await?;
            writer.flush().await?;
            let meta = fs::metadata(&path).await?;
            let stored_size = meta.len();
            tracing::debug!(key = %blob.key, stored_size, "Stored blob via FS");
            metas.push(BlobMeta {
                key: blob.key,
                stored_size,
                modified_at: meta
                    .modified()
                    .ok()
                    .map(system_time_to_utc)
                    .unwrap_or_default(),
                etag: None,
            });
        }
        Ok(PutResult::multiple(metas))
    }

    #[instrument(skip(self))]
    async fn get(&self, key: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        self.ensure_root().await?;
        let path = self.key_to_path(key)?;
        if !path.exists() {
            return Err(BlobStorageError::NotFound(key.to_string()));
        }
        let file = fs::File::open(&path).await?;
        tracing::debug!(key, "Retrieved blob via FS");
        Ok(Box::new(file))
    }

    #[instrument(skip(self))]
    async fn delete(&self, keys: &[&str]) -> Result<()> {
        self.ensure_root().await?;
        let mut succeeded = Vec::new();
        let mut errors = Vec::new();

        for key in keys {
            let path = match self.key_to_path(key) {
                Ok(p) => p,
                Err(e) => {
                    // InvalidInput na klíči — to je fatální pro všechny, abort
                    return Err(e);
                }
            };
            match fs::remove_file(&path).await {
                Ok(()) => {
                    tracing::debug!(key, "Deleted blob via FS");
                    succeeded.push(key.to_string());
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // NotFound na delete NENÍ chyba — delete je idempotentní
                    tracing::debug!(key, "Blob already gone (not found) during delete");
                    succeeded.push(key.to_string());
                }
                Err(e) => {
                    let per_key = map_io_error_to_per_key(e);
                    tracing::warn!(key, error = %per_key, "Failed to delete blob via FS");
                    errors.push(KeyError {
                        key: key.to_string(),
                        error: per_key,
                    });
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(BlobStorageError::Batch(BatchError { succeeded, errors }))
        }
    }

    #[instrument(skip(self, filter))]
    async fn list(&self, filter: &dyn ListFilter) -> Result<Vec<String>> {
        self.ensure_root().await?;
        let mut keys = Vec::new();
        let mut dirs = VecDeque::new();

        // Use prefix hint only when it ends with '/' — that indicates a
        // subdirectory scope. When it's just a key prefix (e.g. "a"), we
        // walk the full root and rely on post-filtering.
        let start_root = prefix_subdir(&self.root, filter)?;
        dirs.push_back(start_root);

        while let Some(dir_path) = dirs.pop_front() {
            if !dir_exists(&dir_path).await? {
                tracing::debug!(?dir_path, "skipping non-existent directory in list");
                continue;
            }
            let mut dir = fs::read_dir(&dir_path).await?;
            while let Some(entry) = dir.next_entry().await? {
                let path = entry.path();
                if path.is_file() {
                    if let Ok(relative) = path.strip_prefix(&self.root) {
                        let key = relative
                            .to_str()
                            .ok_or_else(|| {
                                BlobStorageError::InvalidInput(format!(
                                    "non-UTF-8 path: {:?}",
                                    relative
                                ))
                            })?
                            .replace(std::path::MAIN_SEPARATOR_STR, "/");
                        if filter.matches(&key, None) {
                            keys.push(key);
                        }
                    }
                } else if path.is_dir() {
                    dirs.push_back(path);
                }
            }
        }

        keys.sort();
        tracing::debug!(count = %keys.len(), "Listed blobs via FS");
        Ok(keys)
    }

    #[instrument(skip(self))]
    async fn exists(&self, key: &str) -> Result<bool> {
        self.ensure_root().await?;
        let path = self.key_to_path(key)?;
        let exists = path.exists();
        tracing::debug!(key, exists, "Checked blob existence via FS");
        Ok(exists)
    }

    #[instrument(skip(self))]
    async fn get_with_metadata(
        &self,
        key: &str,
    ) -> Result<(BlobMeta, Box<dyn AsyncRead + Send + Unpin>)> {
        self.ensure_root().await?;
        let meta = self.file_meta(key).await?;
        let reader = self.get(key).await?;
        tracing::debug!(
            key,
            size = meta.stored_size,
            "Retrieved blob with metadata via FS"
        );
        Ok((meta, reader))
    }

    #[instrument(skip(self, filter, visitor))]
    async fn visit(&self, filter: &dyn ListFilter, visitor: &mut dyn BlobVisitor) -> Result<()> {
        self.ensure_root().await?;
        let mut dirs = VecDeque::new();

        let start_root = prefix_subdir(&self.root, filter)?;
        dirs.push_back(start_root);

        while let Some(dir_path) = dirs.pop_front() {
            if !dir_exists(&dir_path).await? {
                tracing::debug!(?dir_path, "skipping non-existent directory in visit");
                continue;
            }
            let mut dir = fs::read_dir(&dir_path).await?;
            while let Some(entry) = dir.next_entry().await? {
                let path = entry.path();
                if path.is_file() {
                    if let Ok(relative) = path.strip_prefix(&self.root) {
                        let key = relative
                            .to_str()
                            .ok_or_else(|| {
                                BlobStorageError::InvalidInput(format!(
                                    "non-UTF-8 path: {:?}",
                                    relative
                                ))
                            })?
                            .replace(std::path::MAIN_SEPARATOR_STR, "/");
                        if filter.matches(&key, None) {
                            let file_meta = fs::metadata(&path).await.map_err(|e| {
                                BlobStorageError::Storage {
                                    message: format!("stat failed for '{key}'"),
                                    source: Some(Box::new(e)),
                                }
                            })?;
                            let meta = BlobMeta {
                                key: key.clone(),
                                stored_size: file_meta.len(),
                                modified_at: file_meta
                                    .modified()
                                    .ok()
                                    .map(system_time_to_utc)
                                    .unwrap_or_default(),
                                etag: None,
                            };
                            if !visitor.visit(&key, Some(&meta)).await? {
                                return Ok(());
                            }
                        }
                    }
                } else if path.is_dir() {
                    dirs.push_back(path);
                }
            }
        }

        Ok(())
    }

    #[instrument(skip(self, filter))]
    async fn list_with_metadata(&self, filter: &dyn ListFilter) -> Result<Vec<BlobMeta>> {
        self.ensure_root().await?;
        let mut metas = Vec::new();
        let mut dirs = VecDeque::new();

        let start_root = prefix_subdir(&self.root, filter)?;
        dirs.push_back(start_root);

        while let Some(dir_path) = dirs.pop_front() {
            if !dir_exists(&dir_path).await? {
                tracing::debug!(
                    ?dir_path,
                    "skipping non-existent directory in list_with_metadata"
                );
                continue;
            }
            let mut dir = fs::read_dir(&dir_path).await?;
            while let Some(entry) = dir.next_entry().await? {
                let path = entry.path();
                if path.is_file() {
                    if let Ok(relative) = path.strip_prefix(&self.root) {
                        let key = relative
                            .to_str()
                            .ok_or_else(|| {
                                BlobStorageError::InvalidInput(format!(
                                    "non-UTF-8 path: {:?}",
                                    relative
                                ))
                            })?
                            .replace(std::path::MAIN_SEPARATOR_STR, "/");
                        if filter.matches(&key, None) {
                            let file_meta = fs::metadata(&path).await.map_err(|e| {
                                BlobStorageError::Storage {
                                    message: format!("stat failed for '{key}'"),
                                    source: Some(Box::new(e)),
                                }
                            })?;
                            metas.push(BlobMeta {
                                key,
                                stored_size: file_meta.len(),
                                modified_at: file_meta
                                    .modified()
                                    .ok()
                                    .map(system_time_to_utc)
                                    .unwrap_or_default(),
                                etag: None,
                            });
                        }
                    }
                } else if path.is_dir() {
                    dirs.push_back(path);
                }
            }
        }

        metas.sort_by(|a, b| a.key.cmp(&b.key));
        tracing::debug!(count = %metas.len(), "Listed blobs with metadata via FS");
        Ok(metas)
    }
}

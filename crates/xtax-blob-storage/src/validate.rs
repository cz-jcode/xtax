/// Universal blob key validation.
///
/// Rejects keys that would be unsafe or **ambiguous** across any backend
/// (filesystem, S3, …):
///
/// | Rejected pattern | Why |
/// |------------------|-----|
/// | Empty string `""` | No backend (not even S3) can store an empty key |
/// | Starts with `"/"` | Resolves to an absolute path on FS; S3 allows it but the mapping is inconsistent |
/// | Contains `".."` as a path component | **Traversal** — `a/../../x` would escape the root directory |
/// | Contains `"."` as a path component | The current directory — `./foo` is equivalent to `foo` but ambiguous |
/// | Contains `"\"` (backslash) | On Windows this is a path separator; on Linux it's a legal filename — **ambiguous** across platforms |
/// | Contains empty component `"//"` | Ambiguous — maps to nested empty directory on FS, but is a flat key on S3 |
/// | Ends with `"/"` (trailing slash) | Ambiguous — FS creates an empty directory entry; S3 treats it as a flat key |
///
/// ## What S3 allows
///
/// S3 object keys *may* legally contain `..`, start with `/`, include
/// backslash, contain empty components, or end with `/`, but **this
/// library rejects them** to preserve a consistent, portable contract
/// across all backends:
///
/// > "Keys containing `/` are mapped to nested directories — identical
/// > behaviour to S3."
///
/// If a key passes `../etc/passwd` to the FS backend, it would escape the
/// root; if it passes the same key to S3, S3 would store it as a literal
/// string `../etc/passwd` — two completely different behaviours for the
/// same input. **This library refuses the ambiguity.**
///
/// ## Usage
///
/// ```rust
/// use xtax_blob_storage::validate_blob_key;
///
/// assert!(validate_blob_key("hello.txt").is_ok());
/// assert!(validate_blob_key("").is_err());
/// assert!(validate_blob_key("../outside.txt").is_err());
/// assert!(validate_blob_key("/absolute").is_err());
/// assert!(validate_blob_key("a/../../x").is_err());
/// assert!(validate_blob_key("./foo").is_err());
/// assert!(validate_blob_key("a\\b").is_err());
/// assert!(validate_blob_key("a//b").is_err());
/// assert!(validate_blob_key("foo/").is_err());
/// ```
///
/// All built-in [`BlobStore`](crate::BlobStore) implementations call
/// `validate_blob_key` on every `put`, `get`, `delete`, `exists`, and
/// `get_with_metadata` operation **before** touching any storage.
use crate::error::{BlobStorageError, Result};
use crate::types::KEY_SEPARATOR;

/// Validate a blob key against the subset that all backends agree on.
///
/// Returns `Ok(())` if the key is valid, `Err(BlobStorageError(InvalidInput))`
/// if it would be rejected by any backend.
///
/// See the [module-level documentation](self) for details.
pub fn validate_blob_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(BlobStorageError::InvalidInput(
            "blob key must not be empty".to_string(),
        ));
    }

    if key.starts_with(KEY_SEPARATOR) {
        return Err(BlobStorageError::InvalidInput(format!(
            "blob key must not start with '/': '{key}'"
        )));
    }

    if key.ends_with(KEY_SEPARATOR) {
        return Err(BlobStorageError::InvalidInput(format!(
            "blob key must not end with '/': '{key}'"
        )));
    }

    if key.contains('\\') {
        return Err(BlobStorageError::InvalidInput(format!(
            "blob key must not contain backslash: '{key}'"
        )));
    }

    for component in key.split(KEY_SEPARATOR) {
        if component.is_empty() {
            return Err(BlobStorageError::InvalidInput(format!(
                "blob key must not contain empty components: '{key}'"
            )));
        }
        if component == ".." || component == "." {
            return Err(BlobStorageError::InvalidInput(format!(
                "blob key must not contain '..' or '.' components: '{key}'",
            )));
        }
    }

    Ok(())
}

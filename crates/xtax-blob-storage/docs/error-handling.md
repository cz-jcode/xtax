# Error handling

All errors in `xtax-blob-storage` are represented by the `BlobStorageError` enum, which uses `#[derive(thiserror::Error)]` for automatic `Display`, `Error`, and `From` implementations — no manual boilerplate required.

## Error types

### `BlobStorageError` variants

```rust
#[derive(Debug, thiserror::Error)]
pub enum BlobStorageError {
    /// The requested blob was not found.
    #[error("blob not found: {0}")]
    NotFound(String),

    /// A blob with this key already exists.
    #[error("blob already exists: {0}")]
    AlreadyExists(String),

    /// The operation is not supported by this backend.
    #[error("operation not supported: {0}")]
    NotSupported(String),

    /// The backend is misconfigured — for example, the S3 bucket does not
    /// exist, or the FS root directory has been deleted.
    ///
    /// This is distinct from [`Storage`] errors: it indicates
    /// a backend configuration problem, not a transient storage failure.
    #[error("backend misconfigured: {0}")]
    BackendMisconfigured(String),

    /// The provided input is invalid (empty key, path traversal, etc.).
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// Backend storage error — wraps an underlying cause.
    #[error("storage error: {message}")]
    Storage {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },

    /// Encryption-layer error.
    #[error("encryption error: {message}")]
    Encryption {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },

    /// The caller does not have permission to perform this operation.
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// Batch operation partially failed.
    /// Contains details about which keys succeeded and which failed.
    #[error("batch error: {0}")]
    Batch(#[from] BatchError),
}
```

## Batch errors

Batch operations (`put`, `delete`) can partially succeed. When at least one key fails, a `BlobStorageError::Batch` is returned containing a `BatchError`.

### `BatchError`

```rust
pub struct BatchError {
    /// Keys that were processed successfully (including `NotFound` on delete).
    pub succeeded: Vec<String>,
    /// Keys that failed, with per-key error details.
    pub errors: Vec<KeyError>,
}
```

The `BatchError` provides `total_count()` and `failed_count()` helpers so callers can decide whether to retry or report.

### `KeyError`

```rust
pub struct KeyError {
    /// The blob key that failed.
    pub key: String,
    /// The categorised error.
    pub error: PerKeyError,
}
```

### `PerKeyError`

```rust
pub enum PerKeyError {
    /// The key was not found.
    NotFound,
    /// The operation failed due to insufficient permissions.
    PermissionDenied(String),
    /// Any other unexpected error.
    Unknown { message: String },
}
```

## Idempotent delete semantics

`NotFound` on delete is **NOT an error** — it is treated as success.

- **`get()`** and **`get_with_metadata()`**: `NotFound` means the blob does not exist. The backend returns `BlobStorageError::NotFound` directly.
- **`delete()`**: If a key doesn't exist, it is considered successfully processed — the key was already gone. This makes `delete()` idempotent.
- In both FS and S3 backends, a `NotFound` during delete is added to `BatchError::succeeded`, not `BatchError::errors`.

This difference is important when handling batch errors: a `PerKeyError::NotFound` from `delete` means the key was processed successfully, while `BlobStorageError::NotFound` from `get` means the key genuinely doesn't exist.

## `std::error::Error` compatibility

Because `BlobStorageError` derives `thiserror::Error`, it automatically implements `std::error::Error`. This means:

- It works with `Box<dyn std::error::Error>` and `anyhow::Error` for application-level error propagation.
- The `#[source]` fields provide `Error::source()` chaining — the underlying cause is preserved.
- The `#[error]` attributes provide idiomatic `Display` messages without manual `fmt::Display` impls.

`BatchError` also implements `std::error::Error`.

## Result type

```rust
pub type Result<T> = std::result::Result<T, BlobStorageError>;
```

## Matching errors

Because `BlobStorageError` is an enum, you match on variants directly:

```rust
match store.get("nonexistent.txt").await {
    Err(BlobStorageError::NotFound(_)) => {
        // Handle "not found" case
    }
    Err(BlobStorageError::BackendMisconfigured(msg)) => {
        // Handle misconfigured backend — e.g. S3 bucket missing, FS root deleted
        eprintln!("Backend misconfigured: {msg}");
        std::process::exit(1);
    }
    Err(BlobStorageError::Storage { message, .. }) => {
        // Handle storage error
    }
    Err(BlobStorageError::Batch(batch)) => {
        // Handle partial batch failure
        eprintln!("{} keys failed", batch.failed_count());
    }
    Ok(reader) => {
        // Use the reader
    }
}
```

## Backend misconfiguration errors

`BackendMisconfigured` indicates a backend configuration problem, not a transient failure or a genuine "not found" condition:

- **S3 backend**: Returned when the configured S3 bucket does not exist (`NoSuchBucket`). This can be caused by wrong credentials, wrong region, or the bucket being deleted. Previously, this was incorrectly mapped to `NotFound(key)`, which hid the misconfiguration.
- **FS backend**: Returned when the root directory has been deleted from outside the process (e.g., `rm -rf /tmp/my-blobs`). The `FsBlobStore::new()` creates the root on construction, so a missing root indicates external interference.

This variant should typically be treated as **fatal** — the application cannot recover from a misconfigured backend without administrator intervention.


## From conversions

- `From<std::io::Error>` — maps I/O errors to `BlobStorageError::Storage { source: ... }`
- `From<String>` — maps string errors to `BlobStorageError::Storage { source: None }`
- `From<&str>` — maps string slices to `BlobStorageError::Storage { source: None }`
- `From<BatchError>` — maps batch errors to `BlobStorageError::Batch`. The `#[from]` attribute on the `Batch` variant generates this automatically.

## Feature

`thiserror` is the only error-derive dependency. It is always available (no feature gate).
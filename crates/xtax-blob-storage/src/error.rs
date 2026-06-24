use std::fmt;

/// Categorised error for a single key in a batch operation.
#[derive(Debug, Clone)]
pub enum PerKeyError {
    /// The key was not found.
    ///
    /// Idempotent — on delete this is NOT an error (the key is treated as
    /// successfully processed). On get / get_with_metadata the backend
    /// returns `BlobStorageError::NotFound` directly.
    NotFound,

    /// The operation failed due to insufficient permissions.
    PermissionDenied(String),

    /// Any other unexpected error. The `message` contains the original error
    /// description.
    Unknown { message: String },
}

impl fmt::Display for PerKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PerKeyError::NotFound => write!(f, "not found"),
            PerKeyError::PermissionDenied(msg) => write!(f, "permission denied: {msg}"),
            PerKeyError::Unknown { message } => write!(f, "unknown: {message}"),
        }
    }
}

/// A single failed key in a batch operation, with its categorised error.
#[derive(Debug, Clone)]
pub struct KeyError {
    /// The blob key that failed.
    pub key: String,
    /// The categorised error.
    pub error: PerKeyError,
}

/// Batch error — returned when at least one key in a batch operation failed.
///
/// Contains **all** keys that succeeded and those that failed.
/// The caller can programmatically decide what to do next (e.g. retry failed keys).
#[derive(Debug, Clone)]
pub struct BatchError {
    /// Keys that were processed successfully (including `NotFound` on delete).
    pub succeeded: Vec<String>,
    /// Keys that failed, with per-key error details.
    pub errors: Vec<KeyError>,
}

impl BatchError {
    /// Total number of keys processed.
    pub fn total_count(&self) -> usize {
        self.succeeded.len() + self.errors.len()
    }

    /// Number of keys that failed.
    pub fn failed_count(&self) -> usize {
        self.errors.len()
    }
}

impl fmt::Display for BatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "batch operation failed: {} keys failed ({} succeeded, {} total)",
            self.failed_count(),
            self.succeeded.len(),
            self.total_count(),
        )
    }
}

impl std::error::Error for BatchError {}

/// Blob storage error.
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
    /// This is distinct from [`Storage`](Self::Storage) errors: it indicates
    /// a backend configuration problem, not a transient storage failure.
    #[error("backend misconfigured: {0}")]
    BackendMisconfigured(String),

    /// The provided input is invalid (empty key, path traversal, etc.).
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// Backend storage error. The inner `String` provides context;
    /// the optional `source` carries the underlying cause.
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

pub type Result<T> = std::result::Result<T, BlobStorageError>;

impl From<std::io::Error> for BlobStorageError {
    fn from(e: std::io::Error) -> Self {
        Self::Storage {
            message: "I/O error".to_string(),
            source: Some(Box::new(e)),
        }
    }
}

impl From<String> for BlobStorageError {
    fn from(msg: String) -> Self {
        Self::Storage {
            message: msg,
            source: None,
        }
    }
}

impl From<&str> for BlobStorageError {
    fn from(msg: &str) -> Self {
        Self::Storage {
            message: msg.to_string(),
            source: None,
        }
    }
}

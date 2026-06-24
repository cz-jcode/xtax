use std::sync::Arc;

use crate::blob_store::BlobStore;
use crate::error::Result;

/// Internal backend variants for the builder.
pub(crate) enum BackendKind {
    #[cfg(feature = "fs")]
    Fs(std::path::PathBuf),
    #[cfg(feature = "s3")]
    S3 {
        client: aws_sdk_s3::Client,
        bucket: String,
        part_size: u64,
    },
    /// User-supplied custom backend (any `BlobStore` implementation).
    Custom(Arc<dyn BlobStore>),
    /// Dummy variant when no features are enabled (prevents compilation errors).
    #[allow(dead_code)]
    NoneBackend,
}

impl BackendKind {
    pub(crate) async fn build_raw_arc(&self) -> Result<Arc<dyn BlobStore>> {
        match self {
            #[cfg(feature = "fs")]
            BackendKind::Fs(root) => {
                let store = crate::fs::FsBlobStore::new(root.clone()).await?;
                Ok(Arc::new(store))
            }
            #[cfg(feature = "s3")]
            BackendKind::S3 {
                client,
                bucket,
                part_size,
            } => {
                let store = crate::s3::S3BlobStore::new(client.clone(), bucket)
                    .with_multipart_part_size(*part_size);
                Ok(Arc::new(store))
            }
            BackendKind::Custom(store) => Ok(store.clone()),
            _ => unreachable!("build_raw_arc called without a backend selected"),
        }
    }
}

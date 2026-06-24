use std::collections::HashSet;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use tokio::io::{AsyncRead, BufReader, ReadBuf};
use tracing::instrument;

use crate::blob_store::BlobStore;
use crate::encrypt::EncryptionProvider;
use crate::encrypt::rekey::RekeyVisitor;
use crate::encrypt::visitors::EncryptVisitor;
use crate::error::{BatchError, BlobStorageError, KeyError, Result};
use crate::list_filter::{ListFilter, SuffixFilter};
use crate::types::{BlobInput, BlobMeta, PutResult, RekeyResult};
use crate::visitor::BlobVisitor;

/// Wraps an `AsyncRead` and checks for a stored decryption error on EOF.
///
/// When the inner stream ends, if the spawned decryption task recorded an
/// error, this reader converts the EOF into an I/O error so the caller
/// never silently gets truncated data.
struct ErrorAwareReader {
    inner: Box<dyn AsyncRead + Send + Unpin>,
    decryption_error: Arc<Mutex<Option<BlobStorageError>>>,
}

impl AsyncRead for ErrorAwareReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                if buf.filled().len() == before {
                    // EOF from inner — check for decryption error
                    if let Some(err) = self.decryption_error.lock().unwrap().take() {
                        return Poll::Ready(Err(std::io::Error::other(err.to_string())));
                    }
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

/// Transparent encryption layer over a [`BlobStore`].
///
/// Encrypted data and headers are stored as separate blobs. The header
/// suffix is completely internal — callers never see it.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use xtax_blob_storage::{BlobStore, BlobInput, BlobStoreBuilder, EncryptionProvider};
///
/// # #[cfg(feature = "fs")]
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # #[cfg(feature = "fs")]
/// # {
/// # let provider: Arc<dyn EncryptionProvider> = todo!();
/// let store = BlobStoreBuilder::new()
///     .with_fs("/tmp/data")
///     .with_encryption(provider)
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// # }
/// # #[cfg(not(feature = "fs"))]
/// # fn main() {}
/// ```
pub struct EncryptedBlobStore {
    pub(crate) inner: Arc<dyn BlobStore>,
    pub(crate) encryption: Arc<dyn EncryptionProvider>,
    pub(crate) header_suffix: String,
}

impl EncryptedBlobStore {
    const DEFAULT_HEADER_SUFFIX: &'static str = ".enc-header";

    /// Create a new encrypted blob store wrapping the given inner store.
    ///
    /// Uses the default header suffix (`.enc-header`).
    pub fn new(inner: Arc<dyn BlobStore>, encryption: Arc<dyn EncryptionProvider>) -> Self {
        Self {
            inner,
            encryption,
            header_suffix: Self::DEFAULT_HEADER_SUFFIX.to_string(),
        }
    }

    /// Create a new encrypted blob store with a custom header suffix.
    ///
    /// Use this if the default suffix (`.enc-header`) conflicts with
    /// existing blob keys.
    pub fn with_suffix(
        inner: Arc<dyn BlobStore>,
        encryption: Arc<dyn EncryptionProvider>,
        header_suffix: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            encryption,
            header_suffix: header_suffix.into(),
        }
    }

    pub(crate) fn header_key(&self, key: &str) -> String {
        format!("{}{}", key, self.header_suffix)
    }

    /// Reject user keys that end with the internal header suffix.
    ///
    /// A user key like `foo.enc-header` would collide with the internal header
    /// blob for key `foo`, making the encrypted layer unable to distinguish
    /// between a real header and a user's payload blob.
    fn validate_key_no_header_collision(&self, key: &str) -> Result<()> {
        if key.ends_with(&self.header_suffix) {
            return Err(BlobStorageError::InvalidInput(format!(
                "blob key must not end with reserved header suffix '{}': '{key}'",
                self.header_suffix
            )));
        }
        Ok(())
    }

    /// Re-key all encryption headers.
    ///
    /// Uses streaming `visit()` to process headers as they become available,
    /// without buffering the entire listing in memory first.
    #[instrument(skip(self))]
    pub async fn rekey(&self) -> Result<RekeyResult> {
        let suffix_filter = SuffixFilter::new(&self.header_suffix);
        let mut visitor = RekeyVisitor {
            store: &*self.inner,
            encryption: &*self.encryption,
            rekeyed: 0u64,
        };
        self.inner.visit(&suffix_filter, &mut visitor).await?;
        let count = visitor.rekeyed;
        tracing::debug!(rekeyed_count = count, "Rekey completed");
        Ok(RekeyResult {
            rekeyed_count: count,
        })
    }

    /// Decrypt a stream using the encryption provider and a header key.
    ///
    /// Decryption runs in a background task. If it fails, the error is
    /// surfaced to the caller when the returned stream is read to completion.
    #[instrument(skip(self, enc_data))]
    async fn decrypt_stream(
        &self,
        key: &str,
        enc_data: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        let header_key = self.header_key(key);

        // Header is small — read it into memory
        let header_data = match self.inner.get(&header_key).await {
            Ok(data) => data,
            Err(BlobStorageError::NotFound(_)) => {
                return Err(BlobStorageError::NotFound(key.to_string()));
            }
            Err(e) => return Err(e),
        };
        let mut header_buf = Vec::new();
        tokio::io::copy(&mut BufReader::new(header_data), &mut header_buf).await?;

        // Create a duplex channel: decrypt writes to tx, rx is returned to caller
        let (mut tx, rx) = tokio::io::duplex(64 * 1024);

        // Shared slot for the decryption error, checked by ErrorAwareReader on EOF
        let decryption_error: Arc<Mutex<Option<BlobStorageError>>> = Arc::new(Mutex::new(None));
        let decryption_error_clone = decryption_error.clone();

        let enc = self.encryption.clone();
        let header = header_buf.clone();
        let key_owned = key.to_string();

        tokio::spawn(async move {
            if let Err(e) = enc
                .decrypt_stream(&mut BufReader::new(enc_data), &mut tx, &header)
                .await
            {
                tracing::error!("decryption failed for key '{key_owned}': {e}");
                *decryption_error_clone.lock().unwrap() = Some(BlobStorageError::Encryption {
                    message: format!("decryption failed for key '{key_owned}'"),
                    source: Some(Box::new(e)),
                });
            }
        });

        tracing::debug!(key, header_key, "Decrypting blob stream");
        Ok(Box::new(ErrorAwareReader {
            inner: Box::new(rx),
            decryption_error,
        }))
    }
}

#[async_trait]
impl BlobStore for EncryptedBlobStore {
    /// Store encrypted blobs — writes data blob first, then header blob.
    ///
    /// **Non-atomic write**: This method writes two independent backend
    /// objects (`{key}` and `{key}.enc-header`) sequentially. It is NOT
    /// transactional — a crash or failure between the two writes can leave
    /// the store in an inconsistent state. See the
    /// [Failure semantics](https://github.com/cz-jcode/xtax/blob/main/crates/xtax-blob-storage/docs/encryption.md#failure-semantics)
    /// documentation for all failure modes and recovery strategies.
    #[instrument(skip(self, blobs))]
    async fn put(&self, blobs: Vec<BlobInput>) -> Result<PutResult> {
        let count = blobs.len();
        let mut metas = Vec::with_capacity(count);
        for blob in blobs {
            self.validate_key_no_header_collision(&blob.key)?;
            let BlobInput {
                key,
                mut data,
                size_hint,
            } = blob;

            // Create a 64 KiB pipe: encryption writes to tx, inner store reads from rx
            let (mut tx, rx) = tokio::io::duplex(64 * 1024);

            // Shared slot for the encryption result (header bytes or error)
            let enc_result: Arc<Mutex<Option<Result<Vec<u8>>>>> = Arc::new(Mutex::new(None));
            let enc_result_clone = enc_result.clone();
            let enc = self.encryption.clone();
            let enc_key = key.clone();

            // Spawn encryption — writes encrypted data to tx
            tokio::spawn(async move {
                let result = enc.encrypt_stream(&mut data, &mut tx).await.map_err(|e| {
                    BlobStorageError::Encryption {
                        message: format!("encryption failed for key '{enc_key}'"),
                        source: Some(Box::new(e)),
                    }
                });
                *enc_result_clone.lock().unwrap() = Some(result);
            });

            // Stream encrypted data from rx into the inner store
            let enc_input = BlobInput::with_size(key.clone(), rx, size_hint.unwrap_or(0));
            tracing::debug!(key = %key, "Storing encrypted blob data");
            let result = self.inner.put(vec![enc_input]).await?;

            // Wait for encryption task to finish and retrieve the header.
            // If encryption failed, perform best-effort rollback of the data blob.
            // (Extract the result into a local to drop the MutexGuard before any await.)
            let enc_task_result = enc_result.lock().unwrap().take();
            let header_bytes = match enc_task_result {
                Some(Ok(bytes)) => bytes,
                Some(Err(enc_err)) => {
                    tracing::warn!(key = %key, error = %enc_err,
                        "Encryption failed after data blob was stored; attempting rollback");
                    let _ = self.inner.delete(&[&key]).await;
                    return Err(enc_err);
                }
                None => {
                    return Err(BlobStorageError::Encryption {
                        message: format!("encryption task did not complete for key '{key}'"),
                        source: None,
                    });
                }
            };

            // Store the encryption header as a separate blob.
            // If header write fails, attempt best-effort rollback of the data blob.
            // This is not fully transactional — a crash between data write and
            // header write may leave an orphan data blob. On overwrite, the
            // old header may become orphaned if rollback succeeds but the old
            // header was already present.
            let header_len = header_bytes.len() as u64;
            let header_input =
                BlobInput::with_size(self.header_key(&key), Cursor::new(header_bytes), header_len);
            tracing::debug!(key = %key, header_len, "Storing encryption header");
            if let Err(e) = self.inner.put(vec![header_input]).await {
                tracing::warn!(key = %key, error = %e,
                    "Header write failed; attempting rollback of data blob \
                     (orphan header may remain if this was an overwrite)");
                let _ = self.inner.delete(&[&key]).await;
                return Err(e);
            }

            metas.extend(
                result
                    .blobs
                    .into_iter()
                    .filter(|b| !b.key.ends_with(&self.header_suffix)),
            );
        }
        tracing::debug!(count, "Stored encrypted blobs");
        Ok(PutResult::multiple(metas))
    }

    #[instrument(skip(self))]
    async fn get(&self, key: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        self.validate_key_no_header_collision(key)?;
        let enc_data = match self.inner.get(key).await {
            Ok(data) => data,
            Err(BlobStorageError::NotFound(_)) => {
                return Err(BlobStorageError::NotFound(key.to_string()));
            }
            Err(e) => return Err(e),
        };
        tracing::debug!(key, "Retrieving encrypted blob");
        self.decrypt_stream(key, enc_data).await
    }

    #[instrument(skip(self))]
    async fn delete(&self, keys: &[&str]) -> Result<()> {
        for key in keys {
            self.validate_key_no_header_collision(key)?;
        }
        let all_keys: Vec<String> = keys
            .iter()
            .flat_map(|k| vec![k.to_string(), self.header_key(k)])
            .collect();
        let refs: Vec<&str> = all_keys.iter().map(|s| s.as_str()).collect();
        tracing::debug!(count = %keys.len(), "Deleting encrypted blobs (with headers)");

        match self.inner.delete(&refs).await {
            Ok(()) => Ok(()),
            Err(BlobStorageError::Batch(batch)) => {
                // Filter out header-keys from the batch error — caller
                // shouldn't see internal `.enc-header` keys.
                let data_keys: HashSet<&str> = keys.iter().copied().collect();

                let filtered_errors: Vec<KeyError> = batch
                    .errors
                    .into_iter()
                    .filter(|e| data_keys.contains(e.key.as_str()))
                    .collect();

                let filtered_succeeded: Vec<String> = batch
                    .succeeded
                    .into_iter()
                    .filter(|s| data_keys.contains(s.as_str()))
                    .collect();

                if filtered_errors.is_empty() {
                    // Only header keys failed — caller sees success
                    Ok(())
                } else {
                    Err(BlobStorageError::Batch(BatchError {
                        succeeded: filtered_succeeded,
                        errors: filtered_errors,
                    }))
                }
            }
            Err(other) => Err(other),
        }
    }

    #[instrument(skip(self, filter))]
    async fn list(&self, filter: &dyn ListFilter) -> Result<Vec<String>> {
        let all_keys = self.inner.list(&SuffixFilter::new("")).await?;
        let header_suffix = &self.header_suffix;

        let candidate_keys: Vec<String> = all_keys
            .into_iter()
            .filter(|k| !k.ends_with(header_suffix))
            .filter(|k| filter.matches(k, None))
            .collect();

        // Filter out orphan data blobs that have no header
        let mut filtered = Vec::new();
        for key in candidate_keys {
            let header_exists = self.inner.exists(&self.header_key(&key)).await?;
            if header_exists {
                filtered.push(key);
            } else {
                tracing::debug!(key, "Skipping orphan data blob (header missing) in list");
            }
        }

        tracing::debug!(count = %filtered.len(), "Listed encrypted blobs");
        Ok(filtered)
    }

    #[instrument(skip(self, filter, visitor))]
    async fn visit(&self, filter: &dyn ListFilter, visitor: &mut dyn BlobVisitor) -> Result<()> {
        let header_suffix = self.header_suffix.clone();
        let mut encrypt_visitor = EncryptVisitor {
            inner: visitor,
            header_suffix: &header_suffix,
            filter,
            store: &*self.inner,
        };
        tracing::debug!("Visiting encrypted blobs");
        self.inner
            .visit(&SuffixFilter::new(""), &mut encrypt_visitor)
            .await
    }

    #[instrument(skip(self))]
    async fn exists(&self, key: &str) -> Result<bool> {
        self.validate_key_no_header_collision(key)?;
        let data_exists = self.inner.exists(key).await?;
        if !data_exists {
            tracing::debug!(
                key,
                false,
                "Checked encrypted blob existence (data missing)"
            );
            return Ok(false);
        }
        let header_exists = self.inner.exists(&self.header_key(key)).await?;
        tracing::debug!(key, header_exists, "Checked encrypted blob existence");
        Ok(header_exists)
    }

    #[instrument(skip(self))]
    async fn get_with_metadata(
        &self,
        key: &str,
    ) -> Result<(BlobMeta, Box<dyn AsyncRead + Send + Unpin>)> {
        self.validate_key_no_header_collision(key)?;
        let (inner_meta_data, reader) = match self.inner.get_with_metadata(key).await {
            Ok(result) => result,
            Err(BlobStorageError::NotFound(_)) => {
                return Err(BlobStorageError::NotFound(key.to_string()));
            }
            Err(e) => return Err(e),
        };

        let meta = BlobMeta {
            key: key.to_string(),
            stored_size: inner_meta_data.stored_size,
            modified_at: inner_meta_data.modified_at,
            etag: inner_meta_data.etag,
        };

        let decrypted = self.decrypt_stream(key, reader).await?;
        tracing::debug!(key, "Retrieved encrypted blob with metadata");
        Ok((meta, decrypted))
    }

    #[instrument(skip(self, filter))]
    async fn list_with_metadata(&self, filter: &dyn ListFilter) -> Result<Vec<BlobMeta>> {
        let all_keys = self.inner.list(&SuffixFilter::new("")).await?;
        let header_suffix = &self.header_suffix;

        let mut metas = Vec::new();
        for key in all_keys {
            if key.ends_with(header_suffix) {
                continue;
            }
            if !filter.matches(&key, None) {
                continue;
            }
            // Skip orphan data blobs without a header
            let header_exists = match self.inner.exists(&self.header_key(&key)).await {
                Ok(exists) => exists,
                Err(e) => {
                    tracing::warn!(key, error = %e, "Failed to check header existence during list_with_metadata, skipping");
                    continue;
                }
            };
            if !header_exists {
                continue;
            }
            match self.inner.get_with_metadata(&key).await {
                Ok((inner_meta, _)) => {
                    metas.push(BlobMeta {
                        key,
                        stored_size: inner_meta.stored_size,
                        modified_at: inner_meta.modified_at,
                        etag: inner_meta.etag,
                    });
                }
                Err(BlobStorageError::NotFound(_)) => {
                    // Data blob disappeared between list and get_with_metadata — skip
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        metas.sort_by(|a, b| a.key.cmp(&b.key));
        tracing::debug!(count = %metas.len(), "Listed encrypted blobs with metadata");
        Ok(metas)
    }
}

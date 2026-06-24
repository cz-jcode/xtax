use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::Result;

pub(crate) mod rekey;
pub(crate) mod store;
pub(crate) mod visitors;

/// Encryption provider — abstracts the encryption operations needed
/// by [`EncryptedBlobStore`](store::EncryptedBlobStore).
///
/// This trait allows the blob store to work with any encryption backend
/// that supports detached-header stream encryption.
///
/// For full documentation see the
/// [Encryption guide](https://github.com/cz-jcode/xtax/blob/main/crates/xtax-blob-storage/docs/encryption.md).
///
/// # Example (custom provider)
///
/// ```rust,no_run
/// use async_trait::async_trait;
/// use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, AsyncReadExt};
/// use xtax_blob_storage::{EncryptionProvider, Result};
///
/// struct NoopEncryption;
///
/// #[async_trait]
/// impl EncryptionProvider for NoopEncryption {
///     async fn encrypt_stream(
///         &self,
///         input: &mut (dyn AsyncRead + Send + Unpin),
///         output: &mut (dyn AsyncWrite + Send + Unpin),
///     ) -> Result<Vec<u8>> {
///         let mut buf = Vec::new();
///         input.read_to_end(&mut buf).await.unwrap();
///         output.write_all(&buf).await.unwrap();
///         Ok(vec![])
///     }
///
///     async fn decrypt_stream(
///         &self,
///         input: &mut (dyn AsyncRead + Send + Unpin),
///         output: &mut (dyn AsyncWrite + Send + Unpin),
///         _header_bytes: &[u8],
///     ) -> Result<()> {
///         let mut buf = Vec::new();
///         input.read_to_end(&mut buf).await.unwrap();
///         output.write_all(&buf).await.unwrap();
///         Ok(())
///     }
///
///     async fn rekey_header(&self, _header_bytes: &[u8]) -> Result<Option<Vec<u8>>> {
///         Ok(None)
///     }
/// }
/// ```
#[async_trait]
pub trait EncryptionProvider: Send + Sync {
    /// Encrypt data from `input` and write the encrypted stream to `output`.
    /// Returns the serialisable encryption header.
    async fn encrypt_stream(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        output: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<Vec<u8>>;

    /// Decrypt data from `input` using `header_bytes` and write plaintext to `output`.
    async fn decrypt_stream(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        output: &mut (dyn AsyncWrite + Send + Unpin),
        header_bytes: &[u8],
    ) -> Result<()>;

    /// Try to re-key (re-wrap) an encryption header with the current master key.
    ///
    /// Returns `None` if the header is already using the current key.
    /// Returns `Some(new_header_bytes)` if the header was re-wrapped.
    async fn rekey_header(&self, header_bytes: &[u8]) -> Result<Option<Vec<u8>>>;
}

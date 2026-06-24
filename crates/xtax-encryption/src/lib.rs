//! # xtax-encryption
//!
//! Trait-only encryption provider interface — no backend, no storage, no I/O
//! decisions. Implement [`EncryptionProvider`] to plug any encryption scheme
//! into crates like `xtax-blob-storage`.
//!
//! ## Crate architecture
//!
//! ```text
//! xtax-encryption               ←  this crate (trait + error types only)
//!      ↑
//! xtax-blob-storage             ←  re-exports and uses the trait
//! ```
//!
//! ## Usage
//!
//! ```rust,no_run
//! use async_trait::async_trait;
//! use tokio::io::{AsyncRead, AsyncWrite};
//! use xtax_encryption::{EncryptionProvider, EncryptionResult};
//!
//! struct NoopEncryption;
//!
//! #[async_trait]
//! impl EncryptionProvider for NoopEncryption {
//!     async fn encrypt_stream(
//!         &self,
//!         _input: &mut (dyn AsyncRead + Send + Unpin),
//!         _output: &mut (dyn AsyncWrite + Send + Unpin),
//!     ) -> EncryptionResult<Vec<u8>> {
//!         Ok(vec![])
//!     }
//!
//!     async fn decrypt_stream(
//!         &self,
//!         _input: &mut (dyn AsyncRead + Send + Unpin),
//!         _output: &mut (dyn AsyncWrite + Send + Unpin),
//!         _header_bytes: &[u8],
//!     ) -> EncryptionResult<()> {
//!         Ok(())
//!     }
//!
//!     async fn rekey_header(&self, _header_bytes: &[u8]) -> EncryptionResult<Option<Vec<u8>>> {
//!         Ok(None)
//!     }
//! }
//! ```
//!
//! ## Feature flags
//!
//! This crate has no features — it's a minimal dependency.

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// An error returned by [`EncryptionProvider`] methods.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EncryptionError {
    /// Encrypt or decrypt operation failed.
    #[error("encryption operation failed: {message}")]
    Operation {
        /// Human-readable description of the failure.
        message: String,
        /// Optional underlying cause.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },

    /// Invalid or corrupted header data.
    #[error("invalid encryption header: {0}")]
    InvalidHeader(String),

    /// I/O error during encryption or decryption.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Convenience alias for `Result<T, EncryptionError>`.
pub type EncryptionResult<T> = Result<T, EncryptionError>;

// ---------------------------------------------------------------------------
// EncryptionProvider trait
// ---------------------------------------------------------------------------

/// Encryption provider — abstracts the encryption operations needed
/// by encrypted storage layers.
///
/// This trait allows any crate to work with a pluggable encryption backend
/// that supports detached-header stream encryption.
///
/// # Implementations
///
/// - Must be [`Send`] + [`Sync`] (required by async storage layers).
/// - The [`encrypt_stream`](EncryptionProvider::encrypt_stream) method
///   **must** flush the output stream before returning.
/// - The returned header bytes are stored separately from the encrypted data
///   and later passed back to [`decrypt_stream`](EncryptionProvider::decrypt_stream).
#[async_trait]
pub trait EncryptionProvider: Send + Sync {
    /// Encrypt data from `input` and write the encrypted stream to `output`.
    ///
    /// Returns the serialisable encryption header that must be stored
    /// alongside the data (e.g. as a separate blob).
    async fn encrypt_stream(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        output: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> EncryptionResult<Vec<u8>>;

    /// Decrypt data from `input` using the previously stored `header_bytes`
    /// and write plaintext to `output`.
    async fn decrypt_stream(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        output: &mut (dyn AsyncWrite + Send + Unpin),
        header_bytes: &[u8],
    ) -> EncryptionResult<()>;

    /// Try to re-key (re-wrap) an existing encryption header with the
    /// current master key.
    ///
    /// - Returns `None` if the header is already using the current key.
    /// - Returns `Some(new_header_bytes)` if the header was re-wrapped.
    async fn rekey_header(&self, header_bytes: &[u8]) -> EncryptionResult<Option<Vec<u8>>>;
}

//! # xtax-blob-storage
//!
//! > **Experimental** blob storage abstraction for Rust with filesystem and S3
//! > backends, streaming uploads, optional encryption, and composable layers.
//!
//! A compact, builder-driven blob storage abstraction. See the
//! [crate-level README](https://github.com/cz-jcode/xtax/blob/main/crates/xtax-blob-storage/README.md)
//! for the full rationale and comparisons.
//!
//! ## Status
//!
//! **v0.1.0 — Experimental / learning project.** Not production-ready.
//!
//! ## Architecture
//!
//! ```text
//!  BlobStore trait  ←  everyone implements this
//!       ↑
//!  ┌────┴──────────────┐
//!  │  FsBlobStore      │  filesystem backend (feature = "fs")
//!  │  S3BlobStore      │  S3/Garage backend (feature = "s3")
//!  └────┬──────────────┘
//!       │
//!  ┌────┴───────────────────────────────┐
//!  │  PrefixBlobStore                   │  key prefix manipulation
//!  │  EncryptedBlobStore                │  encryption
//!  │  BlobCleanup                       │  cleanup by predicate
//!  └────┬───────────────────────────────┘
//!       │
//!  BlobStore trait  ←  still the same trait, fully composable
//! ```
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use xtax_blob_storage::{BlobStoreBuilder, BlobInput};
//!
//! # #[cfg(feature = "fs")]
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # #[cfg(feature = "fs")]
//! # {
//! let store = BlobStoreBuilder::new()
//!     .with_fs("/tmp/data")
//!     .with_prefix("my-app/")
//!     .build()
//!     .await?;
//!
//! use tokio::io::AsyncReadExt;
//!
//! store.put(vec![BlobInput::new("hello.txt", b"data".as_slice())]).await?;
//!
//! let mut reader = store.get("hello.txt").await?;
//! let mut buf = String::new();
//! reader.read_to_string(&mut buf).await?;
//! assert_eq!(buf, "data");
//! # Ok(())
//! # }
//! # }
//! # #[cfg(not(feature = "fs"))]
//! # fn main() {}
//! ```
//!
//! ## AI contribution note
//!
//! This library was developed with LLM assistance under continuous human supervision.
//!

// ---------------------------------------------------------------------------
// Internal modules (private) — types are re-exported below
// ---------------------------------------------------------------------------

mod blob_store;
mod builder;
mod cleanup;
mod encrypt;
mod error;
mod list_filter;
mod prefix;
mod types;
pub mod validate;
mod visitor;

// ---------------------------------------------------------------------------
// Backend modules — public so users can construct them directly if needed
// ---------------------------------------------------------------------------

/// Filesystem-backed blob store.
///
/// Blobs are stored as individual files under a root directory. Keys
/// containing `/` are mapped to nested directories — identical behaviour
/// to S3.
///
/// For full documentation see the
/// [Backends guide](https://github.com/cz-jcode/xtax/blob/main/crates/xtax-blob-storage/docs/backends.md).
///
/// *Requires the `fs` feature (enabled by default).*
#[cfg(feature = "fs")]
#[cfg_attr(docsrs, doc(cfg(feature = "fs")))]
pub mod fs;

/// S3-compatible blob store.
///
/// Works with AWS S3, Garage, MinIO, and any S3-compatible service.
/// Supports multipart uploads with configurable threshold.
///
/// For full documentation see the
/// [Backends guide](https://github.com/cz-jcode/xtax/blob/main/crates/xtax-blob-storage/docs/backends.md).
///
/// *Requires the `s3` feature (opt-in).*
#[cfg(feature = "s3")]
#[cfg_attr(docsrs, doc(cfg(feature = "s3")))]
pub mod s3;

// ---------------------------------------------------------------------------
// Re-exports — the public API surface
// ---------------------------------------------------------------------------

pub use blob_store::BlobStore;
pub use builder::{
    BackgroundCancellation, BackgroundContext, BackgroundStrategy, BlobStoreBuilder,
    MaintenanceTrigger, Manual, OnStart, Periodic,
};
pub use cleanup::{BlobCleanup, CleanupPredicate};
pub use encrypt::{EncryptionProvider, store::EncryptedBlobStore};
pub use error::{BatchError, BlobStorageError, KeyError, PerKeyError, Result};
pub use list_filter::{ListFilter, NotFilter, PrefixFilter, SuffixFilter};
pub use types::{BlobInput, BlobMeta, CleanupResult, KEY_SEPARATOR, PutResult, RekeyResult};
pub use validate::validate_blob_key;
pub use visitor::BlobVisitor;

//! # xtax — Facade crate for the xtax Rust infrastructure ecosystem
//!
//! `xtax` is a lightweight re-export crate. It contains no business logic of its own.
//! Instead, it re-exports selected subcrates behind Cargo feature flags, giving you
//! a single dependency with composable, opt-in functionality.
//!
//! ## Usage
//!
//! By default, the `xtax` facade enables no functionality:
//!
//! ```toml
//! [dependencies]
//! xtax = "0.1"
//! ```
//!
//! Enable only the parts you need.
//!
//! ### Blob storage with filesystem backend
//!
//! ```toml
//! [dependencies]
//! xtax = { version = "0.1", features = ["blob-storage"] }
//! ```
//!
//! ```rust,ignore
//! use xtax::blob_storage::BlobStoreBuilder;
//! ```
//!
//! ### Blob storage with S3 backend
//!
//! ```toml
//! [dependencies]
//! xtax = { version = "0.1", features = ["blob-storage-s3"] }
//! ```
//!
//! ```rust,ignore
//! use xtax::blob_storage::BlobStoreBuilder;
//! ```
//!
//! ### Encryption provider interface
//!
//! ```toml
//! [dependencies]
//! xtax = { version = "0.1", features = ["encryption"] }
//! ```
//!
//! ```rust,ignore
//! use xtax::encryption::EncryptionProvider;
//! ```
//!
//! Or depend on subcrates directly — both paths are valid:
//!
//! ```toml
//! [dependencies]
//! xtax-blob-storage = "0.1"
//! ```
//!
//! ```rust,ignore
//! use xtax_blob_storage::BlobStoreBuilder;
//! ```
//!
//! ## Feature flags
//!
//! | Feature             | Crate re-exported       | Description                                      |
//! |---------------------|-------------------------|--------------------------------------------------|
//! | `blob-storage`      | `xtax-blob-storage`     | Blob storage with filesystem backend             |
//! | `blob-storage-s3`   | `xtax-blob-storage`     | Blob storage with S3 backend enabled             |
//! | `blob-storage-full` | `xtax-blob-storage`     | Enables all blob-storage backends exposed here   |
//! | `encryption`        | `xtax-encryption`       | Trait-only encryption provider interface         |
//! | `full`              | all facade features     | Enables all currently exposed facade features    |
//!
//! ## Architecture
//!
//! - The `xtax` facade crate contains **no logic**.
//! - Each subcrate has **standalone value** and can be used **without** the facade.
//! - Subcrates **must not** depend on the facade crate.
//! - Dependency direction is always: `xtax` → subcrate, never the reverse.

#[cfg(any(
    feature = "blob-storage",
    feature = "blob-storage-s3",
    feature = "blob-storage-full"
))]
pub use xtax_blob_storage as blob_storage;

#[cfg(feature = "encryption")]
pub use xtax_encryption as encryption;

/// Convenience prelude — re-exports the most commonly used items from subcrates.
#[cfg(any(
    feature = "blob-storage",
    feature = "blob-storage-s3",
    feature = "blob-storage-full",
    feature = "encryption"
))]
pub mod prelude {
    #[cfg(any(
        feature = "blob-storage",
        feature = "blob-storage-s3",
        feature = "blob-storage-full"
    ))]
    pub use xtax_blob_storage::*;

    #[cfg(feature = "encryption")]
    #[cfg_attr(
        any(
            feature = "blob-storage",
            feature = "blob-storage-s3",
            feature = "blob-storage-full"
        ),
        allow(unused_imports)
    )]
    pub use xtax_encryption::*;
}
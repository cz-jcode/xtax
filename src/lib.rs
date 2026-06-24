//! # xtax — Facade crate for the xtax Rust infrastructure ecosystem
//!
//! `xtax` is a lightweight re-export crate. It contains no business logic of its own.
//! Instead, it re-exports selected subcrates behind Cargo feature flags, giving you
//! a single dependency with composable, opt-in functionality.
//!
//! ## Usage
//!
//! Enable only what you need:
//!
//! ```toml
//! [dependencies]
//! xtax = { version = "0.1", features = ["blob-storage"] }
//! ```
//!
//! Then import through the facade:
//!
//! ```rust
//! #[cfg(feature = "blob-storage")]
//! use xtax::blob_storage::BlobStoreBuilder;
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
//! // Requires xtax-blob-storage in Cargo.toml
//! use xtax_blob_storage::BlobStoreBuilder;
//! ```
//!
//! ## Feature flags
//!
//! | Feature             | Crate re-exported       | Description                                       |
//! |---------------------|-------------------------|---------------------------------------------------|
//! | `blob-storage`      | `xtax-blob-storage`     | Blob storage with filesystem backend              |
//! | `blob-storage-s3`   | `xtax-blob-storage`     | Enables the S3 backend                            |
//! | `encryption`        | `xtax-encryption`       | Trait-only encryption provider interface          |
//! | `full`              | all facade features     | Enables all currently exposed features            |
//!
//! ## Architecture
//!
//! - The `xtax` facade crate contains **no logic**.
//! - Each subcrate has **standalone value** and can be used **without** the facade.
//! - Subcrates **must not** depend on the facade crate.
//! - Dependency direction is always: `xtax` → subcrate, never the reverse.

#[cfg(feature = "blob-storage")]
pub use xtax_blob_storage as blob_storage;

#[cfg(feature = "encryption")]
pub use xtax_encryption as encryption;

/// Convenience prelude — re-exports the most commonly used items from subcrates.
#[cfg(feature = "blob-storage")]
pub mod prelude {
    pub use xtax_blob_storage::*;
}

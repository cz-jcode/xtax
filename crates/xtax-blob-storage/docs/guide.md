# Getting Started

## Installation

Add `xtax-blob-storage` to your `Cargo.toml`:

```toml
[dependencies]
xtax-blob-storage = "0.1"
tokio = { version = "1.52", features = ["rt", "io-util"] }
```

By default only the `fs` feature is enabled. To add S3 support:

```toml
[dependencies]
xtax-blob-storage = { version = "0.1", features = ["s3"] }
tokio = { version = "1.52", features = ["rt", "io-util"] }
```

## Step 1 — Create a store

```rust,no_run
use xtax_blob_storage::BlobStoreBuilder;

# #[tokio::main]
# async fn main() -> xtax_blob_storage::Result<()> {
let store = BlobStoreBuilder::new()
    .with_fs("/tmp/data")
    .build()
    .await?;
# Ok(())
# }
```

This creates a filesystem-backed blob store. Blobs are stored as individual files under `/tmp/data`.

## Step 2 — Store and retrieve blobs

```rust,no_run
use xtax_blob_storage::{BlobStoreBuilder, BlobInput};
use tokio::io::AsyncReadExt;

# #[tokio::main]
# async fn main() -> xtax_blob_storage::Result<()> {
# let store = BlobStoreBuilder::new()
#     .with_fs("/tmp/data")
#     .build()
#     .await?;
// Store a blob
store.put(vec![
    BlobInput::new("hello.txt", b"Hello, world!".as_slice())
]).await?;

// Retrieve it
let mut reader = store.get("hello.txt").await?;
let mut text = String::new();
reader.read_to_string(&mut text).await?;
assert_eq!(text, "Hello, world!");
# Ok(())
# }
```

## Step 3 — Add a prefix

Use `with_prefix()` to scope all blobs under a namespace. The prefix is transparent — it's prepended internally and stripped from list results.

```rust,no_run
use xtax_blob_storage::{BlobStoreBuilder, BlobInput, SuffixFilter};

# #[tokio::main]
# async fn main() -> xtax_blob_storage::Result<()> {
# let pdf_data = b"fake pdf";
let store = BlobStoreBuilder::new()
    .with_fs("/tmp/data")
    .with_prefix("customer-42/")
    .build()
    .await?;

// Stored as "customer-42/report.pdf"
store.put(vec![
    BlobInput::new("report.pdf", pdf_data.as_slice())
]).await?;

// Listed as "report.pdf" (prefix stripped)
let keys = store.list(&SuffixFilter::new(".pdf")).await?;
# Ok(())
# }
```

## Step 4 — Switch to S3

Change one line. Nothing else needs to change.

```rust,no_run
use aws_sdk_s3::Client;
use xtax_blob_storage::{BlobStoreBuilder, BlobInput, SuffixFilter};

# #[tokio::main]
# async fn main() -> xtax_blob_storage::Result<()> {
// Create your S3 client (see aws-config crate)
let config = aws_config::load_from_env().await;
let client = Client::new(&config);

let store = BlobStoreBuilder::new()
    .with_s3(client, "my-bucket")   // ← was .with_fs()
    .with_prefix("customer-42/")
    .build()
    .await?;
# Ok(())
# }
```

The same `store.put()`, `store.get()`, `store.list()` calls work identically.

## Step 5 — Add encryption

```rust,no_run
use std::sync::Arc;
use xtax_blob_storage::{BlobStoreBuilder, EncryptionProvider};

# #[tokio::main]
# async fn main() -> xtax_blob_storage::Result<()> {
# use aws_sdk_s3::Client;
# let config = aws_config::load_from_env().await;
# let client = Client::new(&config);
# let my_provider: Arc<dyn EncryptionProvider> = Arc::new(todo!());
let store = BlobStoreBuilder::new()
    .with_s3(client, "documents")
    .with_prefix("customer-42/")
    .with_encryption(my_provider)   // ← implements EncryptionProvider
    .build()
    .await?;
# Ok(())
# }
```

Data is transparently encrypted on write and decrypted on read. See [Encryption](encryption.md) for details on implementing `EncryptionProvider`.

## Step 6 — Add lifecycle cleanup

```rust,no_run
use std::sync::Arc;
use xtax_blob_storage::{BlobStoreBuilder, Periodic, BlobMeta, CleanupPredicate};

# #[tokio::main]
# async fn main() -> xtax_blob_storage::Result<()> {
let predicate: CleanupPredicate =
    Box::new(|key, meta: &BlobMeta| {
        // Delete blobs older than 30 days
        key.starts_with("tmp-") || meta.modified_at
            < (chrono::Utc::now() - chrono::Duration::days(30))
    });

let store = BlobStoreBuilder::new()
    .with_fs("/tmp/data")
    .with_clean(predicate, Arc::new(Periodic(
        std::time::Duration::from_secs(3600)  // run every hour
    )))
    .build()
    .await?;
# Ok(())
# }
```

## Putting it all together — production setup

```rust,no_run
use std::sync::Arc;
use std::time::Duration;
use xtax_blob_storage::{
    BlobStoreBuilder, BlobInput, Periodic, BlobMeta,
    EncryptionProvider, CleanupPredicate,
};

# #[tokio::main]
# async fn main() -> xtax_blob_storage::Result<()> {
# use aws_sdk_s3::Client;
# let config = aws_config::load_from_env().await;
# let client = Client::new(&config);
# let encryption_provider: Arc<dyn EncryptionProvider> = Arc::new(todo!());
# let cleanup_predicate: CleanupPredicate = Box::new(|_, _| false);
# let data = b"fake invoice";
let store = BlobStoreBuilder::new()
    .with_s3(client, "documents")
    .with_multipart_part_size(100 * 1024 * 1024)  // 100 MiB
    .with_prefix("prod/")
    .with_encryption(encryption_provider)
    .with_rekey(Arc::new(Periodic(Duration::from_secs(86400))))  // daily key rotation
    .with_clean(cleanup_predicate, Arc::new(Periodic(Duration::from_secs(3600))))
    .build()
    .await?;

// Use the store — all layers are transparent
store.put(vec![BlobInput::new("invoice.pdf", data)]).await?;
let blob = store.get("invoice.pdf").await?;
# Ok(())
# }
```

## Next steps

- [Architecture](architecture.md) — understand how layers compose
- [Builder reference](builder.md) — all builder methods
- [Backends](backends.md) — FS and S3 configuration options
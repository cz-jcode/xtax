# Cleanup

## BlobCleanup

`BlobCleanup` is a manipulation layer that provides lifecycle management for blobs. It delegates all regular `BlobStore` operations transparently, but also exposes a `cleanup()` method that deletes blobs matching a predicate.

```rust
use std::sync::Arc;
use xtax_blob_storage::{BlobCleanup, BlobStore, BlobInput, BlobStoreBuilder, BlobMeta};

let inner = BlobStoreBuilder::new()
    .with_fs("/tmp/data")
    .build().await?;

// Delete blobs whose key starts with "tmp-"
let predicate: xtax_blob_storage::CleanupPredicate =
    Box::new(|key, _meta| key.starts_with("tmp-"));

let store = BlobCleanup::new(inner, predicate);

// Regular blob operations work transparently:
store.put(vec![BlobInput::new("hello.txt", b"data".as_slice())]).await?;

// Cleanup deletes blobs matching the predicate:
let result = store.cleanup().await?;
println!("deleted {} blobs", result.deleted_count);
```

## CleanupPredicate

```rust
pub type CleanupPredicate = Box<dyn Fn(&str, &BlobMeta) -> bool + Send + Sync>;
```

The predicate receives the blob key and its metadata. Return `true` to delete the blob.

```rust
// Delete by key pattern
let by_prefix = Box::new(|key, _meta| key.starts_with("tmp-"));

// Delete by age
let by_age = Box::new(|_key, meta| {
    meta.modified_at < (chrono::Utc::now() - chrono::Duration::days(30))
});

// Delete by size
let by_size = Box::new(|_key, meta| meta.stored_size > 1024 * 1024 * 100);  // > 100 MB

// Combined
let combined = Box::new(|key, meta| {
    key.starts_with("tmp-") || meta.modified_at < chrono::Utc::now() - chrono::Duration::days(7)
});
```

## Batch size

Keys are accumulated in batches before deletion. This reduces the number of round-trips to the backend while keeping memory usage bounded.

```rust
let store = BlobCleanup::new(inner, predicate)
    .with_batch_size(500);  // default: 1000
```

## Background scheduling

When used through the builder, cleanup can run automatically in the background:

```rust
use std::sync::Arc;
use std::time::Duration;
use xtax_blob_storage::{BlobStoreBuilder, Periodic, OnStart, Manual};

// Run every hour
let store = BlobStoreBuilder::new()
    .with_fs("/tmp/data")
    .with_clean(predicate, Arc::new(Periodic(Duration::from_secs(3600))))
    .build().await?;

// Run once on startup
let store = BlobStoreBuilder::new()
    .with_fs("/tmp/data")
    .with_clean(predicate, Arc::new(OnStart))
    .build().await?;

// Manual — trigger cleanup when needed
let manual = Arc::new(Manual::new());
let store = BlobStoreBuilder::new()
    .with_fs("/tmp/data")
    .with_clean(predicate, manual.clone())
    .build().await?;

manual.trigger();
```

### Background strategies

| Strategy | Behaviour |
|----------|-----------|
| `OnStart` | Runs once when the store is built |
| `Periodic(duration)` | Runs repeatedly at the given interval |
| `Manual::new()` | Runs when `Manual::trigger()` is called |

All background tasks run on a shared sequential maintenance queue (FIFO order, single worker).

## Visitor pattern

The `BlobVisitor` trait enables streaming iteration over matching blobs without loading everything into memory:

```rust
#[async_trait]
pub trait BlobVisitor: Send {
    /// Called for each matching blob.
    /// Return `false` to stop iteration early.
    async fn visit(&mut self, key: &str, meta: Option<&BlobMeta>) -> Result<bool>;
}
```

The `BlobStore::visit()` method uses this trait:

```rust
use xtax_blob_storage::{BlobVisitor, BlobMeta, PrefixFilter};

struct MyVisitor {
    count: u64,
}

#[async_trait]
impl BlobVisitor for MyVisitor {
    async fn visit(&mut self, key: &str, meta: Option<&BlobMeta>) -> Result<bool> {
        self.count += 1;
        println!("blob {}: {} ({} bytes)", self.count, key,
            meta.map(|m| m.stored_size).unwrap_or(0));
        Ok(true)  // continue
    }
}

let mut visitor = MyVisitor { count: 0 };
store.visit(&PrefixFilter::new(""), &mut visitor).await?;
```

The `CleanupVisitor` (used internally by `BlobCleanup`) is a good example of the pattern — it accumulates matching keys and deletes them in batches.

## How cleanup works internally

```plantuml
@startuml
participant "BlobCleanup" as BC
participant "Inner Store" as Store
participant "CleanupVisitor" as V

BC -> BC : cleanup()
BC -> Store : visit(empty prefix, CleanupVisitor)
loop for each blob
    Store -> V : visit(key, meta)
    V -> V : predicate(key, meta)?
    alt matches
        V -> V : add key to batch
        alt batch full
            V -> Store : delete(batch)
            Store --> V : ok
            V -> V : reset batch
        end
    end
end
V -> Store : delete(remaining batch)
Store --> V : ok
V --> BC : CleanupResult { deleted_count }
@enduml
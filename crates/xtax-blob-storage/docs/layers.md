# Custom layers

A manipulation layer is just a `BlobStore` wrapper around an inner store. Because every layer implements the same trait as backends, they are fully composable.

## Built-in layers

| Layer | Description |
|-------|-------------|
| `PrefixBlobStore` | Transparent key prefixing |
| `EncryptedBlobStore` | Envelope encryption and re-key support |
| `BlobCleanup` | Background lifecycle management |

These are documented in their respective pages:
- [Encryption](encryption.md)
- [Cleanup](cleanup.md)
- [Filters](filters.md) (for `PrefixFilter` used internally by `PrefixBlobStore`)

## Writing a custom layer

Implement `BlobStore` for a wrapper struct:

```rust
use async_trait::async_trait;
use std::sync::Arc;
use tokio::io::AsyncRead;
use xtax_blob_storage::{
    BlobStore, BlobInput, PutResult, ListFilter, BlobMeta, Result,
};

struct LoggingStore {
    inner: Arc<dyn BlobStore>,
}

#[async_trait]
impl BlobStore for LoggingStore {
    async fn put(&self, blobs: Vec<BlobInput>) -> Result<PutResult> {
        tracing::info!("putting {} blob(s)", blobs.len());
        let result = self.inner.put(blobs).await;
        tracing::info!("put finished: {:?}", result.as_ref().ok());
        result
    }

    async fn get(&self, key: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        tracing::info!("getting {}", key);
        self.inner.get(key).await
    }

    async fn delete(&self, keys: &[&str]) -> Result<()> {
        tracing::info!("deleting {:?}", keys);
        self.inner.delete(keys).await
    }

    async fn list(&self, filter: &dyn ListFilter) -> Result<Vec<String>> {
        self.inner.list(filter).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<(BlobMeta, Box<dyn AsyncRead + Send + Unpin>)> {
        self.inner.get_with_metadata(key).await
    }

    async fn list_with_metadata(&self, filter: &dyn ListFilter) -> Result<Vec<BlobMeta>> {
        self.inner.list_with_metadata(filter).await
    }
}
```

## Using a custom layer with the builder

```rust
let store = BlobStoreBuilder::new()
    .with_fs("/tmp/data")
    .with_layer(|inner| Arc::new(LoggingStore { inner }))
    .build().await?;
```

The `with_layer()` closure receives the current `Arc<dyn BlobStore>` and returns a wrapped version. The layer is applied at the position where `with_layer()` is called in the chain:

```rust
let store = BlobStoreBuilder::new()
    .with_fs("/tmp/data")
    .with_prefix("app/")
    .with_layer(|inner| Arc::new(LoggingStore { inner }))  // wraps Prefix(Fs)
    .with_encryption(provider)                               // wraps Logging(Prefix(Fs))
    .build().await?;
```

## Common use cases for custom layers

### Auditing

```rust
struct AuditStore {
    inner: Arc<dyn BlobStore>,
    audit_log: Arc<dyn AuditLog>,
}

#[async_trait]
impl BlobStore for AuditStore {
    async fn put(&self, blobs: Vec<BlobInput>) -> Result<PutResult> {
        let result = self.inner.put(blobs).await;
        for blob in &blobs {
            self.audit_log.record("put", &blob.key, result.is_ok()).await;
        }
        result
    }

    async fn delete(&self, keys: &[&str]) -> Result<()> {
        for key in keys {
            self.audit_log.record("delete", key, true).await;
        }
        self.inner.delete(keys).await
    }

    // ... delegate other methods
}
```

### Caching

```rust
struct CachingStore {
    inner: Arc<dyn BlobStore>,
    cache: Arc<dyn Cache>,
}

#[async_trait]
impl BlobStore for CachingStore {
    async fn get(&self, key: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        if let Some(cached) = self.cache.get(key).await {
            return Ok(Box::new(std::io::Cursor::new(cached)));
        }
        let reader = self.inner.get(key).await?;
        // Cache the content (in a real implementation, buffer and cache)
        Ok(reader)
    }

    // ... delegate other methods
}
```

### Rate limiting

```rust
struct RateLimitedStore {
    inner: Arc<dyn BlobStore>,
    limiter: Arc<dyn RateLimiter>,
}

#[async_trait]
impl BlobStore for RateLimitedStore {
    async fn put(&self, blobs: Vec<BlobInput>) -> Result<PutResult> {
        self.limiter.acquire().await;
        self.inner.put(blobs).await
    }

    async fn get(&self, key: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        self.limiter.acquire().await;
        self.inner.get(key).await
    }

    // ... delegate other methods
}
```

### Metrics

```rust
struct MetricsStore {
    inner: Arc<dyn BlobStore>,
    put_duration: Histogram,
    get_duration: Histogram,
}

#[async_trait]
impl BlobStore for MetricsStore {
    async fn put(&self, blobs: Vec<BlobInput>) -> Result<PutResult> {
        let start = std::time::Instant::now();
        let result = self.inner.put(blobs).await;
        self.put_duration.observe(start.elapsed());
        result
    }

    async fn get(&self, key: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        let start = std::time::Instant::now();
        let result = self.inner.get(key).await;
        self.get_duration.observe(start.elapsed());
        result
    }

    // ... delegate other methods
}
```

## Important: delegate `get_with_metadata` and `list_with_metadata`

If your layer doesn't need to modify metadata, delegate these methods to the inner store. The default implementations in the trait fall back to `get()` and `list()`, which may be less efficient.

```rust
async fn get_with_metadata(&self, key: &str) -> Result<(BlobMeta, Box<dyn AsyncRead + Send + Unpin>)> {
    self.inner.get_with_metadata(key).await
}

async fn list_with_metadata(&self, filter: &dyn ListFilter) -> Result<Vec<BlobMeta>> {
    self.inner.list_with_metadata(filter).await
}
```

## Layer contract

When writing a custom layer, follow these rules:

1. **Delegate everything** — pass through all methods you don't need to modify
2. **Preserve semantics** — don't change the contract of `BlobStore` methods
3. **Be transparent** — don't modify keys, metadata, or data unless that's the layer's purpose
4. **Override `visit()`** if your layer modifies keys or needs to scope iteration
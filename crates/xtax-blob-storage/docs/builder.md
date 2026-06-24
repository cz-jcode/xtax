# Builder reference

The `BlobStoreBuilder` uses a **typestate pattern** to enforce correct construction at compile time. The `build()` method is only available on states with a backend selected (`FsChosen`, `S3Chosen`, or `CustomChosen`). Calling `build()` on `NoBackend` is rejected by the compiler — you must call `with_fs()`, `with_s3()`, or `with_backend()` first.

## Constructor

```rust
pub fn new() -> BlobStoreBuilder<NoBackend>
```

Creates a new builder in the `NoBackend` state. Layers (prefix, encryption, cleanup, custom) can be added before or after backend selection — they accumulate in call order. The only restriction is that `build()` is not available until a backend is selected.

## Backend selection

These methods are only available on `BlobStoreBuilder<NoBackend>`:

| Method | State transition | Description |
|--------|-----------------|-------------|
| `with_fs(root)` | `NoBackend → FsChosen` | Filesystem backend, blobs stored under `root` |
| `with_s3(client, bucket)` | `NoBackend → S3Chosen` | S3-compatible backend |
| `with_backend(store)` | `NoBackend → CustomChosen` | Custom user-supplied backend |

### `with_fs(root)`

```rust
pub fn with_fs(self, root: impl Into<PathBuf>) -> BlobStoreBuilder<FsChosen>
```

Blobs are stored as individual files under `root`. The directory is created if it doesn't exist.

Requires the `fs` feature (enabled by default).

### `with_s3(client, bucket)`

```rust
pub fn with_s3(self, client: aws_sdk_s3::Client, bucket: impl Into<String>) -> BlobStoreBuilder<S3Chosen>
```

Works with AWS S3, Garage, MinIO, and any S3-compatible service.

Requires the `s3` feature (opt-in).

### `with_backend(store)`

```rust
pub fn with_backend(self, store: Arc<dyn BlobStore>) -> BlobStoreBuilder<CustomChosen>
```

Any type that implements `BlobStore` can be used. All layers (prefix, encryption, cleanup) work transparently on top.

## Layer methods

These methods are available on **any** builder state (`FsChosen`, `S3Chosen`, `CustomChosen`):

| Method | Description |
|--------|-------------|
| `with_prefix(prefix)` | Transparent key prefix |
| `with_encryption(provider)` | Envelope encryption |
| `with_rekey(strategy)` | Key rotation for the most recent encryption layer |
| `with_clean(predicate, strategy)` | Lifecycle cleanup with background scheduling |
| `with_clean_batch_size(n)` | Batch size for the most recent cleanup layer |
| `with_layer(f)` | Custom manipulation layer |

### `with_prefix(prefix)`

```rust
pub fn with_prefix(self, prefix: impl Into<String>) -> Self
```

Prepends `prefix` to all blob keys. The prefix is stripped from list results, making it transparent to the caller.

```rust
let store = BlobStoreBuilder::new()
    .with_fs("/tmp/data")
    .with_prefix("customer-42/")
    .build().await?;

// Stored as "customer-42/report.pdf"
store.put(vec![BlobInput::new("report.pdf", data)]).await?;

// Listed as "report.pdf"
let keys = store.list(&SuffixFilter::new(".pdf")).await?;
```

### `with_encryption(provider)`

```rust
pub fn with_encryption(self, provider: Arc<dyn EncryptionProvider>) -> Self
```

Adds transparent envelope encryption. All data is encrypted on write and decrypted on read. Encryption headers are stored alongside the data with a `.enc-header` suffix.

See [Encryption](encryption.md) for details on implementing `EncryptionProvider`.

### `with_rekey(strategy)`

```rust
pub fn with_rekey(self, strategy: Arc<dyn BackgroundStrategy>) -> Self
```

Configures automatic key rotation for the **most recently added** encryption layer.

**Must be called directly after `with_encryption()`.** If the most recent layer is not an encryption layer (e.g. called after `with_prefix()`, `with_clean()`, or without a preceding `with_encryption()`), `with_rekey()` is a silent no-op — the strategy is discarded and the store logs a warning at runtime. This conservative approach avoids runtime panics but means you should always pair `with_rekey()` immediately after `with_encryption()`.

```rust
let store = BlobStoreBuilder::new()
    .with_fs("/tmp/data")
    .with_encryption(provider)
    .with_rekey(Arc::new(Periodic(Duration::from_secs(86400))))  // daily
    .build().await?;
```

### `with_clean(predicate, strategy)`

```rust
pub fn with_clean(
    self,
    predicate: CleanupPredicate,
    strategy: Arc<dyn BackgroundStrategy>,
) -> Self
```

Adds lifecycle cleanup. The `predicate` is called for each blob during cleanup — return `true` to delete.

```rust
let predicate: CleanupPredicate = Box::new(|key, meta| {
    key.starts_with("tmp-")
});

let store = BlobStoreBuilder::new()
    .with_fs("/tmp/data")
    .with_clean(predicate, Arc::new(Periodic(Duration::from_secs(3600))))
    .build().await?;
```

### `with_clean_batch_size(n)`

```rust
pub fn with_clean_batch_size(self, batch_size: usize) -> Self
```

Sets the batch size for the most recently added cleanup layer. Keys are accumulated until `batch_size` is reached, then deleted in a single batch. Default: `1000`.

### `with_layer(f)`

```rust
pub fn with_layer<F>(self, f: F) -> Self
where
    F: Fn(Arc<dyn BlobStore>) -> Arc<dyn BlobStore> + Send + Sync + 'static,
```

Adds a custom manipulation layer. The closure receives the current `Arc<dyn BlobStore>` and returns a wrapped version.

```rust
let store = BlobStoreBuilder::new()
    .with_fs("/tmp/data")
    .with_layer(|inner| Arc::new(LoggingStore { inner }))
    .build().await?;
```

## S3-specific methods

These methods are only available on `BlobStoreBuilder<S3Chosen>`:

### `with_multipart_part_size(bytes)`

```rust
pub fn with_multipart_part_size(self, size: u64) -> Self
```

Sets the size (in bytes) of each part in a multipart upload. Must be at least 5 MiB (AWS minimum). Default: 50 MiB.

Blobs ≥ 5 MiB (S3 minimum) automatically use multipart upload. Memory usage per part is bounded by the configured `part_size`.

```rust
let store = BlobStoreBuilder::new()
    .with_s3(client, "my-bucket")
    .with_multipart_part_size(100 * 1024 * 1024)  // 100 MiB
    .build().await?;
```

## Build

Build is available on each backend state separately. Each `build()` applies all configured layers in order and returns the composed store:

### `BlobStoreBuilder<FsChosen>`

```rust
pub async fn build(self) -> Result<Arc<dyn BlobStore>>
```

Builds an FS-backed store with any configured layers.

### `BlobStoreBuilder<S3Chosen>`

```rust
pub async fn build(self) -> Result<Arc<dyn BlobStore>>
```

Builds an S3-backed store with any configured layers.

### `BlobStoreBuilder<CustomChosen>`

```rust
pub async fn build(self) -> Result<Arc<dyn BlobStore>>
```

Builds a custom-backed store with any configured layers.

All three create a shared sequential maintenance queue — cleanup and rekey tasks (if configured) execute in FIFO order on a single background worker.

## Layer ordering reference

| Call order | Resulting structure |
|------------|-------------------|
| `with_fs().with_prefix("a/")` | `Prefix(Fs)` |
| `with_fs().with_prefix("a/").with_encryption(p)` | `Encrypted(Prefix(Fs))` |
| `with_fs().with_encryption(p).with_prefix("a/")` | `Prefix(Encrypted(Fs))` |
| `with_fs().with_encryption(p).with_rekey(s)` | `Encrypted(Fs)` + rekey task |
| `with_fs().with_clean(p, s)` | `BlobCleanup(Fs)` + cleanup task |
| `with_fs().with_prefix("a/").with_encryption(p).with_clean(p, s)` | `BlobCleanup(Encrypted(Prefix(Fs)))` + cleanup + rekey tasks |
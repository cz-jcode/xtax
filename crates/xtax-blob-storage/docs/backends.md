# Backends

## Filesystem backend (`FsBlobStore`)

The filesystem backend stores blobs as individual files under a root directory.

### Root path normalisation

The root path is **always** normalised to end with `/` (the `KEY_SEPARATOR`).
This ensures that the `prefix_subdir` helper can correctly split prefix
hints into directory + basename components:

- `"/tmp/my-blobs"` → `"/tmp/my-blobs/"` (trailing `/` added)
- `"/tmp/my-blobs/"` → `"/tmp/my-blobs/"` (unchanged)

### Configuration

```rust
use xtax_blob_storage::BlobStoreBuilder;

let store = BlobStoreBuilder::new()
    .with_fs("/tmp/my-blobs")
    .build().await?;
```

The root directory is created automatically if it doesn't exist.

### Key validation

All blob keys are validated by [`validate_blob_key`](https://docs.rs/xtax-blob-storage/latest/xtax_blob_storage/fn.validate_blob_key.html)
**before** any storage operation. This ensures a consistent contract
across all backends:

| Pattern | Behaviour |
|---------|----------|
| Empty key `""` | Rejected — `Err(InvalidInput)` |
| Leading `"/"` | Rejected — would resolve to an absolute path |
| `".."` or `"."` component | Rejected — path traversal |
| Valid key like `a/b/c.txt` | Allowed — stored as `{root}/a/b/c.txt` |

The validation is applied to **every** `put`, `get`, `delete`, `exists`, and
`get_with_metadata` call — both in the FS backend and the S3 backend.

### Key nesting

Keys containing `/` are mapped to nested directories — identical behaviour to S3:

| Key | File path |
|-----|-----------|
| `hello.txt` | `{root}/hello.txt` |
| `a/b/c.txt` | `{root}/a/b/c.txt` |
| `invoices/2024/report.pdf` | `{root}/invoices/2024/report.pdf` |

### Prefix optimisation (`prefix_subdir`)

When a `ListFilter` provides a `prefix_hint()`, the FS backend uses it to
narrow the filesystem walk to a subdirectory of the root. This avoids
walking the entire root and then filtering.

**Directory filter** — the hint ends with `/` (e.g. `"a/b/"`):
- Walk only that subdirectory (`{root}/a/b/`)
- Most efficient — no post-filtering needed

**File filter** — the hint does NOT end with `/` (e.g. `"a/b"`):
- Walk the parent directory (`{root}/a/`) and apply `starts_with` filtering
- Less efficient but still correct — the caller's `ListFilter::matches()`
  checks each key

**No hint** — walk the full root (`{root}/`)

### Listing

`list()` and `list_with_metadata()` walk the root directory **recursively** (breadth-first). Every file found is included; empty directories are ignored.

- The relative path from root is used as the blob key
- Platform path separators are replaced by `/` for consistent, platform-independent keys

### Metadata

| Field | Source | Notes |
|-------|--------|-------|
| `stored_size` | `fs::metadata::len()` | Exact byte count |
| `modified_at` | `fs::metadata::modified()` | Filesystem mtime (best-effort) |
| `etag` | — | Always `None` (filesystems have no native ETag) |

### Feature flag

Requires the `fs` feature (enabled by default).

```toml
[dependencies]
xtax-blob-storage = { version = "0.1", default-features = false, features = ["fs"] }
```

---

## S3-compatible backend (`S3BlobStore`)

The S3 backend works with any S3-compatible service: AWS S3, Garage, MinIO, DigitalOcean Spaces, etc.

### Configuration

```rust
use aws_sdk_s3::Client;
use xtax_blob_storage::BlobStoreBuilder;

let client = Client::new(&aws_config::load_from_env().await);

let store = BlobStoreBuilder::new()
    .with_s3(client, "my-bucket")
    .build().await?;
```

### Multipart uploads

Blobs ≥ 5 MiB (S3 minimum) automatically use multipart upload. Smaller blobs use a single `PutObject` call. Memory usage per multipart part is bounded by the configured `part_size`.

| Setting | Default | Description |
|---------|---------|-------------|
| `part_size` | 50 MiB (52,428,800 bytes) | Size of each multipart part. Minimum 5 MiB (AWS requirement). Multipart threshold is always 5 MiB (S3 minimum). |

```rust
let store = BlobStoreBuilder::new()
    .with_s3(client, "my-bucket")
    .with_multipart_part_size(100 * 1024 * 1024)  // 100 MiB
    .build().await?;
```

### Metadata

| Field | Source | Notes |
|-------|--------|-------|
| `stored_size` | `ContentLength` from `HeadObject` / `ListObjectsV2` | Exact byte count |
| `modified_at` | `LastModified` from `HeadObject` / `ListObjectsV2` | S3 server timestamp |
| `etag` | `ETag` from `HeadObject` / `ListObjectsV2` | S3 ETag (MD5 or multipart hash) |

Metadata piggybacks on existing S3 API calls — no extra round-trips.

### Error handling

The backend maps S3 errors to `BlobStorageError`:

| S3 error | `BlobStorageError` variant |
|---------|---------------------------|
| `NoSuchKey` | `NotFound` |
| `NotFound` | `NotFound` |
| `NoSuchBucket` | `BackendMisconfigured` |
| Other | `Storage { source: ... }` |

### Feature flag

Requires the `s3` feature (opt-in).

```toml
[dependencies]
xtax-blob-storage = { version = "0.1", default-features = false, features = ["s3"] }
```

---

## Custom backend

Any type that implements `BlobStore` can be used as a backend:

```rust
use std::sync::Arc;
use xtax_blob_storage::{BlobStore, BlobStoreBuilder};

let store = BlobStoreBuilder::new()
    .with_backend(Arc::new(MyBackend))
    .with_prefix("tenant/")
    .build().await?;
```

All built-in layers (prefix, encryption, cleanup) work transparently on top of custom backends. See [Custom layers](layers.md) for details on implementing `BlobStore`.
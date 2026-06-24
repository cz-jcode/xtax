# Filters

Filters are used with `list()` and `list_with_metadata()` to select which blobs to include in results.

## ListFilter trait

```rust
pub trait ListFilter: Send + Sync {
    /// Returns `true` if the blob should be included in results.
    fn matches(&self, key: &str, meta: Option<&BlobMeta>) -> bool;
}
```

The `meta` parameter allows filters to make decisions based on blob metadata (size, timestamp, etag) when available.

## Built-in filters

### SuffixFilter

Match keys ending with a given suffix:

```rust
use xtax_blob_storage::SuffixFilter;

// List all PDF files
let pdfs = store.list(&SuffixFilter::new(".pdf")).await?;

// List all files with no extension
let no_ext = store.list(&SuffixFilter::new("")).await?;
```

### PrefixFilter

Match keys starting with a given prefix:

```rust
use xtax_blob_storage::PrefixFilter;

// List all blobs under "invoices/2024/"
let invoices = store.list(&PrefixFilter::new("invoices/2024/")).await?;
```

### NotFilter

Invert any other filter:

```rust
use xtax_blob_storage::{NotFilter, SuffixFilter};

// List everything that is NOT a .tmp file
let not_tmp = store.list(
    &NotFilter::new(Box::new(SuffixFilter::new(".tmp")))
).await?;
```

### Convenience: `exclude_suffix`

```rust
use xtax_blob_storage::ListFilter;  // for the trait method

// List everything except .part files
let complete = store.list(&ListFilter::exclude_suffix(".part")).await?;
```

## Composing filters

Filters can be composed using `NotFilter`:

```rust
use xtax_blob_storage::{PrefixFilter, SuffixFilter, NotFilter};

// List all .jpg files NOT in the "tmp/" prefix
let filter = NotFilter::new(Box::new(PrefixFilter::new("tmp/")));
// Then apply suffix filter separately:
let jpgs = store.list(&SuffixFilter::new(".jpg")).await?;
```

For more complex composition, implement `ListFilter` directly:

```rust
struct AndFilter {
    a: Box<dyn ListFilter>,
    b: Box<dyn ListFilter>,
}

impl ListFilter for AndFilter {
    fn matches(&self, key: &str, meta: Option<&BlobMeta>) -> bool {
        self.a.matches(key, meta) && self.b.matches(key, meta)
    }
}
```

## Custom filter

Implement `ListFilter` for any type:

```rust
use xtax_blob_storage::{ListFilter, BlobMeta};

struct SizeFilter {
    min_bytes: u64,
}

impl ListFilter for SizeFilter {
    fn matches(&self, _key: &str, meta: Option<&BlobMeta>) -> bool {
        match meta {
            Some(m) => m.stored_size >= self.min_bytes,
            None => true,  // include if no metadata available
        }
    }
}

// List blobs larger than 1 MB
let large = store.list(&SizeFilter { min_bytes: 1024 * 1024 }).await?;
```

## How filters interact with layers

### Prefix layer

When a `PrefixBlobStore` is in use, the filter is applied **after** the prefix is stripped:

```rust
let store = BlobStoreBuilder::new()
    .with_fs("/tmp/data")
    .with_prefix("customer-42/")
    .build().await?;

// This lists keys matching "*.pdf" — the prefix is transparent
let pdfs = store.list(&SuffixFilter::new(".pdf")).await?;
```

### Encryption layer

The `EncryptedBlobStore` automatically filters out `.enc-header` blobs from list results — you don't need to exclude them manually.

### Backend-specific optimisation

Backends may optimise known filter types. For example, the S3 backend could use `PrefixFilter` to scope the `ListObjectsV2` call to a specific prefix, reducing the amount of data returned.
# Logging, tracing & observability

The crate uses [`tracing`](https://docs.rs/tracing/) for structured, async-aware observability. Events are organised by **level** — production operators, debug investigators and deep-dive developers each see what they need.

---

## Level conventions

| Level | Purpose | Typical consumer |
|-------|---------|-----------------|
| `error!` | Unexpected failures in background tasks | On-call operator |
| `warn!` | Recoverable errors — individual blob fails, system continues | Operator / SRE |
| `info!` | Normal lifecycle events (cleanup/rekey completed) | Operator |
| `debug!` | Per-operation diagnostics — stored, deleted, listed, etc. | Engineer debugging an incident |
| `trace!` | Spans with full call-chain & parameters via `#[instrument]` | Developer (deep debugging) |

---

## Instrumentation (`#[instrument]`)

Every method on every `BlobStore` implementation is annotated with `#[instrument(skip(self, …))]`.

When `trace!` level is enabled, each call creates a **span** that:

- Captures the method name and key parameters (`key`, `count`, `prefix`, …)
- Propagates through the entire **layer chain** — e.g. `encrypt::put → prefix::put → fs::put`
- Records the duration of each nested call

### Example output (RUST_LOG=trace)

```
TRACE encrypt::store::put{ key="invoice.pdf" }: encrypt::store: Encrypting blob key="invoice.pdf" header_len=64
TRACE encrypt::store::put{ key="invoice.pdf" }: prefix::put{ prefix="prod/" }: prefix: Storing blobs via prefix layer prefix="prod/" count=2
TRACE encrypt::store::put{ key="invoice.pdf" }: prefix::put{ prefix="prod/" }: fs::put: Stored blob via FS key="prod/invoice.pdf" stored_size=1048576
TRACE encrypt::store::put{ key="invoice.pdf" }: prefix::put{ prefix="prod/" }: fs::put: Stored blob via FS key="prod/invoice.pdf.enc-header" stored_size=128
TRACE encrypt::store::put{ key="invoice.pdf" }: encrypt::store: Stored encrypted blobs
```

Each nested span shows the exact path through the layer stack — without any manual log propagation.

---

## Operational events (`debug!`)

When `debug!` level is enabled (recommended for production incident response), you see per-operation events:

| Event | Example |
|-------|---------|
| **Blob stored** | `Stored blob via FS key="report.pdf" stored_size=4096` |
| **Blob retrieved** | `Retrieved blob via S3 key="report.pdf"` |
| **Blob deleted** | `Deleted blob via FS key="temp/file.tmp"` |
| **Blob listed** | `Listed blobs via S3 count=127 page_count=3` |
| **Existence check** | `Checked blob existence via FS key="config.json" exists=true` |
| **Multipart started** | `Started multipart upload to S3 key="large.iso" upload_id="abc123"` |
| **Part uploaded** | `Uploaded part to S3 key="large.iso" upload_id="abc123" part_number=7` |
| **Multipart completed** | `Completed multipart upload to S3 key="large.iso" total_size=52428800 parts_count=10` |
| **Encryption** | `Encrypting blob key="invoice.pdf" header_len=64` |
| **Cleanup** | `Starting cleanup batch_size=1000` → `Cleanup completed deleted_count=42` |

---

## Warning events (`warn!`)

Events at `warn!` level indicate recoverable problems:

- `decryption failed for key '{key}': {error}` — a single blob's streaming decrypt failed
- `rekey: failed to fetch header '{key}': {error}` — header blob not accessible, skipping
- `rekey: failed to write header '{key}': {error}` — could not persist rekeyed header
- `S3 UploadPart failed; aborting multipart upload` — part-level upload failure triggers abort
- `Aborting multipart upload to S3` — multipart upload cleaned up after failure

---

## Background tasks (`info!`, `error!`)

Maintenance tasks scheduled via the builder (rekey, cleanup) log their results:

```
INFO  Rekey completed: 128 headers rekeyed
INFO  Cleanup completed: 57 blobs deleted
ERROR Rekey failed: [Encryption] provider unavailable
ERROR Cleanup failed: [Storage] S3 connection refused
```

---

## Enabling log output

Add `tracing-subscriber` to your `Cargo.toml`:

```toml
[dependencies]
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

Initialise at application start:

```rust
tracing_subscriber::fmt()
    .with_env_filter("xtax_blob_storage=debug")  // ← adjust level as needed
    .init();
```

### Recommended levels for different scenarios

| Scenario | Filter |
|----------|--------|
| Normal production | `xtax_blob_storage=info` |
| Investigating an issue | `xtax_blob_storage=debug` |
| Deep debugging / development | `xtax_blob_storage=trace` |
| Full details including backends | `xtax_blob_storage=trace,aws_sdk_s3=warn` |

---

## Custom layers

If you write a [custom layer](layers.md) (logging, audit, metrics), you can use the same `#[instrument]` macro and `debug!`/`info!` conventions to integrate seamlessly with the crate's observability model.
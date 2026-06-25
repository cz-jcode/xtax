# xtax-blob-storage

[![CI](https://github.com/cz-jcode/xtax/actions/workflows/ci.yml/badge.svg)](https://github.com/cz-jcode/xtax/actions/workflows/ci.yml)
[![CodeQL](https://github.com/cz-jcode/xtax/actions/workflows/codeql.yml/badge.svg)](https://github.com/cz-jcode/xtax/actions/workflows/codeql.yml)
[![Dependabot](https://img.shields.io/badge/dependabot-active-blue?logo=dependabot)](https://github.com/cz-jcode/xtax/network/updates)
[![crates.io](https://img.shields.io/crates/v/xtax-blob-storage.svg)](https://crates.io/crates/xtax-blob-storage)
[![docs.rs](https://docs.rs/xtax-blob-storage/badge.svg)](https://docs.rs/xtax-blob-storage)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/5f6106e413274dfcac3179c96ed643bf)](https://app.codacy.com/gh/cz-jcode/xtax/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

> **Experimental** blob storage abstraction for Rust with filesystem and S3
> backends, streaming uploads, optional encryption, and composable layers.

A compact, builder-driven blob storage abstraction. Not a general-purpose
object store — see [Relation to Apache object_store](#relation-to-apache-object_store)
below.

## Status

**v0.1.0 — Experimental / learning project.** Not production-ready.

## Motivation

`xtax-blob-storage` was created for the needs of the `xtax` project.

The goal is not to compete with large general-purpose object storage libraries.
The goal is to provide a small, practical, builder-driven blob storage abstraction
that fits the way `xtax` stores, reads, streams, and optionally encrypts blobs.

The API intentionally favors clarity and explicit configuration over covering
every possible storage backend or advanced object-store feature.

## Relation to Apache object_store

Apache `object_store` is a mature and much broader object storage abstraction.

`xtax-blob-storage` targets a different use case: a compact application-level
blob API with builder-style configuration, composable layers, filesystem/S3
support, streaming I/O, and optional encryption.

It was built primarily to support the `xtax` project, while remaining useful
as a small experimental crate for similar applications.

## Features

| Feature | What it gives you | Status |
|---------|-------------------|--------|
| **Unified trait** | `get()`, `put()`, `delete()`, `list()`, `exists()` — one API | Stable |
| **FS backend** | Blobs as files under a root directory | Stable |
| **S3 backend** | AWS S3, Garage, MinIO — any S3 API | Needs `features = ["s3"]` |
| **Prefix layer** | Transparent key prefixing | Stable |
| **Encryption layer** | Envelope encryption with detached headers | **Experimental** — streaming with in-memory headers |
| **Cleanup** | Lifecycle cleanup with predicate | Stable |
| **Background tasks** | `OnStart`, `Periodic`, `Manual` scheduling | Stable |
| **Builder safety** | Compile-time-safe typestate builder — impossible to call `build()` without a backend | Stable |

## Quick start

```rust
use xtax_blob_storage::{BlobStoreBuilder, BlobInput};

let store = BlobStoreBuilder::new()
    .with_fs("/tmp/data")
    .with_prefix("my-app/")
    .build()
    .await
    .unwrap();

store.put(vec![BlobInput::new("hello.txt", b"data".as_slice())]).await.unwrap();
let mut reader = store.get("hello.txt").await.unwrap();
// ...
```

## Minimum dependencies

```toml
[dependencies]
xtax-blob-storage = "0.1"
tokio = { version = "1.52", features = ["rt", "io-util"] }
```

No database. No gRPC. No framework lock-in. Just blobs.

## Features

| Feature | Dependencies | When to use           |
|---------|-------------|-----------------------|
| `fs` (default) | `tokio/fs` | Local filesystem |
| `s3` (opt-in) | `aws-sdk-s3`, `aws-config` | S3-compatible storage |

Both features can be enabled at once — switch at runtime via the builder.

## Documentation

- [**Getting Started**](docs/guide.md) — step-by-step tutorial
- [**Architecture**](docs/architecture.md) — how layers compose
- [**Builder reference**](docs/builder.md) — all builder methods with state constraints
- [**Backends**](docs/backends.md) — FS and S3 in detail
- [**Encryption**](docs/encryption.md) — envelope encryption and online key rotation
- [**Cleanup**](docs/cleanup.md) — lifecycle management and visitor pattern
- [**Filters**](docs/filters.md) — all built-in filters and custom filter API
- [**Logging & tracing**](docs/logging.md) — instrumentation, log levels, operational events, recommended subscriber configuration
- [**Custom layers**](docs/layers.md) — write your own logging, caching, audit layers
- [**API reference on docs.rs**](https://docs.rs/xtax-blob-storage)

## CI

The CI workflow lives in `.github/workflows/ci.yml` and runs `check`, `test`,
`lint`, and `cargo publish --dry-run`. You can run it locally with
[act](https://github.com/nektos/act) (`act -j check`) or by executing the
same cargo commands from the crate root:

```bash
cargo check --lib --no-default-features
cargo check --lib --all-features
cargo test --all-features
cargo test --no-default-features
cargo fmt --check
cargo clippy --all-features -- -D warnings
cargo publish --dry-run
```

## License

Licensed under MIT or Apache-2.0 at your option.

## AI contribution note

This library was developed with LLM assistance under continuous human supervision.
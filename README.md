# xtax

Rust infrastructure ecosystem — a Cargo workspace of independent, composable crates.

## What is xtax?

`xtax` is **not** a monolithic framework. It's a **workspace** / semi-monorepo that
hosts a growing collection of standalone Rust crates for building backend infrastructure.

The top-level `xtax` crate is a thin **facade** — it contains **no logic** of its own.
It only re-exports selected subcrates behind Cargo feature flags, giving you a single
dependency with composable, opt-in functionality.

## Crates

| Crate | Description | Status |
|-------|-------------|---|
| [`xtax-blob-storage`](crates/xtax-blob-storage/) | Application-level blob storage abstraction with filesystem/S3 backends, optional encryption, rekey, cleanup, and background maintenance. | 0.1.0 publish candidate |

Future planned crates (not yet created):
- `xtax-encryption` — standalone encryption utilities
- `xtax-state-store` — generic state persistence layer
- `xtax-state-query` — query layer on top of state store

## Usage — via the facade

Add `xtax` with feature flags:

```toml
[dependencies]
xtax = { version = "0.1", features = ["blob-storage"] }
```

```rust
use xtax::blob_storage::BlobStoreBuilder;
```

## Usage — direct dependency

You can also depend directly on any subcrate:

```toml
[dependencies]
xtax-blob-storage = "0.1"
```

```rust
use xtax_blob_storage::BlobStoreBuilder;
```

Both paths are valid and supported.

## Architecture rules

- The `xtax` facade crate contains **no logic** — only re-exports.
- Each subcrate has **standalone value** and can be used independently.
- Subcrates **must not** depend on the facade crate.
- Dependency direction: `xtax` → subcrate, never the reverse.
- Private/internal application glue (runtime bootstrap, telemetry, S3/OpenObserve
  setup, encryption config) must **not** be moved into public crates.
- Runtime/bootstrap crates remain private unless intentionally generalized.
- Application config crates remain private unless intentionally generalized.
- Public crates must not know about concrete deployment infrastructure unless
  that is their explicit domain.

## Feature flags (facade)
| Feature             | Description |
|---------------------|-------------|
| `blob-storage`      | Re-exports `xtax-blob-storage` with filesystem backend |
| `blob-storage-s3`   | Re-exports `xtax-blob-storage` with S3 backend |
| `blob-storage-full` | Enables all `xtax-blob-storage` backends exposed by the facade |
| `full`              | Enables all currently exposed facade features |

Default features: **none**. You opt in to exactly what you need.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for release/publishing instructions and contribution guidelines.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
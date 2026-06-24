# Contributing to xtax

Thanks for considering contributing! This document outlines the basics to get started.

## Development setup

```bash
git clone https://github.com/cz-jcode/xtax
cd xtax
cargo build --workspace
```

## Running tests

```bash
# Full workspace
cargo test --workspace --all-features

# Individual crate
cargo test -p xtax-blob-storage --all-features

# Specific feature combo
cargo test -p xtax-blob-storage --no-default-features
cargo test -p xtax-blob-storage --features fs
```

## Code quality

CI enforces these checks on every PR (see [ci.yml](.github/workflows/ci.yml)):

```bash
cargo fmt --all --check
cargo clippy --workspace --all-features -- -D warnings
```

Run them locally before pushing to avoid round-trips.

## Pull requests

1. Fork the repo and create a branch from `main`.
2. Make your changes — keep them focused and well-scoped.
3. Run `cargo fmt` and `cargo clippy` locally.
4. Ensure all tests pass: `cargo test --workspace --all-features`.
5. Open a PR against `main`. CI will run automatically.
6. PRs need at least one approving review before merge.

## Architecture rules

- The `xtax` facade crate contains **no logic** — only re-exports.
- Each subcrate has **standalone value** and can be used independently.
- Subcrates **must not** depend on the facade crate.
- Dependency direction: `xtax` → subcrate, never the reverse.
- Private/internal application glue must stay out of public crates.

## Releasing / Publishing

Releases are driven by Git tags. Pushing a tag with the right prefix triggers
a GitHub Actions workflow that publishes the crate to [crates.io](https://crates.io).

| Tag prefix                    | Workflow                                  | What it publishes   |
|-------------------------------|-------------------------------------------|---------------------|
| `xtax-encryption-v*`          | `publish-xtax-encryption.yml`             | `xtax-encryption`   |
| `xtax-blob-storage-v*`        | `publish-xtax-blob-storage.yml`           | `xtax-blob-storage` |
| `xtax-v*`                     | `publish-xtax.yml`                        | `xtax`              |

### Step-by-step

1. Bump the version in the crate's `Cargo.toml` (and downstream dependency versions if needed).
2. Commit and push to `main`. Wait for CI to pass.
3. Create and push the tags **in this order** (each subsequent crate depends on the previous):

```bash
# 1. Publish xtax-encryption (do this FIRST — xtax-blob-storage depends on it)
git tag xtax-encryption-v0.1.1
git push origin xtax-encryption-v0.1.1

# 2. Publish xtax-blob-storage (xtax depends on it)
git tag xtax-blob-storage-v0.1.1
git push origin xtax-blob-storage-v0.1.1

# 3. Publish xtax facade
git tag xtax-v0.1.1
git push origin xtax-v0.1.1
```

4. GitHub Actions will run `cargo publish --dry-run` first, then publish.

> **Note:** Publishing requires a `release` environment with a `CARGO_REGISTRY_TOKEN`
> secret configured in the GitHub repository settings. Both publish workflows use
> `environment: release`, so the token is only exposed to those jobs.
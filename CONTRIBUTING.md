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

Releases use **lockstep versioning**: all crates share the same version number.
Pushing a single `v*` tag triggers `release.yml`, which validates that all three
crates have matching versions, then publishes them to [crates.io](https://crates.io)
in order: `xtax-encryption` → `xtax-blob-storage` → `xtax`.

| Tag        | Workflow         | What it publishes                           |
|------------|------------------|---------------------------------------------|
| `v*`       | `release.yml`    | `xtax-encryption`, `xtax-blob-storage`, `xtax` |

### Step-by-step

1. Bump the version in **all three** `Cargo.toml` files to the same version:
   - `Cargo.toml` (root — facade + dependency versions)
   - `crates/xtax-encryption/Cargo.toml`
   - `crates/xtax-blob-storage/Cargo.toml`
2. Update any intra-workspace dependency versions (e.g. `xtax-blob-storage` → `xtax-encryption`).
3. Commit and push to `main`. Wait for CI to pass.
4. Create and push a single tag:
   ```bash
   git tag v0.1.2
   git push origin v0.1.2
   ```
5. `release.yml` will:
   - Extract the version from the tag
   - Validate that all three crates have the same version
   - Publish each crate (skipping any that already exist on crates.io)
   - Create a GitHub Release with auto-generated notes

> **Note:** Publishing requires a `release` environment with a `CARGO_REGISTRY_TOKEN`
> secret configured in the GitHub repository settings.

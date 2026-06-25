# xtax-encryption

[![CI](https://github.com/cz-jcode/xtax/actions/workflows/ci.yml/badge.svg)](https://github.com/cz-jcode/xtax/actions/workflows/ci.yml)
[![CodeQL](https://github.com/cz-jcode/xtax/actions/workflows/codeql.yml/badge.svg)](https://github.com/cz-jcode/xtax/actions/workflows/codeql.yml)
[![Dependabot](https://img.shields.io/badge/dependabot-active-blue?logo=dependabot)](https://github.com/cz-jcode/xtax/network/updates)
[![crates.io](https://img.shields.io/crates/v/xtax-encryption.svg)](https://crates.io/crates/xtax-encryption)
[![docs.rs](https://docs.rs/xtax-encryption/badge.svg)](https://docs.rs/xtax-encryption)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/5f6106e413274dfcac3179c96ed643bf)](https://app.codacy.com/gh/cz-jcode/xtax/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

> **Trait-only** encryption provider interface — no backend, no storage,
> no I/O decisions.

`xtax-encryption` defines the [`EncryptionProvider`] trait used by
[`xtax-blob-storage`] and other crates that need a pluggable stream
encryption layer with detached headers.

## Status

**v0.1.2 — Experimental / learning project.** Not production-ready.

## Design

This crate contains **only**:

- The [`EncryptionProvider`] trait (3 async methods)
- A lightweight [`EncryptionError`] error type
- A [`EncryptionResult`] type alias

No storage. No backends. No encryption implementations. Just the contract.

## Usage

```toml
[dependencies]
xtax-encryption = "0.1"
```

```rust
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};
use xtax_encryption::{EncryptionProvider, EncryptionResult};

struct MyProvider;

#[async_trait]
impl EncryptionProvider for MyProvider {
    async fn encrypt_stream(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        output: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> EncryptionResult<Vec<u8>> {
        // ... encrypt ...
        Ok(vec![])  // header bytes
    }

    async fn decrypt_stream(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        output: &mut (dyn AsyncWrite + Send + Unpin),
        header_bytes: &[u8],
    ) -> EncryptionResult<()> {
        // ... decrypt ...
        Ok(())
    }

    async fn rekey_header(&self, header_bytes: &[u8]) -> EncryptionResult<Option<Vec<u8>>> {
        Ok(None)  // already current
    }
}
```

## License

Licensed under MIT or Apache-2.0 at your option.
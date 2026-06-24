# xtax-encryption

> **Trait-only** encryption provider interface — no backend, no storage,
> no I/O decisions.

`xtax-encryption` defines the [`EncryptionProvider`] trait used by
[`xtax-blob-storage`] and other crates that need a pluggable stream
encryption layer with detached headers.

## Status

**v0.1.0 — Experimental / learning project.** Not production-ready.

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
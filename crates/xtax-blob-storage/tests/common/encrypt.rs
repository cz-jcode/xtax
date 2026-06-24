//! Shared dummy `ShiftEncryption` and `RekeyableShiftEncryption` providers
//! used by multiple test files.
//!
//! # Note
//!
//! `#[allow(dead_code)]` is needed because this file is compiled as part of
//! each integration test crate separately — not all functions are used in
//! every context.

#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use xtax_blob_storage::{BlobStore, BlobStoreBuilder, EncryptionProvider, EncryptionResult};

// ============================================================================
// Dummy encryption: rotate (shift) each byte by a fixed offset.
//
//   Header format: [wrapped_dek: u8, master_key_id: u8]   (2 bytes)
//
//   The data encryption key (DEK) = shift value. It is XOR-"wrapped" with
//   the master key ID:
//
//     wrapped_dek = data_shift XOR master_key_id
//
//   Encrypt:  output[i] = input[i].wrapping_add(data_shift)
//   Decrypt:  output[i] = input[i].wrapping_sub(dek)   where dek = header[0] XOR header[1]
// ============================================================================

pub struct ShiftEncryption {
    pub shift: u8,
}

impl ShiftEncryption {
    pub fn new(shift: u8) -> Self {
        Self { shift }
    }
}

#[async_trait]
impl EncryptionProvider for ShiftEncryption {
    async fn encrypt_stream(
        &self,
        input: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
        output: &mut (dyn tokio::io::AsyncWrite + Send + Unpin),
    ) -> EncryptionResult<Vec<u8>> {
        let mut buf = Vec::new();
        input.read_to_end(&mut buf).await?;

        // Shift each byte right
        let encrypted: Vec<u8> = buf.iter().map(|b| b.wrapping_add(self.shift)).collect();

        output.write_all(&encrypted).await?;
        output.flush().await?;

        // Header: [wrapped_dek, master_key_id=0]
        // For plain ShiftEncryption, master_key_id is always 0
        Ok(vec![self.shift ^ 0, 0])
    }

    async fn decrypt_stream(
        &self,
        input: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
        output: &mut (dyn tokio::io::AsyncWrite + Send + Unpin),
        header_bytes: &[u8],
    ) -> EncryptionResult<()> {
        // Unwrap DEK from header: dek = wrapped_dek XOR master_key_id
        let wrapped_dek = header_bytes.first().copied().unwrap_or(0);
        let master_key_id = header_bytes.get(1).copied().unwrap_or(0);
        let dek = wrapped_dek ^ master_key_id;

        let mut buf = Vec::new();
        input.read_to_end(&mut buf).await?;

        // Shift each byte left by dek
        let decrypted: Vec<u8> = buf.iter().map(|b| b.wrapping_sub(dek)).collect();

        output.write_all(&decrypted).await?;
        output.flush().await?;

        Ok(())
    }

    async fn rekey_header(&self, header_bytes: &[u8]) -> EncryptionResult<Option<Vec<u8>>> {
        // Plain ShiftEncryption always uses master_key_id=0, so rekey is a no-op
        let master_key_id = header_bytes.get(1).copied().unwrap_or(0);
        if master_key_id == 0 {
            return Ok(None);
        }
        // Re-wrap with master_key_id=0
        let wrapped_dek = header_bytes.first().copied().unwrap_or(0);
        let dek = wrapped_dek ^ master_key_id;
        Ok(Some(vec![dek ^ 0, 0]))
    }
}

// ============================================================================
// Rekeyable variant: tracks a master key generation.
//
// Each rekey() call with an older master_key_id produces a new header with
// the current master_key_id, simulating real key rotation.
//
//   header[0] = data_shift XOR master_key_id   (wrapped DEK)
//   header[1] = master_key_id
// ============================================================================

pub struct RekeyableShiftEncryption {
    /// The data encryption shift (acts like the DEK — never changes for a given blob).
    pub shift: u8,
    /// Current master key generation (acts like the KEK identifier).
    pub master_key_id: u8,
}

impl RekeyableShiftEncryption {
    pub fn new(shift: u8, master_key_id: u8) -> Self {
        Self {
            shift,
            master_key_id,
        }
    }
}

#[async_trait]
impl EncryptionProvider for RekeyableShiftEncryption {
    async fn encrypt_stream(
        &self,
        input: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
        output: &mut (dyn tokio::io::AsyncWrite + Send + Unpin),
    ) -> EncryptionResult<Vec<u8>> {
        let mut buf = Vec::new();
        input.read_to_end(&mut buf).await?;

        // Shift each byte right by the data shift (DEK)
        let encrypted: Vec<u8> = buf.iter().map(|b| b.wrapping_add(self.shift)).collect();

        output.write_all(&encrypted).await?;
        output.flush().await?;

        // Header: [wrapped_dek, master_key_id]
        // wrapped_dek = shift XOR master_key_id
        Ok(vec![self.shift ^ self.master_key_id, self.master_key_id])
    }

    async fn decrypt_stream(
        &self,
        input: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
        output: &mut (dyn tokio::io::AsyncWrite + Send + Unpin),
        header_bytes: &[u8],
    ) -> EncryptionResult<()> {
        // Unwrap DEK: dek = wrapped_dek XOR master_key_id
        let wrapped_dek = header_bytes.first().copied().unwrap_or(0);
        let master_key_id = header_bytes.get(1).copied().unwrap_or(0);
        let dek = wrapped_dek ^ master_key_id;

        let mut buf = Vec::new();
        input.read_to_end(&mut buf).await?;

        // Shift each byte left by the unwrapped DEK
        let decrypted: Vec<u8> = buf.iter().map(|b| b.wrapping_sub(dek)).collect();

        output.write_all(&decrypted).await?;
        output.flush().await?;

        Ok(())
    }

    async fn rekey_header(&self, header_bytes: &[u8]) -> EncryptionResult<Option<Vec<u8>>> {
        let old_master_key_id = header_bytes.get(1).copied().unwrap_or(0);

        // If already using the current master key, nothing to do
        if old_master_key_id == self.master_key_id {
            return Ok(None);
        }

        // Unwrap the DEK using the OLD master key
        let wrapped_dek = header_bytes.first().copied().unwrap_or(0);
        let dek = wrapped_dek ^ old_master_key_id;

        // Re-wrap with the CURRENT master key
        let new_header = vec![dek ^ self.master_key_id, self.master_key_id];
        Ok(Some(new_header))
    }
}

// ============================================================================
// Provider that always fails on decrypt — for testing error propagation
// ============================================================================

pub struct FailingDecryption;

#[async_trait]
impl EncryptionProvider for FailingDecryption {
    async fn encrypt_stream(
        &self,
        input: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
        output: &mut (dyn tokio::io::AsyncWrite + Send + Unpin),
    ) -> EncryptionResult<Vec<u8>> {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        input.read_to_end(&mut buf).await?;
        output.write_all(&buf).await?;
        output.flush().await?;
        Ok(vec![0, 0])
    }

    async fn decrypt_stream(
        &self,
        _input: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
        _output: &mut (dyn tokio::io::AsyncWrite + Send + Unpin),
        _header_bytes: &[u8],
    ) -> EncryptionResult<()> {
        Err(xtax_encryption::EncryptionError::Operation {
            message: "decryption intentionally failed".to_string(),
            source: None,
        })
    }

    async fn rekey_header(&self, _header_bytes: &[u8]) -> EncryptionResult<Option<Vec<u8>>> {
        Ok(None)
    }
}

// ============================================================================
// Provider that encrypts successfully but reports failure after writing data.
// This simulates an encryption task that writes encrypted data then errors,
// testing the put() rollback logic.
// ============================================================================

pub struct FailingAfterWriteEncryption;

#[async_trait]
impl EncryptionProvider for FailingAfterWriteEncryption {
    async fn encrypt_stream(
        &self,
        input: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
        output: &mut (dyn tokio::io::AsyncWrite + Send + Unpin),
    ) -> EncryptionResult<Vec<u8>> {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        input.read_to_end(&mut buf).await?;
        // Write the data (simulating partial encrypted data being stored)
        output.write_all(&buf).await?;
        output.flush().await?;
        // Then fail — the encryption task reports an error
        Err(xtax_encryption::EncryptionError::Operation {
            message: "encryption intentionally failed after writing data".to_string(),
            source: None,
        })
    }

    async fn decrypt_stream(
        &self,
        _input: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
        _output: &mut (dyn tokio::io::AsyncWrite + Send + Unpin),
        _header_bytes: &[u8],
    ) -> EncryptionResult<()> {
        unreachable!()
    }

    async fn rekey_header(&self, _header_bytes: &[u8]) -> EncryptionResult<Option<Vec<u8>>> {
        Ok(None)
    }
}

// ============================================================================
// Helpers — create an encrypted blob store with the dummy providers
// ============================================================================

/// Create an FS-backed encrypted store in a temp directory.
#[cfg(feature = "fs")]
pub async fn encrypted_fs_store(shift: u8) -> Arc<dyn BlobStore> {
    let dir = tempfile::tempdir().unwrap();
    // Capture path before keep() moves `dir`.
    let path = dir.path().to_path_buf();
    // Keep dir alive — the OS will clean up on reboot.
    // Without this the tempdir is removed when `dir` is dropped,
    // causing `ensure_root()` to return BackendMisconfigured.
    let _ = dir.keep();
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(shift));
    BlobStoreBuilder::new()
        .with_fs(path)
        .with_encryption(provider)
        .build()
        .await
        .unwrap()
}

/// Create an FS-backed encrypted store using RekeyableShiftEncryption.
#[cfg(feature = "fs")]
pub async fn rekeyable_encrypted_fs_store(shift: u8, master_key_id: u8) -> Arc<dyn BlobStore> {
    let dir = tempfile::tempdir().unwrap();
    // Capture path before keep() moves `dir`.
    let path = dir.path().to_path_buf();
    // Keep dir alive — the OS will clean up on reboot.
    let _ = dir.keep();
    let provider: Arc<dyn EncryptionProvider> =
        Arc::new(RekeyableShiftEncryption::new(shift, master_key_id));
    BlobStoreBuilder::new()
        .with_fs(path)
        .with_encryption(provider)
        .build()
        .await
        .unwrap()
}

/// Create an S3-backed encrypted store with a fresh bucket (in-process mock).
///
/// Each call creates its own isolated mock S3 server with a unique temp
/// directory so parallel tests don't interfere with each other.
#[cfg(feature = "s3")]
pub async fn encrypted_s3_store(shift: u8) -> Arc<dyn BlobStore> {
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(shift));
    let (client, bucket) = s3_client_and_bucket().await;
    BlobStoreBuilder::new()
        .with_s3(client, bucket)
        .with_encryption(provider)
        .build()
        .await
        .unwrap()
}

#[cfg(feature = "s3")]
async fn s3_client_and_bucket() -> (aws_sdk_s3::Client, String) {
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region, SharedCredentialsProvider};
    use s3s::auth::SimpleAuth;
    use s3s::host::SingleDomain;
    use s3s::service::S3ServiceBuilder;
    use s3s_fs::FileSystem;

    const DOMAIN_NAME: &str = "localhost:0";
    const REGION: &str = "us-east-1";

    // Unique per-invocation directory — critical for parallel test isolation
    let sub_dir = uuid_like();
    let root = std::env::temp_dir().join("s3s-xtax-enc").join(sub_dir);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let fs = FileSystem::new(&root).unwrap();

    let cred = Credentials::for_tests();

    let service = {
        let mut b = S3ServiceBuilder::new(fs);
        b.set_auth(SimpleAuth::from_single(
            cred.access_key_id(),
            cred.secret_access_key(),
        ));
        b.set_host(SingleDomain::new(DOMAIN_NAME).unwrap());
        b.build()
    };

    let aws_client = s3s_aws::Client::from(service);
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(REGION))
        .credentials_provider(SharedCredentialsProvider::new(cred))
        .http_client(aws_client)
        .build();

    let client = aws_sdk_s3::Client::from_conf(config);
    let bucket = format!("enc-test-{}", uuid_like());

    let _ = client.create_bucket().bucket(&bucket).send().await.unwrap();

    (client, bucket)
}

/// Returns a unique-ish identifier for temp directories and bucket names.
#[cfg(feature = "s3")]
fn uuid_like() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:016x}", n)
}

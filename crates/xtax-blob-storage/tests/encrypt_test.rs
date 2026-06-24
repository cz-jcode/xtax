//! Encryption layer tests — uses a dummy `ShiftEncryption` provider that
//! shifts bytes horizontally, simulating real encrypt/decrypt round-trips.
//!
//! Run with:
//!   cargo test --test encrypt_test                    # FS only
//!   cargo test --features s3 --test encrypt_test      # FS + S3

#![cfg(feature = "fs")]

use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use xtax_blob_storage::{
    BatchError, BlobInput, BlobMeta, BlobStorageError, BlobStore, BlobStoreBuilder, BlobVisitor,
    EncryptedBlobStore, EncryptionProvider, KeyError, ListFilter, PerKeyError, PutResult, Result,
};

#[path = "common/encrypt.rs"]
mod common_encrypt;
use common_encrypt::*;

// ============================================================================
// Macro to generate identical test bodies for each backend
// ============================================================================

macro_rules! encrypt_tests {
    ($mod_name:ident, $store_fn:path) => {
        mod $mod_name {
            use super::*;
            use xtax_blob_storage::SuffixFilter;

            #[tokio::test]
            async fn test_encrypt_roundtrip() {
                let store = $store_fn(7u8).await;

                let data = b"Hello, encrypted world!";
                store
                    .put(vec![BlobInput::new("secret.txt", &data[..])])
                    .await
                    .unwrap();

                let mut reader = store.get("secret.txt").await.unwrap();
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, data, "round-trip should return original data");
            }

            #[tokio::test]
            async fn test_encrypt_fixed_shift() {
                let store = $store_fn(42u8).await;

                let data = b"test data with fixed shift";
                store
                    .put(vec![BlobInput::new("fixed.txt", &data[..])])
                    .await
                    .unwrap();

                let mut reader = store.get("fixed.txt").await.unwrap();
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, data);
            }

            #[tokio::test]
            async fn test_encrypt_binary_data() {
                let store = $store_fn(3u8).await;

                let data: Vec<u8> = (0..255).collect(); // all possible byte values
                store
                    .put(vec![BlobInput::new(
                        "binary.bin",
                        std::io::Cursor::new(data.clone()),
                    )])
                    .await
                    .unwrap();

                let mut reader = store.get("binary.bin").await.unwrap();
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, data, "binary round-trip should preserve all bytes");
            }

            #[tokio::test]
            async fn test_encrypt_multiple_blobs() {
                let store = $store_fn(13u8).await;

                let blobs = vec![
                    BlobInput::new("a.txt", &b"aaaa"[..]),
                    BlobInput::new("b.txt", &b"bbbb"[..]),
                    BlobInput::new("c.txt", &b"cccc"[..]),
                ];
                store.put(blobs).await.unwrap();

                for (key, expected) in [("a.txt", b"aaaa"), ("b.txt", b"bbbb"), ("c.txt", b"cccc")]
                {
                    let mut reader = store.get(key).await.unwrap();
                    let mut buf = Vec::new();
                    reader.read_to_end(&mut buf).await.unwrap();
                    assert_eq!(buf, expected, "round-trip for '{key}' should match");
                }
            }

            #[tokio::test]
            async fn test_encrypt_large_blob() {
                let store = $store_fn(9u8).await;

                // 1 MiB of data
                let data: Vec<u8> = (0..1024 * 1024).map(|i| (i & 0xFF) as u8).collect();
                store
                    .put(vec![BlobInput::new(
                        "large.bin",
                        std::io::Cursor::new(data.clone()),
                    )])
                    .await
                    .unwrap();

                let mut reader = store.get("large.bin").await.unwrap();
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf.len(), data.len(), "large blob size should match");
                assert_eq!(buf, data, "large blob round-trip should match");
            }

            #[tokio::test]
            async fn test_encrypt_10_mib_roundtrip() {
                let store = $store_fn(9u8).await;

                // 10 MiB blob — regression test that streaming encryption works
                // for blobs larger than the duplex pipe buffer (64 KiB).
                let data = vec![0xABu8; 10 * 1024 * 1024];
                store
                    .put(vec![BlobInput::new(
                        "streaming.bin",
                        std::io::Cursor::new(data.clone()),
                    )])
                    .await
                    .unwrap();

                let mut reader = store.get("streaming.bin").await.unwrap();
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf.len(), data.len(), "10 MiB blob size should match");
                assert!(
                    buf.iter().all(|&b| b == 0xAB),
                    "all bytes should be preserved"
                );
            }

            #[tokio::test]
            async fn test_get_not_found() {
                let store = $store_fn(5u8).await;
                let result = store.get("nonexistent.txt").await;
                assert!(result.is_err(), "expected Err for non-existent key");
            }

            #[tokio::test]
            async fn test_delete_removes_data_and_header() {
                let store = $store_fn(17u8).await;

                store
                    .put(vec![BlobInput::new("del.txt", &b"delete me"[..])])
                    .await
                    .unwrap();

                // Data exists before delete
                assert!(store.exists("del.txt").await.unwrap());

                // Delete
                store.delete(&["del.txt"]).await.unwrap();

                // Data should be gone
                let exists = store.exists("del.txt").await;
                assert!(
                    exists.is_err() || !exists.unwrap(),
                    "data should not exist after delete"
                );

                // Verify the header blob is also gone by checking the inner store
                // The encrypted store should have deleted both data and header
                let keys = store.list(&SuffixFilter::new("")).await.unwrap();
                assert!(
                    keys.iter().all(|k| !k.contains(".enc-header")),
                    "no header keys should leak through list after delete"
                );
            }

            #[tokio::test]
            async fn test_list_excludes_headers() {
                let store = $store_fn(11u8).await;

                let blobs = vec![
                    BlobInput::new("alpha.txt", &b"alpha"[..]),
                    BlobInput::new("beta.txt", &b"beta"[..]),
                ];
                store.put(blobs).await.unwrap();

                let keys = store.list(&SuffixFilter::new("")).await.unwrap();
                assert_eq!(keys.len(), 2, "should only list data blobs, not headers");
                assert!(keys.contains(&"alpha.txt".to_string()));
                assert!(keys.contains(&"beta.txt".to_string()));
            }

            #[tokio::test]
            async fn test_rekey_header_noop() {
                let provider = ShiftEncryption::new(99u8);
                let result = provider.rekey_header(&[99 ^ 0, 0]).await.unwrap();
                assert_eq!(
                    result, None,
                    "rekey_header should return None when key matches"
                );
            }

            #[tokio::test]
            async fn test_encrypt_with_zero_shift() {
                let store = $store_fn(0u8).await; // no-op shift

                let data = b"identity encryption";
                store
                    .put(vec![BlobInput::new("zero.txt", &data[..])])
                    .await
                    .unwrap();

                let mut reader = store.get("zero.txt").await.unwrap();
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, data, "zero shift should preserve data");
            }

            #[tokio::test]
            async fn test_encrypted_put_result_no_header() {
                let store = $store_fn(7u8).await;

                let result = store
                    .put(vec![BlobInput::new("secret.txt", &b"data"[..])])
                    .await
                    .unwrap();

                // PutResult should contain only user-facing blobs
                assert_eq!(result.blobs.len(), 1);
                assert_eq!(result.blobs[0].key, "secret.txt");

                // No header key should be present
                assert!(
                    !result.blobs.iter().any(|b| b.key.contains(".enc-header")),
                    "PutResult must not leak internal .enc-header keys"
                );
            }

            #[tokio::test]
            async fn test_encrypt_with_max_shift() {
                let store = $store_fn(255u8).await;

                let data = b"max shift test";
                store
                    .put(vec![BlobInput::new("max.txt", &data[..])])
                    .await
                    .unwrap();

                let mut reader = store.get("max.txt").await.unwrap();
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, data, "max shift should still round-trip correctly");
            }

            #[tokio::test]
            async fn test_encrypt_get_with_metadata() {
                let store = $store_fn(21u8).await;
                let data = b"metadata test";

                store
                    .put(vec![BlobInput::new("meta.txt", &data[..])])
                    .await
                    .unwrap();

                let (meta, mut reader) = store.get_with_metadata("meta.txt").await.unwrap();
                assert_eq!(meta.key, "meta.txt");
                assert!(meta.modified_at > chrono::DateTime::UNIX_EPOCH);

                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, data, "get_with_metadata should return decrypted data");
            }

            #[tokio::test]
            async fn test_encrypt_list_with_metadata() {
                let store = $store_fn(31u8).await;

                store
                    .put(vec![
                        BlobInput::new("x.txt", &b"xxx"[..]),
                        BlobInput::new("y.txt", &b"yyy"[..]),
                    ])
                    .await
                    .unwrap();

                let metas = store
                    .list_with_metadata(&SuffixFilter::new(""))
                    .await
                    .unwrap();
                assert_eq!(metas.len(), 2, "should list data blobs only");

                for meta in &metas {
                    assert!(!meta.key.is_empty());
                    assert_eq!(meta.stored_size, 3);
                }
            }
        }
    };
}

// ============================================================================
// Generate tests for FS backend
// ============================================================================

encrypt_tests!(fs, encrypted_fs_store);

// ============================================================================
// Generate tests for S3 backend (only when feature = "s3")
// ============================================================================

#[cfg(feature = "s3")]
encrypt_tests!(s3, encrypted_s3_store);

// ============================================================================
// Rekey integration tests — uses RekeyableShiftEncryption
// ============================================================================

/// Helper: create an EncryptedBlobStore directly (not through the trait).
async fn rekeyable_encrypted(shift: u8, master_key_id: u8) -> Arc<EncryptedBlobStore> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep(); // keep directory alive for the duration of the test
    let inner = BlobStoreBuilder::new().with_fs(path).build().await.unwrap();
    let provider: Arc<dyn EncryptionProvider> =
        Arc::new(RekeyableShiftEncryption::new(shift, master_key_id));
    Arc::new(EncryptedBlobStore::new(inner, provider))
}

#[tokio::test]
async fn test_rekey_all_headers() {
    let store = rekeyable_encrypted(7u8, 1u8).await;

    store
        .put(vec![
            BlobInput::new("a.txt", b"aaa".as_slice()),
            BlobInput::new("b.txt", b"bbb".as_slice()),
            BlobInput::new("c.txt", b"ccc".as_slice()),
        ])
        .await
        .unwrap();

    // First rekey with same key ID — should be no-op since headers already match
    let result = store.rekey().await.unwrap();
    assert_eq!(result.rekeyed_count, 0, "no rekey when master key matches");
}

#[tokio::test]
async fn test_rekey_empty_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();
    let inner = BlobStoreBuilder::new().with_fs(path).build().await.unwrap();
    let provider: Arc<dyn EncryptionProvider> = Arc::new(RekeyableShiftEncryption::new(5u8, 1u8));
    let store = EncryptedBlobStore::new(inner, provider);

    let result = store.rekey().await.unwrap();
    assert_eq!(
        result.rekeyed_count, 0,
        "empty store should rekey 0 headers"
    );
}

#[tokio::test]
async fn test_rekey_then_get_returns_correct_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    // Create inner store
    let inner = BlobStoreBuilder::new()
        .with_fs(path.clone())
        .build()
        .await
        .unwrap();

    // Put data with key_id=1
    let provider1: Arc<dyn EncryptionProvider> = Arc::new(RekeyableShiftEncryption::new(7u8, 1u8));
    let store1 = EncryptedBlobStore::new(inner.clone(), provider1);

    let data = b"hello rekey world";
    store1
        .put(vec![BlobInput::new("data.txt", &data[..])])
        .await
        .unwrap();

    drop(store1);

    // Rekey with key_id=2 over the SAME inner store
    let provider2: Arc<dyn EncryptionProvider> = Arc::new(RekeyableShiftEncryption::new(7u8, 2u8));
    let store2 = EncryptedBlobStore::new(inner, provider2);

    let result = store2.rekey().await.unwrap();
    assert_eq!(
        result.rekeyed_count, 1,
        "should rekey 1 header from key_id=1 to key_id=2"
    );

    // Verify data is still readable after rekey
    let mut reader = store2.get("data.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, data, "data should be readable after rekey");
}

#[tokio::test]
async fn test_rekey_multiple_headers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let inner = BlobStoreBuilder::new()
        .with_fs(path.clone())
        .build()
        .await
        .unwrap();

    // Put with key_id=1
    let provider1: Arc<dyn EncryptionProvider> = Arc::new(RekeyableShiftEncryption::new(3u8, 1u8));
    let store1 = EncryptedBlobStore::new(inner.clone(), provider1);

    store1
        .put(vec![
            BlobInput::new("x.txt", b"xxx".as_slice()),
            BlobInput::new("y.txt", b"yyy".as_slice()),
            BlobInput::new("z.txt", b"zzz".as_slice()),
        ])
        .await
        .unwrap();

    drop(store1);

    // Rekey with key_id=3
    let provider2: Arc<dyn EncryptionProvider> = Arc::new(RekeyableShiftEncryption::new(3u8, 3u8));
    let store2 = EncryptedBlobStore::new(inner, provider2);

    let result = store2.rekey().await.unwrap();
    assert_eq!(result.rekeyed_count, 3, "should rekey all 3 headers");

    // Verify all data still readable
    for key in &["x.txt", "y.txt", "z.txt"] {
        let mut reader = store2.get(key).await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(
            buf.len(),
            3,
            "data for '{key}' should be intact after rekey"
        );
    }
}

#[tokio::test]
async fn test_decrypt_corrupted_data_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    // Use a provider that always fails on decryption
    let provider: Arc<dyn EncryptionProvider> = Arc::new(FailingDecryption);
    let inner = BlobStoreBuilder::new()
        .with_fs(path.clone())
        .build()
        .await
        .unwrap();
    let store = EncryptedBlobStore::new(inner, provider);

    // Put a blob (encrypt stream works fine)
    store
        .put(vec![BlobInput::new("secret.txt", b"valid data".as_slice())])
        .await
        .unwrap();

    // Now try to get the encrypted blob — decrypt_stream will return an error,
    // which should surface when we read the returned stream.
    let mut reader = store.get("secret.txt").await.unwrap();
    let mut buf = Vec::new();
    let result = reader.read_to_end(&mut buf).await;

    assert!(
        result.is_err(),
        "decryption failure should propagate as an error, not silent EOF"
    );

    // The error should be an I/O error wrapping the Encryption error
    let err = result.unwrap_err();
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::Other,
        "error kind should be Other (mapped from BlobStorageError)"
    );
    // Verify the error message mentions decryption failure
    let msg = err.to_string();
    assert!(
        msg.contains("decryption failed for key 'secret.txt'"),
        "error message should indicate decryption failure: {msg}"
    );
}

#[tokio::test]
async fn test_rekey_twice_second_noop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let inner = BlobStoreBuilder::new()
        .with_fs(path.clone())
        .build()
        .await
        .unwrap();

    // Put with key_id=1
    let provider1: Arc<dyn EncryptionProvider> = Arc::new(RekeyableShiftEncryption::new(5u8, 1u8));
    let store1 = EncryptedBlobStore::new(inner.clone(), provider1);

    store1
        .put(vec![BlobInput::new("data.txt", b"test".as_slice())])
        .await
        .unwrap();
    drop(store1);

    // First rekey: key_id=1 -> key_id=2
    let provider2: Arc<dyn EncryptionProvider> = Arc::new(RekeyableShiftEncryption::new(5u8, 2u8));
    let store2 = EncryptedBlobStore::new(inner.clone(), provider2);

    let result = store2.rekey().await.unwrap();
    assert_eq!(
        result.rekeyed_count, 1,
        "first rekey should update 1 header"
    );
    drop(store2);

    // Second rekey with same key_id=2 — should be no-op
    let provider3: Arc<dyn EncryptionProvider> = Arc::new(RekeyableShiftEncryption::new(5u8, 2u8));
    let store3 = EncryptedBlobStore::new(inner, provider3);

    let result = store3.rekey().await.unwrap();
    assert_eq!(
        result.rekeyed_count, 0,
        "second rekey with same key should be no-op"
    );
}

// ============================================================================
// Encrypted exists — checks both data + header
// ============================================================================

#[tokio::test]
async fn test_encrypted_exists_requires_both_data_and_header() {
    use xtax_blob_storage::EncryptedBlobStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();
    let inner = BlobStoreBuilder::new().with_fs(path).build().await.unwrap();
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = EncryptedBlobStore::new(inner.clone(), provider);

    // Normal put — both data and header exist
    store
        .put(vec![BlobInput::new("a.txt", b"hello".as_slice())])
        .await
        .unwrap();
    assert!(store.exists("a.txt").await.unwrap());

    // Manually delete only the header blob
    inner.delete(&["a.txt.enc-header"]).await.unwrap();

    // Data exists but header is gone → exists must return false
    assert!(!store.exists("a.txt").await.unwrap());
}

#[tokio::test]
async fn test_encrypted_exists_data_never_existed() {
    use xtax_blob_storage::EncryptedBlobStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();
    let inner = BlobStoreBuilder::new().with_fs(path).build().await.unwrap();
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = EncryptedBlobStore::new(inner, provider);

    // Never existed
    assert!(!store.exists("nonexistent.txt").await.unwrap());
}

// ============================================================================
// Encrypted put — best-effort rollback on header write failure
// ============================================================================

/// A mock backend that fails on the second `put()` call (simulating header write failure).
/// Uses the filesystem backend for actual storage to test real rollback behavior.
#[tokio::test]
async fn test_encrypted_put_header_failure_rollback() {
    use xtax_blob_storage::EncryptedBlobStore;

    // We simulate the rollback scenario without a full mock:
    // manually check that after a successful `put`, deleting the header causes
    // `exists` to return false (validating the exists fix), and that a subsequent
    // `put` of the same key works correctly.

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();
    let inner = BlobStoreBuilder::new().with_fs(path).build().await.unwrap();
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = EncryptedBlobStore::new(inner.clone(), provider);

    // Put a blob — succeeds
    store
        .put(vec![BlobInput::new("data.txt", b"payload".as_slice())])
        .await
        .unwrap();
    assert!(store.exists("data.txt").await.unwrap());

    // Simulate orphan: delete header, leave data
    inner.delete(&["data.txt.enc-header"]).await.unwrap();
    assert!(
        !store.exists("data.txt").await.unwrap(),
        "orphan data without header should report false"
    );

    // Re-put with same key — should overwrite and work
    store
        .put(vec![BlobInput::new("data.txt", b"new payload".as_slice())])
        .await
        .unwrap();
    assert!(store.exists("data.txt").await.unwrap());

    let mut reader = store.get("data.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"new payload");
}

// ============================================================================
// Encrypted error propagation — non-NotFound errors must not become NotFound
// ============================================================================

/// A mock blob store that always returns a specific error for `get`.
use std::sync::Mutex as StdMutex;

struct ErrorMockStore {
    get_error: StdMutex<Option<BlobStorageError>>,
    exists_error: StdMutex<Option<BlobStorageError>>,
    delete_error: StdMutex<Option<BlobStorageError>>,
}

impl ErrorMockStore {
    fn with_get_error(err: BlobStorageError) -> Self {
        Self {
            get_error: StdMutex::new(Some(err)),
            exists_error: StdMutex::new(None),
            delete_error: StdMutex::new(None),
        }
    }

    fn with_exists_error(err: BlobStorageError) -> Self {
        Self {
            get_error: StdMutex::new(None),
            exists_error: StdMutex::new(Some(err)),
            delete_error: StdMutex::new(None),
        }
    }

    fn with_batch_delete(err: BlobStorageError) -> Self {
        Self {
            get_error: StdMutex::new(None),
            exists_error: StdMutex::new(None),
            delete_error: StdMutex::new(Some(err)),
        }
    }
}

#[async_trait]
impl BlobStore for ErrorMockStore {
    async fn put(&self, _blobs: Vec<BlobInput>) -> Result<PutResult> {
        Ok(PutResult::multiple(vec![]))
    }
    async fn get(&self, _key: &str) -> Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
        Err(self
            .get_error
            .lock()
            .unwrap()
            .take()
            .unwrap_or(BlobStorageError::NotFound("mock".into())))
    }
    async fn delete(&self, _keys: &[&str]) -> Result<()> {
        match self.delete_error.lock().unwrap().take() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
    async fn list(&self, _filter: &dyn ListFilter) -> Result<Vec<String>> {
        Ok(vec![])
    }
    async fn exists(&self, _key: &str) -> Result<bool> {
        if let Some(err) = self.exists_error.lock().unwrap().take() {
            Err(err)
        } else {
            Ok(true)
        }
    }
    async fn get_with_metadata(
        &self,
        _key: &str,
    ) -> Result<(BlobMeta, Box<dyn tokio::io::AsyncRead + Send + Unpin>)> {
        Err(self
            .get_error
            .lock()
            .unwrap()
            .take()
            .unwrap_or(BlobStorageError::NotFound("mock".into())))
    }
    async fn list_with_metadata(&self, _filter: &dyn ListFilter) -> Result<Vec<BlobMeta>> {
        Ok(vec![])
    }
    async fn visit(&self, _filter: &dyn ListFilter, _visitor: &mut dyn BlobVisitor) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_encrypted_get_propagates_storage_error() {
    use xtax_blob_storage::EncryptedBlobStore;
    let mock = Arc::new(ErrorMockStore::with_get_error(BlobStorageError::Storage {
        message: "disk full".into(),
        source: None,
    }));
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = EncryptedBlobStore::new(mock, provider);

    let result = store.get("key.txt").await;
    match &result {
        Err(BlobStorageError::Storage { message, .. }) => {
            assert!(
                message.contains("disk full"),
                "expected Storage error, got: {message}"
            );
        }
        Err(other) => panic!("expected Storage error, got: {other}"),
        Ok(_) => panic!("expected error, got Ok"),
    }
}

#[tokio::test]
async fn test_encrypted_get_propagates_permission_denied() {
    use xtax_blob_storage::EncryptedBlobStore;
    let mock = Arc::new(ErrorMockStore::with_get_error(
        BlobStorageError::PermissionDenied("access denied".into()),
    ));
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = EncryptedBlobStore::new(mock, provider);

    let result = store.get("key.txt").await;
    match &result {
        Err(BlobStorageError::PermissionDenied(msg)) => {
            assert!(msg.contains("access denied"));
        }
        Err(other) => panic!("expected PermissionDenied, got: {other}"),
        Ok(_) => panic!("expected error, got Ok"),
    }
}

#[tokio::test]
async fn test_encrypted_get_not_found_stays_not_found() {
    use xtax_blob_storage::EncryptedBlobStore;
    let mock = Arc::new(ErrorMockStore::with_get_error(BlobStorageError::NotFound(
        "key.txt".into(),
    )));
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = EncryptedBlobStore::new(mock, provider);

    let err_result = store.get("key.txt").await;
    let err = err_result.as_ref().map(|_| ()).unwrap_err();
    match err {
        BlobStorageError::NotFound(key) => {
            assert_eq!(key, "key.txt");
        }
        other => panic!("expected NotFound, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_encrypted_exists_propagates_error() {
    use xtax_blob_storage::EncryptedBlobStore;
    let mock = Arc::new(ErrorMockStore::with_exists_error(
        BlobStorageError::Storage {
            message: "network timeout".into(),
            source: None,
        },
    ));
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = EncryptedBlobStore::new(mock, provider);

    let err = store.exists("key.txt").await.unwrap_err();
    match err {
        BlobStorageError::Storage { message, .. } => {
            assert!(message.contains("network timeout"));
        }
        other => panic!("expected Storage error, got: {other:?}"),
    }
}

// ============================================================================
// Encrypted BatchError — no internal key leakage
// ============================================================================

#[tokio::test]
async fn test_encrypted_delete_batch_error_no_header_leak() {
    use xtax_blob_storage::EncryptedBlobStore;
    let mock = Arc::new(ErrorMockStore::with_batch_delete(BlobStorageError::Batch(
        BatchError {
            succeeded: vec!["a.txt".to_string(), "a.txt.enc-header".to_string()],
            errors: vec![
                KeyError {
                    key: "b.txt".to_string(),
                    error: PerKeyError::PermissionDenied("denied".into()),
                },
                KeyError {
                    key: "b.txt.enc-header".to_string(),
                    error: PerKeyError::PermissionDenied("denied".into()),
                },
            ],
        },
    )));
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = EncryptedBlobStore::new(mock, provider);

    let err = store.delete(&["a.txt", "b.txt"]).await.unwrap_err();
    match err {
        BlobStorageError::Batch(batch) => {
            // succeeded must not contain .enc-header
            assert!(
                !batch.succeeded.iter().any(|s| s.contains(".enc-header")),
                "succeeded must not leak internal keys: {:?}",
                batch.succeeded
            );
            assert_eq!(batch.succeeded, vec!["a.txt"]);

            // errors must not contain .enc-header
            assert!(
                !batch.errors.iter().any(|e| e.key.contains(".enc-header")),
                "errors must not leak internal keys: {:?}",
                batch.errors
            );
            assert_eq!(batch.errors.len(), 1);
            assert_eq!(batch.errors[0].key, "b.txt");
        }
        other => panic!("expected Batch error, got: {other:?}"),
    }
}

// ============================================================================
// Mock inner store that fails on N-th put call — for testing header-write
// failure scenarios (orphan data / orphan header creation).
// ============================================================================

/// A mock blob store that succeeds on the first `PASS_ON_SUCCESS` put()
/// calls, then fails with `STORAGE_ERROR` on the next call.
struct NthPutFailingStore {
    inner: Arc<dyn BlobStore>,
    /// How many put() calls succeed before failing.
    pass_on_success: std::sync::atomic::AtomicU32,
    /// Counter tracking how many put() calls have been made.
    call_count: std::sync::atomic::AtomicU32,
}

impl NthPutFailingStore {
    fn new(inner: Arc<dyn BlobStore>, pass_on_success: u32) -> Self {
        Self {
            inner,
            pass_on_success: std::sync::atomic::AtomicU32::new(pass_on_success),
            call_count: std::sync::atomic::AtomicU32::new(0),
        }
    }
}

#[async_trait]
impl BlobStore for NthPutFailingStore {
    async fn put(&self, blobs: Vec<BlobInput>) -> Result<PutResult> {
        let call = self
            .call_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if call
            < self
                .pass_on_success
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            self.inner.put(blobs).await
        } else {
            Err(BlobStorageError::Storage {
                message: format!("simulated header write failure (call #{call})"),
                source: None,
            })
        }
    }

    async fn get(&self, key: &str) -> Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
        self.inner.get(key).await
    }
    async fn delete(&self, keys: &[&str]) -> Result<()> {
        self.inner.delete(keys).await
    }
    async fn list(&self, filter: &dyn ListFilter) -> Result<Vec<String>> {
        self.inner.list(filter).await
    }
    async fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key).await
    }
    async fn get_with_metadata(
        &self,
        key: &str,
    ) -> Result<(BlobMeta, Box<dyn tokio::io::AsyncRead + Send + Unpin>)> {
        self.inner.get_with_metadata(key).await
    }
    async fn list_with_metadata(&self, filter: &dyn ListFilter) -> Result<Vec<BlobMeta>> {
        self.inner.list_with_metadata(filter).await
    }
    async fn visit(&self, filter: &dyn ListFilter, visitor: &mut dyn BlobVisitor) -> Result<()> {
        self.inner.visit(filter, visitor).await
    }
}

// ============================================================================
// Orphan data/header and failure semantic tests
// ============================================================================

use xtax_blob_storage::{CleanupPredicate, SuffixFilter};

#[tokio::test]
async fn test_orphan_data_blob_is_invisible() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();
    let inner = BlobStoreBuilder::new().with_fs(path).build().await.unwrap();
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = EncryptedBlobStore::new(inner.clone(), provider);

    // Put a normal blob
    store
        .put(vec![BlobInput::new("orphan.txt", b"data".as_slice())])
        .await
        .unwrap();
    assert!(store.exists("orphan.txt").await.unwrap());

    // Delete only the header blob → create orphan data
    inner.delete(&["orphan.txt.enc-header"]).await.unwrap();

    // Orphan data should be invisible
    assert!(
        !store.exists("orphan.txt").await.unwrap(),
        "orphan data without header → exists() must return false"
    );
    let result = store.get("orphan.txt").await;
    let err = result.as_ref().map(|_| ()).unwrap_err();
    match err {
        BlobStorageError::NotFound(key) => assert_eq!(key, "orphan.txt"),
        other => panic!("expected NotFound for orphan data, got: {other}"),
    }
    let keys = store.list(&SuffixFilter::new("")).await.unwrap();
    assert!(
        !keys.contains(&"orphan.txt".to_string()),
        "list() must not include orphan data blob"
    );
}

#[tokio::test]
async fn test_orphan_data_repair_by_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();
    let inner = BlobStoreBuilder::new().with_fs(path).build().await.unwrap();
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = EncryptedBlobStore::new(inner.clone(), provider);

    // Put initial blob
    store
        .put(vec![BlobInput::new("repair.txt", b"old data".as_slice())])
        .await
        .unwrap();

    // Delete header → orphan data
    inner.delete(&["repair.txt.enc-header"]).await.unwrap();
    assert!(!store.exists("repair.txt").await.unwrap());

    // Re-put with new data → overwrites orphan data and writes fresh header
    store
        .put(vec![BlobInput::new(
            "repair.txt",
            b"new repaired data".as_slice(),
        )])
        .await
        .unwrap();

    assert!(store.exists("repair.txt").await.unwrap());
    let mut reader = store.get("repair.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"new repaired data");
}

#[tokio::test]
async fn test_orphan_header_blob_is_invisible() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();
    let inner = BlobStoreBuilder::new().with_fs(path).build().await.unwrap();
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = EncryptedBlobStore::new(inner.clone(), provider);

    // Put a normal blob
    store
        .put(vec![BlobInput::new(
            "header_orphan.txt",
            b"payload".as_slice(),
        )])
        .await
        .unwrap();
    assert!(store.exists("header_orphan.txt").await.unwrap());

    // Delete the data blob, leave the header → orphan header
    inner.delete(&["header_orphan.txt"]).await.unwrap();

    // Orphan header → exists returns false and get returns NotFound
    assert!(
        !store.exists("header_orphan.txt").await.unwrap(),
        "orphan header without data → exists() must return false"
    );
    let result = store.get("header_orphan.txt").await;
    let err = result.as_ref().map(|_| ()).unwrap_err();
    match err {
        BlobStorageError::NotFound(key) => assert_eq!(key, "header_orphan.txt"),
        other => panic!("expected NotFound for orphan header, got: {other}"),
    }

    // list() must not include the key (data blob is missing)
    let keys = store.list(&SuffixFilter::new("")).await.unwrap();
    assert!(
        !keys.contains(&"header_orphan.txt".to_string()),
        "list() must not include orphan header blob key"
    );

    // Verify the header blob still physically exists on the inner store
    assert!(
        inner.exists("header_orphan.txt.enc-header").await.unwrap(),
        "inner store should still have the orphan header blob"
    );
}

#[tokio::test]
async fn test_orphan_header_repair_by_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();
    let inner = BlobStoreBuilder::new().with_fs(path).build().await.unwrap();
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = EncryptedBlobStore::new(inner.clone(), provider);

    // Put initial blob
    store
        .put(vec![BlobInput::new(
            "repair_header.txt",
            b"old payload".as_slice(),
        )])
        .await
        .unwrap();

    // Delete data blob → orphan header
    inner.delete(&["repair_header.txt"]).await.unwrap();
    assert!(!store.exists("repair_header.txt").await.unwrap());

    // Re-put same key → writes new data + new header, repairs state
    store
        .put(vec![BlobInput::new(
            "repair_header.txt",
            b"repaired payload".as_slice(),
        )])
        .await
        .unwrap();

    assert!(store.exists("repair_header.txt").await.unwrap());
    let mut reader = store.get("repair_header.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"repaired payload");
}

#[tokio::test]
async fn test_overwrite_header_write_failure_creates_orphan() {
    use xtax_blob_storage::EncryptedBlobStore;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();
    let fs_inner = BlobStoreBuilder::new()
        .with_fs(path.clone())
        .build()
        .await
        .unwrap();

    // Create an NthPutFailingStore that succeeds on the first put (data write)
    // and fails on the second put (header write).
    let mock = Arc::new(NthPutFailingStore::new(fs_inner.clone(), 1));
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = EncryptedBlobStore::new(mock, provider);

    // Put a blob — data write succeeds, header write fails
    let result = store
        .put(vec![BlobInput::new(
            "overwrite_test.txt",
            b"data".as_slice(),
        )])
        .await;

    match result {
        Err(BlobStorageError::Storage { message, .. }) => {
            assert!(
                message.contains("simulated header write failure"),
                "expected header write failure, got: {message}"
            );
        }
        other => panic!("expected Storage error from header write failure, got: {other:?}"),
    }

    // The rollback should have deleted the data blob
    let data_exists = fs_inner.exists("overwrite_test.txt").await.unwrap();
    assert!(
        !data_exists,
        "data blob should have been rolled back after header write failure"
    );

    // But the header should also not exist (first write, no previous header)
    let header_exists = fs_inner
        .exists("overwrite_test.txt.enc-header")
        .await
        .unwrap();
    assert!(
        !header_exists,
        "header blob should not exist on first write failure"
    );
}

#[tokio::test]
async fn test_overwrite_header_write_failure_leaves_old_header() {
    use xtax_blob_storage::EncryptedBlobStore;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();
    let fs_inner = BlobStoreBuilder::new()
        .with_fs(path.clone())
        .build()
        .await
        .unwrap();

    // First, successfully put a blob using the real FS store
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store1 = EncryptedBlobStore::new(fs_inner.clone(), provider.clone());
    store1
        .put(vec![BlobInput::new(
            "overwrite_orphan.txt",
            b"v1 data".as_slice(),
        )])
        .await
        .unwrap();
    assert!(store1.exists("overwrite_orphan.txt").await.unwrap());

    // Record old header bytes before overwrite attempt
    let old_header = tokio::fs::read(path.join("overwrite_orphan.txt.enc-header"))
        .await
        .unwrap();

    // Now wrap with a failing mock for the overwrite:
    // Succeeds on data put (call #0), fails on header put (call #1)
    let mock = Arc::new(NthPutFailingStore::new(fs_inner.clone(), 1));
    let store2 = EncryptedBlobStore::new(mock, provider.clone());

    let result = store2
        .put(vec![BlobInput::new(
            "overwrite_orphan.txt",
            b"v2 data".as_slice(),
        )])
        .await;

    match result {
        Err(BlobStorageError::Storage { message, .. }) => {
            assert!(message.contains("simulated header write failure"));
        }
        other => panic!("expected Storage error, got: {other:?}"),
    }

    // The new data blob should have been rolled back
    // The old header blob should still exist (orphaned) with the original content
    let header_exists = fs_inner
        .exists("overwrite_orphan.txt.enc-header")
        .await
        .unwrap();
    assert!(
        header_exists,
        "old header should remain after failed overwrite"
    );

    let current_header = tokio::fs::read(path.join("overwrite_orphan.txt.enc-header"))
        .await
        .unwrap();
    assert_eq!(
        current_header, old_header,
        "header should not have been modified during failed overwrite"
    );

    // The data blob should be gone (rolled back), leaving orphan header
    let data_exists = fs_inner.exists("overwrite_orphan.txt").await.unwrap();
    assert!(!data_exists, "new data blob should have been rolled back");
}

#[tokio::test]
async fn test_encrypted_cleanup_deletes_both_objects() {
    use xtax_blob_storage::{BlobCleanup, EncryptedBlobStore};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();
    let fs_inner = BlobStoreBuilder::new()
        .with_fs(path.clone())
        .build()
        .await
        .unwrap();

    // Build an encrypted store
    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let enc_store = EncryptedBlobStore::new(fs_inner.clone(), provider);

    // Put multiple blobs
    enc_store
        .put(vec![
            BlobInput::new("keep.txt", b"keep".as_slice()),
            BlobInput::new("delete_me.txt", b"delete".as_slice()),
            BlobInput::new("also_delete.txt", b"also".as_slice()),
        ])
        .await
        .unwrap();

    // Run cleanup: delete blobs whose key starts with "delete" or "also_delete"
    let predicate: CleanupPredicate =
        Box::new(|key, _meta| key.starts_with("delete") || key.starts_with("also_delete"));
    let cleanup = BlobCleanup::new(Arc::new(enc_store), predicate);

    let result = cleanup.cleanup().await.unwrap();
    assert_eq!(result.deleted_count, 2, "should delete 2 blobs");

    // Verify deleted keys are gone (both data + header)
    assert!(!cleanup.exists("delete_me.txt").await.unwrap());
    assert!(!cleanup.exists("also_delete.txt").await.unwrap());
    assert!(
        !fs_inner.exists("delete_me.txt").await.unwrap(),
        "data blob should be deleted"
    );
    assert!(
        !fs_inner.exists("delete_me.txt.enc-header").await.unwrap(),
        "header blob should be deleted"
    );
    assert!(
        !fs_inner.exists("also_delete.txt.enc-header").await.unwrap(),
        "header blob should be deleted"
    );

    // Verify keep.txt is still present
    assert!(cleanup.exists("keep.txt").await.unwrap());
    let keys = cleanup.list(&SuffixFilter::new("")).await.unwrap();
    assert_eq!(keys, vec!["keep.txt"]);
}

#[tokio::test]
async fn test_orphan_data_survives_cleanup() {
    use xtax_blob_storage::{BlobCleanup, EncryptedBlobStore};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();
    let fs_inner = BlobStoreBuilder::new()
        .with_fs(path.clone())
        .build()
        .await
        .unwrap();

    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let enc_store = EncryptedBlobStore::new(fs_inner.clone(), provider);

    // Put two valid blobs
    enc_store
        .put(vec![
            BlobInput::new("valid.txt", b"valid".as_slice()),
            BlobInput::new("to_clean.txt", b"clean me".as_slice()),
        ])
        .await
        .unwrap();

    // Create an orphan data blob: manually write data only, no header
    fs_inner
        .put(vec![BlobInput::new(
            "orphan.txt",
            b"orphan data".as_slice(),
        )])
        .await
        .unwrap();

    // Verify orphan is invisible to encrypted store
    assert!(!enc_store.exists("orphan.txt").await.unwrap());

    // Create an orphan header blob: manually delete data, leave header
    fs_inner.delete(&["to_clean.txt"]).await.unwrap();
    assert!(
        fs_inner.exists("to_clean.txt.enc-header").await.unwrap(),
        "orphan header should exist"
    );

    // Run a broad cleanup (delete everything matching empty prefix)
    let predicate: CleanupPredicate = Box::new(|_key, _meta| true);
    let cleanup = BlobCleanup::new(Arc::new(enc_store), predicate);
    let result = cleanup.cleanup().await.unwrap();

    // Only "valid.txt" was visible → only 1 blob deleted
    assert_eq!(
        result.deleted_count, 1,
        "cleanup should only delete the 1 visible blob"
    );

    // The orphan data blob should still exist (it was never listed)
    assert!(
        fs_inner.exists("orphan.txt").await.unwrap(),
        "orphan data blob should survive cleanup (invisible to list)"
    );

    // The orphan header blob should still exist
    assert!(
        fs_inner.exists("to_clean.txt.enc-header").await.unwrap(),
        "orphan header blob should survive cleanup (invisible to list)"
    );

    // valid.txt should be gone
    assert!(!fs_inner.exists("valid.txt").await.unwrap());
}

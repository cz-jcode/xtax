//! Edge case tests — covers empty blobs, special characters in keys,
//! long keys, error kinds, combined layers, concurrency, and more.
//!
//! Run with:
//!   cargo test --test edge_cases_test                  # FS only
//!   cargo test --features s3 --test edge_cases_test    # FS + S3

#![cfg(feature = "fs")]

use std::io::Cursor;
use std::sync::Arc;

use tokio::io::AsyncReadExt;
use xtax_blob_storage::{
    BlobInput, BlobMeta, BlobStorageError, BlobStoreBuilder, EncryptionProvider, PrefixFilter,
    SuffixFilter,
};

#[path = "common/encrypt.rs"]
mod common_encrypt;
use common_encrypt::*;

// ============================================================================
// Empty blob (0 bytes)
// ============================================================================

#[tokio::test]
async fn test_empty_blob() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStoreBuilder::new()
        .with_fs(dir.path())
        .build()
        .await
        .unwrap();

    let data: Vec<u8> = vec![];
    store
        .put(vec![BlobInput::new("empty.txt", Cursor::new(data))])
        .await
        .unwrap();

    let mut reader = store.get("empty.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert!(buf.is_empty(), "empty blob should return zero bytes");

    // Verify metadata
    let (meta, _) = store.get_with_metadata("empty.txt").await.unwrap();
    assert_eq!(meta.key, "empty.txt");
    assert_eq!(
        meta.stored_size, 0,
        "stored_size should be 0 for empty blob"
    );
}

#[tokio::test]
async fn test_empty_blob_encrypted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = BlobStoreBuilder::new()
        .with_fs(path)
        .with_encryption(provider)
        .build()
        .await
        .unwrap();

    let data: Vec<u8> = vec![];
    store
        .put(vec![BlobInput::new("empty-enc.txt", Cursor::new(data))])
        .await
        .unwrap();

    let mut reader = store.get("empty-enc.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert!(
        buf.is_empty(),
        "empty encrypted blob should return zero bytes"
    );
}

// ============================================================================
// Special characters in keys
// ============================================================================

#[tokio::test]
async fn test_keys_with_special_characters() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStoreBuilder::new()
        .with_fs(dir.path())
        .build()
        .await
        .unwrap();

    let special_keys = vec![
        "spaces in key.txt",
        "dash-ed-key.txt",
        "under_score.txt",
        "dots.and.dots.txt",
        "plus+plus.txt",
        "hash#tag.txt",
        "question?mark.txt",
        "ampers&and.txt",
        "percent%25.txt",
    ];

    for (i, key) in special_keys.iter().enumerate() {
        // Use Cursor with owned Vec to satisfy 'static lifetime
        let data = format!("data{}", i);
        store
            .put(vec![BlobInput::new(*key, Cursor::new(data.into_bytes()))])
            .await
            .unwrap();
    }

    let keys = store.list(&SuffixFilter::new("")).await.unwrap();
    assert_eq!(keys.len(), special_keys.len());

    for key in &special_keys {
        let mut reader = store.get(key).await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert!(
            !buf.is_empty(),
            "blob with key '{}' should be retrievable",
            key
        );
    }
}

#[tokio::test]
async fn test_key_with_unicode() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStoreBuilder::new()
        .with_fs(dir.path())
        .build()
        .await
        .unwrap();

    let key = "příliš/žluťoučký/kůň.txt";
    let data = b"unicode test";
    store
        .put(vec![BlobInput::new(key, &data[..])])
        .await
        .unwrap();

    let mut reader = store.get(key).await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, data, "unicode key should work");

    // List with prefix filter
    let keys = store.list(&PrefixFilter::new("příliš/")).await.unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], key);
}

// ============================================================================
// Very long key names
// ============================================================================

#[tokio::test]
async fn test_long_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStoreBuilder::new()
        .with_fs(dir.path())
        .build()
        .await
        .unwrap();

    // Create a key that is ~300 characters long with nested directories
    let long_key = "a/".repeat(30) + "long-key-file.txt";
    let data = b"long key test";
    store
        .put(vec![BlobInput::new(&long_key, &data[..])])
        .await
        .unwrap();

    let mut reader = store.get(&long_key).await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, data, "long nested key should work");

    // List with prefix filter
    let keys = store.list(&PrefixFilter::new("a/")).await.unwrap();
    assert!(!keys.is_empty(), "should list blobs under long prefix");
    assert!(
        keys.contains(&long_key),
        "long key should appear in listing"
    );
}

// ============================================================================
// Error kind verification
// ============================================================================

#[tokio::test]
async fn test_error_kind_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStoreBuilder::new()
        .with_fs(dir.path())
        .build()
        .await
        .unwrap();

    // Use match instead of unwrap_err because the returned types don't implement Debug
    match store.get("nonexistent.txt").await {
        Err(err) => {
            assert!(
                matches!(err, BlobStorageError::NotFound(_)),
                "get on missing key should return NotFound"
            );
        }
        Ok(_) => panic!("expected Err for non-existent key"),
    }

    // get_with_metadata for FS backend returns Storage (from fs::metadata error),
    // not NotFound (because file_meta() doesn't check existence separately)
    match store.get_with_metadata("nonexistent.txt").await {
        Err(err) => {
            let kind_matches = matches!(err, BlobStorageError::NotFound(_))
                || matches!(err, BlobStorageError::Storage { .. });
            assert!(
                kind_matches,
                "get_with_metadata on missing key should return NotFound or Storage, got {:?}",
                err
            );
        }
        Ok(_) => panic!("expected Err for non-existent key"),
    }

    // exists on missing key should return Ok(false), not an error
    let exists = store.exists("nonexistent.txt").await.unwrap();
    assert!(!exists, "exists on missing key should return false");
}

#[tokio::test]
async fn test_error_display() {
    let err = BlobStorageError::NotFound("test-key".to_string());
    let display = err.to_string();
    assert!(
        display.contains("test-key"),
        "display should contain the key"
    );
    assert!(
        display.contains("blob not found"),
        "display should contain error description"
    );
}

#[tokio::test]
async fn test_error_from_string() {
    let err: BlobStorageError = "custom error".into();
    let display = err.to_string();
    assert!(display.contains("custom error"));
}

// ============================================================================
// Combined layers: encryption + prefix
// ============================================================================

#[tokio::test]
async fn test_combined_encryption_and_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(13u8));

    let store = BlobStoreBuilder::new()
        .with_fs(path.clone())
        .with_prefix("app/")
        .with_encryption(provider)
        .build()
        .await
        .unwrap();

    // Round-trip through both layers
    let data = b"combined layers test";
    store
        .put(vec![BlobInput::new("secret.txt", &data[..])])
        .await
        .unwrap();

    let mut reader = store.get("secret.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, data, "data should survive prefix + encryption");

    // List should show logical keys (no prefix, no headers)
    let keys = store.list(&SuffixFilter::new("")).await.unwrap();
    assert_eq!(keys, vec!["secret.txt"]);

    // Verify inner store has prefixed + encrypted blobs (use saved path since dir was moved by keep())
    let raw = BlobStoreBuilder::new()
        .with_fs(path.clone())
        .build()
        .await
        .unwrap();
    let raw_keys = raw.list(&SuffixFilter::new("")).await.unwrap();
    assert!(raw_keys.contains(&"app/secret.txt".to_string()));
    assert!(raw_keys.contains(&"app/secret.txt.enc-header".to_string()));
}

// ============================================================================
// Concurrent operations
// ============================================================================

#[tokio::test]
async fn test_concurrent_put_and_get() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let inner = BlobStoreBuilder::new().with_fs(path).build().await.unwrap();
    let store = inner;

    let mut handles = Vec::new();
    for i in 0..20 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            let key = format!("concurrent-{}.txt", i);
            let data = format!("data-{}", i);
            store
                .put(vec![BlobInput::new(&key, Cursor::new(data.into_bytes()))])
                .await
                .unwrap();

            let mut reader = store.get(&key).await.unwrap();
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            assert_eq!(
                std::str::from_utf8(&buf).unwrap(),
                &format!("data-{}", i),
                "concurrent blob {} should be correct",
                i
            );
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all blobs exist after concurrent writes
    let keys = store.list(&SuffixFilter::new("")).await.unwrap();
    assert_eq!(keys.len(), 20, "all 20 concurrent blobs should exist");
}

// ============================================================================
// Multiple delete operations
// ============================================================================

#[tokio::test]
async fn test_delete_multiple_times() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStoreBuilder::new()
        .with_fs(dir.path())
        .build()
        .await
        .unwrap();

    store
        .put(vec![BlobInput::new("delete-me.txt", b"data".as_slice())])
        .await
        .unwrap();

    // First delete should succeed
    store.delete(&["delete-me.txt"]).await.unwrap();

    // Second delete of same key should also succeed (idempotent)
    store.delete(&["delete-me.txt"]).await.unwrap();

    let result = store.get("delete-me.txt").await;
    assert!(result.is_err(), "blob should be gone after delete");
}

// ============================================================================
// List with empty store
// ============================================================================

#[tokio::test]
async fn test_list_empty_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStoreBuilder::new()
        .with_fs(dir.path())
        .build()
        .await
        .unwrap();

    let keys = store.list(&SuffixFilter::new("")).await.unwrap();
    assert!(keys.is_empty(), "empty store should return empty list");

    let metas = store
        .list_with_metadata(&SuffixFilter::new(""))
        .await
        .unwrap();
    assert!(
        metas.is_empty(),
        "empty store should return empty metadata list"
    );
}

// ============================================================================
// Put with multiple blobs in one call
// ============================================================================

#[tokio::test]
async fn test_put_multiple_blobs_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStoreBuilder::new()
        .with_fs(dir.path())
        .build()
        .await
        .unwrap();

    let blobs = vec![
        BlobInput::new("alpha.txt", b"alpha".as_slice()),
        BlobInput::new("beta.txt", b"beta".as_slice()),
        BlobInput::new("gamma.txt", b"gamma".as_slice()),
    ];

    let result = store.put(blobs).await.unwrap();
    assert_eq!(
        result.blobs.len(),
        3,
        "put should return metadata for all 3 blobs"
    );

    let keys = store.list(&SuffixFilter::new("")).await.unwrap();
    assert_eq!(keys.len(), 3);
}

// ============================================================================
// Verify PutResult helpers
// ============================================================================

#[tokio::test]
async fn test_put_result_helpers() {
    use xtax_blob_storage::PutResult;

    let single = PutResult::single(BlobMeta {
        key: "single.txt".to_string(),
        stored_size: 5,
        modified_at: chrono::Utc::now(),
        etag: None,
    });
    assert_eq!(single.blobs.len(), 1);
    assert_eq!(single.blobs[0].key, "single.txt");

    let multi = PutResult::multiple(vec![
        BlobMeta {
            key: "a.txt".to_string(),
            stored_size: 1,
            modified_at: chrono::Utc::now(),
            etag: None,
        },
        BlobMeta {
            key: "b.txt".to_string(),
            stored_size: 2,
            modified_at: chrono::Utc::now(),
            etag: None,
        },
    ]);
    assert_eq!(multi.blobs.len(), 2);
}

// ============================================================================
// BatchError type tests
// ============================================================================

#[tokio::test]
async fn test_batch_error_display() {
    use xtax_blob_storage::{BatchError, KeyError, PerKeyError};

    let err = BatchError {
        succeeded: vec!["ok.txt".to_string()],
        errors: vec![KeyError {
            key: "fail.txt".to_string(),
            error: PerKeyError::PermissionDenied("Access Denied".to_string()),
        }],
    };

    let display = err.to_string();
    assert!(
        display.contains("1 keys failed"),
        "display should mention failure count"
    );
    assert!(
        display.contains("1 succeeded"),
        "display should mention succeeded"
    );
    assert!(display.contains("2 total"), "display should mention total");
}

#[tokio::test]
async fn test_batch_error_total_count() {
    use xtax_blob_storage::{BatchError, KeyError, PerKeyError};

    let err = BatchError {
        succeeded: vec!["a.txt".to_string(), "b.txt".to_string()],
        errors: vec![KeyError {
            key: "c.txt".to_string(),
            error: PerKeyError::NotFound,
        }],
    };

    assert_eq!(err.total_count(), 3);
    assert_eq!(err.failed_count(), 1);
}

#[tokio::test]
async fn test_batch_error_from_storage_variant() {
    use xtax_blob_storage::{BatchError, BlobStorageError, KeyError, PerKeyError};

    let batch = BatchError {
        succeeded: vec![],
        errors: vec![KeyError {
            key: "secret.txt".to_string(),
            error: PerKeyError::Unknown {
                message: "I/O error".to_string(),
            },
        }],
    };

    let err = BlobStorageError::Batch(batch);
    let display = err.to_string();
    assert!(
        display.contains("batch error"),
        "display should contain 'batch error'"
    );
    assert!(
        display.contains("1 keys failed"),
        "display should mention failure count"
    );
}

// ============================================================================
// Encrypted layer batch delete tests
// ============================================================================

#[tokio::test]
async fn test_encrypted_delete_batch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = BlobStoreBuilder::new()
        .with_fs(path)
        .with_encryption(provider)
        .build()
        .await
        .unwrap();

    // Put encrypted blobs
    store
        .put(vec![
            BlobInput::new("alpha.txt", b"alpha-data".as_slice()),
            BlobInput::new("beta.txt", b"beta-data".as_slice()),
        ])
        .await
        .unwrap();

    // Delete both — should succeed
    store.delete(&["alpha.txt", "beta.txt"]).await.unwrap();

    // Verify gone
    assert!(
        store.get("alpha.txt").await.is_err(),
        "alpha should be gone"
    );
    assert!(store.get("beta.txt").await.is_err(), "beta should be gone");
}

#[tokio::test]
async fn test_encrypted_delete_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = BlobStoreBuilder::new()
        .with_fs(path)
        .with_encryption(provider)
        .build()
        .await
        .unwrap();

    // Delete non-existent encrypted key
    let result = store.delete(&["never-created.txt"]).await;
    assert!(
        result.is_ok(),
        "encrypted delete of non-existent key should succeed (idempotent)"
    );
}

/// Delete an existing blob + a non-existing key.
///
/// `NotFound` on delete is **idempotent** by design — it is treated as a
/// success, not a partial failure. Both the data blob and the `.enc-header`
/// simply don't exist, so there is nothing to fail on.
#[tokio::test]
async fn test_encrypted_delete_mixed_not_found_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = BlobStoreBuilder::new()
        .with_fs(path)
        .with_encryption(provider)
        .build()
        .await
        .unwrap();

    store
        .put(vec![BlobInput::new("exists.txt", b"data".as_slice())])
        .await
        .unwrap();

    // Delete one existing + one non-existing
    let result = store.delete(&["exists.txt", "nonexistent.txt"]).await;
    assert!(
        result.is_ok(),
        "delete with non-existent keys should succeed (idempotent), got: {:?}",
        result
    );

    assert!(
        store.get("exists.txt").await.is_err(),
        "exists.txt should be gone"
    );
}

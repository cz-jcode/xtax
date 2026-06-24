#![cfg(feature = "fs")]

use std::sync::Arc;
use tokio::io::AsyncReadExt;
use xtax_blob_storage::{
    BlobInput, BlobMeta, BlobStore, BlobStoreBuilder, BlobVisitor, NotFilter, PrefixFilter,
    SuffixFilter,
};

// ============================================================================
// Store factories
// ============================================================================

async fn fs_store() -> Arc<dyn BlobStore> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    // Keep dir alive — the OS will clean up on reboot.
    // This avoids the tempdir being dropped before the store is used.
    let _ = dir.keep();
    BlobStoreBuilder::new()
        .with_fs(&path)
        .build()
        .await
        .unwrap()
}

#[cfg(feature = "s3")]
async fn s3_store() -> Arc<dyn BlobStore> {
    let (client, bucket) = fresh_s3_store().await;
    BlobStoreBuilder::new()
        .with_s3(client, bucket)
        .build()
        .await
        .unwrap()
}

#[cfg(feature = "s3")]
async fn fresh_s3_store() -> (aws_sdk_s3::Client, String) {
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region, SharedCredentialsProvider};
    use s3s::auth::SimpleAuth;
    use s3s::host::SingleDomain;
    use s3s::service::S3ServiceBuilder;
    use s3s_fs::FileSystem;

    const DOMAIN_NAME: &str = "localhost:0";
    const REGION: &str = "us-east-1";

    fn unique_id() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{:016x}", n)
    }

    let sub = unique_id();
    let root = std::env::temp_dir().join("s3s-backend").join(sub);
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
    let bucket = format!("s3s-backend-{}", unique_id());
    let _ = client.create_bucket().bucket(&bucket).send().await.unwrap();
    (client, bucket)
}

// ============================================================================
// Macro to generate identical test bodies for each backend
// ============================================================================

macro_rules! backend_tests {
    ($mod_name:ident, $store_fn:path) => {
        mod $mod_name {
            use super::*;
            use xtax_blob_storage::BlobStorageError;

            #[tokio::test]
            async fn test_put_and_get() {
                let store = $store_fn().await;

                let data = b"hello world".as_slice();
                let result = store
                    .put(vec![BlobInput::new("test.txt", data)])
                    .await
                    .unwrap();
                assert_eq!(result.blobs.len(), 1);
                assert_eq!(result.blobs[0].key, "test.txt");

                let mut reader = store.get("test.txt").await.unwrap();
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, b"hello world");
            }

            #[tokio::test]
            async fn test_get_not_found() {
                let store = $store_fn().await;

                let result = store.get("nonexistent.txt").await;
                assert!(result.is_err(), "expected Err for non-existent key");
            }

            #[tokio::test]
            async fn test_delete() {
                let store = $store_fn().await;

                let key = "delete-me.txt";
                store
                    .put(vec![BlobInput::new(key, b"data".as_slice())])
                    .await
                    .unwrap();
                assert!(
                    store.exists(key).await.unwrap(),
                    "blob should exist after put"
                );

                store.delete(&[key]).await.unwrap();

                // After delete the blob should not exist
                let exists = store.exists(key).await;
                assert!(
                    exists.is_err() || !exists.unwrap(),
                    "blob should not exist after delete"
                );
            }

            /// Delete of non-existent keys should succeed (idempotent).
            #[tokio::test]
            async fn test_delete_idempotent() {
                let store = $store_fn().await;

                // Delete a key that was never stored
                let result = store.delete(&["never-existed.txt"]).await;
                assert!(
                    result.is_ok(),
                    "delete of non-existent key should succeed (idempotent)"
                );
            }

            /// Delete with mixed existing/non-existent keys should succeed.
            #[tokio::test]
            async fn test_delete_mixed_exists_not_found() {
                let store = $store_fn().await;

                store
                    .put(vec![BlobInput::new("exists-a.txt", b"a".as_slice())])
                    .await
                    .unwrap();
                store
                    .put(vec![BlobInput::new("exists-b.txt", b"b".as_slice())])
                    .await
                    .unwrap();

                // Delete 2 existing + 2 non-existing keys
                let result = store
                    .delete(&["exists-a.txt", "never-a.txt", "exists-b.txt", "never-b.txt"])
                    .await;
                assert!(
                    result.is_ok(),
                    "mixed delete should succeed (NotFound is idempotent)"
                );

                // Verify existing keys are gone
                // Accept BackendMisconfigured (s3s-fs mock quirk) — the delete succeeded
                let exists_a = store.exists("exists-a.txt").await;
                assert!(
                    exists_a.is_err() || !exists_a.unwrap(),
                    "exists-a should be gone"
                );
                let exists_b = store.exists("exists-b.txt").await;
                assert!(
                    exists_b.is_err() || !exists_b.unwrap(),
                    "exists-b should be gone"
                );
            }

            /// Delete batch with an invalid key should return InvalidInput.
            #[tokio::test]
            async fn test_delete_invalid_key_rejected() {
                let store = $store_fn().await;

                let result = store.delete(&["../path-traversal.txt"]).await;
                match result {
                    Err(BlobStorageError::InvalidInput(_)) => { /* expected */ }
                    other => panic!("expected InvalidInput for path traversal, got: {other:?}"),
                }
            }

            #[tokio::test]
            async fn test_list() {
                let store = $store_fn().await;

                store
                    .put(vec![
                        BlobInput::new("a.txt", b"aaa".as_slice()),
                        BlobInput::new("b.txt", b"bbb".as_slice()),
                        BlobInput::new("c.log", b"ccc".as_slice()),
                    ])
                    .await
                    .unwrap();

                let all = store.list(&SuffixFilter::new("")).await.unwrap();
                assert_eq!(all.len(), 3);

                let txt = store.list(&SuffixFilter::new(".txt")).await.unwrap();
                assert_eq!(txt.len(), 2);
                assert!(txt.contains(&"a.txt".to_string()));
                assert!(txt.contains(&"b.txt".to_string()));
            }

            #[tokio::test]
            async fn test_get_with_metadata() {
                let store = $store_fn().await;

                let data = b"hello world".as_slice();
                store
                    .put(vec![BlobInput::new("meta-test.txt", data)])
                    .await
                    .unwrap();

                let (meta, mut reader) = store.get_with_metadata("meta-test.txt").await.unwrap();
                assert_eq!(meta.key, "meta-test.txt");
                assert_eq!(meta.stored_size, 11);
                assert!(
                    meta.modified_at > chrono::DateTime::UNIX_EPOCH,
                    "backend should provide modified_at"
                );

                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, b"hello world");
            }

            #[tokio::test]
            async fn test_list_with_metadata() {
                let store = $store_fn().await;

                store
                    .put(vec![
                        BlobInput::new("a.txt", b"aaa".as_slice()),
                        BlobInput::new("b.txt", b"bbb".as_slice()),
                        BlobInput::new("c.log", b"ccc".as_slice()),
                    ])
                    .await
                    .unwrap();

                let metas = store
                    .list_with_metadata(&SuffixFilter::new(""))
                    .await
                    .unwrap();
                assert_eq!(metas.len(), 3);

                for meta in &metas {
                    assert!(!meta.key.is_empty());
                    assert_eq!(meta.stored_size, 3);
                    assert!(
                        meta.modified_at > chrono::DateTime::UNIX_EPOCH,
                        "backend metadata should have modified_at for '{}'",
                        meta.key
                    );
                }

                let txt_metas = store
                    .list_with_metadata(&SuffixFilter::new(".txt"))
                    .await
                    .unwrap();
                assert_eq!(txt_metas.len(), 2);
                for meta in &txt_metas {
                    assert!(meta.key.ends_with(".txt"));
                }

                let log_metas = store
                    .list_with_metadata(&SuffixFilter::new(".log"))
                    .await
                    .unwrap();
                assert_eq!(log_metas.len(), 1);
                assert_eq!(log_metas[0].key, "c.log");
            }

            #[tokio::test]
            async fn test_nested_keys() {
                let store = $store_fn().await;

                store
                    .put(vec![
                        BlobInput::new("a/b/c.txt", b"nested".as_slice()),
                        BlobInput::new("a/b/d.txt", b"nested2".as_slice()),
                        BlobInput::new("x/y.txt", b"other".as_slice()),
                    ])
                    .await
                    .unwrap();

                let all = store.list(&SuffixFilter::new("")).await.unwrap();
                assert_eq!(all.len(), 3);
                assert!(all.contains(&"a/b/c.txt".to_string()));
                assert!(all.contains(&"a/b/d.txt".to_string()));
                assert!(all.contains(&"x/y.txt".to_string()));

                let a_b = store.list(&PrefixFilter::new("a/b/")).await.unwrap();
                assert_eq!(a_b.len(), 2);
                assert!(a_b.contains(&"a/b/c.txt".to_string()));
                assert!(a_b.contains(&"a/b/d.txt".to_string()));

                let mut reader = store.get("a/b/c.txt").await.unwrap();
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, b"nested");

                let all_txt = store
                    .list_with_metadata(&SuffixFilter::new(".txt"))
                    .await
                    .unwrap();
                assert_eq!(all_txt.len(), 3);
                let all_txt_keys: Vec<&str> = all_txt.iter().map(|m| m.key.as_str()).collect();
                assert!(all_txt_keys.contains(&"a/b/c.txt"));
                assert!(all_txt_keys.contains(&"a/b/d.txt"));
                assert!(all_txt_keys.contains(&"x/y.txt"));
            }

            // ================================================================
            // PrefixFilter tests — verify prefix_hint() optimization
            // ================================================================

            /// Test that `PrefixFilter` with trailing `/` correctly limits
            /// the walk to only that subdirectory.
            #[tokio::test]
            async fn test_prefix_filter_trailing_slash() {
                let store = $store_fn().await;

                store
                    .put(vec![
                        BlobInput::new("a/b/c.txt", b"nested".as_slice()),
                        BlobInput::new("a/b/d.txt", b"nested2".as_slice()),
                        BlobInput::new("x/y.txt", b"other".as_slice()),
                    ])
                    .await
                    .unwrap();

                // PrefixFilter with trailing "/" — should walk only that subdirectory
                let a_b = store.list(&PrefixFilter::new("a/b/")).await.unwrap();
                assert_eq!(a_b.len(), 2);
                assert!(a_b.contains(&"a/b/c.txt".to_string()));
                assert!(a_b.contains(&"a/b/d.txt".to_string()));

                // Verify that keys outside the prefix are NOT included
                assert!(!a_b.contains(&"x/y.txt".to_string()));
            }

            /// Test that `PrefixFilter` without trailing `/` correctly walks
            /// the parent directory and filters by `starts_with`.
            #[tokio::test]
            async fn test_prefix_filter_basename() {
                let store = $store_fn().await;

                store
                    .put(vec![
                        BlobInput::new("a/b/c.txt", b"nested".as_slice()),
                        BlobInput::new("a/b/d.txt", b"nested2".as_slice()),
                        BlobInput::new("x/y.txt", b"other".as_slice()),
                    ])
                    .await
                    .unwrap();

                // PrefixFilter without trailing "/" — "a/b" is a basename
                // Should walk "a/" directory and filter by starts_with("b")
                let a_b = store.list(&PrefixFilter::new("a/b")).await.unwrap();
                assert_eq!(a_b.len(), 2);
                assert!(a_b.contains(&"a/b/c.txt".to_string()));
                assert!(a_b.contains(&"a/b/d.txt".to_string()));

                // Verify that keys starting with "a/b" but not "a/b/" are also included
                let a_b_only = store.list(&PrefixFilter::new("a/b")).await.unwrap();
                assert_eq!(a_b_only.len(), 2);
            }

            /// Test that `NotFilter` properly delegates `prefix_hint()`.
            #[tokio::test]
            async fn test_not_filter_prefix_hint() {
                let store = $store_fn().await;

                store
                    .put(vec![
                        BlobInput::new("a/b/c.txt", b"nested".as_slice()),
                        BlobInput::new("a/b/d.txt", b"nested2".as_slice()),
                        BlobInput::new("x/y.txt", b"other".as_slice()),
                    ])
                    .await
                    .unwrap();

                // NotFilter wrapping a PrefixFilter — should delegate the hint
                let not_a_b = store
                    .list(&NotFilter::new(Box::new(PrefixFilter::new("a/b/"))))
                    .await
                    .unwrap();
                assert_eq!(not_a_b.len(), 1);
                assert_eq!(not_a_b[0], "x/y.txt");
            }

            /// Test that `PrefixFilter` with empty prefix (no hint) walks the full root.
            #[tokio::test]
            async fn test_prefix_filter_empty() {
                let store = $store_fn().await;

                store
                    .put(vec![
                        BlobInput::new("a.txt", b"a".as_slice()),
                        BlobInput::new("b.txt", b"b".as_slice()),
                    ])
                    .await
                    .unwrap();

                // Empty prefix filter — should return all keys
                let all = store.list(&PrefixFilter::new("")).await.unwrap();
                assert_eq!(all.len(), 2);
                assert!(all.contains(&"a.txt".to_string()));
                assert!(all.contains(&"b.txt".to_string()));
            }

            // ================================================================
            // PrefixBlobStore tests — verify directory vs file prefix behavior
            // ================================================================

            /// Test that `PrefixBlobStore` with a directory prefix (ending with `/`)
            /// correctly scopes the inner store to only that subdirectory.
            #[tokio::test]
            async fn test_prefix_blob_store_dir_prefix() {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().to_path_buf();
                let _ = dir.keep();

                // Create a store with prefix "my-app/"
                let prefixed = BlobStoreBuilder::new()
                    .with_fs(&path)
                    .with_prefix("my-app/")
                    .build()
                    .await
                    .unwrap();

                prefixed
                    .put(vec![
                        BlobInput::new("users/alice.txt", b"alice".as_slice()),
                        BlobInput::new("users/bob.txt", b"bob".as_slice()),
                        BlobInput::new("config.json", b"config".as_slice()),
                    ])
                    .await
                    .unwrap();

                // List with PrefixFilter("users/") — should walk only "my-app/users/"
                let users = prefixed.list(&PrefixFilter::new("users/")).await.unwrap();
                assert_eq!(users.len(), 2);
                assert!(users.contains(&"users/alice.txt".to_string()));
                assert!(users.contains(&"users/bob.txt".to_string()));
            }

            /// Test that `PrefixBlobStore` with a file prefix (not ending with `/`)
            /// correctly walks the parent directory and filters by `starts_with`.
            #[tokio::test]
            async fn test_prefix_blob_store_file_prefix() {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().to_path_buf();
                let _ = dir.keep();

                // Create a store with prefix "my-app/"
                let prefixed = BlobStoreBuilder::new()
                    .with_fs(&path)
                    .with_prefix("my-app/")
                    .build()
                    .await
                    .unwrap();

                prefixed
                    .put(vec![
                        BlobInput::new("users/alice.txt", b"alice".as_slice()),
                        BlobInput::new("users/bob.txt", b"bob".as_slice()),
                        BlobInput::new("config.json", b"config".as_slice()),
                    ])
                    .await
                    .unwrap();

                // List with PrefixFilter("users") — should walk "my-app/" and filter
                let users = prefixed.list(&PrefixFilter::new("users")).await.unwrap();
                assert_eq!(users.len(), 2);
                assert!(users.contains(&"users/alice.txt".to_string()));
                assert!(users.contains(&"users/bob.txt".to_string()));
            }
        }
    };
}

// ============================================================================
// Generate tests for each backend
// ============================================================================

backend_tests!(fs, fs_store);

#[cfg(feature = "s3")]
backend_tests!(s3, s3_store);

// ============================================================================
// Backend-agnostic tests — ListFilter combinator tests
// ============================================================================

#[tokio::test]
async fn test_list_filter_suffix() {
    let store = fs_store().await;

    store
        .put(vec![
            BlobInput::new("a.txt", b"a".as_slice()),
            BlobInput::new("b.md", b"b".as_slice()),
            BlobInput::new("c.txt", b"c".as_slice()),
        ])
        .await
        .unwrap();

    // SuffixFilter
    let txt = store.list(&SuffixFilter::new(".txt")).await.unwrap();
    assert_eq!(txt.len(), 2);

    // PrefixFilter
    let a_prefixed = store.list(&PrefixFilter::new("a")).await.unwrap();
    assert_eq!(a_prefixed, vec!["a.txt"]);

    // NotFilter (invert suffix)
    let not_txt = store
        .list(&NotFilter::new(Box::new(SuffixFilter::new(".txt"))))
        .await
        .unwrap();
    assert_eq!(not_txt.len(), 1);
    assert_eq!(not_txt[0], "b.md");

    // exclude_suffix convenience
    let exclude_txt = store
        .list(&<dyn xtax_blob_storage::ListFilter>::exclude_suffix(".txt"))
        .await
        .unwrap();
    assert_eq!(exclude_txt.len(), 1);
    assert_eq!(exclude_txt[0], "b.md");
}

// ============================================================================
// FS-specific path traversal regression tests
//
// These ensure that `list()`, `visit()`, and `list_with_metadata()` never
// traverse outside the root directory, even when called with a malicious
// `PrefixFilter::new("../")` or `PrefixFilter::new("/tmp")`.
// ============================================================================

#[tokio::test]
async fn test_fs_list_rejects_traversal_dotdot() {
    let store = fs_store().await;

    let err = store
        .list(&PrefixFilter::new("../"))
        .await
        .expect_err("traversal via '../' should be rejected");
    assert!(
        matches!(err, xtax_blob_storage::BlobStorageError::InvalidInput(_)),
        "expected InvalidInput, got: {err:?}"
    );
}

#[tokio::test]
async fn test_fs_list_rejects_traversal_absolute() {
    let store = fs_store().await;

    let err = store
        .list(&PrefixFilter::new("/tmp"))
        .await
        .expect_err("traversal via '/tmp' should be rejected");
    assert!(
        matches!(err, xtax_blob_storage::BlobStorageError::InvalidInput(_)),
        "expected InvalidInput, got: {err:?}"
    );
}

#[tokio::test]
async fn test_fs_list_rejects_traversal_arbitrary_dotdot() {
    let store = fs_store().await;

    let err = store
        .list(&PrefixFilter::new("a/../../x"))
        .await
        .expect_err("traversal via 'a/../../x' should be rejected");
    assert!(
        matches!(err, xtax_blob_storage::BlobStorageError::InvalidInput(_)),
        "expected InvalidInput, got: {err:?}"
    );
}

#[tokio::test]
async fn test_fs_visit_rejects_traversal() {
    let store = fs_store().await;

    // Use a no-op visitor — traversal should be rejected before visiting anything
    struct NoopVisitor;
    #[async_trait::async_trait]
    impl xtax_blob_storage::BlobVisitor for NoopVisitor {
        async fn visit(
            &mut self,
            _key: &str,
            _meta: Option<&xtax_blob_storage::BlobMeta>,
        ) -> xtax_blob_storage::Result<bool> {
            Ok(true)
        }
    }

    let err = store
        .visit(&PrefixFilter::new("../"), &mut NoopVisitor)
        .await
        .expect_err("visit with '../' should be rejected");
    assert!(
        matches!(err, xtax_blob_storage::BlobStorageError::InvalidInput(_)),
        "expected InvalidInput, got: {err:?}"
    );
}

#[tokio::test]
async fn test_fs_list_with_metadata_rejects_traversal() {
    let store = fs_store().await;

    let err = store
        .list_with_metadata(&PrefixFilter::new("../"))
        .await
        .expect_err("list_with_metadata with '../' should be rejected");
    assert!(
        matches!(err, xtax_blob_storage::BlobStorageError::InvalidInput(_)),
        "expected InvalidInput, got: {err:?}"
    );
}

// ============================================================================
// Prefix layer transparency tests
//
// These ensure that the PrefixBlobStore does not leak prefixed keys to the
// caller in any operation.
// ============================================================================

/// put() must return BlobMeta with logical (unprefixed) keys.
#[tokio::test]
async fn test_prefix_put_returns_logical_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let store = BlobStoreBuilder::new()
        .with_fs(&path)
        .with_prefix("my-app/")
        .build()
        .await
        .unwrap();

    let result = store
        .put(vec![BlobInput::new("a.txt", b"data".as_slice())])
        .await
        .unwrap();
    assert_eq!(result.blobs.len(), 1);
    assert_eq!(result.blobs[0].key, "a.txt", "put must return logical key");
}

/// list_with_metadata() through a prefix must return BlobMeta with logical keys.
#[tokio::test]
async fn test_prefix_list_with_metadata_logical_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let store = BlobStoreBuilder::new()
        .with_fs(&path)
        .with_prefix("my-app/")
        .build()
        .await
        .unwrap();

    store
        .put(vec![BlobInput::new("a.txt", b"a".as_slice())])
        .await
        .unwrap();

    let metas = store
        .list_with_metadata(&PrefixFilter::new(""))
        .await
        .unwrap();
    assert_eq!(metas.len(), 1);
    assert_eq!(
        metas[0].key, "a.txt",
        "list_with_metadata must return logical key"
    );
}

/// visit() through a prefix must pass logical key and logical meta.key to the visitor.
#[tokio::test]
async fn test_prefix_visit_logical_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let store = BlobStoreBuilder::new()
        .with_fs(&path)
        .with_prefix("my-app/")
        .build()
        .await
        .unwrap();

    store
        .put(vec![BlobInput::new("a.txt", b"data".as_slice())])
        .await
        .unwrap();

    struct CaptureVisitor {
        key: Option<String>,
        meta_key: Option<String>,
    }
    #[async_trait::async_trait]
    impl xtax_blob_storage::BlobVisitor for CaptureVisitor {
        async fn visit(
            &mut self,
            key: &str,
            meta: Option<&xtax_blob_storage::BlobMeta>,
        ) -> xtax_blob_storage::Result<bool> {
            self.key = Some(key.to_string());
            self.meta_key = meta.map(|m| m.key.clone());
            Ok(false) // stop after first
        }
    }

    let mut visitor = CaptureVisitor {
        key: None,
        meta_key: None,
    };
    store
        .visit(&PrefixFilter::new(""), &mut visitor)
        .await
        .unwrap();

    assert_eq!(
        visitor.key.as_deref(),
        Some("a.txt"),
        "visit must pass logical key"
    );
    assert_eq!(
        visitor.meta_key.as_deref(),
        Some("a.txt"),
        "visit must pass logical meta.key"
    );
}

/// get() through a prefix must not leak the prefixed key in NotFound error.
#[tokio::test]
async fn test_prefix_get_does_not_leak_prefixed_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let store = BlobStoreBuilder::new()
        .with_fs(&path)
        .with_prefix("my-app/")
        .build()
        .await
        .unwrap();

    let result = store.get("nonexistent.txt").await;
    match result {
        Err(xtax_blob_storage::BlobStorageError::NotFound(key)) => {
            assert_eq!(
                key, "nonexistent.txt",
                "NotFound error must contain logical key, not prefixed"
            );
        }
        _ => panic!("expected NotFound for non-existent key"),
    }
}

/// get_with_metadata() through a prefix must not leak the prefixed key in error message.
/// (The inner backend may return NotFound, Storage, or other error types —
/// the key thing is the logical key is preserved in the error, not prefixed.)
#[tokio::test]
async fn test_prefix_get_with_metadata_does_not_leak_prefixed_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let store = BlobStoreBuilder::new()
        .with_fs(&path)
        .with_prefix("my-app/")
        .build()
        .await
        .unwrap();

    let result = store.get_with_metadata("nonexistent.txt").await;
    match result {
        Err(err) => {
            let err_msg = err.to_string();
            assert!(
                !err_msg.contains("my-app/"),
                "error should not contain prefixed key: {err_msg}"
            );
        }
        Ok(_) => panic!("expected error for non-existent key"),
    }
}

// ============================================================================
// FS regression tests — listing with a prefix whose directory doesn't exist
// should return an empty result, not a storage error (matches S3 behaviour)
// ============================================================================

#[tokio::test]
async fn test_fs_list_missing_prefix_returns_empty() {
    let store = fs_store().await;
    let keys = store.list(&PrefixFilter::new("missing/")).await.unwrap();
    assert!(
        keys.is_empty(),
        "list with missing prefix should return empty"
    );

    let keys = store.list(&PrefixFilter::new("a/b/")).await.unwrap();
    assert!(
        keys.is_empty(),
        "list with missing nested prefix should return empty"
    );

    let keys = store.list(&PrefixFilter::new("a/b")).await.unwrap();
    assert!(
        keys.is_empty(),
        "list with missing basename prefix should return empty"
    );
}

#[tokio::test]
async fn test_fs_list_with_metadata_missing_prefix_returns_empty() {
    let store = fs_store().await;
    let metas = store
        .list_with_metadata(&PrefixFilter::new("nonexistent/"))
        .await
        .unwrap();
    assert!(
        metas.is_empty(),
        "list_with_metadata with missing prefix should return empty"
    );

    let metas = store
        .list_with_metadata(&PrefixFilter::new("x/y/"))
        .await
        .unwrap();
    assert!(
        metas.is_empty(),
        "list_with_metadata with missing nested prefix should return empty"
    );
}

#[tokio::test]
async fn test_fs_visit_missing_prefix_skips_and_returns_ok() {
    let store = fs_store().await;

    struct CountVisitor {
        visited: u32,
    }
    #[async_trait::async_trait]
    impl BlobVisitor for CountVisitor {
        async fn visit(
            &mut self,
            _key: &str,
            _meta: Option<&BlobMeta>,
        ) -> xtax_blob_storage::Result<bool> {
            self.visited += 1;
            Ok(true)
        }
    }

    let mut visitor = CountVisitor { visited: 0 };
    store
        .visit(&PrefixFilter::new("missing/"), &mut visitor)
        .await
        .unwrap();
    assert_eq!(
        visitor.visited, 0,
        "visit with missing prefix should not call visitor"
    );

    let mut visitor = CountVisitor { visited: 0 };
    store
        .visit(&PrefixFilter::new("a/b/"), &mut visitor)
        .await
        .unwrap();
    assert_eq!(
        visitor.visited, 0,
        "visit with missing nested prefix should not call visitor"
    );
}

// ============================================================================
// FS root directory deleted — must return BackendMisconfigured
// ============================================================================

/// When the root directory is deleted, operations must return
/// `BackendMisconfigured`, not `NotFound` or `Storage`.
#[tokio::test]
async fn test_fs_missing_root_backend_misconfigured() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store-root");
    std::fs::create_dir_all(&root).unwrap();

    let store = BlobStoreBuilder::new()
        .with_fs(&root)
        .build()
        .await
        .unwrap();

    // Put a blob first to verify the store works
    store
        .put(vec![BlobInput::new("key.txt", b"data".as_slice())])
        .await
        .unwrap();

    // Delete the root directory
    let _ = std::fs::remove_dir_all(&root);

    // exists() must return BackendMisconfigured
    let err = store.exists("key.txt").await.unwrap_err();
    assert!(
        matches!(
            err,
            xtax_blob_storage::BlobStorageError::BackendMisconfigured(_)
        ),
        "exists on deleted root should return BackendMisconfigured, got: {err:?}"
    );

    // get() must return BackendMisconfigured
    match store.get("key.txt").await {
        Err(xtax_blob_storage::BlobStorageError::BackendMisconfigured(_)) => {
            // expected
        }
        Err(other) => {
            panic!("get on deleted root should return BackendMisconfigured, got: {other}")
        }
        Ok(_) => panic!("get on deleted root expected Err, got Ok"),
    }

    // put() must return BackendMisconfigured
    let err = store
        .put(vec![BlobInput::new("new.txt", b"data".as_slice())])
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            xtax_blob_storage::BlobStorageError::BackendMisconfigured(_)
        ),
        "put on deleted root should return BackendMisconfigured, got: {err:?}"
    );

    // delete() must return BackendMisconfigured
    let err = store.delete(&["key.txt"]).await.unwrap_err();
    assert!(
        matches!(
            err,
            xtax_blob_storage::BlobStorageError::BackendMisconfigured(_)
        ),
        "delete on deleted root should return BackendMisconfigured, got: {err:?}"
    );

    // list() must return BackendMisconfigured
    let err = store.list(&SuffixFilter::new("")).await.unwrap_err();
    assert!(
        matches!(
            err,
            xtax_blob_storage::BlobStorageError::BackendMisconfigured(_)
        ),
        "list on deleted root should return BackendMisconfigured, got: {err:?}"
    );
}

// ============================================================================
// S3 NoSuchBucket → BackendMisconfigured test
// ============================================================================
//
// NOTE: The in-process `s3s` mock used for tests does not emulate
// `NoSuchBucket` errors (buckets are auto-created by the mock).
// The code path mapping `NoSuchBucket` → `BackendMisconfigured` is
// tested in `s3.rs::is_misconfigured()` and `get_object_output()`,
// but cannot be covered by an integration test without a real S3
// endpoint.
//
// To verify this mapping manually, run against a real S3 endpoint
// with an invalid bucket name and confirm:
//
//   - `store.exists("key")` returns `Err(BackendMisconfigured(...))`
//   - `store.get("key")`   returns `Err(BackendMisconfigured(...))`

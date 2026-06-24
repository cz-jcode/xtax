//! Cleanup layer tests — uses `BlobCleanup` with various predicates to verify
//! correct deletion behavior, batching, and passthrough of regular operations.
//!
//! Run with:
//!   cargo test --test cleanup_test                    # FS only
//!   cargo test --features s3 --test cleanup_test      # FS + S3

#![cfg(feature = "fs")]

use std::sync::Arc;

use tokio::io::AsyncReadExt;
use xtax_blob_storage::{
    BlobCleanup, BlobInput, BlobStore, BlobStoreBuilder, CleanupPredicate, SuffixFilter,
};

// ============================================================================
// Store factories
// ============================================================================

async fn fs_store() -> Arc<dyn BlobStore> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();
    BlobStoreBuilder::new().with_fs(path).build().await.unwrap()
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
    let root = std::env::temp_dir().join("s3s-cleanup").join(sub);
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
    let bucket = format!("s3s-cleanup-{}", unique_id());
    let _ = client.create_bucket().bucket(&bucket).send().await.unwrap();
    (client, bucket)
}

// ============================================================================
// Macro to generate identical test bodies for each backend
// ============================================================================

macro_rules! cleanup_tests {
    ($mod_name:ident, $store_fn:path) => {
        mod $mod_name {
            use super::*;

            #[tokio::test]
            async fn test_cleanup_deletes_matching() {
                let store = $store_fn().await;
                let predicate: CleanupPredicate = Box::new(|key, _meta| key.starts_with("tmp-"));
                let store = BlobCleanup::new(store, predicate);

                // Insert blobs: some match, some don't
                store
                    .put(vec![
                        BlobInput::new("tmp-file.txt", b"temp".as_slice()),
                        BlobInput::new("keep.txt", b"permanent".as_slice()),
                        BlobInput::new("tmp-data.bin", b"temp data".as_slice()),
                        BlobInput::new("important.doc", b"important".as_slice()),
                    ])
                    .await
                    .unwrap();

                let result = store.cleanup().await.unwrap();
                assert_eq!(result.deleted_count, 2, "should delete 2 tmp- blobs");

                // Verify remaining blobs
                let remaining = store.list(&SuffixFilter::new("")).await.unwrap();
                assert_eq!(remaining.len(), 2);
                assert!(remaining.contains(&"keep.txt".to_string()));
                assert!(remaining.contains(&"important.doc".to_string()));
            }

            #[tokio::test]
            async fn test_cleanup_all() {
                let store = $store_fn().await;
                let predicate: CleanupPredicate = Box::new(|_key, _meta| true);
                let store = BlobCleanup::new(store, predicate);

                store
                    .put(vec![
                        BlobInput::new("a.txt", b"aaa".as_slice()),
                        BlobInput::new("b.txt", b"bbb".as_slice()),
                        BlobInput::new("c.txt", b"ccc".as_slice()),
                    ])
                    .await
                    .unwrap();

                let result = store.cleanup().await.unwrap();
                assert_eq!(result.deleted_count, 3, "should delete all 3 blobs");

                let remaining = store.list(&SuffixFilter::new("")).await.unwrap();
                assert!(remaining.is_empty(), "store should be empty");
            }

            #[tokio::test]
            async fn test_cleanup_none() {
                let store = $store_fn().await;
                let predicate: CleanupPredicate = Box::new(|_key, _meta| false);
                let store = BlobCleanup::new(store, predicate);

                store
                    .put(vec![
                        BlobInput::new("a.txt", b"aaa".as_slice()),
                        BlobInput::new("b.txt", b"bbb".as_slice()),
                    ])
                    .await
                    .unwrap();

                let result = store.cleanup().await.unwrap();
                assert_eq!(result.deleted_count, 0, "should delete nothing");

                let remaining = store.list(&SuffixFilter::new("")).await.unwrap();
                assert_eq!(remaining.len(), 2, "all blobs should remain");
            }

            #[tokio::test]
            async fn test_cleanup_empty_store() {
                let store = $store_fn().await;
                let predicate: CleanupPredicate = Box::new(|_key, _meta| true);
                let store = BlobCleanup::new(store, predicate);

                let result = store.cleanup().await.unwrap();
                assert_eq!(result.deleted_count, 0, "empty store should delete nothing");
            }

            #[tokio::test]
            async fn test_cleanup_batch_size() {
                let store = $store_fn().await;
                let predicate: CleanupPredicate = Box::new(|_key, _meta| true);
                let store = BlobCleanup::new(store, predicate).with_batch_size(2);

                // Insert 5 blobs -> will be deleted in batches of 2 (2+2+1)
                store
                    .put(vec![
                        BlobInput::new("a.txt", b"aaa".as_slice()),
                        BlobInput::new("b.txt", b"bbb".as_slice()),
                        BlobInput::new("c.txt", b"ccc".as_slice()),
                        BlobInput::new("d.txt", b"ddd".as_slice()),
                        BlobInput::new("e.txt", b"eee".as_slice()),
                    ])
                    .await
                    .unwrap();

                let result = store.cleanup().await.unwrap();
                assert_eq!(result.deleted_count, 5, "all 5 blobs should be deleted");
            }

            #[tokio::test]
            async fn test_cleanup_passthrough_put_get() {
                let store = $store_fn().await;
                let predicate: CleanupPredicate = Box::new(|_key, _meta| false);
                let store = BlobCleanup::new(store, predicate);

                let data = b"passthrough test";
                store
                    .put(vec![BlobInput::new("test.txt", &data[..])])
                    .await
                    .unwrap();

                let mut reader = store.get("test.txt").await.unwrap();
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await.unwrap();
                assert_eq!(
                    buf, data,
                    "get should return original data through cleanup layer"
                );
            }

            #[tokio::test]
            async fn test_cleanup_passthrough_delete() {
                let store = $store_fn().await;
                let predicate: CleanupPredicate = Box::new(|_key, _meta| false);
                let store = BlobCleanup::new(store, predicate);

                store
                    .put(vec![BlobInput::new("del.txt", b"delete me".as_slice())])
                    .await
                    .unwrap();

                // Direct delete should work through cleanup layer
                store.delete(&["del.txt"]).await.unwrap();

                let result = store.get("del.txt").await;
                assert!(result.is_err(), "should be gone after direct delete");
            }

            #[tokio::test]
            async fn test_cleanup_passthrough_exists() {
                let store = $store_fn().await;
                let predicate: CleanupPredicate = Box::new(|_key, _meta| false);
                let store = BlobCleanup::new(store, predicate);

                store
                    .put(vec![BlobInput::new("exists.txt", b"exists".as_slice())])
                    .await
                    .unwrap();

                assert!(store.exists("exists.txt").await.unwrap());
                assert!(!store.exists("nonexistent.txt").await.unwrap_or(false));
            }

            #[tokio::test]
            async fn test_cleanup_metadata_based_predicate() {
                let store = $store_fn().await;
                // Predicate that uses metadata: delete blobs with stored_size > 50
                let predicate: CleanupPredicate = Box::new(|_key, meta| meta.stored_size > 50);
                let store = BlobCleanup::new(store, predicate);

                // Small blob (< 50 bytes)
                store
                    .put(vec![BlobInput::new("small.txt", b"small".as_slice())])
                    .await
                    .unwrap();
                // Large blob (> 50 bytes) — use Cursor for Vec<u8> to satisfy 'static
                let large_data: Vec<u8> = vec![b'x'; 100];
                store
                    .put(vec![BlobInput::new(
                        "large.txt",
                        std::io::Cursor::new(large_data),
                    )])
                    .await
                    .unwrap();

                let result = store.cleanup().await.unwrap();
                assert_eq!(
                    result.deleted_count, 1,
                    "only the large blob should be deleted"
                );

                let remaining = store.list(&SuffixFilter::new("")).await.unwrap();
                assert_eq!(remaining, vec!["small.txt"]);
            }

            #[tokio::test]
            async fn test_cleanup_multiple_invocations() {
                let store = $store_fn().await;
                let predicate: CleanupPredicate = Box::new(|key, _meta| key.starts_with("tmp-"));
                let store = BlobCleanup::new(store, predicate);

                store
                    .put(vec![BlobInput::new("tmp-1.txt", b"temp1".as_slice())])
                    .await
                    .unwrap();

                let result = store.cleanup().await.unwrap();
                assert_eq!(result.deleted_count, 1);

                // Add more and cleanup again
                store
                    .put(vec![
                        BlobInput::new("keep.txt", b"keep".as_slice()),
                        BlobInput::new("tmp-2.txt", b"temp2".as_slice()),
                    ])
                    .await
                    .unwrap();

                let result = store.cleanup().await.unwrap();
                assert_eq!(
                    result.deleted_count, 1,
                    "second cleanup should only delete tmp-2"
                );

                let remaining = store.list(&SuffixFilter::new("")).await.unwrap();
                assert_eq!(remaining, vec!["keep.txt"]);
            }
        }
    };
}

// ============================================================================
// Generate tests for FS backend
// ============================================================================

cleanup_tests!(fs, fs_store);

// ============================================================================
// Generate tests for S3 backend (only when feature = "s3")
// ============================================================================

#[cfg(feature = "s3")]
cleanup_tests!(s3, s3_store);

// ============================================================================
// Layer-specific tests — BlobCleanup wrapping PrefixBlobStore
// ============================================================================

#[tokio::test]
async fn test_cleanup_with_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let inner = BlobStoreBuilder::new()
        .with_fs(dir.path())
        .with_prefix("app/")
        .build()
        .await
        .unwrap();

    let predicate: CleanupPredicate = Box::new(|key, _meta| key.starts_with("tmp-"));
    let store = BlobCleanup::new(inner, predicate);

    store
        .put(vec![
            BlobInput::new("tmp-file.txt", b"temp".as_slice()),
            BlobInput::new("keep.txt", b"permanent".as_slice()),
        ])
        .await
        .unwrap();

    let result = store.cleanup().await.unwrap();
    assert_eq!(
        result.deleted_count, 1,
        "should delete tmp-file through prefix layer"
    );

    let remaining = store.list(&SuffixFilter::new("")).await.unwrap();
    assert_eq!(remaining, vec!["keep.txt"]);
}

#[tokio::test]
async fn test_cleanup_empty_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();
    let store = BlobStoreBuilder::new().with_fs(path).build().await.unwrap();

    // Edge case: predicate that matches everything (no blobs inserted)
    let predicate: CleanupPredicate = Box::new(|_key, _meta| true);
    let store = BlobCleanup::new(store, predicate);

    // No blobs inserted — should not crash
    let result = store.cleanup().await.unwrap();
    assert_eq!(result.deleted_count, 0);
}

#[tokio::test]
async fn test_cleanup_batch_size_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();
    let store = BlobStoreBuilder::new().with_fs(path).build().await.unwrap();

    let predicate: CleanupPredicate = Box::new(|_key, _meta| true);
    let store = BlobCleanup::new(store, predicate).with_batch_size(1); // Every blob triggers an immediate batch delete

    store
        .put(vec![
            BlobInput::new("a.txt", b"a".as_slice()),
            BlobInput::new("b.txt", b"b".as_slice()),
            BlobInput::new("c.txt", b"c".as_slice()),
        ])
        .await
        .unwrap();

    let result = store.cleanup().await.unwrap();
    assert_eq!(
        result.deleted_count, 3,
        "batch_size=1 should still delete all"
    );
}

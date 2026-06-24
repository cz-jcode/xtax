//! Tests for streaming `visit()` on all backends and layers.
//!
//! Runs against:
//! - FS backend (requires `--features fs`)
//! - S3 backend (requires `--features s3`)
//! - PrefixBlobStore layer (FS-backed)
//! - EncryptedBlobStore layer (FS-backed)
//!
//! Run with:
//!   cargo test --test visitor_test                    # FS only
//!   cargo test --features s3 --test visitor_test      # FS + S3

#![cfg(feature = "fs")]

use std::sync::Arc;

use xtax_blob_storage::{BlobInput, BlobStore, BlobStoreBuilder, SuffixFilter};

#[path = "common/encrypt.rs"]
mod common_encrypt;
use common_encrypt::*;

#[path = "common/collecting_visitor.rs"]
mod collecting_visitor;
use collecting_visitor::*;

// ============================================================================
// FS store factory
// ============================================================================

async fn fs_store() -> Arc<dyn BlobStore> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    // Keep dir alive — the OS will clean up on reboot.
    // This avoids the tempdir being dropped before the store is used.
    let _ = dir.keep();
    BlobStoreBuilder::new().with_fs(path).build().await.unwrap()
}

// ============================================================================
// S3 store factory (only when feature = "s3")
// ============================================================================

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
    let root = std::env::temp_dir().join("s3s-visit").join(sub);
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
    let bucket = format!("s3s-visit-{}", unique_id());
    let _ = client.create_bucket().bucket(&bucket).send().await.unwrap();
    (client, bucket)
}

// ============================================================================
// Macro to generate identical test bodies for each backend
// ============================================================================

macro_rules! visit_tests {
    ($mod_name:ident, $store_fn:path) => {
        mod $mod_name {
            use super::*;
            use xtax_blob_storage::PrefixFilter;

            #[tokio::test]
            async fn test_visit_all() {
                let store = $store_fn().await;

                store
                    .put(vec![
                        BlobInput::new("a.txt", b"aaa".as_slice()),
                        BlobInput::new("b.txt", b"bbb".as_slice()),
                        BlobInput::new("c.log", b"ccc".as_slice()),
                    ])
                    .await
                    .unwrap();

                let mut visitor = CollectingVisitor::new();
                store
                    .visit(&SuffixFilter::new(""), &mut visitor)
                    .await
                    .unwrap();

                assert_eq!(visitor.keys.len(), 3);
                assert!(visitor.keys.contains(&"a.txt".to_string()));
                assert!(visitor.keys.contains(&"b.txt".to_string()));
                assert!(visitor.keys.contains(&"c.log".to_string()));
            }

            #[tokio::test]
            async fn test_visit_with_filter() {
                let store = $store_fn().await;

                store
                    .put(vec![
                        BlobInput::new("a.txt", b"aaa".as_slice()),
                        BlobInput::new("b.txt", b"bbb".as_slice()),
                        BlobInput::new("c.log", b"ccc".as_slice()),
                    ])
                    .await
                    .unwrap();

                let mut visitor = CollectingVisitor::new();
                store
                    .visit(&SuffixFilter::new(".txt"), &mut visitor)
                    .await
                    .unwrap();

                assert_eq!(visitor.keys.len(), 2);
                assert!(visitor.keys.contains(&"a.txt".to_string()));
                assert!(visitor.keys.contains(&"b.txt".to_string()));
            }

            #[tokio::test]
            async fn test_visit_metadata_provided() {
                let store = $store_fn().await;

                store
                    .put(vec![
                        BlobInput::new("alpha.txt", b"hello".as_slice()),
                        BlobInput::new("beta.txt", b"world".as_slice()),
                    ])
                    .await
                    .unwrap();

                let mut visitor = CollectingVisitor::new();
                store
                    .visit(&SuffixFilter::new(""), &mut visitor)
                    .await
                    .unwrap();

                // Both backends (FS, S3) provide metadata during visit
                assert_eq!(visitor.metas.len(), 2, "metadata should be provided");
                for meta in &visitor.metas {
                    assert!(
                        meta.stored_size > 0,
                        "stored_size should be > 0 for '{}'",
                        meta.key
                    );
                    assert!(
                        meta.modified_at > chrono::DateTime::UNIX_EPOCH,
                        "modified_at should be set for '{}'",
                        meta.key
                    );
                }
            }

            #[tokio::test]
            async fn test_visit_early_stop() {
                let store = $store_fn().await;

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

                let mut visitor = CollectingVisitor::stop_after(2);
                store
                    .visit(&SuffixFilter::new(""), &mut visitor)
                    .await
                    .unwrap();

                assert_eq!(visitor.keys.len(), 2, "should stop after 2 items");
            }

            #[tokio::test]
            async fn test_visit_empty_store() {
                let store = $store_fn().await;

                let mut visitor = CollectingVisitor::new();
                store
                    .visit(&SuffixFilter::new(""), &mut visitor)
                    .await
                    .unwrap();

                assert!(visitor.keys.is_empty(), "empty store should yield no keys");
            }

            #[tokio::test]
            async fn test_visit_nested_keys() {
                let store = $store_fn().await;

                store
                    .put(vec![
                        BlobInput::new("a/b/c.txt", b"nested".as_slice()),
                        BlobInput::new("a/b/d.txt", b"nested2".as_slice()),
                        BlobInput::new("x/y.txt", b"other".as_slice()),
                    ])
                    .await
                    .unwrap();

                let mut visitor = CollectingVisitor::new();
                store
                    .visit(&SuffixFilter::new(""), &mut visitor)
                    .await
                    .unwrap();

                assert_eq!(visitor.keys.len(), 3);
                assert!(visitor.keys.contains(&"a/b/c.txt".to_string()));
                assert!(visitor.keys.contains(&"a/b/d.txt".to_string()));
                assert!(visitor.keys.contains(&"x/y.txt".to_string()));

                // Prefix filter for subdirectory
                let mut visitor = CollectingVisitor::new();
                store
                    .visit(&PrefixFilter::new("a/b/"), &mut visitor)
                    .await
                    .unwrap();
                assert_eq!(visitor.keys.len(), 2);
                assert!(visitor.keys.contains(&"a/b/c.txt".to_string()));
                assert!(visitor.keys.contains(&"a/b/d.txt".to_string()));
            }

            #[tokio::test]
            async fn test_visit_equals_list() {
                let store = $store_fn().await;

                store
                    .put(vec![
                        BlobInput::new("x.txt", b"x".as_slice()),
                        BlobInput::new("y.log", b"y".as_slice()),
                        BlobInput::new("z.txt", b"z".as_slice()),
                    ])
                    .await
                    .unwrap();

                // visit should produce the same results as list
                let mut list_keys = store.list(&SuffixFilter::new("")).await.unwrap();
                list_keys.sort();

                let mut visitor = CollectingVisitor::new();
                store
                    .visit(&SuffixFilter::new(""), &mut visitor)
                    .await
                    .unwrap();
                visitor.keys.sort();

                assert_eq!(visitor.keys, list_keys, "visit should match list results");
            }
        }
    };
}

// ============================================================================
// Generate tests for FS backend
// ============================================================================

visit_tests!(fs, fs_store);

// ============================================================================
// Generate tests for S3 backend (only when feature = "s3")
// ============================================================================

#[cfg(feature = "s3")]
visit_tests!(s3, s3_store);

// ============================================================================
// Layer-specific tests — PrefixBlobStore
// ============================================================================

#[tokio::test]
async fn test_visit_with_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStoreBuilder::new()
        .with_fs(dir.path())
        .with_prefix("app/")
        .build()
        .await
        .unwrap();

    store
        .put(vec![
            BlobInput::new("a.txt", b"aaa".as_slice()),
            BlobInput::new("b.txt", b"bbb".as_slice()),
        ])
        .await
        .unwrap();

    // Visit via prefixed store should return logical keys (no prefix)
    let mut visitor = CollectingVisitor::new();
    store
        .visit(&SuffixFilter::new(""), &mut visitor)
        .await
        .unwrap();
    visitor.keys.sort();
    assert_eq!(visitor.keys, vec!["a.txt", "b.txt"]);

    // Verify underlying store has prefixed keys
    let raw = BlobStoreBuilder::new()
        .with_fs(dir.path())
        .build()
        .await
        .unwrap();
    let mut raw_visitor = CollectingVisitor::new();
    raw.visit(&SuffixFilter::new(""), &mut raw_visitor)
        .await
        .unwrap();
    assert!(raw_visitor.keys.contains(&"app/a.txt".to_string()));
    assert!(raw_visitor.keys.contains(&"app/b.txt".to_string()));
}

#[tokio::test]
async fn test_visit_with_prefix_filter() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStoreBuilder::new()
        .with_fs(dir.path())
        .with_prefix("app/")
        .build()
        .await
        .unwrap();

    store
        .put(vec![
            BlobInput::new("a.txt", b"aaa".as_slice()),
            BlobInput::new("b.log", b"bbb".as_slice()),
        ])
        .await
        .unwrap();

    // Filter by suffix through prefixed store
    let mut visitor = CollectingVisitor::new();
    store
        .visit(&SuffixFilter::new(".txt"), &mut visitor)
        .await
        .unwrap();
    assert_eq!(visitor.keys, vec!["a.txt"]);
}

#[tokio::test]
async fn test_visit_with_prefix_early_stop() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStoreBuilder::new()
        .with_fs(dir.path())
        .with_prefix("app/")
        .build()
        .await
        .unwrap();

    store
        .put(vec![
            BlobInput::new("a.txt", b"aaa".as_slice()),
            BlobInput::new("b.txt", b"bbb".as_slice()),
            BlobInput::new("c.txt", b"ccc".as_slice()),
        ])
        .await
        .unwrap();

    let mut visitor = CollectingVisitor::stop_after(1);
    store
        .visit(&SuffixFilter::new(""), &mut visitor)
        .await
        .unwrap();
    assert_eq!(
        visitor.keys.len(),
        1,
        "should stop after 1 item through prefix layer"
    );
}

// ============================================================================
// Layer-specific tests — EncryptedBlobStore
// ============================================================================

async fn encrypted_fs_store() -> Arc<dyn BlobStore> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();
    BlobStoreBuilder::new()
        .with_fs(path)
        .with_encryption(Arc::new(ShiftEncryption::new(7)))
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_visit_with_encryption_excludes_headers() {
    let store = encrypted_fs_store().await;

    store
        .put(vec![
            BlobInput::new("secret.txt", b"secret data".as_slice()),
            BlobInput::new("notes.txt", b"notes".as_slice()),
        ])
        .await
        .unwrap();

    // Visit should only return data blobs, not .enc-header blobs
    let mut visitor = CollectingVisitor::new();
    store
        .visit(&SuffixFilter::new(""), &mut visitor)
        .await
        .unwrap();

    assert_eq!(
        visitor.keys.len(),
        2,
        "should list only data blobs, not headers"
    );
    assert!(visitor.keys.contains(&"secret.txt".to_string()));
    assert!(visitor.keys.contains(&"notes.txt".to_string()));

    // Verify no header keys leaked through
    for key in &visitor.keys {
        assert!(
            !key.ends_with(".enc-header"),
            "header key '{}' should not appear in visit results",
            key
        );
    }
}

#[tokio::test]
async fn test_visit_with_encryption_and_filter() {
    let store = encrypted_fs_store().await;

    store
        .put(vec![
            BlobInput::new("alpha.txt", b"alpha".as_slice()),
            BlobInput::new("beta.log", b"beta".as_slice()),
        ])
        .await
        .unwrap();

    // Filter by suffix through encrypted store
    let mut visitor = CollectingVisitor::new();
    store
        .visit(&SuffixFilter::new(".txt"), &mut visitor)
        .await
        .unwrap();
    assert_eq!(visitor.keys, vec!["alpha.txt"]);
}

#[tokio::test]
async fn test_visit_with_encryption_metadata() {
    let store = encrypted_fs_store().await;

    store
        .put(vec![BlobInput::new("data.txt", b"some data".as_slice())])
        .await
        .unwrap();

    let mut visitor = CollectingVisitor::new();
    store
        .visit(&SuffixFilter::new(""), &mut visitor)
        .await
        .unwrap();

    assert_eq!(visitor.keys.len(), 1);
    // Encrypted store delegates visit to inner, which provides metadata
    assert_eq!(
        visitor.metas.len(),
        1,
        "metadata should be provided through encrypted layer"
    );
    if let Some(meta) = visitor.metas.first() {
        assert!(meta.stored_size > 0, "stored_size should be > 0");
    }
}

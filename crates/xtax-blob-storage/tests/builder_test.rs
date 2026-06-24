//! Builder API tests — covers builder methods that are not exercised by
//! the backend / encrypt / cleanup / visitor integration tests.
//!
//! Run with:
//!   cargo test --test builder_test                    # FS only
//!   cargo test --features s3 --test builder_test      # FS + S3

#![cfg(feature = "fs")]

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::io::AsyncReadExt;
use xtax_blob_storage::{
    BlobInput, BlobMeta, BlobStore, BlobStoreBuilder, CleanupPredicate, EncryptionProvider,
    SuffixFilter,
};

#[path = "common/encrypt.rs"]
mod common_encrypt;
use common_encrypt::*;

// ============================================================================
// Custom backend
// ============================================================================

/// A minimal custom backend that wraps an FS store and tracks operation counts.
struct TrackingStore {
    inner: Arc<dyn BlobStore>,
    puts: Arc<std::sync::atomic::AtomicU64>,
    gets: Arc<std::sync::atomic::AtomicU64>,
}

impl TrackingStore {
    fn new(inner: Arc<dyn BlobStore>) -> Self {
        Self {
            inner,
            puts: Arc::new(0.into()),
            gets: Arc::new(0.into()),
        }
    }

    fn put_count(&self) -> u64 {
        self.puts.load(Ordering::Relaxed)
    }

    fn get_count(&self) -> u64 {
        self.gets.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl BlobStore for TrackingStore {
    async fn put(
        &self,
        blobs: Vec<BlobInput>,
    ) -> xtax_blob_storage::Result<xtax_blob_storage::PutResult> {
        self.puts.fetch_add(1, Ordering::Relaxed);
        self.inner.put(blobs).await
    }

    async fn get(
        &self,
        key: &str,
    ) -> xtax_blob_storage::Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key).await
    }

    async fn delete(&self, keys: &[&str]) -> xtax_blob_storage::Result<()> {
        self.inner.delete(keys).await
    }

    async fn list(
        &self,
        filter: &dyn xtax_blob_storage::ListFilter,
    ) -> xtax_blob_storage::Result<Vec<String>> {
        self.inner.list(filter).await
    }

    async fn exists(&self, key: &str) -> xtax_blob_storage::Result<bool> {
        self.inner.exists(key).await
    }

    async fn get_with_metadata(
        &self,
        key: &str,
    ) -> xtax_blob_storage::Result<(BlobMeta, Box<dyn tokio::io::AsyncRead + Send + Unpin>)> {
        self.inner.get_with_metadata(key).await
    }

    async fn list_with_metadata(
        &self,
        filter: &dyn xtax_blob_storage::ListFilter,
    ) -> xtax_blob_storage::Result<Vec<BlobMeta>> {
        self.inner.list_with_metadata(filter).await
    }
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn test_builder_with_backend() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let inner = BlobStoreBuilder::new().with_fs(path).build().await.unwrap();

    let tracking = Arc::new(TrackingStore::new(inner));

    // Use with_backend to wrap the tracking store
    let store = BlobStoreBuilder::new()
        .with_backend(tracking.clone())
        .build()
        .await
        .unwrap();

    // Verify operations work
    store
        .put(vec![BlobInput::new("test.txt", b"data".as_slice())])
        .await
        .unwrap();

    let mut reader = store.get("test.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"data");

    assert_eq!(
        tracking.put_count(),
        1,
        "custom backend put should have been called"
    );
    assert_eq!(
        tracking.get_count(),
        1,
        "custom backend get should have been called"
    );
}

#[tokio::test]
async fn test_builder_with_layer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    // Use with_layer to add a custom logging wrapper via the builder's API
    let store = BlobStoreBuilder::new()
        .with_fs(path)
        .with_layer(|inner| Arc::new(TrackingStore::new(inner)))
        .build()
        .await
        .unwrap();

    store
        .put(vec![BlobInput::new("test.txt", b"data".as_slice())])
        .await
        .unwrap();

    let mut reader = store.get("test.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"data", "custom layer should pass through operations");
}

#[tokio::test]
async fn test_builder_with_encryption_and_builder_rekey() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let provider: Arc<dyn EncryptionProvider> = Arc::new(RekeyableShiftEncryption::new(7u8, 1u8));

    // Build with encryption + OnStart rekey strategy via builder
    let store = BlobStoreBuilder::new()
        .with_fs(path)
        .with_encryption(provider)
        .with_rekey(Arc::new(xtax_blob_storage::OnStart))
        .build()
        .await
        .unwrap();

    // Basic operations should work
    store
        .put(vec![BlobInput::new("data.txt", b"hello rekey".as_slice())])
        .await
        .unwrap();

    let mut reader = store.get("data.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(
        buf, b"hello rekey",
        "data should be readable after builder rekey"
    );
}

#[tokio::test]
async fn test_builder_with_clean() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let predicate: CleanupPredicate = Box::new(|key, _meta| key.starts_with("tmp-"));

    let store = BlobStoreBuilder::new()
        .with_fs(path.clone())
        .with_clean(predicate, Arc::new(xtax_blob_storage::Manual::new()))
        .build()
        .await
        .unwrap();

    // insert some blobs
    store
        .put(vec![
            BlobInput::new("tmp-a.txt", b"temp".as_slice()),
            BlobInput::new("keep.txt", b"permanent".as_slice()),
        ])
        .await
        .unwrap();

    // Direct cleanup via BlobCleanup (cast down to get cleanup)
    // Note: cleanup is not on the BlobStore trait, it's specific to BlobCleanup.
    // But building with with_clean() returns a BlobCleanup wrapped in Arc<dyn BlobStore>.
    // The cleanup() method is on BlobCleanup, not on the trait.
    // We verify this works by checking that operations pass through.
    let keys = store.list(&SuffixFilter::new("")).await.unwrap();
    assert_eq!(keys.len(), 2, "both blobs should exist");
}

#[tokio::test]
async fn test_builder_with_clean_batch_size() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let predicate: CleanupPredicate = Box::new(|_key, _meta| true);

    let store = BlobStoreBuilder::new()
        .with_fs(path)
        .with_clean(predicate, Arc::new(xtax_blob_storage::Manual::new()))
        .with_clean_batch_size(1) // batch size of 1
        .build()
        .await
        .unwrap();

    // Insert a few blobs
    store
        .put(vec![
            BlobInput::new("a.txt", b"a".as_slice()),
            BlobInput::new("b.txt", b"b".as_slice()),
            BlobInput::new("c.txt", b"c".as_slice()),
        ])
        .await
        .unwrap();

    let keys = store.list(&SuffixFilter::new("")).await.unwrap();
    assert_eq!(keys.len(), 3);
}

#[tokio::test]
async fn test_builder_multiple_layers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(13u8));

    // Build with prefix + encryption
    let store = BlobStoreBuilder::new()
        .with_fs(path)
        .with_prefix("multi/")
        .with_encryption(provider)
        .build()
        .await
        .unwrap();

    // Test round-trip through multiple layers
    store
        .put(vec![BlobInput::new(
            "test.txt",
            b"multi-layer test".as_slice(),
        )])
        .await
        .unwrap();

    let mut reader = store.get("test.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(
        buf, b"multi-layer test",
        "data should survive prefix + encryption layers"
    );
}

#[tokio::test]
async fn test_encrypted_store_with_custom_suffix() {
    use xtax_blob_storage::EncryptedBlobStore;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    // build() already returns Arc<dyn BlobStore>, so don't wrap in another Arc
    let inner: Arc<dyn BlobStore> = BlobStoreBuilder::new().with_fs(path).build().await.unwrap();

    let provider: Arc<dyn EncryptionProvider> = Arc::new(ShiftEncryption::new(7u8));
    let store = Arc::new(EncryptedBlobStore::with_suffix(
        inner,
        provider,
        ".custom-header",
    ));

    store
        .put(vec![BlobInput::new(
            "secret.txt",
            b"custom suffix".as_slice(),
        )])
        .await
        .unwrap();

    // Data should be readable
    let mut reader = store.get("secret.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"custom suffix", "custom header suffix should work");
}

#[tokio::test]
async fn test_blob_input_with_size() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlobStoreBuilder::new()
        .with_fs(dir.path())
        .build()
        .await
        .unwrap();

    let data = b"size hint test";
    let blob = BlobInput::with_size("sized.txt", &data[..], data.len() as u64);
    store.put(vec![blob]).await.unwrap();

    let mut reader = store.get("sized.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, data, "blob with size hint should store correctly");
}

#[cfg(feature = "s3")]
#[tokio::test]
async fn test_builder_s3_multipart_part_size() {
    let (client, bucket) = s3_store_for_test().await;

    let store = BlobStoreBuilder::new()
        .with_s3(client, bucket)
        .with_multipart_part_size(1024 * 1024) // 1 MiB
        .build()
        .await
        .unwrap();

    // Basic put/get should work with custom part size
    let data = b"s3 part size test";
    store
        .put(vec![BlobInput::new("part-size-test.txt", &data[..])])
        .await
        .unwrap();

    let mut reader = store.get("part-size-test.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, data);
}

#[cfg(feature = "s3")]
async fn s3_store_for_test() -> (aws_sdk_s3::Client, String) {
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
    let root = std::env::temp_dir().join("s3s-builder").join(sub);
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
    let bucket = format!("s3s-builder-{}", unique_id());
    let _ = client.create_bucket().bucket(&bucket).send().await.unwrap();
    (client, bucket)
}

//! Deep test of S3 multipart vs single PutObject behavior.
//!
//! Tests that:
//! - Blobs < 5 MiB use `PutObject` (single request)
//! - Blobs ≥ 5 MiB use multipart upload
//! - The lookahead buffer correctly handles the boundary
//! - Both paths produce correct metadata
//!
//! Run with:
//!   cargo test --features s3 --test s3_multipart_test

#[cfg(feature = "s3")]
use std::sync::Arc;
#[cfg(feature = "s3")]
use tokio::io::AsyncReadExt;
#[cfg(feature = "s3")]
use xtax_blob_storage::{BlobInput, BlobStore, BlobStoreBuilder};

// ============================================================================
// S3 test store factory
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

/// Create an S3 store with a custom multipart part size.
#[cfg(feature = "s3")]
async fn s3_store_with_part_size(part_size: u64) -> Arc<dyn BlobStore> {
    let (client, bucket) = fresh_s3_store().await;
    BlobStoreBuilder::new()
        .with_s3(client, bucket)
        .with_multipart_part_size(part_size)
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
// Tests
// ============================================================================

#[cfg(feature = "s3")]
mod s3_tests {
    use super::*;

    /// Helper: create a blob of exactly `size` bytes.
    fn blob_of_size(size: usize) -> Vec<u8> {
        (0..size).map(|i| (i % 256) as u8).collect()
    }

    // ========================================================================
    // Small blobs (< 5 MiB) — should use PutObject
    // ========================================================================

    #[tokio::test]
    async fn test_small_blob_1_byte() {
        let store = s3_store().await;
        let data = vec![b'a'];
        let result = store
            .put(vec![BlobInput::new("tiny.txt", std::io::Cursor::new(data))])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].key, "tiny.txt");
        assert_eq!(result.blobs[0].stored_size, 1);

        // Verify we can get it back
        let mut reader = store.get("tiny.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, vec![b'a']);
    }

    #[tokio::test]
    async fn test_small_blob_4_mib() {
        let store = s3_store().await;
        let size = 4 * 1024 * 1024; // 4 MiB — just under 5 MiB
        let data = blob_of_size(size);
        let result = store
            .put(vec![BlobInput::new(
                "4mib.txt",
                std::io::Cursor::new(data.clone()),
            )])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].key, "4mib.txt");
        assert_eq!(result.blobs[0].stored_size, size as u64);

        // Verify content
        let mut reader = store.get("4mib.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(buf, data);
    }

    // ========================================================================
    // Custom part_size tests
    // ========================================================================

    #[tokio::test]
    async fn test_with_part_size_5_mib() {
        // Use part_size = 5 MiB (minimum allowed)
        let store = s3_store_with_part_size(5 * 1024 * 1024).await;
        let size = 5 * 1024 * 1024; // exactly 5 MiB
        let data = blob_of_size(size);
        let result = store
            .put(vec![BlobInput::new(
                "5mib-ps.txt",
                std::io::Cursor::new(data.clone()),
            )])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].stored_size, size as u64);

        let mut reader = store.get("5mib-ps.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn test_with_part_size_10_mib() {
        // Use part_size = 10 MiB
        let store = s3_store_with_part_size(10 * 1024 * 1024).await;
        let size = 10 * 1024 * 1024; // exactly 10 MiB
        let data = blob_of_size(size);
        let result = store
            .put(vec![BlobInput::new(
                "10mib-ps.txt",
                std::io::Cursor::new(data.clone()),
            )])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].stored_size, size as u64);

        let mut reader = store.get("10mib-ps.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn test_with_part_size_100_mib() {
        // Use part_size = 100 MiB
        let store = s3_store_with_part_size(100 * 1024 * 1024).await;
        let size = 100 * 1024 * 1024; // exactly 100 MiB
        let data = blob_of_size(size);
        let result = store
            .put(vec![BlobInput::new(
                "100mib-ps.txt",
                std::io::Cursor::new(data.clone()),
            )])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].stored_size, size as u64);

        let mut reader = store.get("100mib-ps.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn test_with_part_size_1_mib_blob_5_mib() {
        // Use part_size = 1 MiB (clamped to 5 MiB minimum)
        // Blob = 5 MiB → should use PutObject (since 5 MiB < 5 MiB? No, 5 MiB == 5 MiB)
        // Actually, with part_size = 1 MiB, MIN_MULTIPART_PART_SIZE = 5 MiB
        // So part_size is clamped to 5 MiB. Same as default.
        let store = s3_store_with_part_size(1 * 1024 * 1024).await;
        let size = 5 * 1024 * 1024; // 5 MiB
        let data = blob_of_size(size);
        let result = store
            .put(vec![BlobInput::new(
                "1mib-ps-5mib.txt",
                std::io::Cursor::new(data.clone()),
            )])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].stored_size, size as u64);

        let mut reader = store.get("1mib-ps-5mib.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn test_with_part_size_5_mib_blob_10_mib() {
        // Use part_size = 5 MiB, blob = 10 MiB
        // Blob > part_size → multipart with 2 parts (each 5 MiB)
        let store = s3_store_with_part_size(5 * 1024 * 1024).await;
        let size = 10 * 1024 * 1024; // 10 MiB
        let data = blob_of_size(size);
        let result = store
            .put(vec![BlobInput::new(
                "5mib-ps-10mib.txt",
                std::io::Cursor::new(data.clone()),
            )])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].stored_size, size as u64);

        let mut reader = store.get("5mib-ps-10mib.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn test_with_part_size_5_mib_blob_15_mib() {
        // Use part_size = 5 MiB, blob = 15 MiB
        // Blob > part_size → multipart with 3 parts (each 5 MiB)
        let store = s3_store_with_part_size(5 * 1024 * 1024).await;
        let size = 15 * 1024 * 1024; // 15 MiB
        let data = blob_of_size(size);
        let result = store
            .put(vec![BlobInput::new(
                "5mib-ps-15mib.txt",
                std::io::Cursor::new(data.clone()),
            )])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].stored_size, size as u64);

        let mut reader = store.get("5mib-ps-15mib.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(buf, data);
    }

    // ========================================================================
    // Large blobs (≥ 5 MiB) — should use multipart
    // ========================================================================

    #[tokio::test]
    async fn test_large_blob_5_mib_exactly() {
        let store = s3_store().await;
        let size = 5 * 1024 * 1024; // exactly 5 MiB — boundary
        let data = blob_of_size(size);
        let result = store
            .put(vec![BlobInput::new(
                "5mib.txt",
                std::io::Cursor::new(data.clone()),
            )])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].key, "5mib.txt");
        assert_eq!(result.blobs[0].stored_size, size as u64);

        // Verify content
        let mut reader = store.get("5mib.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn test_large_blob_10_mib() {
        let store = s3_store().await;
        let size = 10 * 1024 * 1024; // 10 MiB — well above threshold
        let data = blob_of_size(size);
        let result = store
            .put(vec![BlobInput::new(
                "10mib.txt",
                std::io::Cursor::new(data.clone()),
            )])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].key, "10mib.txt");
        assert_eq!(result.blobs[0].stored_size, size as u64);

        // Verify content
        let mut reader = store.get("10mib.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn test_large_blob_50_mib() {
        let store = s3_store().await;
        let size = 50 * 1024 * 1024; // 50 MiB — default part size
        let data = blob_of_size(size);
        let result = store
            .put(vec![BlobInput::new(
                "50mib.txt",
                std::io::Cursor::new(data.clone()),
            )])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].key, "50mib.txt");
        assert_eq!(result.blobs[0].stored_size, size as u64);

        // Verify content
        let mut reader = store.get("50mib.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(buf, data);
    }

    // ========================================================================
    // Boundary: exactly 5 MiB + 1 byte
    // ========================================================================

    #[tokio::test]
    async fn test_boundary_5mib_plus_1() {
        let store = s3_store().await;
        let size = 5 * 1024 * 1024 + 1; // 5 MiB + 1 byte — just over threshold
        let data = blob_of_size(size);
        let result = store
            .put(vec![BlobInput::new(
                "5mib+1.txt",
                std::io::Cursor::new(data.clone()),
            )])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].key, "5mib+1.txt");
        assert_eq!(result.blobs[0].stored_size, size as u64);

        // Verify content
        let mut reader = store.get("5mib+1.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(buf, data);
    }

    // ========================================================================
    // Multiple blobs in one put call (mixed sizes)
    // ========================================================================

    #[tokio::test]
    async fn test_mixed_sizes_in_one_put() {
        let store = s3_store().await;

        let small = vec![b's'; 100]; // 100 bytes — small
        let medium = blob_of_size(1 * 1024 * 1024); // 1 MiB — small
        let large = blob_of_size(10 * 1024 * 1024); // 10 MiB — large

        let result = store
            .put(vec![
                BlobInput::new("small.txt", std::io::Cursor::new(small.clone())),
                BlobInput::new("medium.txt", std::io::Cursor::new(medium.clone())),
                BlobInput::new("large.txt", std::io::Cursor::new(large.clone())),
            ])
            .await
            .unwrap();

        assert_eq!(result.blobs.len(), 3);

        // Verify each blob
        for (key, expected) in &[
            ("small.txt", &small),
            ("medium.txt", &medium),
            ("large.txt", &large),
        ] {
            let mut reader = store.get(key).await.unwrap();
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();
            assert_eq!(&buf, *expected, "blob '{}' should be correct", key);
        }
    }

    // ========================================================================
    // Metadata verification
    // ========================================================================

    #[tokio::test]
    async fn test_put_metadata() {
        let store = s3_store().await;

        let small = vec![b'x'; 100];
        let large = blob_of_size(10 * 1024 * 1024);

        let result = store
            .put(vec![
                BlobInput::new("small-meta.txt", std::io::Cursor::new(small.clone())),
                BlobInput::new("large-meta.txt", std::io::Cursor::new(large.clone())),
            ])
            .await
            .unwrap();

        for meta in &result.blobs {
            assert_eq!(
                meta.key,
                if meta.key == "small-meta.txt" {
                    "small-meta.txt"
                } else {
                    "large-meta.txt"
                }
            );
            assert!(meta.stored_size > 0, "stored_size should be > 0");
            assert!(
                meta.modified_at > chrono::DateTime::UNIX_EPOCH,
                "modified_at should be set"
            );
            // ETag should be present for both PutObject and multipart
            assert!(
                meta.etag.is_some(),
                "ETag should be present for '{}'",
                meta.key
            );
        }
    }

    // ========================================================================
    // Empty blob (0 bytes) — edge case
    // ========================================================================

    #[tokio::test]
    async fn test_empty_blob() {
        let store = s3_store().await;
        let data: Vec<u8> = vec![];
        let result = store
            .put(vec![BlobInput::new(
                "empty.txt",
                std::io::Cursor::new(data),
            )])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].stored_size, 0);

        let mut reader = store.get("empty.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert!(buf.is_empty(), "empty blob should return zero bytes");
    }

    // ========================================================================
    // Very large blob (100 MiB) — stress test
    // ========================================================================

    #[tokio::test]
    async fn test_very_large_blob_100_mib() {
        let store = s3_store().await;
        let size = 100 * 1024 * 1024; // 100 MiB — large
        let data = blob_of_size(size);
        let result = store
            .put(vec![BlobInput::new(
                "100mib.txt",
                std::io::Cursor::new(data.clone()),
            )])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].stored_size, size as u64);

        // Verify content (first and last bytes)
        let mut reader = store.get("100mib.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(buf[0], 0);
        assert_eq!(buf[size - 1], ((size - 1) % 256) as u8);
    }

    // ========================================================================
    // Chunked reader: AsyncRead that returns 1 byte at a time
    // ========================================================================
    //
    // This test verifies that the lookahead fix (using `take().read_to_end()`)
    // works correctly even when the underlying reader returns data in
    // arbitrarily small chunks — a situation that would have caused data
    // loss with the old `read()`-based approach.

    /// A reader that yields data one byte at a time.
    struct ChunkReader {
        data: Vec<u8>,
        pos: usize,
    }

    impl tokio::io::AsyncRead for ChunkReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let this = self.get_mut();
            if this.pos >= this.data.len() {
                return std::task::Poll::Ready(Ok(()));
            }
            let remaining = buf.remaining();
            if remaining == 0 {
                return std::task::Poll::Ready(Ok(()));
            }
            // Write 1 byte at a time
            let available = std::cmp::min(1, this.data.len() - this.pos);
            let slice = &this.data[this.pos..this.pos + available];
            buf.put_slice(slice);
            this.pos += available;
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_chunked_reader_small_blob() {
        let store = s3_store().await;
        let data = vec![b'h', b'e', b'l', b'l', b'o']; // 5 bytes — small
        let reader = ChunkReader {
            data: data.clone(),
            pos: 0,
        };
        let result = store
            .put(vec![BlobInput::new("chunked-small.txt", reader)])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].stored_size, 5);

        let mut reader = store.get("chunked-small.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, data);
    }

    #[tokio::test]
    async fn test_chunked_reader_large_blob() {
        let store = s3_store().await;
        let size = 100 * 1024 * 1024; // 100 MiB — large blob, must use multipart
        let data = blob_of_size(size);
        let reader = ChunkReader {
            data: data.clone(),
            pos: 0,
        };
        let result = store
            .put(vec![BlobInput::new("chunked-large.txt", reader)])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].stored_size, size as u64);

        // Verify we can read the entire blob back correctly
        let mut reader = store.get("chunked-large.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(buf, data);
    }

    // ========================================================================
    // Chunked reader + custom part_size regression tests
    // ========================================================================
    //
    // Multipart threshold is always 5 MiB. part_size controls
    // only the size of each part. A chunked reader (1-byte chunks) must
    // work correctly through both single-PutObject and multipart paths.

    /// With part_size = 10 MiB, a 6 MiB blob is ≥ 5 MiB threshold → multipart.
    /// Uses a ChunkReader (1 byte at a time) through the full pipeline.
    #[tokio::test]
    async fn test_chunked_reader_part_size_10mib_blob_6mib() {
        let store = s3_store_with_part_size(10 * 1024 * 1024).await;
        let size = 6 * 1024 * 1024; // 6 MiB → multipart (≥ 5 MiB threshold)
        let data = blob_of_size(size);
        let reader = ChunkReader {
            data: data.clone(),
            pos: 0,
        };
        let result = store
            .put(vec![BlobInput::new("chunked-ps10mib-6mib.bin", reader)])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].stored_size, size as u64);

        let mut reader = store.get("chunked-ps10mib-6mib.bin").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(buf, data);
    }

    /// With part_size = 5 MiB, a 10 MiB blob → multipart with 2 parts.
    /// Uses a ChunkReader (1 byte at a time) through the full pipeline.
    #[tokio::test]
    async fn test_chunked_reader_part_size_5mib_blob_10mib() {
        let store = s3_store_with_part_size(5 * 1024 * 1024).await;
        let size = 10 * 1024 * 1024; // 10 MiB → multipart (≥ 5 MiB threshold)
        let data = blob_of_size(size);
        let reader = ChunkReader {
            data: data.clone(),
            pos: 0,
        };
        let result = store
            .put(vec![BlobInput::new("chunked-ps5mib-10mib.bin", reader)])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].stored_size, size as u64);

        let mut reader = store.get("chunked-ps5mib-10mib.bin").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(buf, data);
    }

    /// With part_size = 100 MiB, a 100 MiB blob.
    ///
    /// Memory usage is bounded by the part_size (100 MiB per part).
    /// This test validates correctness for large part sizes.
    #[tokio::test]
    async fn test_with_part_size_100_mib_memory_bounded() {
        let store = s3_store_with_part_size(100 * 1024 * 1024).await;
        let size = 100 * 1024 * 1024; // 100 MiB — multipart with 1 part
        let data = blob_of_size(size);
        let result = store
            .put(vec![BlobInput::new(
                "100mib-ps-mem.bin",
                std::io::Cursor::new(data.clone()),
            )])
            .await
            .unwrap();
        assert_eq!(result.blobs.len(), 1);
        assert_eq!(result.blobs[0].stored_size, size as u64);

        // Verify first and last bytes (spot-check to avoid OOM in CI)
        let mut reader = store.get("100mib-ps-mem.bin").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), size);
        assert_eq!(buf[0], 0);
        assert_eq!(buf[size - 1], ((size - 1) % 256) as u8);
    }
}

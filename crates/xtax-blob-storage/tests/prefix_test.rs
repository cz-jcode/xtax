//! Regression tests for key validation through the prefix layer.
//! Run with: cargo test -p xtax-blob-storage --test prefix_test

#![cfg(feature = "fs")]

use std::sync::Arc;

use tokio::io::AsyncReadExt;
use xtax_blob_storage::{BlobInput, BlobStorageError, BlobStore, BlobStoreBuilder};

mod empty_key_validation {
    use super::*;

    async fn store_with_file_prefix() -> (tempfile::TempDir, Arc<dyn BlobStore>) {
        let directory = tempfile::tempdir().unwrap();
        let backend = BlobStoreBuilder::new()
            .with_fs(directory.path())
            .build()
            .await
            .unwrap();

        // This valid physical key must never be accessible through an empty logical key.
        backend
            .put(vec![BlobInput::new("tenant-", b"existing data".as_slice())])
            .await
            .unwrap();
        let store = BlobStoreBuilder::new()
            .with_backend(backend)
            .with_prefix("tenant-")
            .build()
            .await
            .unwrap();
        (directory, store)
    }

    #[tokio::test]
    async fn putting_an_empty_key_is_rejected_even_with_a_file_prefix() {
        let (directory, store) = store_with_file_prefix().await;

        let result = store
            .put(vec![BlobInput::new("", b"replacement data".as_slice())])
            .await;

        assert!(
            matches!(result, Err(BlobStorageError::InvalidInput(_))),
            "prefixing must not turn an empty user key into a valid write"
        );
        assert_eq!(
            tokio::fs::read(directory.path().join("tenant-"))
                .await
                .unwrap(),
            b"existing data",
            "rejecting the empty key must preserve the existing file's contents"
        );
    }

    #[tokio::test]
    async fn an_empty_key_at_the_end_rejects_the_entire_prefixed_put_batch() {
        let (_directory, store) = store_with_file_prefix().await;
        store
            .put(vec![BlobInput::new(
                "invoice.pdf",
                b"original invoice".as_slice(),
            )])
            .await
            .unwrap();

        let result = store
            .put(vec![
                BlobInput::new("invoice.pdf", b"replacement invoice".as_slice()),
                BlobInput::new("new.pdf", b"new invoice".as_slice()),
                BlobInput::new("", b"invalid data".as_slice()),
            ])
            .await;

        assert!(
            matches!(result, Err(BlobStorageError::InvalidInput(_))),
            "an empty logical key must reject the batch before any prefixed write"
        );
        let mut reader = store.get("invoice.pdf").await.unwrap();
        let mut content = Vec::new();
        reader.read_to_end(&mut content).await.unwrap();
        assert_eq!(
            content, b"original invoice",
            "the original invoice must survive"
        );
        assert!(
            !store.exists("new.pdf").await.unwrap(),
            "the rejected batch must not create a new invoice"
        );
    }

    #[tokio::test]
    async fn a_valid_key_can_still_be_written_and_read_with_a_file_prefix() {
        let (directory, store) = store_with_file_prefix().await;
        store
            .put(vec![BlobInput::new(
                "invoice.pdf",
                b"invoice data".as_slice(),
            )])
            .await
            .unwrap();

        let mut reader = store.get("invoice.pdf").await.unwrap();
        let mut content = Vec::new();
        reader.read_to_end(&mut content).await.unwrap();

        assert_eq!(content, b"invoice data");
        assert!(
            directory.path().join("tenant-invoice.pdf").is_file(),
            "file prefixes without a trailing slash remain supported"
        );
    }
}

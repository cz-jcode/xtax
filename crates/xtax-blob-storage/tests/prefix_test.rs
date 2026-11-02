//! Regression tests for filters and key validation through the prefix layer.
//! Run with: cargo test -p xtax-blob-storage --test prefix_test

#![cfg(feature = "fs")]

use std::sync::Arc;

use tokio::io::AsyncReadExt;
use xtax_blob_storage::{
    BlobInput, BlobMeta, BlobStorageError, BlobStore, BlobStoreBuilder, ListFilter,
};

mod metadata_filters {
    use super::*;

    /// Unknown sizes are included, as in the custom filter example in docs/filters.md.
    struct AtLeastOneHundredBytes;

    impl ListFilter for AtLeastOneHundredBytes {
        fn matches(&self, _key: &str, meta: Option<&BlobMeta>) -> bool {
            match meta {
                Some(meta) => meta.stored_size >= 100,
                None => true,
            }
        }
    }

    #[tokio::test]
    async fn listing_through_a_prefix_must_not_report_a_large_blob_as_zero_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let store = BlobStoreBuilder::new()
            .with_fs(directory.path())
            .with_prefix("customer-42/")
            .build()
            .await
            .unwrap();
        store
            .put(vec![BlobInput::new(
                "invoice.pdf",
                std::io::Cursor::new(b"x".repeat(128)),
            )])
            .await
            .unwrap();

        let keys = store.list(&AtLeastOneHundredBytes).await.unwrap();

        assert_eq!(
            keys,
            vec!["invoice.pdf"],
            "a 128-byte blob must pass a 100-byte filter; unavailable metadata must stay None"
        );
    }

    struct InvoiceByLogicalKey;

    impl ListFilter for InvoiceByLogicalKey {
        fn matches(&self, key: &str, meta: Option<&BlobMeta>) -> bool {
            key == "invoice.pdf" && meta.is_none_or(|meta| meta.key == "invoice.pdf")
        }
    }

    #[tokio::test]
    async fn metadata_filters_must_receive_the_key_without_the_storage_prefix() {
        let directory = tempfile::tempdir().unwrap();
        let store = BlobStoreBuilder::new()
            .with_fs(directory.path())
            .with_prefix("customer-42/")
            .build()
            .await
            .unwrap();
        store
            .put(vec![BlobInput::new(
                "invoice.pdf",
                b"invoice data".as_slice(),
            )])
            .await
            .unwrap();

        let blobs = store
            .list_with_metadata(&InvoiceByLogicalKey)
            .await
            .unwrap();

        assert_eq!(
            blobs.len(),
            1,
            "both key and meta.key must be invoice.pdf when the filter runs"
        );
        assert_eq!(blobs[0].key, "invoice.pdf");
    }
}

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
    async fn getting_an_empty_key_is_rejected_even_when_the_prefixed_file_exists() {
        let (_directory, store) = store_with_file_prefix().await;

        let result = store.get("").await;

        assert!(
            matches!(result, Err(BlobStorageError::InvalidInput(_))),
            "an empty user key must not expose the file named tenant-"
        );
    }

    #[tokio::test]
    async fn deleting_an_empty_key_is_rejected_even_when_the_prefixed_file_exists() {
        let (directory, store) = store_with_file_prefix().await;

        let result = store.delete(&[""]).await;

        assert!(
            matches!(result, Err(BlobStorageError::InvalidInput(_))),
            "an empty user key must not delete the file named tenant-"
        );
        assert_eq!(
            tokio::fs::read(directory.path().join("tenant-"))
                .await
                .unwrap(),
            b"existing data",
            "rejecting the empty key must preserve the physical file"
        );
    }

    #[tokio::test]
    async fn an_empty_key_at_the_end_rejects_the_entire_prefixed_delete_batch() {
        let (directory, store) = store_with_file_prefix().await;
        store
            .put(vec![BlobInput::new(
                "invoice.pdf",
                b"original invoice".as_slice(),
            )])
            .await
            .unwrap();

        let result = store.delete(&["invoice.pdf", ""]).await;

        assert!(
            matches!(result, Err(BlobStorageError::InvalidInput(_))),
            "an empty logical key must reject the entire batch before deleting any files"
        );
        let mut reader = store.get("invoice.pdf").await.unwrap();
        let mut content = Vec::new();
        reader.read_to_end(&mut content).await.unwrap();
        assert_eq!(content, b"original invoice", "the valid blob must survive");
        assert_eq!(
            tokio::fs::read(directory.path().join("tenant-"))
                .await
                .unwrap(),
            b"existing data",
            "the empty key must not delete the file named tenant-"
        );
    }

    #[tokio::test]
    async fn checking_an_empty_key_is_rejected_even_when_the_prefixed_file_exists() {
        let (_directory, store) = store_with_file_prefix().await;

        let result = store.exists("").await;

        assert!(
            matches!(result, Err(BlobStorageError::InvalidInput(_))),
            "an empty user key must be rejected before checking the physical file"
        );
    }

    #[tokio::test]
    async fn getting_metadata_for_an_empty_key_is_rejected_even_with_a_file_prefix() {
        let (_directory, store) = store_with_file_prefix().await;

        let result = store.get_with_metadata("").await;

        assert!(
            matches!(result, Err(BlobStorageError::InvalidInput(_))),
            "an empty user key must not expose metadata for the file named tenant-"
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

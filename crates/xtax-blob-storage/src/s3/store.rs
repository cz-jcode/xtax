use crate::blob_store::BlobStore;
use crate::error::{BatchError, BlobStorageError, KeyError, PerKeyError, Result};
use crate::list_filter::ListFilter;
use crate::s3::MIN_MULTIPART_PART_SIZE;
use crate::types::{BlobInput, BlobMeta, PutResult};
use crate::validate::validate_blob_key;
use crate::visitor::BlobVisitor;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::DateTime;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::instrument;

/// Convert an `aws_sdk_s3::primitives::DateTime` into a chrono `DateTime<Utc>`.
fn s3_time_to_utc(t: &aws_sdk_s3::primitives::DateTime) -> DateTime<chrono::Utc> {
    DateTime::from_timestamp(t.secs(), t.subsec_nanos()).unwrap_or_default()
}

#[async_trait]
impl BlobStore for super::S3BlobStore {
    #[instrument(skip(self, blobs))]
    async fn put(&self, blobs: Vec<BlobInput>) -> Result<PutResult> {
        let count = blobs.len();
        let mut metas = Vec::with_capacity(count);

        for blob in blobs {
            validate_blob_key(&blob.key)?;
            let mut reader = blob.data;

            // 1. Allocate a BytesMut container up to the 5 MiB capacity floor.
            // This does not zero-out or touch the memory until data fills it.
            let mut lookahead = BytesMut::with_capacity(MIN_MULTIPART_PART_SIZE as usize);
            let mut total_read = 0usize;

            // 2. Read DIRECTLY into the lookahead container. No intermediate buffer!
            while total_read < MIN_MULTIPART_PART_SIZE as usize {
                // read_buf reads straight from the reader into the spare capacity of lookahead
                let n = reader.read_buf(&mut lookahead).await.map_err(|e| {
                    BlobStorageError::Storage {
                        message: format!("failed to read initial chunk for key '{}'", blob.key),
                        source: Some(Box::new(e)),
                    }
                })?;

                if n == 0 {
                    break; // EOF
                }
                total_read += n;
            }

            // Freeze turns BytesMut into a read-only, thread-safe, incredibly cheap-to-clone Bytes token.
            // It does not clone the underlying data arrays.
            let lookahead_bytes: Bytes = lookahead.freeze();

            if (total_read as u64) < MIN_MULTIPART_PART_SIZE {
                // Blob fits in lookahead entirely. Pass the zero-copy Bytes object.
                let meta = self.put_object(&blob.key, lookahead_bytes).await?;
                tracing::debug!(key = %blob.key, size = meta.stored_size, "Stored blob via S3 PutObject");
                metas.push(meta);
            } else {
                // Blob is >= 5 MiB.
                // Turn the frozen Bytes directly into an AsyncRead slice wrapper.
                let lookahead_reader: &[u8] = &lookahead_bytes;
                let combined = lookahead_reader.chain(reader);
                let mut boxed: Box<dyn AsyncRead + Send + Unpin> = Box::new(combined);

                let meta = self.upload_multipart(&blob.key, &mut boxed).await?;
                tracing::debug!(key = %blob.key, size = meta.stored_size, "Stored blob via S3 multipart");
                metas.push(meta);
            }
        }

        tracing::debug!(count, "Stored blobs via S3");
        Ok(PutResult::multiple(metas))
    }

    #[instrument(skip(self))]
    async fn get(&self, key: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        validate_blob_key(key)?;
        let output = self.get_object_output(key).await?;
        tracing::debug!(key, "Retrieved blob via S3");
        Ok(Box::new(output.body.into_async_read()))
    }

    #[instrument(skip(self))]
    async fn delete(&self, keys: &[&str]) -> Result<()> {
        for key in keys {
            validate_blob_key(key)?;
        }

        let mut succeeded = Vec::new();
        let mut errors = Vec::new();

        for chunk in keys.chunks(1000) {
            let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = chunk
                .iter()
                .map(|k| {
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key(k.to_string())
                        .build()
                        .expect("valid ObjectIdentifier")
                })
                .collect();

            let delete = aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(objects))
                .build()
                .expect("valid Delete");

            let response = self
                .client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete)
                .send()
                .await
                .map_err(|e| {
                    if self.is_misconfigured(&e) {
                        BlobStorageError::BackendMisconfigured(format!(
                            "S3 bucket '{}' does not exist or is not accessible",
                            self.bucket
                        ))
                    } else {
                        BlobStorageError::Storage {
                            message: "S3 batch delete failed".to_string(),
                            source: Some(Box::new(e)),
                        }
                    }
                })?;

            // Collect successfully deleted keys from response
            for deleted in response.deleted() {
                if let Some(key) = deleted.key() {
                    succeeded.push(key.to_string());
                }
            }

            // Map per-object errors to structured PerKeyError
            for s3_err in response.errors() {
                let key = s3_err.key().unwrap_or("unknown").to_string();
                let code = s3_err.code().unwrap_or("Unknown");
                let message = s3_err.message().unwrap_or("").to_string();

                let per_key = match code {
                    "AccessDenied" => PerKeyError::PermissionDenied(if message.is_empty() {
                        code.to_string()
                    } else {
                        format!("{code}: {message}")
                    }),
                    "NoSuchKey" | "NotFound" => {
                        // S3 DeleteObjects typically doesn't return NoSuchKey,
                        // but handle it for consistency — idempotent delete.
                        tracing::debug!(key, "Blob already gone (not found) during S3 delete");
                        succeeded.push(key);
                        continue;
                    }
                    _ => PerKeyError::Unknown {
                        message: if message.is_empty() {
                            code.to_string()
                        } else {
                            format!("{code}: {message}")
                        },
                    },
                };

                tracing::warn!(key, error = %per_key, "Failed to delete blob via S3");
                errors.push(KeyError {
                    key,
                    error: per_key,
                });
            }

            tracing::debug!(count = %chunk.len(), "Processed batch of blobs via S3");
        }

        if errors.is_empty() {
            let total = keys.len();
            tracing::debug!(total, "Deleted blobs via S3");
            Ok(())
        } else {
            Err(BlobStorageError::Batch(BatchError { succeeded, errors }))
        }
    }

    #[instrument(skip(self, filter))]
    async fn list(&self, filter: &dyn ListFilter) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;
        let mut page_count = 0u32;

        loop {
            let mut req = self.client.list_objects_v2().bucket(&self.bucket);

            // Use prefix hint to reduce the number of keys S3 needs to enumerate
            if let Some(prefix) = filter.prefix_hint()
                && !prefix.is_empty()
            {
                req = req.prefix(prefix);
            }

            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }

            let output = req.send().await.map_err(|e| {
                if self.is_misconfigured(&e) {
                    BlobStorageError::BackendMisconfigured(format!(
                        "S3 bucket '{}' does not exist or is not accessible",
                        self.bucket
                    ))
                } else {
                    BlobStorageError::Storage {
                        message: "S3 list failed".to_string(),
                        source: Some(Box::new(e)),
                    }
                }
            })?;
            page_count += 1;

            for obj in output.contents() {
                if let Some(key) = obj.key()
                    && filter.matches(key, None)
                {
                    keys.push(key.to_string());
                }
            }

            continuation_token = output.next_continuation_token().map(|s| s.to_string());
            if continuation_token.is_none() {
                break;
            }
        }

        keys.sort();
        tracing::debug!(count = %keys.len(), page_count, "Listed blobs via S3");
        Ok(keys)
    }

    #[instrument(skip(self))]
    async fn exists(&self, key: &str) -> Result<bool> {
        validate_blob_key(key)?;
        let result = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;

        let exists = match result {
            Ok(_) => Ok(true),
            Err(e) if self.is_misconfigured(&e) => {
                Err(BlobStorageError::BackendMisconfigured(format!(
                    "S3 bucket '{}' does not exist or is not accessible",
                    self.bucket
                )))
            }
            Err(e) if self.is_not_found(&e) => Ok(false),
            Err(e) => Err(BlobStorageError::Storage {
                message: format!("S3 head failed for key '{key}'"),
                source: Some(Box::new(e)),
            }),
        };
        tracing::debug!(key, ?exists, "Checked blob existence via S3");
        exists
    }

    #[instrument(skip(self))]
    async fn get_with_metadata(
        &self,
        key: &str,
    ) -> Result<(BlobMeta, Box<dyn AsyncRead + Send + Unpin>)> {
        validate_blob_key(key)?;
        let output = self.get_object_output(key).await?;

        let size = output.content_length().unwrap_or(0) as u64;
        let etag = output.e_tag().map(|s| s.to_string());
        let modified_at = output.last_modified().map(s3_time_to_utc);

        let meta = BlobMeta {
            key: key.to_string(),
            stored_size: size,
            modified_at: modified_at.unwrap_or_default(),
            etag,
        };

        tracing::debug!(key, size, "Retrieved blob with metadata via S3");
        Ok((meta, Box::new(output.body.into_async_read())))
    }

    #[instrument(skip(self, filter, visitor))]
    async fn visit(&self, filter: &dyn ListFilter, visitor: &mut dyn BlobVisitor) -> Result<()> {
        let mut continuation_token: Option<String> = None;
        let mut visited_count = 0u64;

        loop {
            let mut req = self.client.list_objects_v2().bucket(&self.bucket);

            // Use prefix hint to limit the S3 list scope
            if let Some(prefix) = filter.prefix_hint()
                && !prefix.is_empty()
            {
                req = req.prefix(prefix);
            }

            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }

            let output = req.send().await.map_err(|e| {
                if self.is_misconfigured(&e) {
                    BlobStorageError::BackendMisconfigured(format!(
                        "S3 bucket '{}' does not exist or is not accessible",
                        self.bucket
                    ))
                } else {
                    BlobStorageError::Storage {
                        message: "S3 list failed".to_string(),
                        source: Some(Box::new(e)),
                    }
                }
            })?;

            for obj in output.contents() {
                if let Some(key) = obj.key()
                    && filter.matches(key, None)
                {
                    let size = obj.size().unwrap_or(0) as u64;
                    let etag = obj.e_tag().map(|s| s.to_string());
                    let last_modified = obj.last_modified().map(s3_time_to_utc);
                    let meta = BlobMeta {
                        key: key.to_string(),
                        stored_size: size,
                        modified_at: last_modified.unwrap_or_default(),
                        etag,
                    };
                    visited_count += 1;
                    if !visitor.visit(key, Some(&meta)).await? {
                        return Ok(());
                    }
                }
            }

            continuation_token = output.next_continuation_token().map(|s| s.to_string());
            if continuation_token.is_none() {
                break;
            }
        }

        tracing::debug!(visited_count, "Visited blobs via S3");
        Ok(())
    }

    #[instrument(skip(self, filter))]
    async fn list_with_metadata(&self, filter: &dyn ListFilter) -> Result<Vec<BlobMeta>> {
        let mut metas = Vec::new();
        let mut continuation_token: Option<String> = None;
        let mut page_count = 0u32;

        loop {
            let mut req = self.client.list_objects_v2().bucket(&self.bucket);

            // Use prefix hint to reduce the S3 list scope
            if let Some(prefix) = filter.prefix_hint()
                && !prefix.is_empty()
            {
                req = req.prefix(prefix);
            }

            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }

            let output = req.send().await.map_err(|e| {
                if self.is_misconfigured(&e) {
                    BlobStorageError::BackendMisconfigured(format!(
                        "S3 bucket '{}' does not exist or is not accessible",
                        self.bucket
                    ))
                } else {
                    BlobStorageError::Storage {
                        message: "S3 list failed".to_string(),
                        source: Some(Box::new(e)),
                    }
                }
            })?;
            page_count += 1;

            for obj in output.contents() {
                if let Some(key) = obj.key()
                    && filter.matches(key, None)
                {
                    let size = obj.size().unwrap_or(0) as u64;
                    let etag = obj.e_tag().map(|s| s.to_string());
                    let last_modified = obj.last_modified().map(s3_time_to_utc);

                    metas.push(BlobMeta {
                        key: key.to_string(),
                        stored_size: size,
                        modified_at: last_modified.unwrap_or_default(),
                        etag,
                    });
                }
            }

            continuation_token = output.next_continuation_token().map(|s| s.to_string());
            if continuation_token.is_none() {
                break;
            }
        }

        metas.sort_by(|a, b| a.key.cmp(&b.key));
        tracing::debug!(count = %metas.len(), page_count, "Listed blobs with metadata via S3");
        Ok(metas)
    }
}

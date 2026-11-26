use std::future;

use async_trait::async_trait;
use chrono::Utc;
use futures_util::{Stream, TryStreamExt};
use opendal::{Error, ErrorKind, Metadata, Operator};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::compat::FuturesAsyncReadCompatExt;

use crate::{
    BatchError, BlobInput, BlobMeta, BlobStorageError, BlobStore, BlobVisitor, KeyError,
    ListFilter, PerKeyError, PutResult, Result, validate_blob_key,
};

/// Blob storage backed by an Apache OpenDAL operator.
///
/// The caller configures the operator with a service supporting read, stat,
/// write, delete and list. It can be composed with all built-in blob layers.
pub struct OpenDalBlobStore {
    operator: Operator,
}

impl OpenDalBlobStore {
    /// Wrap an already configured OpenDAL operator.
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    async fn put_blob(&self, mut blob: BlobInput) -> Result<BlobMeta> {
        let key = &blob.key;
        let (size, meta) = if self.operator.info().capability().write_can_multi {
            let mut writer = self
                .operator
                .writer_with(key)
                .chunk(8 * 1024 * 1024)
                .await
                .map_err(|err| map_error(key, err))?;
            let upload: Result<_> = async {
                let mut buffer = vec![0; 64 * 1024];
                let mut size = 0;
                loop {
                    let count = blob.data.read(&mut buffer).await?;
                    if count == 0 {
                        break;
                    }
                    writer
                        .write(bytes::Bytes::copy_from_slice(&buffer[..count]))
                        .await
                        .map_err(|err| map_error(key, err))?;
                    size += count as u64;
                }
                let meta = writer.close().await.map_err(|err| map_error(key, err))?;
                Ok((size, meta))
            }
            .await;
            if upload.is_err()
                && let Err(error) = writer.abort().await
            {
                tracing::warn!(key, %error, "Failed to abort OpenDAL write");
            }
            upload?
        } else {
            // Services that only accept a single write need the entire payload.
            let mut data = Vec::new();
            let size = blob.data.read_to_end(&mut data).await? as u64;
            let meta = self
                .operator
                .write(key, data)
                .await
                .map_err(|err| map_error(key, err))?;
            (size, meta)
        };

        let mut meta = blob_meta(blob.key, &meta);
        meta.stored_size = size;
        Ok(meta)
    }

    async fn matching_blobs<'a>(
        &self,
        filter: &'a dyn ListFilter,
    ) -> Result<impl Stream<Item = Result<BlobMeta>> + use<'a>> {
        let lister = self
            .operator
            .lister_with("/")
            .recursive(true)
            .await
            .map_err(|err| map_error("/", err))?;

        Ok(lister
            .map_err(|err| map_error("/", err))
            .try_filter_map(move |entry| {
                let matched = if entry.metadata().is_file() {
                    let meta = blob_meta(entry.path().to_owned(), entry.metadata());
                    filter.matches(&meta.key, Some(&meta)).then_some(meta)
                } else {
                    None
                };
                future::ready(Ok(matched))
            }))
    }
}

#[async_trait]
impl BlobStore for OpenDalBlobStore {
    async fn put(&self, blobs: Vec<BlobInput>) -> Result<PutResult> {
        for blob in &blobs {
            validate_key(&blob.key)?;
        }
        let mut metas = Vec::with_capacity(blobs.len());
        for blob in blobs {
            metas.push(self.put_blob(blob).await?);
        }
        Ok(PutResult::multiple(metas))
    }

    async fn get(&self, key: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        let (_, reader) = self.get_with_metadata(key).await?;
        Ok(reader)
    }

    async fn get_with_metadata(
        &self,
        key: &str,
    ) -> Result<(BlobMeta, Box<dyn AsyncRead + Send + Unpin>)> {
        validate_key(key)?;
        let meta = self
            .operator
            .stat(key)
            .await
            .map_err(|err| map_error(key, err))?;
        if !meta.is_file() {
            return Err(BlobStorageError::NotFound(key.to_owned()));
        }
        // An overwrite may change the size between stat and the streamed read.
        let reader = self
            .operator
            .reader(key)
            .await
            .map_err(|err| map_error(key, err))?
            .into_futures_async_read(..)
            .await
            .map_err(|err| map_error(key, err))?;
        Ok((blob_meta(key.to_owned(), &meta), Box::new(reader.compat())))
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        validate_key(key)?;
        match self.operator.stat(key).await {
            Ok(meta) => Ok(meta.is_file()),
            Err(err) if matches!(err.kind(), ErrorKind::NotFound | ErrorKind::IsADirectory) => {
                Ok(false)
            }
            Err(err) => Err(map_error(key, err)),
        }
    }

    async fn delete(&self, keys: &[&str]) -> Result<()> {
        for key in keys {
            validate_key(key)?;
        }
        let mut batch = BatchError {
            succeeded: Vec::with_capacity(keys.len()),
            errors: Vec::new(),
        };
        for &key in keys {
            let err = match self.operator.delete(key).await {
                Ok(()) => None,
                Err(err) => match err.kind() {
                    ErrorKind::NotFound | ErrorKind::IsADirectory => None,
                    ErrorKind::PermissionDenied => {
                        Some(PerKeyError::PermissionDenied(err.to_string()))
                    }
                    _ => Some(PerKeyError::Unknown {
                        message: err.to_string(),
                    }),
                },
            };
            if let Some(error) = err {
                batch.errors.push(KeyError {
                    key: key.to_owned(),
                    error,
                });
            } else {
                batch.succeeded.push(key.to_owned());
            }
        }
        if batch.errors.is_empty() {
            Ok(())
        } else {
            Err(batch.into())
        }
    }

    async fn list(&self, filter: &dyn ListFilter) -> Result<Vec<String>> {
        let mut keys: Vec<_> = self
            .matching_blobs(filter)
            .await?
            .map_ok(|meta| meta.key)
            .try_collect()
            .await?;
        keys.sort_unstable();
        Ok(keys)
    }

    async fn list_with_metadata(&self, filter: &dyn ListFilter) -> Result<Vec<BlobMeta>> {
        let mut metas: Vec<_> = self.matching_blobs(filter).await?.try_collect().await?;
        metas.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        Ok(metas)
    }

    async fn visit(&self, filter: &dyn ListFilter, visitor: &mut dyn BlobVisitor) -> Result<()> {
        let mut entries = std::pin::pin!(self.matching_blobs(filter).await?);
        while let Some(meta) = entries.try_next().await? {
            if !visitor.visit(&meta.key, Some(&meta)).await? {
                break;
            }
        }
        Ok(())
    }
}

fn validate_key(key: &str) -> Result<()> {
    validate_blob_key(key)?;
    // OpenDAL trims the entire path before accessing the service.
    if key.trim() != key {
        return Err(BlobStorageError::InvalidInput(format!(
            "OpenDAL blob keys must not start or end with whitespace: '{key}'"
        )));
    }
    Ok(())
}

fn blob_meta(key: String, meta: &Metadata) -> BlobMeta {
    BlobMeta {
        key,
        stored_size: meta.content_length(),
        modified_at: meta
            .last_modified()
            .map(|time| chrono::DateTime::from(std::time::SystemTime::from(time)))
            .unwrap_or_else(Utc::now),
        etag: meta.etag().map(str::to_owned),
    }
}

fn map_error(key: &str, err: Error) -> BlobStorageError {
    match err.kind() {
        ErrorKind::NotFound | ErrorKind::IsADirectory => BlobStorageError::NotFound(key.to_owned()),
        ErrorKind::AlreadyExists => BlobStorageError::AlreadyExists(key.to_owned()),
        ErrorKind::PermissionDenied => BlobStorageError::PermissionDenied(err.to_string()),
        ErrorKind::Unsupported => BlobStorageError::NotSupported(err.to_string()),
        ErrorKind::ConfigInvalid => BlobStorageError::BackendMisconfigured(err.to_string()),
        _ => BlobStorageError::Storage {
            message: format!("OpenDAL operation failed for '{key}'"),
            source: Some(Box::new(err)),
        },
    }
}

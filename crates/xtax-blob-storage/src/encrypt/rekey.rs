use std::io::Cursor;

use async_trait::async_trait;
use tokio::io::BufReader;

use crate::blob_store::BlobStore;
use crate::encrypt::EncryptionProvider;
use crate::error::{BlobStorageError, Result};
use crate::types::{BlobInput, BlobMeta};
use crate::visitor::BlobVisitor;

/// Visitor that processes encryption headers for rekeying.
pub(crate) struct RekeyVisitor<'a> {
    pub(crate) store: &'a dyn BlobStore,
    pub(crate) encryption: &'a dyn EncryptionProvider,
    pub(crate) rekeyed: u64,
}

#[async_trait]
impl BlobVisitor for RekeyVisitor<'_> {
    async fn visit(&mut self, key: &str, _meta: Option<&BlobMeta>) -> Result<bool> {
        let header_data = match self.store.get(key).await {
            Ok(reader) => {
                let mut buf = Vec::new();
                tokio::io::copy(&mut BufReader::new(reader), &mut buf)
                    .await
                    .map_err(|e| BlobStorageError::Storage {
                        message: format!("failed to read header '{key}'"),
                        source: Some(Box::new(e)),
                    })?;
                buf
            }
            Err(e) => {
                tracing::warn!("rekey: failed to fetch header '{key}': {e}");
                return Ok(true); // continue with next header
            }
        };

        match self.encryption.rekey_header(&header_data).await {
            Ok(Some(new_header)) => {
                let new_input = BlobInput::with_size(
                    key.to_string(),
                    Cursor::new(new_header),
                    header_data.len() as u64,
                );
                match self.store.put(vec![new_input]).await {
                    Ok(_) => self.rekeyed += 1,
                    Err(e) => tracing::warn!("rekey: failed to write header '{key}': {e}"),
                }
            }
            Ok(None) => {
                // Already using current key — nothing to do
            }
            Err(e) => {
                tracing::warn!("rekey failed for header '{key}': {e}");
            }
        }

        Ok(true) // continue iteration
    }
}

//! Shared `CollectingVisitor` — a test `BlobVisitor` that records visited keys
//! and metadata for later assertion.
//!
//! # Note
//!
//! `#[allow(dead_code)]` is needed because this file is compiled as part of
//! each integration test crate separately — not all functions are used in
//! every context.

#![allow(dead_code)]

use async_trait::async_trait;
use xtax_blob_storage::{BlobMeta, BlobVisitor};

/// A test visitor that collects keys and metadata from a visit() call.
pub struct CollectingVisitor {
    pub keys: Vec<String>,
    pub metas: Vec<BlobMeta>,
    stop_after: Option<usize>,
}

impl CollectingVisitor {
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            metas: Vec::new(),
            stop_after: None,
        }
    }

    pub fn stop_after(n: usize) -> Self {
        Self {
            keys: Vec::new(),
            metas: Vec::new(),
            stop_after: Some(n),
        }
    }
}

#[async_trait]
impl BlobVisitor for CollectingVisitor {
    async fn visit(
        &mut self,
        key: &str,
        meta: Option<&BlobMeta>,
    ) -> xtax_blob_storage::Result<bool> {
        self.keys.push(key.to_string());
        if let Some(m) = meta {
            self.metas.push(m.clone());
        }
        match self.stop_after {
            Some(n) if self.keys.len() >= n => Ok(false),
            _ => Ok(true),
        }
    }
}

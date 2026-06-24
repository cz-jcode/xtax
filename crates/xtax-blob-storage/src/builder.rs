use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use crate::blob_store::BlobStore;
use crate::cleanup::BlobCleanup;
use crate::cleanup::CleanupPredicate;
use crate::encrypt::EncryptionProvider;
use crate::encrypt::store::EncryptedBlobStore;
use crate::error::Result;
use crate::prefix::PrefixBlobStore;

use self::maintenance::MaintenanceTask;

pub(crate) mod backend;
pub(crate) mod maintenance;
pub(crate) mod strategies;

pub use strategies::{
    BackgroundCancellation, BackgroundContext, BackgroundStrategy, MaintenanceTrigger, Manual,
    OnStart, Periodic,
};

/// A boxed future factory — produces a new future each time it's called.
pub(crate) type TaskFactory =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

// ---------------------------------------------------------------------------
// ShutdownGuard — cancels the token when the outer Arc is dropped
// ---------------------------------------------------------------------------

/// Thin wrapper that holds a `CancellationToken`. When the last `Arc` to this
/// store is dropped, the token is cancelled → all strategy loops stop.
struct ShutdownGuard {
    inner: Arc<dyn BlobStore>,
    cancellation: tokio_util::sync::CancellationToken,
}

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

// Delegate all BlobStore methods to inner
#[async_trait::async_trait]
impl BlobStore for ShutdownGuard {
    async fn put(&self, blobs: Vec<crate::types::BlobInput>) -> Result<crate::types::PutResult> {
        self.inner.put(blobs).await
    }

    async fn get(&self, key: &str) -> Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
        self.inner.get(key).await
    }

    async fn delete(&self, keys: &[&str]) -> Result<()> {
        self.inner.delete(keys).await
    }

    async fn list(&self, filter: &dyn crate::list_filter::ListFilter) -> Result<Vec<String>> {
        self.inner.list(filter).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key).await
    }

    async fn get_with_metadata(
        &self,
        key: &str,
    ) -> Result<(
        crate::types::BlobMeta,
        Box<dyn tokio::io::AsyncRead + Send + Unpin>,
    )> {
        self.inner.get_with_metadata(key).await
    }

    async fn list_with_metadata(
        &self,
        filter: &dyn crate::list_filter::ListFilter,
    ) -> Result<Vec<crate::types::BlobMeta>> {
        self.inner.list_with_metadata(filter).await
    }

    async fn visit(
        &self,
        filter: &dyn crate::list_filter::ListFilter,
        visitor: &mut dyn crate::visitor::BlobVisitor,
    ) -> Result<()> {
        self.inner.visit(filter, visitor).await
    }
}

// ---------------------------------------------------------------------------
// Typestate marker types
// ---------------------------------------------------------------------------

/// Marker: no backend selected yet.
pub struct NoBackend;
/// Marker: filesystem backend selected.
pub struct FsChosen;
/// Marker: S3 backend selected.
#[cfg(feature = "s3")]
pub struct S3Chosen;
/// Marker: custom user-supplied backend selected.
pub struct CustomChosen;

// ---------------------------------------------------------------------------
// LayerKind — internal enum for all manipulation layers
// ---------------------------------------------------------------------------

/// Type alias for a custom layer constructor function.
pub(crate) type CustomLayerFn = Arc<dyn Fn(Arc<dyn BlobStore>) -> Arc<dyn BlobStore> + Send + Sync>;

/// Internal representation of a single layer in the pipeline.
///
/// Layers are stored in order and applied one by one during `build()`.
/// Both built-in (prefix, encryption, cleanup) and custom user-supplied
/// layers use the same mechanism, so order is fully determined by the
/// user's method call order.
pub(crate) enum LayerKind {
    /// Prefix manipulation layer.
    Prefix(String),
    /// Encryption layer with a provider and optional rekey strategy.
    Encryption {
        provider: Arc<dyn EncryptionProvider>,
        rekey_strategy: Option<Arc<dyn BackgroundStrategy>>,
    },
    /// Cleanup layer with predicate, batch size, and optional strategy.
    Cleanup {
        predicate: CleanupPredicate,
        batch_size: usize,
        strategy: Option<Arc<dyn BackgroundStrategy>>,
    },
    /// User-supplied custom layer.
    Custom(CustomLayerFn),
}

impl LayerKind {
    /// Apply this layer on top of `inner`.
    ///
    /// Returns the wrapped store and any maintenance tasks that should
    /// be scheduled (e.g. cleanup or rekey).
    ///
    /// Task factories use `Weak` references to the store so that when the
    /// outer `Arc` is dropped, `weak.upgrade()` returns `None` and the
    /// task is skipped — no leak, no explicit shutdown needed.
    fn wrap(self, inner: Arc<dyn BlobStore>) -> (Arc<dyn BlobStore>, Vec<MaintenanceTask>) {
        match self {
            LayerKind::Prefix(prefix) => (Arc::new(PrefixBlobStore::new(inner, prefix)), vec![]),
            LayerKind::Encryption {
                provider,
                rekey_strategy,
            } => {
                let enc = Arc::new(EncryptedBlobStore::new(inner, provider));
                let mut tasks = vec![];
                if let Some(strategy) = rekey_strategy {
                    let enc_weak: Weak<EncryptedBlobStore> = Arc::downgrade(&enc);
                    let factory: TaskFactory = Arc::new(move || {
                        let enc_weak = enc_weak.clone();
                        Box::pin(async move {
                            let Some(enc) = enc_weak.upgrade() else {
                                tracing::debug!("Rekey task skipped: store already dropped");
                                return;
                            };
                            match enc.rekey().await {
                                Ok(result) => {
                                    tracing::info!(
                                        "Rekey completed: {} headers rekeyed",
                                        result.rekeyed_count
                                    );
                                }
                                Err(e) => {
                                    tracing::error!("Rekey failed: {e}");
                                }
                            }
                        })
                    });
                    tasks.push(MaintenanceTask { factory, strategy });
                }
                (enc as Arc<dyn BlobStore>, tasks)
            }
            LayerKind::Cleanup {
                predicate,
                batch_size,
                strategy,
            } => {
                let mut c = BlobCleanup::new(inner, predicate);
                c = c.with_batch_size(batch_size);
                let c = Arc::new(c);
                let mut tasks = vec![];
                if let Some(strategy) = strategy {
                    let c_weak: Weak<BlobCleanup> = Arc::downgrade(&c);
                    let factory: TaskFactory = Arc::new(move || {
                        let c_weak = c_weak.clone();
                        Box::pin(async move {
                            let Some(c) = c_weak.upgrade() else {
                                tracing::debug!("Cleanup task skipped: store already dropped");
                                return;
                            };
                            match c.cleanup().await {
                                Ok(result) => {
                                    tracing::info!(
                                        "Cleanup completed: {} blobs deleted",
                                        result.deleted_count
                                    );
                                }
                                Err(e) => {
                                    tracing::error!("Cleanup failed: {e}");
                                }
                            }
                        })
                    });
                    tasks.push(MaintenanceTask { factory, strategy });
                }
                (c as Arc<dyn BlobStore>, tasks)
            }
            LayerKind::Custom(f) => (f(inner), vec![]),
        }
    }
}

// ---------------------------------------------------------------------------
// The builder
// ---------------------------------------------------------------------------

/// Typestate builder for constructing a [`BlobStore`] with optional layers.
///
/// Layers are applied in the order they are added — call order determines
/// the wrapping order. Use `with_layer()` for custom manipulation layers,
/// or the built-in convenience methods (`with_prefix`, `with_encryption`,
/// `with_clean`, etc.).
///
/// For full builder documentation see the
/// [Builder reference](https://github.com/cz-jcode/xtax/blob/main/crates/xtax-blob-storage/docs/builder.md).
///
/// # Background tasks & cancellation
///
/// When a cleanup or rekey strategy is configured, background tasks run on a
/// shared sequential worker. Task factories use `Weak` references — when the
/// user drops the store, tasks detect it and skip themselves. The store wrapper
/// cancels a `CancellationToken` that stops all strategy loops. No explicit
/// cleanup is needed.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use std::time::Duration;
/// use xtax_blob_storage::{BlobStoreBuilder, Periodic};
///
/// # #[cfg(feature = "fs")]
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # #[cfg(feature = "fs")]
/// # {
/// let store = BlobStoreBuilder::new()
///     .with_fs("/tmp/data")
///     .with_prefix("my-app/")
///     .with_clean(
///         Box::new(|key, _meta| key.starts_with("tmp-")),
///         Arc::new(Periodic(Duration::from_secs(3600))),
///     )
///     .build()
///     .await?;
///
/// // ... use store ...
/// // When store goes out of scope, background cleanup stops automatically.
/// # Ok(())
/// # }
/// # }
/// # #[cfg(not(feature = "fs"))]
/// # fn main() {}
/// ```
pub struct BlobStoreBuilder<B = NoBackend> {
    backend: Option<backend::BackendKind>,
    /// Ordered list of layers — applied in this order during `build()`.
    layers: Vec<LayerKind>,
    _phantom: PhantomData<B>,
}

impl BlobStoreBuilder<NoBackend> {
    /// Start building a new blob store.
    pub fn new() -> Self {
        Self {
            backend: None,
            layers: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Use the local filesystem as storage backend.
    ///
    /// Blobs are stored as individual files under `root`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use xtax_blob_storage::BlobStoreBuilder;
    ///
    /// # #[cfg(feature = "fs")]
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # #[cfg(feature = "fs")]
    /// # {
    /// let store = BlobStoreBuilder::new()
    ///     .with_fs("/tmp/data")
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// # }
    /// # #[cfg(not(feature = "fs"))]
    /// # fn main() {}
    /// ```
    #[cfg(feature = "fs")]
    pub fn with_fs(self, root: impl Into<std::path::PathBuf>) -> BlobStoreBuilder<FsChosen> {
        BlobStoreBuilder {
            backend: Some(backend::BackendKind::Fs(root.into())),
            layers: self.layers,
            _phantom: PhantomData,
        }
    }

    /// Use S3-compatible storage as backend.
    ///
    /// Works with AWS S3, Garage, MinIO, and any S3-compatible service.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use aws_sdk_s3::Client;
    /// use xtax_blob_storage::BlobStoreBuilder;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::new(&aws_config::load_from_env().await);
    /// let store = BlobStoreBuilder::new()
    ///     .with_s3(client, "my-bucket")
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "s3")]
    pub fn with_s3(
        self,
        client: aws_sdk_s3::Client,
        bucket: impl Into<String>,
    ) -> BlobStoreBuilder<S3Chosen> {
        BlobStoreBuilder {
            backend: Some(backend::BackendKind::S3 {
                client,
                bucket: bucket.into(),
                part_size: crate::s3::DEFAULT_MULTIPART_PART_SIZE,
            }),
            layers: self.layers,
            _phantom: PhantomData,
        }
    }

    /// Use a custom user-supplied backend.
    ///
    /// Any type that implements [`BlobStore`] can be used, enabling
    /// third-party or project-specific backends without modifying this crate.
    ///
    /// All layers (prefix, encryption, cleanup, rekey) work transparently
    /// on top of the custom backend, just as they do with FS and S3.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use xtax_blob_storage::{BlobStore, BlobStoreBuilder};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let custom: Arc<dyn BlobStore> = todo!();
    /// let store = BlobStoreBuilder::new()
    ///     .with_backend(custom)
    ///     .with_prefix("my-app/")
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_backend(self, store: Arc<dyn BlobStore>) -> BlobStoreBuilder<CustomChosen> {
        BlobStoreBuilder {
            backend: Some(backend::BackendKind::Custom(store)),
            layers: self.layers,
            _phantom: PhantomData,
        }
    }
}

impl Default for BlobStoreBuilder<NoBackend> {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Layer methods — available on any builder state
// ---------------------------------------------------------------------------

impl<B> BlobStoreBuilder<B> {
    /// Add a prefix that will be prepended to all blob keys.
    ///
    /// The prefix is stripped from list results, making it transparent
    /// to the caller.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use xtax_blob_storage::BlobStoreBuilder;
    ///
    /// # #[cfg(feature = "fs")]
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # #[cfg(feature = "fs")]
    /// # {
    /// let store = BlobStoreBuilder::new()
    ///     .with_fs("/tmp/data")
    ///     .with_prefix("my-app/")
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// # }
    /// # #[cfg(not(feature = "fs"))]
    /// # fn main() {}
    /// ```
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.layers.push(LayerKind::Prefix(prefix.into()));
        self
    }

    /// Add an encryption layer using the given [`EncryptionProvider`].
    ///
    /// All data will be transparently encrypted/decrypted. Encryption headers
    /// are stored alongside the data with a `.enc-header` suffix.
    ///
    /// Use [`with_rekey`](Self::with_rekey) after this method to configure
    /// automatic re-keying for this encryption layer.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use xtax_blob_storage::{BlobStoreBuilder, EncryptionProvider};
    ///
    /// # #[cfg(feature = "fs")]
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # #[cfg(feature = "fs")]
    /// # {
    /// # let provider: Arc<dyn EncryptionProvider> = todo!();
    /// let store = BlobStoreBuilder::new()
    ///     .with_fs("/tmp/data")
    ///     .with_encryption(provider)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// # }
    /// # #[cfg(not(feature = "fs"))]
    /// # fn main() {}
    /// ```
    pub fn with_encryption(mut self, provider: Arc<dyn EncryptionProvider>) -> Self {
        self.layers.push(LayerKind::Encryption {
            provider,
            rekey_strategy: None,
        });
        self
    }

    /// Configure automatic re-keying for the **most recently added** encryption layer.
    ///
    /// Must be called directly after [`with_encryption`](Self::with_encryption).
    /// If the most recent layer is not an encryption layer, this is a no-op.
    ///
    /// Rekey tasks run on the shared sequential maintenance queue. Tasks use
    /// `Weak` references and auto-skip when the store is dropped.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use xtax_blob_storage::{BlobStoreBuilder, Periodic};
    ///
    /// # #[cfg(feature = "fs")]
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # #[cfg(feature = "fs")]
    /// # {
    /// # let provider: Arc<dyn xtax_blob_storage::EncryptionProvider> = todo!();
    /// let store = BlobStoreBuilder::new()
    ///     .with_fs("/tmp/data")
    ///     .with_encryption(provider)
    ///         .with_rekey(Arc::new(Periodic(Duration::from_secs(3600))))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// # }
    /// # #[cfg(not(feature = "fs"))]
    /// # fn main() {}
    /// ```
    pub fn with_rekey(mut self, strategy: Arc<dyn BackgroundStrategy>) -> Self {
        if let Some(LayerKind::Encryption { rekey_strategy, .. }) = self.layers.last_mut() {
            *rekey_strategy = Some(strategy);
        } else {
            tracing::warn!(
                "with_rekey() called but the most recent layer is not an encryption layer \
                 — ignoring rekey strategy. Call with_rekey() directly after with_encryption()."
            );
        }
        self
    }

    /// Add a cleanup layer with the given predicate and background strategy.
    ///
    /// The `predicate` is called for each blob during cleanup. Return `true` to delete.
    /// Cleanup tasks are pushed to the shared sequential queue. Tasks use `Weak`
    /// references and auto-skip when the store is dropped.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use xtax_blob_storage::{BlobStoreBuilder, Manual};
    ///
    /// # #[cfg(feature = "fs")]
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # #[cfg(feature = "fs")]
    /// # {
    /// let predicate: xtax_blob_storage::CleanupPredicate =
    ///     Box::new(|key, _meta| key.starts_with("tmp-"));
    ///
    /// let store = BlobStoreBuilder::new()
    ///     .with_fs("/tmp/data")
    ///     .with_clean(predicate, Arc::new(Manual::new()))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// # }
    /// # #[cfg(not(feature = "fs"))]
    /// # fn main() {}
    /// ```
    pub fn with_clean(
        mut self,
        predicate: CleanupPredicate,
        strategy: Arc<dyn BackgroundStrategy>,
    ) -> Self {
        self.layers.push(LayerKind::Cleanup {
            predicate,
            batch_size: 1000,
            strategy: Some(strategy),
        });
        self
    }

    /// Set the batch size for the most recently added cleanup layer.
    ///
    /// Keys are accumulated until `batch_size` is reached, then deleted
    /// in a single batch. Defaults to 1000.
    ///
    /// Only effective when `with_clean()` is also configured.
    pub fn with_clean_batch_size(mut self, batch_size: usize) -> Self {
        if let Some(LayerKind::Cleanup { batch_size: bs, .. }) = self.layers.last_mut() {
            *bs = batch_size;
        }
        self
    }

    /// Add a custom manipulation layer.
    ///
    /// The closure receives the current [`Arc<dyn BlobStore>`] and returns
    /// a wrapped version. All built-in layers (prefix, encryption, cleanup)
    /// are internally added the same way — call order determines wrapping order.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use xtax_blob_storage::{BlobStore, BlobStoreBuilder};
    ///
    /// # #[cfg(feature = "fs")]
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # #[cfg(feature = "fs")]
    /// # {
    /// let store = BlobStoreBuilder::new()
    ///     .with_fs("/tmp/data")
    ///     .with_layer(|inner| inner)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// # }
    /// # #[cfg(not(feature = "fs"))]
    /// # fn main() {}
    /// ```
    pub fn with_layer<F>(mut self, f: F) -> Self
    where
        F: Fn(Arc<dyn BlobStore>) -> Arc<dyn BlobStore> + Send + Sync + 'static,
    {
        self.layers.push(LayerKind::Custom(Arc::new(f)));
        self
    }

    /// Validate that all strategies in the configured layers are sane.
    ///
    /// Currently rejects [`Periodic`] with `Duration::ZERO`.
    fn validate_strategies(&self) -> Result<()> {
        for layer in &self.layers {
            match layer {
                LayerKind::Encryption {
                    rekey_strategy: Some(strategy),
                    ..
                } => {
                    strategy.validate()?;
                }
                LayerKind::Cleanup {
                    strategy: Some(strategy),
                    ..
                } => {
                    strategy.validate()?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// S3-specific builder methods
// ---------------------------------------------------------------------------

#[cfg(feature = "s3")]
impl BlobStoreBuilder<S3Chosen> {
    /// Set the size (in bytes) of each part in a multipart upload.
    ///
    /// Must be at least 5 MiB (AWS minimum). Default: 50 MiB.
    /// Blobs ≥ 5 MiB (S3 minimum) automatically use multipart.
    /// Memory usage per part is bounded by this value.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use aws_sdk_s3::Client;
    /// use xtax_blob_storage::BlobStoreBuilder;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::new(&aws_config::load_from_env().await);
    /// let store = BlobStoreBuilder::new()
    ///     .with_s3(client, "my-bucket")
    ///     .with_multipart_part_size(1024 * 1024 * 100) // 100 MiB
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_multipart_part_size(mut self, size: u64) -> Self {
        if let Some(backend::BackendKind::S3 { part_size, .. }) = &mut self.backend {
            *part_size = size.max(crate::s3::MIN_MULTIPART_PART_SIZE);
        }
        self
    }
}

// ---------------------------------------------------------------------------
// Build — available only on states with a backend selected
// ---------------------------------------------------------------------------

impl BlobStoreBuilder<FsChosen> {
    /// Build the blob store, applying all configured layers in order.
    ///
    /// Layers are applied in the order they were added via `with_*` methods.
    /// A shared sequential maintenance queue is created. Cleanup and rekey
    /// tasks (if configured) are pushed to this queue.
    ///
    /// Background tasks use `Weak` references — when the returned store is
    /// dropped, tasks detect it, a cancellation token fires, and strategy loops
    /// stop. No explicit cleanup needed.
    pub async fn build(self) -> Result<Arc<dyn BlobStore>> {
        self.do_build().await
    }
}

#[cfg(feature = "s3")]
impl BlobStoreBuilder<S3Chosen> {
    /// Build the blob store, applying all configured layers in order.
    pub async fn build(self) -> Result<Arc<dyn BlobStore>> {
        self.do_build().await
    }
}

impl BlobStoreBuilder<CustomChosen> {
    /// Build the blob store, applying all configured layers in order.
    pub async fn build(self) -> Result<Arc<dyn BlobStore>> {
        self.do_build().await
    }
}

// Shared build logic
impl<B> BlobStoreBuilder<B> {
    async fn do_build(self) -> Result<Arc<dyn BlobStore>> {
        self.validate_strategies()?;

        let backend = self.backend.expect("backend always set at this point");
        let mut store = backend.build_raw_arc().await?;
        let mut tasks: Vec<MaintenanceTask> = Vec::new();

        for layer in self.layers {
            let (new_store, mut layer_tasks) = layer.wrap(store);
            store = new_store;
            tasks.append(&mut layer_tasks);
        }

        let cancellation = tokio_util::sync::CancellationToken::new();

        // Wrap the store so that dropping it cancels the token
        let guard = Arc::new(ShutdownGuard {
            inner: store,
            cancellation: cancellation.clone(),
        });

        // Spawn background tasks (bound to the cancellation token)
        if !tasks.is_empty() {
            maintenance::spawn_tasks(tasks, cancellation)?;
        }

        Ok(guard)
    }
}

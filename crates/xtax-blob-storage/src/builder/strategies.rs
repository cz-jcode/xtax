use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::builder::TaskFactory;
use crate::error::{BlobStorageError, Result};

// ---------------------------------------------------------------------------
// Public wrappers — no internal types leaked
// ---------------------------------------------------------------------------

/// A handle that can enqueue maintenance tasks.
///
/// Created internally by the builder and passed to strategies via
/// [`BackgroundContext`]. Downstream users never construct this directly.
#[derive(Clone)]
pub struct MaintenanceTrigger {
    sender: mpsc::Sender<Pin<Box<dyn Future<Output = ()> + Send>>>,
    factory: TaskFactory,
}

impl MaintenanceTrigger {
    pub(crate) fn new(
        sender: mpsc::Sender<Pin<Box<dyn Future<Output = ()> + Send>>>,
        factory: TaskFactory,
    ) -> Self {
        Self { sender, factory }
    }

    /// Enqueue one maintenance task asynchronously.
    ///
    /// Waits until the task can be pushed to the shared sequential queue.
    /// Returns an error if the queue has been shut down.
    pub async fn trigger(&self) -> Result<()> {
        let task = (self.factory)();
        self.sender
            .send(task)
            .await
            .map_err(|_| BlobStorageError::InvalidInput("maintenance queue shut down".to_string()))
    }

    /// Try to enqueue one maintenance task without waiting.
    ///
    /// Returns `Err` if the queue is full or has been shut down.
    pub fn try_trigger(&self) -> Result<()> {
        let task = (self.factory)();
        self.sender.try_send(task).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => {
                BlobStorageError::InvalidInput("maintenance queue full".to_string())
            }
            mpsc::error::TrySendError::Closed(_) => {
                BlobStorageError::InvalidInput("maintenance queue shut down".to_string())
            }
        })
    }
}

/// Read-only cancellation token for background scheduling loops.
///
/// Exposes only `cancelled()` (wait for cancellation) and `is_cancelled()`
/// (poll current state). Strategies can **observe** shutdown but cannot
/// **initiate** it — the `cancel()` method is intentionally absent from
/// the public API.
///
/// The inner [`tokio_util::sync::CancellationToken`] is only ever cancelled
/// internally when the last store reference is dropped.
#[derive(Clone)]
pub struct BackgroundCancellation {
    inner: tokio_util::sync::CancellationToken,
}

impl BackgroundCancellation {
    pub(crate) fn new(inner: tokio_util::sync::CancellationToken) -> Self {
        Self { inner }
    }

    /// Wait until background maintenance has been cancelled.
    ///
    /// Cancellation happens when the user drops the last `Arc` to the store.
    pub async fn cancelled(&self) {
        self.inner.cancelled().await;
    }

    /// Return `true` if background maintenance has already been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }
}

/// Context provided to a [`BackgroundStrategy`] during `schedule()`.
///
/// Contains everything a strategy needs:
/// - [`trigger()`](Self::trigger) / [`try_trigger()`](Self::try_trigger) to enqueue maintenance
/// - [`cancellation()`](Self::cancellation) to listen for store shutdown (read‑only)
#[derive(Clone)]
pub struct BackgroundContext {
    maintenance: MaintenanceTrigger,
    cancellation: BackgroundCancellation,
}

impl BackgroundContext {
    pub(crate) fn new(
        maintenance: MaintenanceTrigger,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            maintenance,
            cancellation: BackgroundCancellation::new(cancellation),
        }
    }

    /// Enqueue one maintenance task asynchronously.
    ///
    /// Waits until the task can be pushed to the shared sequential queue.
    pub async fn trigger(&self) -> Result<()> {
        self.maintenance.trigger().await
    }

    /// Try to enqueue one maintenance task without waiting.
    pub fn try_trigger(&self) -> Result<()> {
        self.maintenance.try_trigger()
    }

    /// Returns a read‑only view of the cancellation signal.
    ///
    /// Strategies that run long-lived scheduling loops **must** listen to
    /// this signal and stop when cancellation is signalled. The returned
    /// [`BackgroundCancellation`] does not expose `cancel()` — only the
    /// crate can initiate shutdown.
    pub fn cancellation(&self) -> &BackgroundCancellation {
        &self.cancellation
    }

    /// Convenience: wait until background maintenance is cancelled.
    ///
    /// Equivalent to `self.cancellation().cancelled().await`.
    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }

    /// Convenience: poll whether cancellation has already been signalled.
    ///
    /// Equivalent to `self.cancellation().is_cancelled()`.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

// ---------------------------------------------------------------------------
// BackgroundStrategy trait — now a real public extension point
// ---------------------------------------------------------------------------

/// Trait for scheduling background maintenance tasks (cleanup, rekey).
///
/// Implementations decide **WHEN** maintenance is enqueued. The crate decides
/// **WHAT** maintenance actually does. Strategies receive a
/// [`BackgroundContext`] that lets them enqueue work via
/// [`ctx.trigger().await`](BackgroundContext::trigger) or
/// [`ctx.try_trigger()`](BackgroundContext::try_trigger), and observe
/// shutdown via [`ctx.cancelled().await`](BackgroundContext::cancelled) or
/// [`ctx.cancellation().cancelled().await`](BackgroundCancellation::cancelled).
///
/// All tasks from all strategies run in FIFO order on a single shared
/// sequential worker. Strategies do not see queue internals, task factories,
/// boxed futures, store internals, or raw shutdown primitives.
///
/// # Built-in strategies
///
/// | Type | Behaviour |
/// |---|---|
/// | [`OnStart`] | Runs once immediately during `build()` |
/// | [`Periodic`] | Runs immediately, then repeats every `Duration` |
/// | [`Manual`] | Runs only when [`Manual::trigger()`] is called |
///
/// # Implementing a custom strategy
///
/// ```rust,no_run
/// use std::time::Duration;
/// use xtax_blob_storage::{
///     BackgroundContext,
///     BackgroundStrategy,
///     Result,
/// };
///
/// pub struct EveryFiveMinutes;
///
/// impl BackgroundStrategy for EveryFiveMinutes {
///     fn schedule(&self, ctx: BackgroundContext) -> Result<()> {
///         tokio::spawn(async move {
///             loop {
///                 tokio::select! {
///                     _ = ctx.cancelled() => {
///                         break;
///                     }
///                     _ = tokio::time::sleep(Duration::from_secs(300)) => {
///                         if let Err(err) = ctx.trigger().await {
///                             tracing::warn!(?err, "failed to enqueue maintenance task");
///                             break;
///                         }
///                     }
///                 }
///             }
///         });
///
///         Ok(())
///     }
/// }
/// ```
///
/// # Shutdown
///
/// Strategies that run long-lived scheduling loops **must** listen to
/// [`ctx.cancelled()`](BackgroundContext::cancelled) or
/// [`ctx.cancellation().cancelled()`](BackgroundCancellation::cancelled).
/// When the signal fires (when the user drops the last `Arc` to the store),
/// the strategy MUST stop its scheduling loop. This prevents background
/// tasks from keeping the store alive forever.
///
/// The cancellation signal is **read‑only** for strategies — custom
/// implementations can observe shutdown but cannot initiate it.
///
/// # Custom validation
///
/// Override [`validate()`](Self::validate) to reject invalid configuration
/// (e.g., zero durations). The builder calls `validate()` before `schedule()`.
pub trait BackgroundStrategy: Send + Sync + 'static {
    /// Called during `build()`.
    ///
    /// The strategy decides when to enqueue maintenance.
    fn schedule(&self, ctx: BackgroundContext) -> Result<()>;

    /// Called during `build()` to reject invalid strategy configuration.
    fn validate(&self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// OnStart
// ---------------------------------------------------------------------------

/// Strategy that runs the task once immediately.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use xtax_blob_storage::{BlobStoreBuilder, OnStart};
///
/// # #[cfg(feature = "fs")]
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # #[cfg(feature = "fs")]
/// # {
/// let store = BlobStoreBuilder::new()
///     .with_fs("/tmp/data")
///     .with_clean(
///         Box::new(|key, _meta| key.starts_with("tmp-")),
///         Arc::new(OnStart),
///     )
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// # }
/// # #[cfg(not(feature = "fs"))]
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy)]
pub struct OnStart;

impl BackgroundStrategy for OnStart {
    fn schedule(&self, ctx: BackgroundContext) -> Result<()> {
        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = ctx.cancelled() => {
                    tracing::debug!("OnStart task skipped — cancellation before send");
                }
                result = ctx.trigger() => {
                    if let Err(e) = result {
                        tracing::debug!(?e, "OnStart task send failed");
                    }
                }
            }
        });

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Periodic
// ---------------------------------------------------------------------------

/// Strategy that runs the task immediately, then repeats periodically.
///
/// The inner [`std::time::Duration`] must be **non-zero**. Passing `Duration::ZERO` causes
/// `build()` to return an `Err(InvalidInput(...))`.
///
/// # Example
///
/// ```rust,no_run
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
///     .with_clean(
///         Box::new(|key, _meta| key.starts_with("tmp-")),
///         std::sync::Arc::new(Periodic(Duration::from_secs(3600))),
///     )
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// # }
/// # #[cfg(not(feature = "fs"))]
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Periodic(pub std::time::Duration);

impl Periodic {
    /// Returns `true` if the duration is zero.
    pub(crate) fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl BackgroundStrategy for Periodic {
    fn validate(&self) -> Result<()> {
        if self.is_zero() {
            return Err(BlobStorageError::InvalidInput(
                "Periodic duration must be non-zero".to_string(),
            ));
        }
        Ok(())
    }

    fn schedule(&self, ctx: BackgroundContext) -> Result<()> {
        if self.is_zero() {
            return Ok(());
        }

        let period = self.0;

        tokio::spawn(async move {
            if ctx.trigger().await.is_err() {
                return; // channel closed
            }

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(period) => {},
                    _ = ctx.cancelled() => {
                        break; // cancellation signalled
                    }
                }

                if ctx.is_cancelled() {
                    break;
                }

                if ctx.trigger().await.is_err() {
                    break; // channel closed
                }
            }
        });

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Manual
// ---------------------------------------------------------------------------

/// Strategy that only runs the task when [`trigger()`](Manual::trigger) is called.
///
/// # Semantics
///
/// - **State-based**: `trigger()` is not edge-only. A trigger fired after
///   the Manual strategy has been registered during build is not lost,
///   even if the spawned scheduling loop has not polled yet.
/// - Triggers fired before the Manual strategy is registered are not
///   replayed.
/// - **Coalescing**: multiple rapid `trigger()` calls may coalesce into a
///   single enqueued task if the receiver has not polled between them.
///   Maintenance tasks are expected to be safe to run repeatedly and are
///   executed sequentially.
/// - **Multi-registration**: a single `Manual` can control multiple background
///   maintenance registrations. A trigger observed by multiple registered
///   strategies may enqueue one maintenance task per registration; all tasks
///   still run through the shared sequential queue.
/// - **Not a counting semaphore**: `Manual` does not guarantee one enqueued
///   task per `trigger()` call. It is intended as a manual “maintenance
///   requested” signal, not an exact event counter.
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
/// let manual = Arc::new(Manual::new());
/// let store = BlobStoreBuilder::new()
///     .with_fs("/tmp/data")
///     .with_rekey(manual.clone())
///     .build()
///     .await?;
///
/// // Later, trigger rekey manually:
/// manual.trigger();
/// # Ok(())
/// # }
/// # }
/// # #[cfg(not(feature = "fs"))]
/// # fn main() {}
/// ```
///
/// # Cancellation
///
/// The spawned scheduling loop stops when the cancellation token fires
/// (when the user drops the last `Arc` to the store) or when the
/// [`watch::Sender`] is dropped.
#[derive(Clone)]
pub struct Manual {
    inner: Arc<ManualInner>,
}

struct ManualInner {
    tx: watch::Sender<u64>,
}

impl Manual {
    /// Create a new `Manual` strategy handle.
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(0);
        Self {
            inner: Arc::new(ManualInner { tx }),
        }
    }

    /// Trigger maintenance.
    ///
    /// State-based — a trigger is observed by all Manual strategies
    /// registered during build, even if the spawned scheduling loop has
    /// not polled yet. Multiple rapid triggers may coalesce (see [Manual]
    /// struct-level docs).
    pub fn trigger(&self) {
        let current = *self.inner.tx.borrow();
        let _ = self.inner.tx.send(current.wrapping_add(1));
    }
}

impl Default for Manual {
    fn default() -> Self {
        Self::new()
    }
}

impl BackgroundStrategy for Manual {
    fn schedule(&self, ctx: BackgroundContext) -> Result<()> {
        let mut rx = self.inner.tx.subscribe();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = ctx.cancelled() => break,

                    changed = rx.changed() => {
                        if changed.is_err() {
                            // Sender has been dropped
                            break;
                        }

                        if ctx.trigger().await.is_err() {
                            tracing::debug!("Manual task send failed — queue shut down");
                            break;
                        }
                    }
                }
            }
        });

        Ok(())
    }
}

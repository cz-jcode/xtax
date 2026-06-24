//! Background strategy tests — verifies OnStart, Periodic, and Manual
//! strategies schedule tasks correctly through the builder.
//!
//! Run with:
//!   cargo test --test strategy_test                    # FS only

#![cfg(feature = "fs")]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use xtax_blob_storage::{
    BlobInput, BlobStoreBuilder, CleanupPredicate, EncryptionProvider, Manual, OnStart, Periodic,
    SuffixFilter,
};

#[path = "common/encrypt.rs"]
mod common_encrypt;
use common_encrypt::*;

// ============================================================================
// Manual strategy via builder
// ============================================================================

#[tokio::test]
async fn test_manual_strategy_trigger_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let manual = Arc::new(Manual::new());
    let predicate: CleanupPredicate = Box::new(|key, _meta| key.starts_with("tmp-"));

    let store = BlobStoreBuilder::new()
        .with_fs(path.clone())
        .with_clean(predicate, manual.clone())
        .build()
        .await
        .unwrap();

    // Put matching + non-matching blobs
    store
        .put(vec![
            BlobInput::new("tmp-file.txt", b"temp".as_slice()),
            BlobInput::new("keep.txt", b"permanent".as_slice()),
        ])
        .await
        .unwrap();

    // Trigger cleanup via Manual
    manual.trigger();

    // Let the background worker process
    tokio::time::sleep(Duration::from_millis(200)).await;

    let remaining = store.list(&SuffixFilter::new("")).await.unwrap();
    assert_eq!(
        remaining.len(),
        1,
        "cleanup should delete tmp-file.txt, remaining: {:?}",
        remaining
    );
    assert!(remaining.contains(&"keep.txt".to_string()));

    // Second trigger — idempotent
    manual.trigger();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let remaining2 = store.list(&SuffixFilter::new("")).await.unwrap();
    assert_eq!(remaining2.len(), 1, "second trigger should be idempotent");
}

// ============================================================================
// OnStart strategy via builder
// ============================================================================

#[tokio::test]
async fn test_onstart_rekey_store_readable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let provider: Arc<dyn EncryptionProvider> = Arc::new(RekeyableShiftEncryption::new(7u8, 1u8));

    let store = BlobStoreBuilder::new()
        .with_fs(path)
        .with_encryption(provider)
        .with_rekey(Arc::new(OnStart))
        .build()
        .await
        .unwrap();

    store
        .put(vec![BlobInput::new("data.txt", b"test data".as_slice())])
        .await
        .unwrap();

    let mut reader = store.get("data.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(
        buf, b"test data",
        "encrypted data should be readable with OnStart rekey"
    );
}

// ============================================================================
// Periodic(Duration::ZERO) rejected
// ============================================================================

#[tokio::test]
async fn test_periodic_duration_zero_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let predicate: CleanupPredicate = Box::new(|_key, _meta| true);

    let result = BlobStoreBuilder::new()
        .with_fs(path)
        .with_clean(predicate, Arc::new(Periodic(Duration::ZERO)))
        .build()
        .await;

    assert!(
        result.is_err(),
        "Periodic(Duration::ZERO) should be rejected"
    );
    let msg = result.err().unwrap().to_string();
    assert!(
        msg.contains("non-zero"),
        "error should mention non-zero duration, got: {msg}"
    );
}

// ============================================================================
// Combined: Manual cleanup with passthrough
// ============================================================================

#[tokio::test]
async fn test_builder_cleanup_with_manual_passthrough() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let predicate: CleanupPredicate = Box::new(|key, _meta| key.starts_with("tmp-"));

    let store = BlobStoreBuilder::new()
        .with_fs(path)
        .with_clean(predicate, Arc::new(Manual::new()))
        .build()
        .await
        .unwrap();

    store
        .put(vec![
            BlobInput::new("test.txt", b"hello".as_slice()),
            BlobInput::new("tmp-data.txt", b"temp".as_slice()),
        ])
        .await
        .unwrap();

    let keys = store.list(&SuffixFilter::new("")).await.unwrap();
    assert_eq!(keys.len(), 2, "both blobs should be present before cleanup");
}

// ============================================================================
// External/custom BackgroundStrategy implementation
// ============================================================================

/// Custom external strategy: triggers once on build, then never again.
struct TriggerOnce;

impl xtax_blob_storage::BackgroundStrategy for TriggerOnce {
    fn schedule(&self, ctx: xtax_blob_storage::BackgroundContext) -> xtax_blob_storage::Result<()> {
        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = ctx.cancelled() => {
                    tracing::debug!("TriggerOnce skipped — cancellation before send");
                }
                result = ctx.trigger() => {
                    if let Err(e) = result {
                        tracing::debug!(?e, "TriggerOnce send failed");
                    }
                }
            }
        });
        Ok(())
    }
}

#[tokio::test]
async fn test_external_strategy() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    // Pre-populate with a blob so cleanup predicate fires
    std::fs::write(path.join("some-blob"), b"data").unwrap();

    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let c = counter.clone();
    let predicate: CleanupPredicate = Box::new(move |_key, _meta| {
        c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        false
    });

    let store = BlobStoreBuilder::new()
        .with_fs(path)
        .with_clean(predicate, Arc::new(TriggerOnce))
        .with_clean_batch_size(1)
        .build()
        .await
        .expect("custom strategy should be accepted");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let count = counter.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        count, 1,
        "custom TriggerOnce strategy should execute exactly once, got {count}"
    );

    drop(store);
}

// ============================================================================
// Manual: trigger immediately after build is observed (state-based)
// ============================================================================

#[tokio::test]
async fn manual_trigger_immediately_after_build() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    // Pre-populate with a blob so cleanup predicate fires
    std::fs::write(path.join("tmp-blob"), b"data").unwrap();

    let manual = Arc::new(Manual::new());
    let predicate: CleanupPredicate = Box::new(|key, _meta| key.starts_with("tmp-"));

    let store = BlobStoreBuilder::new()
        .with_fs(path)
        .with_clean(predicate, manual.clone())
        .with_clean_batch_size(1)
        .build()
        .await
        .unwrap();

    // Trigger IMMEDIATELY after build — with state-based watch, this must
    // not be lost even if the spawned task hasn't polled rx.changed() yet.
    manual.trigger();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let remaining = store.list(&SuffixFilter::new("")).await.unwrap();
    assert!(
        remaining.is_empty(),
        "tmp-blob should be deleted by immediate trigger, but found: {remaining:?}"
    );

    drop(store);
}

// ============================================================================
// Manual: multiple quick triggers coalesce (documented semantics)
// ============================================================================

#[tokio::test]
async fn manual_multiple_quick_triggers_coalesce() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    // Pre-populate
    std::fs::write(path.join("blob-a"), b"data").unwrap();

    let manual = Arc::new(Manual::new());

    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let c = counter.clone();
    let predicate: CleanupPredicate = Box::new(move |_key, _meta| {
        c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        false
    });

    let store = BlobStoreBuilder::new()
        .with_fs(path)
        .with_clean(predicate, manual.clone())
        .with_clean_batch_size(1)
        .build()
        .await
        .unwrap();

    // Fire many triggers rapidly — they should coalesce into fewer tasks
    for _ in 0..20 {
        manual.trigger();
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let count = counter.load(std::sync::atomic::Ordering::SeqCst);
    // At least 1 task must have been enqueued
    assert!(count >= 1, "expected at least 1 task, got {count}");
    // Coalescing means we won't get all 20 triggers — verify that holds
    assert!(count < 20, "expected coalescing (< 20 tasks), got {count}");

    drop(store);
}

// ============================================================================
// Manual: scheduling loop stops on cancellation (store drop)
// ============================================================================

#[tokio::test]
async fn manual_loop_stops_on_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    // Strategy: observe cancellation via the same watch/counter mechanism Manual uses internally
    let cancelled = Arc::new(tokio::sync::Notify::new());
    let cancelled_clone = cancelled.clone();

    struct ManualCancelObserver {
        done: Arc<tokio::sync::Notify>,
    }

    impl xtax_blob_storage::BackgroundStrategy for ManualCancelObserver {
        fn schedule(
            &self,
            ctx: xtax_blob_storage::BackgroundContext,
        ) -> xtax_blob_storage::Result<()> {
            let done = self.done.clone();

            tokio::spawn(async move {
                ctx.cancelled().await;
                done.notify_one();
            });

            Ok(())
        }
    }

    let predicate: CleanupPredicate = Box::new(|_key, _meta| false);
    let strategy = Arc::new(ManualCancelObserver {
        done: cancelled_clone,
    });

    let store = BlobStoreBuilder::new()
        .with_fs(path)
        .with_clean(predicate, strategy)
        .build()
        .await
        .unwrap();

    // Drop the store — cancellation must fire
    drop(store);

    tokio::time::timeout(Duration::from_secs(5), cancelled.notified())
        .await
        .expect("Manual-like loop should receive cancellation and notify done");
}

// ============================================================================
// Custom strategy returning Err makes build() fail
// ============================================================================

/// Strategy that always fails during schedule().
struct BrokenStrategy;

impl xtax_blob_storage::BackgroundStrategy for BrokenStrategy {
    fn schedule(
        &self,
        _ctx: xtax_blob_storage::BackgroundContext,
    ) -> xtax_blob_storage::Result<()> {
        Err(xtax_blob_storage::BlobStorageError::InvalidInput(
            "broken strategy".to_string(),
        ))
    }
}

#[tokio::test]
async fn custom_strategy_err_fails_build() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let predicate: CleanupPredicate = Box::new(|_key, _meta| false);

    let result = BlobStoreBuilder::new()
        .with_fs(path)
        .with_clean(predicate, Arc::new(BrokenStrategy))
        .build()
        .await;

    assert!(
        result.is_err(),
        "build() should fail when strategy.schedule() returns Err"
    );
}

// ============================================================================
// Built-in strategies still build successfully (sanity check)
// ============================================================================

#[tokio::test]
async fn builtin_strategies_build_successfully() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let predicate: CleanupPredicate = Box::new(|_key, _meta| false);

    // OnStart
    let _store = BlobStoreBuilder::new()
        .with_fs(path.clone())
        .with_clean(Box::new(|_key, _meta| false), Arc::new(OnStart))
        .build()
        .await
        .expect("OnStart should build successfully");

    // Periodic (non-zero)
    let _store = BlobStoreBuilder::new()
        .with_fs(path.clone())
        .with_clean(
            Box::new(|_key, _meta| false),
            Arc::new(Periodic(Duration::from_secs(60))),
        )
        .build()
        .await
        .expect("Periodic should build successfully");

    // Manual
    let _store = BlobStoreBuilder::new()
        .with_fs(path.clone())
        .with_clean(predicate, Arc::new(Manual::new()))
        .build()
        .await
        .expect("Manual should build successfully");
}

// ============================================================================
// Custom strategy can await cancellation via cancelled()
// ============================================================================

/// Custom strategy that doesn't trigger — just waits for cancellation
/// and records it.
struct WaitForCancellation {
    done: Arc<tokio::sync::Notify>,
}

impl xtax_blob_storage::BackgroundStrategy for WaitForCancellation {
    fn schedule(&self, ctx: xtax_blob_storage::BackgroundContext) -> xtax_blob_storage::Result<()> {
        let notify = self.done.clone();
        tokio::spawn(async move {
            ctx.cancelled().await;
            notify.notify_one();
        });
        Ok(())
    }
}

#[tokio::test]
async fn test_custom_strategy_can_await_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    let done = Arc::new(tokio::sync::Notify::new());

    let store = BlobStoreBuilder::new()
        .with_fs(path)
        .with_clean(
            Box::new(|_key, _meta| false),
            Arc::new(WaitForCancellation { done: done.clone() }),
        )
        .build()
        .await
        .unwrap();

    // Drop the store — strategy must receive the cancellation signal
    drop(store);

    // Wait for the strategy to record cancellation (with timeout)
    tokio::time::timeout(Duration::from_secs(5), done.notified())
        .await
        .expect("strategy should receive cancellation");
}

// ============================================================================
// One Manual trigger is observed by multiple registered Manual scheduling loops.
// ============================================================================

#[tokio::test]
async fn test_one_manual_triggers_multiple_waiters() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    // Pre-populate with a blob so cleanup predicates fire
    std::fs::write(path.join("some-blob"), b"data").unwrap();

    let manual = Arc::new(Manual::new());

    let counter1 = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter2 = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let c1 = counter1.clone();
    let predicate1: CleanupPredicate = Box::new(move |_key, _meta| {
        c1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        false
    });

    let c2 = counter2.clone();
    let predicate2: CleanupPredicate = Box::new(move |_key, _meta| {
        c2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        false
    });

    let store = BlobStoreBuilder::new()
        .with_fs(path)
        .with_clean(predicate1, manual.clone())
        .with_clean_batch_size(1)
        .with_clean(predicate2, manual.clone())
        .with_clean_batch_size(1)
        .build()
        .await
        .unwrap();

    // Give the spawned Manual tasks time to register their watch receivers
    tokio::time::sleep(Duration::from_millis(50)).await;

    // One trigger — both layers should fire
    manual.trigger();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let count1 = counter1.load(std::sync::atomic::Ordering::SeqCst);
    let count2 = counter2.load(std::sync::atomic::Ordering::SeqCst);

    assert_eq!(
        count1, 1,
        "first cleanup layer should execute once, got {count1}"
    );
    assert_eq!(
        count2, 1,
        "second cleanup layer should also execute once, got {count2}"
    );

    drop(store);
}

// ============================================================================
// Shutdown of periodic strategy — verify strategy stops when store is dropped
// ============================================================================

#[tokio::test]
async fn test_periodic_shutdown_stops() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    // Pre-populate
    std::fs::write(path.join("some-blob"), b"data").unwrap();

    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut builder = BlobStoreBuilder::new().with_fs(&path);

    let pc = counter.clone();
    let predicate: CleanupPredicate = Box::new(move |_key, _meta| {
        pc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        false
    });
    builder = builder
        .with_clean(predicate, Arc::new(Periodic(Duration::from_millis(10))))
        .with_clean_batch_size(1);

    let store = builder.build().await.expect("build with periodic");

    // Let it fire a few times
    tokio::time::sleep(Duration::from_millis(100)).await;

    let before_drop = counter.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        before_drop >= 1,
        "expected at least 1 periodic tick before drop"
    );

    // Drop the store — should stop the strategy loops
    drop(store);

    // Wait a bit to let any in-flight triggers complete
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after_drop = counter.load(std::sync::atomic::Ordering::SeqCst);

    // Wait longer than the interval — counter should NOT increase significantly
    tokio::time::sleep(Duration::from_millis(100)).await;
    let final_count = counter.load(std::sync::atomic::Ordering::SeqCst);

    // Allow at most 1 extra tick for any task that was already in flight
    assert!(
        final_count <= after_drop + 1,
        "periodic strategy should stop after store is dropped \
         (after drop: {after_drop}, final: {final_count})"
    );
}

// ============================================================================
// Sequential execution — tasks from different strategies run in order
// ============================================================================

#[tokio::test]
async fn test_sequential_execution() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    // Pre-populate with a single blob so each layer fires exactly once
    std::fs::write(path.join("blob-1"), b"data1").unwrap();

    let execution_order = Arc::new(std::sync::Mutex::new(Vec::new()));

    // First cleanup layer: OnStart
    let order1 = execution_order.clone();
    let predicate1: CleanupPredicate = Box::new(move |_key, _meta| {
        order1.lock().unwrap().push("OnStart");
        false
    });

    // Second cleanup layer: Manual
    let order2 = execution_order.clone();
    let predicate2: CleanupPredicate = Box::new(move |_key, _meta| {
        order2.lock().unwrap().push("Manual");
        false
    });

    let manual = Arc::new(Manual::new());

    let mut builder = BlobStoreBuilder::new().with_fs(path);
    builder = builder
        .with_clean(predicate1, Arc::new(OnStart))
        .with_clean_batch_size(1);
    builder = builder
        .with_clean(predicate2, manual.clone())
        .with_clean_batch_size(1);

    let store = builder.build().await.expect("build with two strategies");

    // Give the spawned Manual task time to register its watch receiver
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Trigger manual
    manual.trigger();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let order = execution_order.lock().unwrap().clone();
    assert!(
        order.len() >= 2,
        "expected at least 2 callbacks, got {:?}",
        order
    );
    assert_eq!(
        order[0], "OnStart",
        "OnStart cleanup should execute first, got {:?}",
        order
    );
    // With a single blob and batch_size=1, Manual cleanup runs second
    assert_eq!(
        order[1], "Manual",
        "Manual cleanup should execute second (sequential), got {:?}",
        order
    );

    drop(store);
}

// ============================================================================
// Full-queue scenarios — verify that the reliable async send does not
// silently drop tasks when many OnStart layers are configured.
// ============================================================================

/// Verify that when many OnStart layers are configured (more than the
/// internal queue capacity of 16), no tasks are silently lost.
///
/// Pre-populates the FS store with a blob so the cleanup predicate fires
/// on each OnStart task execution.
#[tokio::test]
async fn many_onstart_no_silent_loss() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    // Pre-populate the root with a blob so the store's list/visit finds it.
    std::fs::write(path.join("some-blob"), b"data").unwrap();

    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let num_layers = 32usize; // 2x the queue capacity (16)

    let mut builder = BlobStoreBuilder::new().with_fs(path);
    for _ in 0..num_layers {
        let c = counter.clone();
        let predicate: CleanupPredicate = Box::new(move |_key, _meta| {
            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            false
        });
        builder = builder
            .with_clean(predicate, Arc::new(OnStart))
            .with_clean_batch_size(1);
    }

    let store = builder.build().await.expect("build with many OnStart");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let final_count = counter.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        final_count, num_layers,
        "expected all {num_layers} OnStart tasks to execute, but {final_count} did"
    );

    drop(store);
}

/// Verify that the initial Periodic tick is reliable even when the queue
/// is already full from many OnStart tasks.
#[tokio::test]
async fn periodic_initial_tick_not_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let _ = dir.keep();

    // Pre-populate with a blob so cleanup predicates fire.
    std::fs::write(path.join("some-blob"), b"data").unwrap();

    let onstart_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let periodic_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut builder = BlobStoreBuilder::new().with_fs(&path);

    // Fill the queue with many OnStart tasks — more than the 16 capacity.
    let onstart_num = 30usize;
    for _ in 0..onstart_num {
        let c = onstart_count.clone();
        let predicate: CleanupPredicate = Box::new(move |_key, _meta| {
            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            false
        });
        builder = builder
            .with_clean(predicate, Arc::new(OnStart))
            .with_clean_batch_size(1);
    }

    // Also add a Periodic cleanup with a very short interval.
    let pc = periodic_count.clone();
    let predicate: CleanupPredicate = Box::new(move |_key, _meta| {
        pc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        false
    });
    builder = builder
        .with_clean(predicate, Arc::new(Periodic(Duration::from_millis(50))))
        .with_clean_batch_size(1);

    let store = builder.build().await.expect("build with mixed strategies");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let onstart_done = onstart_count.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        onstart_done, onstart_num,
        "expected all {onstart_num} OnStart tasks, got {onstart_done}"
    );

    let periodic_done = periodic_count.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        periodic_done > 1,
        "expected Periodic to fire multiple times, got {periodic_done}"
    );

    drop(store);
}

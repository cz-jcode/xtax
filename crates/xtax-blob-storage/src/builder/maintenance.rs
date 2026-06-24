use std::future::Future;
use std::pin::Pin;

use crate::builder::BackgroundStrategy;
use crate::builder::TaskFactory;
use crate::builder::strategies::{BackgroundContext, MaintenanceTrigger};
use crate::error::Result;

/// A scheduled maintenance task — pairs a task factory with its scheduling strategy.
pub(crate) struct MaintenanceTask {
    pub(crate) factory: TaskFactory,
    pub(crate) strategy: std::sync::Arc<dyn BackgroundStrategy>,
}

/// Spawn the shared sequential maintenance queue and schedule all tasks.
///
/// All tasks execute in FIFO order on a single background worker.
/// Uses a bounded channel (16 slots) to prevent unbounded growth when
/// strategies push faster than the worker can process.
///
/// The `cancellation` token is monitored by both the worker and the strategy
/// loops. When the outer store is dropped, the guard cancels the token and
/// everything stops.
pub(crate) fn spawn_tasks(
    tasks: Vec<MaintenanceTask>,
    cancellation: tokio_util::sync::CancellationToken,
) -> Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Pin<Box<dyn Future<Output = ()> + Send>>>(16);

    let cancel_clone = cancellation.clone();

    // Shared worker: processes tasks one by one, stops on cancellation
    tokio::spawn(async move {
        loop {
            tokio::select! {
                maybe_task = rx.recv() => {
                    match maybe_task {
                        Some(task) => task.await,
                        None => break, // all senders dropped
                    }
                }
                _ = cancel_clone.cancelled() => {
                    break; // cancellation signalled
                }
            }
        }
    });

    // Schedule each task via its own strategy, passing a BackgroundContext
    for task in tasks {
        let trigger = MaintenanceTrigger::new(tx.clone(), task.factory);
        let ctx = BackgroundContext::new(trigger, cancellation.clone());
        task.strategy.schedule(ctx)?;
    }

    Ok(())
}

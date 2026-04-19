use std::sync::Arc;

mod builder;
mod handler_api;
mod job_api;
mod logic;
#[cfg(feature = "wait-for-job")]
mod wait_for_job;
mod workers_api;

use crate::handler;
use crate::sync::{self, BackoffStrategy};
use dashmap::DashMap;
use sqlx::PgPool;
use tokio::sync::Semaphore;

// Implementations split across modules

/// Queue engine struct
/// Use [`SimpleQueue::new(pool: PgPool)`] to create an instance.
///
/// Queue is configurable by builder-style methods.
///
/// Following parameters are configuratble:
/// - `heartbeat_interval` - how often running job will be touched (thus indicating the job is actually running)
///   when job stops being updated it's recognized as stalled and is requeued by a reaper worker
/// - `empty_poll_sleep` - how long to sleep between between polling when queue is empty -
///   should be low for high throughput queues, high for low throughput queues
/// - `max_reprocess_count` - how many times a job will be reprocessed before being discarded - poison job discovery
///   (count increases only on job reschedule and stalled job recovery)
/// - `janitor_interval` - how often the janitor task will run (i.e. archive completed jobs, move failed jobs to dead queue)
/// - `hold_queue_semaphore` - how long to hold a queue semaphore before releasing it
///   (when queue is congested releasing immediately might result in polling repicking same jobs over and over, starving other queues)
pub struct SimpleQueue {
    pool: PgPool,
    job_registry: DashMap<&'static str, Arc<dyn handler::DynJobHandler>>,
    global_semaphore: Arc<Semaphore>,
    queue_strategies: DashMap<String, Arc<dyn sync::JobStrategy>>,
    queue_semaphores: DashMap<String, Arc<Semaphore>>,
    queue_sem_count: usize,
    heartbeat_interval: tokio::time::Duration,
    default_backoff_strategy: BackoffStrategy,
    default_queue_strategy: Arc<dyn sync::JobStrategy>,
    queue_backoff_strategies: DashMap<String, BackoffStrategy>,
    empty_poll_sleep: tokio::time::Duration,
    max_reprocess_count: usize,
    janitor_interval: tokio::time::Duration,
    hold_queue_semaphore: tokio::time::Duration,
}

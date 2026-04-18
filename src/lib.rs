/*!
This crate provides a persisted job queue backed by PostgreSQL with a simple to use interface.
Its written as an easy to use async job queue library without macros or generics shenanigans.

Usage boils down to 3 points:
1. Implement `Handler` trait
2. Initialize the queue with a `PgPool` and start it
3. Insert jobs

Default configurations for jobs and queues are.. defaults, so make sure to read through appropriate Job and SimpleQueue documentation.

## Features

- Simple handler interface
- Job scheduling and rescheduling
- Job retry support
- Job crash handling
- Job cancellation
- Job fingerprinting (soft identifier)
- Existing jobs deduplication (unique key with noop on job reinsert - only for live jobs)
- Configurable global and per-queue backoff strategy (linear and exponential provided, custom supported)
- 3 tiered job permits (global permit, per queue permit, handler owned limit)
- Stalled job recovery (heartbeat)
- Archive and DLQ (requires `janitor` feature)
- Poison job detection (`reaper`)

## Usage

### Handler Implementation
```no_run
// handlers.rs
use simple_queue::prelude::*;

struct MyHandler {
    counter: std::sync::atomic::AtomicUsize,
};

impl Handler for MyHandler {
    const QUEUE: &'static str = "my-queue";

    async fn process(&self, queue: &SimpleQueue, job: &Job) -> Result<JobResult, BoxDynError> {
        self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(JobResult::Success)
    }
}
```

### Queue Initialization
```no_run
// main.rs
# use sqlx::PgPool;
# use std::sync::Arc;
# use serde_json::json;
use simple_queue::prelude::*;
# struct MyHandler { counter: std::sync::atomic::AtomicUsize }
# impl Handler for MyHandler {
#     const QUEUE: &'static str = "my-queue";
#
#     async fn process(&self, queue: &SimpleQueue, job: &Job) -> Result<JobResult, BoxDynError> {
#         self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
#         Ok(JobResult::Success)
#     }
# }

#[tokio::main]
async fn main() {
    # let PG_URL = "postgres://user:password@localhost:5432/dbname";
    let pool: PgPool = PgPool::connect(PG_URL).await.unwrap();
    let queue = Arc::new(SimpleQueue::new(pool));
    let handler = MyHandler { counter: std::sync::atomic::AtomicUsize::new(0) };
    // Deosn't have to happen before `simple_queue::start`
    queue.register_handler(handler);

    //  Keep clone for insertion
    simple_queue::start(queue.clone()).await;

    let job = Job::new("my-queue", json!({}));
    queue.insert_job(job).await;
}
```

## Thread Structure
```text
                        ┌─────────────┐
                        │ Entry Point │
                        └──────┬──────┘
                               │ spawns
         ┌─────────────────────┼──────────────────────┐
         ▼                     ▼                      ▼
┌────────────────┐    ┌────────────────┐    ┌────────────────┐
│Queue Processor │    │    Janitor     │    │     Reaper     │
│   / Poller     │    └────────────────┘    └────────────────┘
└───────┬────────┘
        │
        │ wait global permit
        ▼
   ┌─────────┐
   │  run()  │
   └────┬────┘
        │ job obtained
        │
        ├──────────────────────┐
        │                      │ spawn first
        │                      ▼
        │              ┌───────────────┐
        │              │   Heartbeat   │
        │              │    Thread     │
        │              └───────┬───────┘
        │                      │ ownership
        ▼                      │
   wait queue permit           │
        │                      │
        ▼                      │
   ┌──────────────────────┐    │
   │      Job Thread      │◄───┘ heartbeat passed in
   │                      │      (drop job == drop heartbeat)
   │  wait handler permit │
   │         │            │
   │         ▼            │
   │  ┌─────────────┐     │
   │  │   Process   │     │
   │  │    Job      │     │
   │  └─────────────┘     │
   └──────────────────────┘
```
## Future Work

- Distributed Semaphores using PostgreSQL
- Temporary store on connection loss
- Job templates
- Queue interactor interface
- PG Listener (to decrease job-to-queue latency)
- Job Queue partitioning (dead tuples accumulation prevention)

## Decisions
- JSON for job data for inspectability of DLQ/Archive
- Poll mechanism / non-distributed semaphores as a good enough (for now)

*/
#![cfg_attr(docsrs, feature(doc_cfg))]

use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::Semaphore;

mod handler;
pub(crate) mod heartbeat;
#[cfg_attr(docsrs, doc(cfg(feature = "janitor")))]
#[cfg(feature = "janitor")]
mod janitor;
mod job;
pub mod queue;
pub mod reaper;
mod result;
pub mod sync;

/// Any error that can be returned by a job handler.
pub type BoxDynError = Box<dyn std::error::Error + 'static + Send + Sync>;
pub mod prelude {
    pub use crate::BoxDynError;
    pub use crate::handler::Handler;
    pub use crate::job::Job;
    pub use crate::job::JobExt;
    pub use crate::queue::SimpleQueue;
    pub use crate::result::JobResult;
}

pub use prelude::*;
/// Sets up the queue schema in the database.
///
/// It's recommended to run this once during application startup.
///
/// Note: Separation from the main database is recommended but not required.
pub async fn setup(pool: &PgPool) -> Result<(), BoxDynError> {
    sqlx::raw_sql(include_str!("../migrations/0001_queue_init.sql"))
        .execute(pool)
        .await?;
    Ok(())
}
/// Sets up the queue schema in the database using a PostgreSQL URL.
pub async fn setup_from_url(url: &str) -> Result<(), BoxDynError> {
    let pool = sqlx::PgPool::connect(url).await?;
    sqlx::raw_sql(include_str!("../migrations/0001_queue_init.sql"))
        .execute(&pool)
        .await?;
    Ok(())
}

/// Queue starting function.
///
/// It starts:
/// - A reaper task that reclaims stalled jobs
/// - A janitor task that periodically checks the queue, archives old jobs and moves errored jobs to a dead queue
///
/// It returns a [`tokio::task::JoinSet`] that can be used to wait for the tasks to complete,
/// and it does so only AFTER the queue has started polling.
pub async fn start(jq: Arc<SimpleQueue>) -> tokio::task::JoinSet<()> {
    #[cfg(feature = "janitor")]
    {
        start_with_janitor(jq).await
    }
    #[cfg(not(feature = "janitor"))]
    {
        start_without_janitor(jq).await
    }
}
#[cfg(feature = "janitor")]
pub async fn start_with_janitor(jq: Arc<SimpleQueue>) -> tokio::task::JoinSet<()> {
    tracing::info!("Starting queue with reaper and janitor");
    let mut joinset = tokio::task::JoinSet::new();
    let mut reaper: reaper::Reaper = jq.reaper().await;
    let mut janitor: janitor::Janitor = jq.janitor().await;
    let _reaper_h = joinset.spawn(async move {
        reaper.run().await;
    });
    let _janitor_h = joinset.spawn(async move {
        janitor.run().await;
    });
    let start_semaphore = Arc::new(Semaphore::new(1));
    let queue_start_permit = start_semaphore
        .clone()
        .acquire_owned()
        .await
        .expect("failed to acquire queue start permit from a just created semaphore");
    let _queue_h = joinset.spawn(async move {
        let _ = jq.run(Some(queue_start_permit)).await;
    });
    joinset
}

/// Starts the queue without a janitor task.
///
/// For cases when you don't need a janitor task, i.e. completed and error queues should stay in the same table.
/// Useful for development and testing.
///
/// It returns a [`tokio::task::JoinSet`] that can be used to wait for the tasks to complete,
/// and it does so only AFTER the queue has started polling.
pub async fn start_without_janitor(jq: Arc<SimpleQueue>) -> tokio::task::JoinSet<()> {
    let mut joinset = tokio::task::JoinSet::new();
    let mut reaper: reaper::Reaper = jq.reaper().await;
    let _reaper_h = joinset.spawn(async move {
        reaper.run().await;
    });
    let start_semaphore = Arc::new(Semaphore::new(1));
    let queue_start_permit = start_semaphore
        .clone()
        .acquire_owned()
        .await
        .expect("failed to acquire queue start permit from a just created semaphore");
    let _queue_h = joinset.spawn(async move {
        let _ = jq.run(Some(queue_start_permit)).await;
    });
    let _ = start_semaphore.acquire().await.unwrap();
    joinset
}

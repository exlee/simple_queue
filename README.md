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

```rust
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

```rust
// main.rs
use simple_queue::prelude::*;

#[tokio::main]
async fn main() {
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
- Saga pattern support

## Decisions

- JSON for job data for inspectability of DLQ/Archive
- Poll mechanism / non-distributed semaphores as a good enough (for now)

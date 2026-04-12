use std::sync::Arc;

use dashmap::DashMap;
use sqlx::PgPool;
use tokio::sync::Semaphore;

use super::JobsQueue;
use crate::sync::{self, BackoffStrategy};

impl JobsQueue {
    /// Creates new Queue with default configuration:
    /// - global semaphore with 500 permits (global limit)
    /// - queue semaphore (per queue limit) with 100 permits
    /// - no custrom semaphore strategy
    /// - heartbeat interval of 5 seconds
    /// - linear backoff strategy with 5s retry interval
    /// - pool sleep when table is empty of 1 second
    /// - max reprocess count (poison/loop detection) of 100
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            job_registry: DashMap::new(),
            default_backoff_strategy: BackoffStrategy::default(),
            default_queue_strategy: Arc::new(sync::InstantStrategy {}),
            queue_backoff_strategies: DashMap::new(),
            global_semaphore: Arc::new(Semaphore::new(500)),
            queue_strategies: DashMap::new(),
            queue_semaphores: DashMap::new(),
            queue_sem_count: 100,
            heartbeat_interval: tokio::time::Duration::from_secs(5),
            empty_poll_sleep: tokio::time::Duration::from_secs(1),
            max_reprocess_count: 100,
            janitor_interval: tokio::time::Duration::from_secs(60),
        }
    }
    /// Set the global semaphore permits
    pub fn with_global_semaphore(self, permits: usize) -> Self {
        Self {
            global_semaphore: Arc::new(Semaphore::new(permits)),
            ..self
        }
    }
    /// Set the queue strategy for a specific queue
    pub fn with_queue_strategy(
        self,
        queue: String,
        strategy: impl sync::JobStrategy + 'static,
    ) -> Self {
        self.queue_strategies
            .insert(queue.clone(), Arc::new(strategy));
        self
    }
    /// Set the semaphore permits for a specific queue
    pub fn with_queue_semaphore(self, queue: String, permits: usize) -> Self {
        self.queue_semaphores
            .insert(queue, Arc::new(Semaphore::new(permits)));
        self
    }
    /// Set the default queue semaphore size
    pub fn with_queue_default_semaphore_size(self, permits: usize) -> Self {
        Self {
            queue_sem_count: permits,
            ..self
        }
    }
    /// Set the heartbeat interval
    pub fn with_heartbeat_interval(self, interval: tokio::time::Duration) -> Self {
        Self {
            heartbeat_interval: interval,
            ..self
        }
    }
    /// Set the default backoff strategy
    pub fn with_default_backoff_strategy(self, strategy: BackoffStrategy) -> Self {
        Self {
            default_backoff_strategy: strategy,
            ..self
        }
    }
    /// Set the default queue strategy
    pub fn with_default_queue_strategy(self, strategy: Arc<dyn sync::JobStrategy>) -> Self {
        Self {
            default_queue_strategy: strategy,
            ..self
        }
    }
    /// Set the backoff strategy for a specific queue
    pub fn with_queue_backoff_strategy(self, queue: String, strategy: BackoffStrategy) -> Self {
        self.queue_backoff_strategies.insert(queue, strategy);
        self
    }

    /// Set the duration to sleep when no jobs are found
    pub fn with_empty_poll_sleep(self, duration: tokio::time::Duration) -> Self {
        Self {
            empty_poll_sleep: duration,
            ..self
        }
    }
    /// Set the maximum number of times a job can be reprocessed
    /// E.g. rescheduled without attempt consumption or recovered from stalled state (default: 100)
    pub fn with_max_reprocess_count(self, count: usize) -> Self {
        Self {
            max_reprocess_count: count,
            ..self
        }
    }

    /// Set the interval between janitor runs (default: 1 minute)
    /// Janitor moves completed and failed jobs to the archive and dlq tables
    pub fn with_janitor_interval(self, duration: tokio::time::Duration) -> Self {
        Self {
            janitor_interval: duration,
            ..self
        }
    }
}

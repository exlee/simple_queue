use std::sync::Arc;

use dashmap::DashMap;
use sqlx::PgPool;
use tokio::sync::Semaphore;

use crate::sync::{self, BackoffStrategy};
use crate::*;

impl SimpleQueue {
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
            hold_queue_semaphore: tokio::time::Duration::from_millis(500),
        }
    }
    /// Create a new `SimpleQueue` with a PostgreSQL pool created from a URL.
    /// See [`SimpleQueue::new`] for default configuration details.
    pub async fn new_from_url(url: &str) -> Result<Self, sqlx::Error> {
        let pool = sqlx::PgPool::connect(url).await?;
        Ok(Self::new(pool))
    }
    /// Set the number of global semaphore permits
    ///
    /// Default value: `500`
    pub fn with_global_semaphore(self, permits: usize) -> Self {
        Self {
            global_semaphore: Arc::new(Semaphore::new(permits)),
            ..self
        }
    }
    /// Set the queue waiting strategy for a specific queue
    ///
    /// Default value if not set is `default_queue_strategy` parameter.
    pub fn with_queue_strategy(
        self,
        queue: String,
        strategy: impl sync::JobStrategy + 'static,
    ) -> Self {
        self.queue_strategies
            .insert(queue.clone(), Arc::new(strategy));
        self
    }
    /// Set the number of semaphore permits for a specific queue
    ///
    /// Default value if not set is in `queue_sem_count` parameter set by `with_queue_default_semaphore_size`.
    pub fn with_queue_semaphore(self, queue: String, permits: usize) -> Self {
        self.queue_semaphores
            .insert(queue, Arc::new(Semaphore::new(permits)));
        self
    }
    /// Set the default number of permits for queues
    ///
    /// Default value: `100`
    pub fn with_queue_default_semaphore_size(self, permits: usize) -> Self {
        Self {
            queue_sem_count: permits,
            ..self
        }
    }
    /// Set the heartbeat interval
    ///
    /// Default values: `5 seconds`
    pub fn with_heartbeat_interval(self, interval: tokio::time::Duration) -> Self {
        Self {
            heartbeat_interval: interval,
            ..self
        }
    }
    /// Set the default backoff strategy.
    ///
    /// See also [`BackoffStrategy`].
    ///
    /// Default strategy: [`BackoffStrategy::Linear`] with delay of `5 seconds`.
    pub fn with_default_backoff_strategy(self, strategy: BackoffStrategy) -> Self {
        Self {
            default_backoff_strategy: strategy,
            ..self
        }
    }
    /// Set the default queue strategy
    ///
    /// Default: [`sync::InstantStrategy`]
    pub fn with_default_queue_strategy(self, strategy: Arc<dyn sync::JobStrategy>) -> Self {
        Self {
            default_queue_strategy: strategy,
            ..self
        }
    }
    /// Set the backoff strategy for a specific queue
    ///
    /// See also [`BackoffStrategy`].
    ///
    /// Default is taken from `default_backoff_strategy`.
    pub fn with_queue_backoff_strategy(self, queue: String, strategy: BackoffStrategy) -> Self {
        self.queue_backoff_strategies.insert(queue, strategy);
        self
    }

    /// Set the duration to sleep between polls when no jobs are found
    ///
    /// This is used only when no jobs are found in the queue. Should be set to low value for critical
    /// and high throughput queues.
    ///
    /// Default: `1 second`.
    pub fn with_empty_poll_sleep(self, duration: tokio::time::Duration) -> Self {
        Self {
            empty_poll_sleep: duration,
            ..self
        }
    }
    /// Set the maximum number of times a job can be reprocessed
    /// E.g. rescheduled without attempt consumption or recovered from stalled state (default: 100)
    ///
    /// Reprocess count is to prevent infinite reschedules (not consuming attempt) and stall job recovery.
    ///
    /// Default: `100`.
    pub fn with_max_reprocess_count(self, count: usize) -> Self {
        Self {
            max_reprocess_count: count,
            ..self
        }
    }

    /// Set the interval between janitor runs.
    ///
    /// Janitor moves completed and failed jobs to the archive and dlq tables
    ///
    /// Default: `1 minute`.
    pub fn with_janitor_interval(self, duration: tokio::time::Duration) -> Self {
        Self {
            janitor_interval: duration,
            ..self
        }
    }

    /// Set the duration of how logn to hold the queue semaphore acquisition.
    ///
    /// When queue semaphore is held, spawned tokio job will wait that much time
    /// before giving up and releasing the semaphore. This is a hold-up mechanism
    /// so that poller don't re-pick too quickly preventing other queues from being processed.
    ///
    /// Default: `500 milliseconds`.
    pub fn with_hold_queue_semaphore(self, duration: tokio::time::Duration) -> Self {
        Self {
            hold_queue_semaphore: duration,
            ..self
        }
    }
}

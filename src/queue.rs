use std::sync::Arc;

use super::sync::BackoffStrategy;
use super::*;
use dashmap::DashMap;
use sqlx::{PgPool, error::BoxDynError};
use tokio::sync::Semaphore;
use tracing::Instrument as _;

pub struct JobsQueue {
    pool: PgPool,
    job_registry: Arc<DashMap<&'static str, Arc<dyn DynJobHandler>>>,
    global_semaphore: Arc<Semaphore>,
    queue_strategies: Arc<DashMap<String, Arc<dyn sync::JobStrategy>>>,
    queue_semaphores: Arc<DashMap<String, Arc<Semaphore>>>,
    queue_sem_count: usize,
    heartbeat_interval: chrono::Duration,
    default_backoff_strategy: Arc<BackoffStrategy>,
    default_queue_strategy: Arc<dyn sync::JobStrategy>,
    queue_backoff_strategies: Arc<DashMap<String, Arc<BackoffStrategy>>>,
    empty_poll_sleep: chrono::Duration,
}

// Builder impl
impl JobsQueue {
    /// Create new Queue with default configuration:
    /// - global semaphore with 500 permits
    /// - queue semaphore with 100 permits
    /// - heartbeat interval of 5 seconds
    /// - backoff strategy of exponential backoff
    /// - pool sleep when empty of 1 second
    /// Create a new JobsQueue with default configuration:
    /// - global semaphore with 500 permits
    /// - queue semaphore with 100 permits
    /// - heartbeat interval of 5 seconds
    /// - backoff strategy of exponential backoff
    /// - pool sleep when empty of 1 second
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            job_registry: Arc::new(DashMap::new()),
            global_semaphore: Arc::new(Semaphore::new(500)),
            default_backoff_strategy: Arc::new(BackoffStrategy::default()),
            default_queue_strategy: Arc::new(sync::InstantStrategy {}),
            queue_backoff_strategies: Arc::new(DashMap::new()),
            queue_strategies: Arc::new(DashMap::new()),
            queue_semaphores: Arc::new(DashMap::new()),
            queue_sem_count: 100,
            heartbeat_interval: chrono::Duration::seconds(5),
            empty_poll_sleep: chrono::Duration::seconds(1),
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
    pub fn with_queue_strategy(self, queue: String, strategy: Arc<dyn sync::JobStrategy>) -> Self {
        self.queue_strategies.insert(queue.clone(), strategy);
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
    pub fn with_heartbeat_interval(self, interval: chrono::Duration) -> Self {
        Self {
            heartbeat_interval: interval,
            ..self
        }
    }
    /// Set the default backoff strategy
    pub fn with_default_backoff_strategy(self, strategy: BackoffStrategy) -> Self {
        Self {
            default_backoff_strategy: Arc::new(strategy),
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
        self.queue_backoff_strategies
            .insert(queue, Arc::new(strategy));
        self
    }

    /// Set the duration to sleep when no jobs are found
    pub fn with_empty_poll_sleep(self, duration: chrono::Duration) -> Self {
        Self {
            empty_poll_sleep: duration,
            ..self
        }
    }
}

// Logic implementation
impl JobsQueue {
    /// Insert a job into the queue. If a job with the same unique key already exists,
    /// and it is still pending or running, the insert will be a no-op.
    pub async fn insert_job(&self, job: Job) -> Result<(), BoxDynError> {
        sqlx::query!(
            r#"
            INSERT INTO job_queue (
            id, fingerprint, unique_key, queue, job_data, status, created_at, run_at, updated_at, attempt, max_attempts
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND status IN ('pending', 'running')
            DO NOTHING
            "#,

            job.id,
            job.fingerprint,
            job.unique_key,
            job.queue,
            job.job_data,
            job.status,
            job.created_at,
            job.run_at,
            job.updated_at,
            job.attempt,
            job.max_attempts
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
    /// Register a job handler for a specific queue.
    pub fn register_handler(&self, handler: impl JobHandler + 'static) {
        let queue = handler.queue();
        self.job_registry.insert(queue, Arc::new(handler));
    }
    #[tracing::instrument(skip(self))]
    async fn fetch_next_job(&self) -> Result<Option<Job>, sqlx::Error> {
        sqlx::query_as!(
            Job,
            r#"
        UPDATE job_queue
        SET
            status = $2,
            attempt = attempt + 1

        WHERE id = (
            SELECT id FROM job_queue
            WHERE status = $1
            AND (CURRENT_TIMESTAMP > run_at OR run_at IS NULL)
            AND attempt < max_attempts
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING *
        "#,
            result::JobResultInternal::Pending.to_string(),
            result::JobResultInternal::Running.to_string(),
        )
        .fetch_optional(&self.pool)
        .await
    }
    fn get_queue_semaphore(&self, queue: String) -> Arc<Semaphore> {
        let queue_semaphores = self.queue_semaphores.clone();
        let entry = queue_semaphores
            .entry(queue)
            .or_insert_with(|| Arc::new(Semaphore::new(self.queue_sem_count)));
        let semaphore = entry.value();
        semaphore.clone()
    }
    /// Poll the queue for the next job to run (in loop). If no jobs are found, sleep for
    /// `empty_poll_sleep` before retrying.
    pub async fn run(&self) -> Result<(), BoxDynError> {
        loop {
            let _span = tracing::info_span!("poll");

            let _global_permit = self.global_semaphore.clone().acquire_owned().await?;
            let job = self.fetch_next_job().await?;
            if let Some(job) = job {
                let _job_span = tracing::info_span!("job", id = %job.id, queue = %job.queue);
                let job = Arc::new(job);
                let _heartbeat = heartbeat::Heartbeat::start(
                    self.pool.clone(),
                    &job.id,
                    self.heartbeat_interval,
                );
                let Ok(_queue_permit) = self
                    .get_queue_semaphore(job.queue.clone())
                    .try_acquire_owned()
                else {
                    tracing::warn!(
                        "Job queue semaphore acquire failed for queue: {}",
                        job.queue
                    );
                    match self.release_job(&job.id).await {
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!("Failed to release {:?}: {}", job, e);
                            return Err(e.into());
                        }
                    }
                    continue;
                };
                // Process the job with the semaphore
                let registry = self.job_registry.clone();
                let pool = self.pool.clone();
                let strategy = self
                    .queue_strategies
                    .get(job.queue.as_str())
                    .map(|r| r.value().clone())
                    .unwrap_or(self.default_queue_strategy.clone());

                let backoff_strategy = self
                    .queue_backoff_strategies
                    .clone()
                    .get(job.queue.as_str())
                    .map(|r| r.value().clone());
                let backoff_strategy =
                    backoff_strategy.unwrap_or(self.default_backoff_strategy.clone());

                tokio::spawn(async move {
                    let _permit = strategy.acquire().await;
                    let result = if let Some(handler) = registry.get(job.queue.as_str()) {

                        let process_result = handler.process_dyn(&job)
                            .instrument(tracing::info_span!("process_job", job_id = %job.id, queue = %job.queue, attempt = %job.attempt, max_attempts = %job.max_attempts, run_at = ?job.run_at)).await;
                        match process_result {
                            Ok(res) => res,
                            Err(e) => {
                                tracing::error!("Handler panicked or returned error: {:?}", e);
                                JobResult::Failed // Default to failed on unexpected errors
                            }
                        }
                    } else {
                        tracing::warn!("Missing handler for: {:?}", job.queue.as_str());
                        JobResult::HandlerMissing
                    };

                    let status_str = result.to_string();
                    use result::{JobResult, JobResultInternal};
                    match result {
                        JobResult::Success => {
                            let _ = sqlx::query!(
                                "UPDATE job_queue SET status = $1 WHERE id = $2",
                                status_str,
                                job.id.clone(),
                            )
                            .execute(&pool.clone())
                            .await;
                        }
                        JobResult::Failed => {
                            // TODO: Add RetryAt
                            let _ = sqlx::query!(
                                "UPDATE job_queue SET status = $1, run_at = $2 WHERE id = $3",
                                status_str,
                                backoff_strategy.next_attempt(job.clone()),
                                job.id.clone(),
                            )
                            .execute(&pool.clone())
                            .await;
                        }
                        JobResult::RetryAt(run_at) => {
                            let _ = sqlx::query!(
                                "UPDATE job_queue SET status = $1, run_at = $2 WHERE id = $3",
                                status_str,
                                run_at,
                                job.id.clone()
                            )
                            .execute(&pool)
                            .await;
                        }
                        JobResult::RescheduleAt(run_at) => {
                            let _ = sqlx::query!(
                                "UPDATE job_queue SET status = $1, run_at = $2, attempt = attempt - 1 WHERE id = $3",
                                status_str, run_at, job.id.clone()
                            ).execute(&pool).await;
                        }
                        JobResult::Critical => {
                            let _ = update_job(&pool, &job.id, JobResultInternal::Critical).await;
                        }
                        JobResult::HandlerMissing => {
                            let _ = update_job(&pool, &job.id, JobResultInternal::Critical).await;
                        }
                        JobResult::Cancel => {
                            let _ = update_job(&pool, &job.id, JobResultInternal::Cancelled).await;
                        }
                        JobResult::Unprocessable => {
                            let _ =
                                update_job(&pool, &job.id, JobResultInternal::Unprocessable).await;
                        }
                    }
                    drop(_permit);
                    drop(_queue_permit);
                    drop(_global_permit);
                    drop(_heartbeat);
                }.instrument(_job_span));
            } else {
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    self.empty_poll_sleep.num_milliseconds().abs() as u64,
                ))
                .await;
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn release_job(&self, id: &uuid::Uuid) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "UPDATE job_queue SET status = $1 WHERE id = $2",
            result::JobResultInternal::Pending.to_string(),
            id
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

// Main loop
impl JobsQueue {
    /// Return a reaper instance that fixes stale jobs
    pub async fn reaper(&self) -> reaper::Reaper {
        let pool = self.pool.clone();
        let heartbeat_interval = self.heartbeat_interval.clone();
        reaper::Reaper {
            pool,
            heartbeat_interval,
        }
    }
}
/// Helper: updates a job's status in the database.
async fn update_job(
    pool: &PgPool,
    id: &uuid::Uuid,
    result: result::JobResultInternal,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "UPDATE job_queue SET status = $1 WHERE id = $2",
        result.to_string(),
        id
    )
    .execute(pool)
    .await?;
    Ok(())
}

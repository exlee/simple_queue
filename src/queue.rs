use std::sync::Arc;

use crate::{
    result::{AnyJobResult, JobResultInternal},
    sync::JobStrategyError,
};

use super::sync::BackoffStrategy;
use super::*;
use dashmap::DashMap;
use sqlx::{PgPool, error::BoxDynError};
use tokio::sync::Semaphore;
use tracing::Instrument as _;

/// Queue engine struct
pub struct JobsQueue {
    pool: PgPool,
    job_registry: Arc<DashMap<&'static str, Arc<dyn DynJobHandler>>>,
    global_semaphore: Arc<Semaphore>,
    queue_strategies: Arc<DashMap<String, Arc<dyn sync::JobStrategy>>>,
    queue_semaphores: Arc<DashMap<String, Arc<Semaphore>>>,
    queue_sem_count: usize,
    heartbeat_interval: chrono::Duration,
    default_backoff_strategy: BackoffStrategy,
    default_queue_strategy: Arc<dyn sync::JobStrategy>,
    queue_backoff_strategies: Arc<DashMap<String, BackoffStrategy>>,
    empty_poll_sleep: chrono::Duration,
    max_reprocess_count: usize,
}

// Builder impl
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
            job_registry: Arc::new(DashMap::new()),
            default_backoff_strategy: BackoffStrategy::default(),
            default_queue_strategy: Arc::new(sync::InstantStrategy {}),
            queue_backoff_strategies: Arc::new(DashMap::new()),
            global_semaphore: Arc::new(Semaphore::new(500)),
            queue_strategies: Arc::new(DashMap::new()),
            queue_semaphores: Arc::new(DashMap::new()),
            queue_sem_count: 100,
            heartbeat_interval: chrono::Duration::seconds(5),
            empty_poll_sleep: chrono::Duration::seconds(1),
            max_reprocess_count: 100,
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
    pub fn with_heartbeat_interval(self, interval: chrono::Duration) -> Self {
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
    pub fn with_empty_poll_sleep(self, duration: chrono::Duration) -> Self {
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
        let entry = self
            .queue_semaphores
            .entry(queue)
            .or_insert_with(|| Arc::new(Semaphore::new(self.queue_sem_count)));
        let semaphore = entry.value();
        semaphore.clone()
    }
    /// Poll the queue for the next job to run (in loop). If no jobs are found, sleep for
    /// `empty_poll_sleep` before retrying.
    pub async fn run(self: Arc<Self>) -> Result<(), BoxDynError> {
        loop {
            let _span = tracing::info_span!("poll");

            let _global_permit = self.global_semaphore.clone().acquire_owned().await?;
            let job = self.fetch_next_job().await?;
            if let Some(job) = job {
                let _job_span = tracing::info_span!("job", id = %job.id, queue = %job.queue);
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
                    .get(job.queue.as_str())
                    .map(|r| r.value().clone());
                let backoff_strategy =
                    backoff_strategy.unwrap_or(self.default_backoff_strategy.clone());

                let max_reprocess = self.max_reprocess_count;

                tokio::spawn(async move {
                    if job.reprocess_count >= max_reprocess as i32 {
                        handle_result(
                            AnyJobResult::Internal(JobResultInternal::BadJob),
                            &job,
                            &pool,
                            &backoff_strategy,
                        ).await;
                        return;
                    }
                    let permit_result = strategy.acquire(&job).await;
                    if let Err(permit_err) = permit_result {
                        handle_strategy_error(permit_err, &job, &pool, &backoff_strategy).await;
                        return;
                    };
                    let _permit = permit_result.unwrap();
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

                    handle_result(AnyJobResult::Public(result), &job, &pool, &backoff_strategy).await;
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

async fn handle_strategy_error(
    err: JobStrategyError,
    job: &Job,
    pool: &PgPool,
    backoff_strategy: &BackoffStrategy,
) {
    match err {
        JobStrategyError::CancelJob => {
            handle_result(JobResult::Cancel.into(), job, pool, backoff_strategy).await
        }
        JobStrategyError::TryAfter(time_delta) => {
            handle_result(
                JobResult::RetryAt(chrono::Utc::now() + time_delta).into(),
                job,
                pool,
                backoff_strategy,
            )
            .await
        }
        JobStrategyError::Retry => {
            handle_result(JobResult::Failed.into(), job, pool, backoff_strategy).await
        }
        JobStrategyError::MarkCompleted => {
            handle_result(JobResult::Success.into(), job, pool, backoff_strategy).await
        }
    }
}

async fn handle_result(
    result: AnyJobResult,
    job: &Job,
    pool: &PgPool,
    backoff_strategy: &BackoffStrategy,
) -> () {
    match result {
        AnyJobResult::Internal(result) => {
            handle_result_internal(result, job, pool, backoff_strategy).await
        }
        AnyJobResult::Public(result) => {
            handle_result_public(result, job, pool, backoff_strategy).await
        }
    }
}
async fn handle_result_internal(
    result: JobResultInternal,
    job: &Job,
    pool: &PgPool,
    _backoff_strategy: &BackoffStrategy,
) -> () {
    match result {
        JobResultInternal::BadJob => {
            // Rest of the variants aren't supported right now, as they should be processed by public result handling
            let _ = sqlx::query!(
                "UPDATE job_queue SET status = $1 WHERE id = $2",
                result.to_string(),
                &job.id,
            )
            .execute(pool)
            .await;
        }
        _ => {
            tracing::error!("Unexpected internal status in job processing: {:?}", result)
        }
    }
}
/// Handles JobResult results
async fn handle_result_public(
    result: JobResult,
    job: &Job,
    pool: &PgPool,
    backoff_strategy: &BackoffStrategy,
) -> () {
    use result::{JobResult, JobResultInternal};
    let status_str = result.to_string();
    match result {
        JobResult::Success => {
            let _ = sqlx::query!(
                "UPDATE job_queue SET status = $1 WHERE id = $2",
                status_str,
                job.id.clone(),
            )
            .execute(pool)
            .await;
        }
        JobResult::Failed => {
            // TODO: Add RetryAt
            let _ = sqlx::query!(
                "UPDATE job_queue SET status = $1, run_at = $2 WHERE id = $3",
                status_str,
                backoff_strategy.next_attempt(job),
                job.id.clone(),
            )
            .execute(pool)
            .await;
        }
        JobResult::RetryAt(run_at) => {
            let _ = sqlx::query!(
                "UPDATE job_queue SET status = $1, run_at = $2 WHERE id = $3",
                status_str,
                run_at,
                job.id.clone()
            )
            .execute(pool)
            .await;
        }
        JobResult::RescheduleAt(run_at) => {
            let _ = sqlx::query!(
                "UPDATE job_queue SET status = $1, run_at = $2, attempt = attempt - 1, reprocess_count = reprocess_count + 1 WHERE id = $3",
                status_str, run_at, job.id.clone()
            ).execute(pool).await;
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
            let _ = update_job(&pool, &job.id, JobResultInternal::Unprocessable).await;
        }
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

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

// Implementations split across modules
pub mod builder;
pub mod handler_api;
pub mod job_api;
pub mod workers_api;

/// Queue engine struct
pub struct JobsQueue {
    pool: PgPool,
    job_registry: DashMap<&'static str, Arc<dyn DynJobHandler>>,
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
}

// Logic implementation
impl JobsQueue {
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
                let q = Arc::clone(&self);
                tokio::spawn(async move {

                    if job.reprocess_count >= q.max_reprocess_count as i32 {
                        handle_result(
                            AnyJobResult::Internal(JobResultInternal::BadJob),
                            &job,
                            &q.pool,
                            &q.get_backoff_strategy(&job),
                        ).await;
                        return;
                    }
                    let permit_result = q.get_job_strategy(&job).acquire(&job).await;
                    let backoff_strategy = q.get_backoff_strategy(&job);

                    if let Err(permit_err) = permit_result {
                        handle_strategy_error(permit_err, &job, &q.pool, &backoff_strategy).await;
                        return;
                    };
                    let _permit = permit_result.unwrap();
                    let result = if let Some(handler) = q.job_registry.get(job.queue.as_str()) {

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

                    handle_result(AnyJobResult::Public(result), &job, &q.pool, &backoff_strategy).await;
                    drop(_permit);
                    drop(_queue_permit);
                    drop(_global_permit);
                    drop(_heartbeat);
                }.instrument(_job_span));
            } else {
                tokio::time::sleep(self.empty_poll_sleep).await;
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

    fn get_backoff_strategy(&self, job: &Job) -> BackoffStrategy {
        self.queue_backoff_strategies
            .get(job.queue.as_str())
            .map(|r| r.value().clone())
            .unwrap_or(self.default_backoff_strategy.clone())
    }
    fn get_job_strategy(&self, job: &Job) -> Arc<dyn sync::JobStrategy> {
        self.queue_strategies
            .get(job.queue.as_str())
            .map(|r| r.value().clone())
            .unwrap_or(self.default_queue_strategy.clone())
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
    let status_str = result.handle().to_string();
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

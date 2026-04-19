use futures::FutureExt as _;
use sqlx::PgPool;
use tokio::select;
use tokio::sync::Semaphore;
use tracing::Instrument as _;

use crate::heartbeat;
use crate::prelude::*;
#[cfg(feature = "wait-for-job")]
use crate::queue::wait_for_job::get_waiting_guard;
use crate::result::{self, AnyJobResult, JobResultInternal};
use crate::sync::{self, BackoffStrategy, JobStrategyError};

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
// Logic implementation
impl SimpleQueue {
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
    ///
    /// Passed `start_permit` is used to guarantee that worker started.
    /// Mostly helpful with tests, as checks might fail if queue was still warming up.
    pub async fn run(
        self: Arc<Self>,
        start_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<(), BoxDynError> {
        drop(start_permit);
        loop {
            let _span = tracing::info_span!("poll");

            let _global_permit = self.global_semaphore.clone().acquire_owned().await?;
            let job = self.fetch_next_job().await?;
            if let Some(job) = job {
                let _job_span = tracing::info_span!("job", id = %job.id, queue = %job.queue);
                #[cfg(feature = "wait-for-job")]
                let _wait_guard = get_waiting_guard(job.id);
                let _heartbeat = heartbeat::Heartbeat::start(
                    self.pool.clone(),
                    &job.id,
                    self.heartbeat_interval,
                );

                let result = select! {
                    sem_result = self.get_queue_semaphore(job.queue.clone())
                        .acquire_owned() => sem_result.map_err(|_| tokio::sync::TryAcquireError::Closed),
                    _ = tokio::time::sleep(self.hold_queue_semaphore) => Err(tokio::sync::TryAcquireError::NoPermits),
                };
                let Ok(_queue_permit) = result else {
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
                let job = Arc::new(job);
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
                    let q_name: String = job.queue.clone();
                    let result = if let Some(handler) = q.job_registry.get(q_name.as_str()) {

                        let wrapped_result = AssertUnwindSafe(
                            handler.process_dyn(&q, &job)
                                .instrument(tracing::info_span!("process_job", job_id = %&job.id, queue = %&job.queue, attempt = %&job.attempt, max_attempts = %&job.max_attempts, run_at = ?&job.run_at)))
                        .catch_unwind().await;

                        match wrapped_result {
                            Ok(Ok(process)) => process,
                            Ok(Err(e)) => {
                                tracing::error!("Handler returned error: {:?}", e);
                                JobResult::InternalError
                            }
                            Err(_) => {
                                tracing::error!("Handler panicked or returned error: {}", &job.id);
                                JobResult::InternalError
                            }
                        }
                    } else {
                        tracing::warn!("Missing handler for: {:?}", (job.queue).clone().as_str());
                        JobResult::HandlerMissing
                    };

                    handle_result(AnyJobResult::Public(result), &job, &q.pool, &backoff_strategy).await;
                    drop(_permit);
                    drop(_queue_permit);
                    drop(_global_permit);
                    drop(_heartbeat);
                    #[cfg(feature = "wait-for-job")]
                    drop(_wait_guard);
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
    let next_status_str = result.handle().to_string();
    match result {
        JobResult::InternalError => {
            Box::pin(async move {
                handle_result_public(JobResult::Failed, job, pool, backoff_strategy).await;
            })
            .await;
        }
        JobResult::Success => {
            let _ = sqlx::query!(
                "UPDATE job_queue SET status = $1 WHERE id = $2",
                next_status_str,
                job.id.clone(),
            )
            .execute(pool)
            .await;
            tracing::info!("[{}] Job {} succeeded", job.queue, job.id);
        }
        JobResult::Failed => {
            // TODO: Add RetryAt
            let _ = sqlx::query!(
                "UPDATE job_queue SET status = $1, run_at = $2 WHERE id = $3",
                next_status_str,
                backoff_strategy.next_attempt(job),
                job.id.clone(),
            )
            .execute(pool)
            .await;
            tracing::info!("Job {} failed", job.id);
        }
        JobResult::RetryAt(run_at) => {
            let _ = sqlx::query!(
                "UPDATE job_queue SET status = $1, run_at = $2 WHERE id = $3",
                next_status_str,
                run_at,
                job.id.clone()
            )
            .execute(pool)
            .await;
        }
        JobResult::RescheduleAt(run_at) => {
            // run_at cannot be closer than backoff
            let backoff = backoff_strategy.next_attempt(job);
            let scheduled = if run_at < backoff { backoff } else { run_at };
            let _ = sqlx::query!(
                "UPDATE job_queue SET status = $1, run_at = $2, attempt = attempt - 1, reprocess_count = reprocess_count + 1 WHERE id = $3",
                next_status_str, scheduled, job.id.clone()
            ).execute(pool).await;
        }
        JobResult::Critical => {
            let _ = update_job(pool, &job.id, JobResultInternal::Critical).await;
        }
        JobResult::HandlerMissing => {
            let _ = update_job(pool, &job.id, JobResultInternal::Critical).await;
            tracing::info!("Handler missing for job {}", job.id);
        }
        JobResult::Cancel => {
            let _ = update_job(pool, &job.id, JobResultInternal::Cancelled).await;
            tracing::info!("Job {} cancelled", job.id);
        }
        JobResult::Unprocessable => {
            let _ = update_job(pool, &job.id, JobResultInternal::Unprocessable).await;
            tracing::info!("Job {} unprocessable", job.id);
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

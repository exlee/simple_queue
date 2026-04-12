use sqlx::error::BoxDynError;

use super::{Job, JobsQueue};

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

    pub async fn cancel_job_by_unique_key(&self, unique_key: &str) -> Result<(), BoxDynError> {
        sqlx::query!(
            r#"
            UPDATE job_queue
            SET status = 'cancelled'
            WHERE unique_key = $1
            "#,
            unique_key
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
    pub async fn cancel_all_jobs_by_fingerprint(
        &self,
        fingerprint: &str,
    ) -> Result<(), BoxDynError> {
        sqlx::query!(
            r#"
            UPDATE job_queue
            SET status = 'cancelled'
            WHERE fingerprint = $1
            "#,
            fingerprint
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

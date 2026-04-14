use super::SimpleQueue;
use crate::*;
use sqlx;
use sqlx::error::BoxDynError;
impl SimpleQueue {
    /// Insert a job into the queue. If a job with the same unique key already exists,
    /// and it is still pending or running, the insert will be a no-op.
    pub async fn insert_job(&self, job: Job) -> Result<Option<uuid::Uuid>, BoxDynError> {
        let id = sqlx::query_scalar!(
        r#"
        INSERT INTO job_queue (
        id, fingerprint, unique_key, queue, job_data, status, created_at, run_at, updated_at, attempt, max_attempts
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND status IN ('pending', 'running') DO NOTHING
        RETURNING id
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
        .fetch_optional(&self.pool)
        .await.inspect_err(|e| tracing::error!("[{}]Failed to insert job {}: {:?}", job.queue, job.id, e))?;
        Ok(id)
    }

    pub async fn insert_jobs(&self, jobs: Vec<Job>) -> Result<Vec<uuid::Uuid>, BoxDynError> {
        let mut qb = sqlx::QueryBuilder::new(
            "INSERT INTO job_queue (id, fingerprint, unique_key, queue, job_data, status, created_at, run_at, updated_at, attempt, max_attempts) ",
        );
        let query = qb
            .push_values(jobs, |mut qb, job| {
                qb.push_bind(job.id)
                    .push_bind(job.fingerprint)
                    .push_bind(job.unique_key)
                    .push_bind(job.queue)
                    .push_bind(job.job_data)
                    .push_bind(job.status)
                    .push_bind(job.created_at)
                    .push_bind(job.run_at)
                    .push_bind(job.updated_at)
                    .push_bind(job.attempt)
                    .push_bind(job.max_attempts);
            })
            .push("RETURNING id")
            .build_query_scalar::<uuid::Uuid>();

        let resulting_ids = query.fetch_all(&self.pool).await?;
        Ok(resulting_ids)
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

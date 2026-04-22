use sqlx::{PgPool, error::BoxDynError};

use super::*;
use result::JobResultInternal;

pub struct Janitor {
    pub pool: PgPool,
    pub interval: tokio::time::Interval,
}

impl Janitor {
    pub fn new(pool: PgPool, interval: tokio::time::Interval) -> Self {
        Self { pool, interval }
    }

    pub async fn run(&mut self) {
        loop {
            self.interval.tick().await;
            self.run_archiver().await.ok();
            self.interval.tick().await;
            self.run_dlq().await.ok();
        }
    }
    #[tracing::instrument(skip(self))]
    pub async fn run_archiver(&self) -> Result<(), BoxDynError> {
        let ids: Vec<uuid::Uuid> = sqlx::query_scalar!(
            r#"
            WITH moved AS (
              DELETE FROM job_queue
              WHERE status = $1
              RETURNING *
            )
            INSERT INTO job_queue_archive SELECT * FROM moved
            RETURNING id;
            "#,
            JobResultInternal::Completed.to_string(),
        )
        .fetch_all(&self.pool)
        .await?;

        if !ids.is_empty() {
            tracing::debug!(name = "archived job ids", ?ids);
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn run_dlq(&self) -> Result<(), BoxDynError> {
        use result::JobResultInternal::*;
        let ids: Vec<uuid::Uuid> = sqlx::query_scalar!(
            r#"
            WITH moved AS (
              DELETE FROM job_queue
              WHERE status IN ($1, $2, $3, $4)
              OR status = $5 and attempt >= max_attempts
              RETURNING *
            )
            INSERT INTO job_queue_dlq SELECT * FROM moved
            RETURNING id;
            "#,
            Unprocessable.to_string(),
            Cancelled.to_string(),
            Critical.to_string(),
            BadJob.to_string(),
            Failed.to_string(),
        )
        .fetch_all(&self.pool)
        .await?;

        if !ids.is_empty() {
            tracing::debug!(name = "archived job ids", ?ids);
        }

        Ok(())
    }
}

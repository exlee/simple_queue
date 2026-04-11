use sqlx::{PgPool, error::BoxDynError};

use super::*;
use result::JobResultInternal;

pub struct Reaper {
    pub heartbeat_interval: chrono::TimeDelta,
    pub pool: PgPool,
}

impl Reaper {
    pub async fn run(&self) {
        loop {
            self.run_reaper().await.ok();
            tokio::time::sleep(tokio::time::Duration::from_millis(
                self.heartbeat_interval.num_milliseconds().abs() as u64,
            ))
            .await;
        }
    }

    fn stale_job_interval(&self) -> chrono::TimeDelta {
        chrono::TimeDelta::milliseconds(
            (self.heartbeat_interval.num_milliseconds() as f32 * 2.5f32) as i64,
        )
    }
    #[tracing::instrument(skip(self))]
    async fn run_reaper(&self) -> Result<(), BoxDynError> {
        let stale = chrono::Utc::now()
            .checked_sub_signed(self.stale_job_interval())
            .ok_or(anyhow::anyhow!("heartbeat interval is too large"))?;

        let ids: Vec<uuid::Uuid> = sqlx::query_scalar!(
            r#"
            UPDATE job_queue
            SET status = $1, updated_at = CURRENT_TIMESTAMP, attempt = attempt - 1
            WHERE status = 'running'
            AND updated_at < $2
            AND attempt < max_attempts
            RETURNING id
            "#,
            JobResultInternal::Pending.to_string(),
            stale,
        )
        .fetch_all(&self.pool)
        .await?;
        if !ids.is_empty() {
            tracing::debug!(name = "stalled job ids", ?ids);
        }

        let ids: Vec<uuid::Uuid> = sqlx::query_scalar!(
            r#"
            UPDATE job_queue
            SET status = $1, updated_at = CURRENT_TIMESTAMP
            WHERE status = $2
            AND attempt >= max_attempts
            RETURNING id
            "#,
            JobResultInternal::Failed.to_string(),
            JobResultInternal::Pending.to_string(),
        )
        .fetch_all(&self.pool)
        .await?;
        if !ids.is_empty() {
            tracing::debug!(name = "no more attempts job ids", ?ids);
        }

        Ok(())
    }
}

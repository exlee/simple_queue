use sqlx::{PgPool, error::BoxDynError};

use super::*;
use result::JobResultInternal;

/// Reaper is a background task that fixes stale jobs.
///
/// It periodically runs and:
/// - Check for stale jobs (not updated for some time but status = `running`)
/// - Marks jobs as failed when their attempt count exceeds the maximum allowed
pub struct Reaper {
    pub heartbeat_interval: tokio::time::Interval,
    pub pool: PgPool,
}

impl Reaper {
    /// Runs the reaper loop.
    pub async fn run(&mut self) {
        loop {
            self.heartbeat_interval.tick().await;
            self.run_reaper().await.ok();
        }
    }

    fn stale_job_interval(&self) -> chrono::TimeDelta {
        chrono::TimeDelta::milliseconds(
            (self.heartbeat_interval.period().as_millis() as f32 * 2.5f32) as i64,
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
            SET
                status = $1,
                updated_at = CURRENT_TIMESTAMP,
                completed_at = CURRENT_TIMESTAMP
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

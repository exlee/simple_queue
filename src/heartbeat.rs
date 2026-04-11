use sqlx::PgPool;
use tracing::Instrument as _;

pub(crate) struct Heartbeat {
    handle: tokio::task::JoinHandle<()>,
    _span: tracing::Span,
}

impl Heartbeat {
    pub(crate) fn start(pool: PgPool, job_id: &uuid::Uuid, interval: ::chrono::Duration) -> Self {
        let _span = tracing::info_span!("heartbeat", job_id = %job_id);
        let job_id = job_id.clone();
        let handle = tokio::spawn(async move {
            let heartbeat_interval = tokio::time::interval(tokio::time::Duration::from_millis(
                interval.num_milliseconds().abs() as u64,
            ));
            tokio::pin!(heartbeat_interval);

            loop {
                heartbeat_interval.tick().await;
                tracing::info!("Heartbeat on job {}", job_id);
                let _ = sqlx::query!(
                    "UPDATE job_queue SET updated_at = CURRENT_TIMESTAMP WHERE id = $1",
                    job_id,
                )
                .execute(&pool)
                .instrument(tracing::info_span!("heartbeat_db"))
                .await;
            }
        });

        Self { handle, _span }
    }
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

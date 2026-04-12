use std::{any::Any, pin::Pin, sync::Arc};

use sqlx::PgPool;
use tracing;

mod heartbeat;
mod job;
mod queue;
mod reaper;
mod result;
pub mod sync;

pub mod prelude {
    pub use super::JobHandler;
    pub use crate::job::Job;
    pub use crate::queue::JobsQueue as Queue;
    pub use crate::result::JobResult;
    pub use ::sqlx::error::BoxDynError;
}

pub use prelude::*;
#[allow(refining_impl_trait)]
pub trait JobHandler: Send + Sync {
    const QUEUE: &'static str;
    fn process(&self, job: &Job) -> impl Future<Output = Result<JobResult, BoxDynError>> + Send;
    fn queue(&self) -> &'static str {
        Self::QUEUE
    }
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
trait DynJobHandler: Send + Sync {
    fn process_dyn<'a>(&'a self, job: &'a Job) -> BoxFuture<'a, Result<JobResult, BoxDynError>>;
}

impl<T: JobHandler> DynJobHandler for T {
    fn process_dyn<'a>(&'a self, job: &'a Job) -> BoxFuture<'a, Result<JobResult, BoxDynError>> {
        Box::pin(self.process(job))
    }
}
pub async fn setup(pool: &PgPool) -> Result<(), BoxDynError> {
    sqlx::raw_sql(include_str!("../migrations/0001_init.sql"))
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn start(jq: Arc<Queue>) -> tokio::task::JoinSet<()> {
    let mut joinset = tokio::task::JoinSet::new();
    let reaper: reaper::Reaper = jq.reaper().await;
    let _reaper_h = joinset.spawn(async move {
        reaper.run().await;
    });
    let _queue_h = joinset.spawn(async move {
        let _ = jq.run().await;
    });
    joinset
}

use std::pin::Pin;

use crate::prelude::*;
#[allow(refining_impl_trait)]
pub trait Handler: Send + Sync {
    const QUEUE: &'static str;

    /// Job processing function.
    ///
    /// While it's running, heartbeat will be sent to indicate that the job is alive.
    ///
    /// It contains reference to [`SimpleQueue`] (for interacting with the queue) and the [`Job`] given for processing.
    ///
    /// Should return [`JobResult`] indicating success or failure,
    ///
    /// Error result is considered a failure and will be retried,
    /// however it is expected that handler will decide what to do with the error.
    fn process(
        &self,
        queue: &SimpleQueue,
        job: &Job,
    ) -> impl Future<Output = Result<JobResult, BoxDynError>> + Send;

    /// Returns the queue name this handler is associated with.
    fn queue(&self) -> &'static str {
        Self::QUEUE
    }
}

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub(crate) trait DynJobHandler: Send + Sync {
    fn process_dyn<'a>(
        &'a self,
        queue: &'a SimpleQueue,
        job: &'a Job,
    ) -> BoxFuture<'a, Result<JobResult, BoxDynError>>;
}

impl<T: Handler> DynJobHandler for T {
    fn process_dyn<'a>(
        &'a self,
        queue: &'a SimpleQueue,
        job: &'a Job,
    ) -> BoxFuture<'a, Result<JobResult, BoxDynError>> {
        Box::pin(self.process(queue, job))
    }
}

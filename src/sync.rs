use super::*;
/// Type erased permit that should be returned by JobStrategy
pub struct Permit {
    _permit: std::sync::Arc<dyn Any + Send + Sync>,
}
impl Permit {
    /// Creates a new Permit from any value that implements `Any + Send + Sync`
    pub fn new<T: Any + Send + Sync>(value: T) -> Self {
        Self {
            _permit: Arc::new(value),
        }
    }
}

/// When aquiring a permit from a JobStrategy, permit giver can return
/// an error to impact on the job's processing
#[derive(Debug)]
pub enum JobStrategyError {
    /// The job shouldn't be processed, mark as canceled
    /// (e.g. feature flags, resource no longer available, deprecated jobs etc.)
    CancelJob,
    /// The job should be tried again after a delay
    /// (e.g. resource temporarily unavailable, renewable limits, etc. )
    TryAfter(chrono::Duration),
    /// Retry with backoff strategy (consumes attempt)
    Retry,
    /// Mark job as completed without processing,
    /// e.g. when seasonal job backed off far into future is no longer relevant, but shouldn't
    /// be sent to dead-letter queue
    MarkCompleted,
}

/// Trait for job strategies that define how to acquire final, 3rd permit
pub trait JobStrategy: Send + Sync {
    fn acquire(&self, job: &Job)
    -> crate::handler::BoxFuture<'_, Result<Permit, JobStrategyError>>;
}
pub struct InstantStrategy {}
impl JobStrategy for InstantStrategy {
    fn acquire(
        &self,
        _job: &Job,
    ) -> crate::handler::BoxFuture<'_, Result<Permit, JobStrategyError>> {
        Box::pin(async move { Ok(Permit::new(())) })
    }
}
#[derive(Clone)]
pub enum BackoffStrategy {
    Linear {
        delay: chrono::Duration,
    },
    Exponential {
        factor: f64,
        max_delay: chrono::Duration,
    },
    Custom(fn(i32) -> chrono::Duration),
}
impl Default for BackoffStrategy {
    fn default() -> Self {
        Self::Linear {
            delay: chrono::Duration::seconds(5),
        }
    }
}
impl BackoffStrategy {
    #[tracing::instrument(skip_all,fields(job_id=%job.id))]
    pub fn next_attempt(&self, job: &Job) -> chrono::DateTime<chrono::Utc> {
        match self {
            BackoffStrategy::Linear { delay } => chrono::Utc::now() + *delay,
            BackoffStrategy::Exponential { factor, max_delay } => {
                let delay = chrono::Duration::seconds((job.attempt as f64).powf(*factor) as i64);
                chrono::Utc::now() + delay.min(*max_delay)
            }
            BackoffStrategy::Custom(f) => chrono::Utc::now() + f(job.attempt),
        }
    }
}

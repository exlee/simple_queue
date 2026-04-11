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

/// Trait for job strategies that define how to acquire final, 3rd permit
pub trait JobStrategy: Send + Sync {
    fn acquire(&self) -> BoxFuture<'_, Permit>;
}
pub struct InstantStrategy {}
impl JobStrategy for InstantStrategy {
    fn acquire(&self) -> BoxFuture<'_, Permit> {
        Box::pin(async move { Permit::new(()) })
    }
}
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
    pub fn next_attempt(&self, job: Arc<Job>) -> chrono::DateTime<chrono::Utc> {
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

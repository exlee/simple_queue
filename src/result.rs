/// Result of a job execution, should be returned by the job handler.
pub enum JobResult {
    /// Job processed successfully.
    Success,
    /// Job failed, subject to retry (with BackoffStrategy).
    Failed,
    /// Retry (i.e. attempt count incremented) at a specific time.
    /// IMPORTANT: Job cannot be retried sooner than the backoff strategy allows
    RetryAt(chrono::DateTime<chrono::Utc>),
    /// Reschedule to specific time.
    /// Attempt it NOT consumed. Use with caution.
    /// Queue's max_reprocess_count is a safety measure to prevent infinite rescheduling.
    RescheduleAt(chrono::DateTime<chrono::Utc>),
    /// Handler not found for the job type, status changes to unprocessable.
    HandlerMissing,
    /// Critical failure, job is not retried.
    Critical,
    /// Cancel the job (i.e. do not retry anymore), status changes to cancelled.
    Cancel,
    /// Handler can't process the job (e.g. deserialization failures), status changes to unprocessable.
    Unprocessable,
    /// Internal error, same as Failed but looks nicer when handler encountered recoverable error.
    /// Attempt is consumed.
    InternalError,
}
impl JobResult {
    pub(crate) fn handle(&self) -> JobResultInternal {
        match self {
            JobResult::Success => JobResultInternal::Completed,
            JobResult::Failed => JobResultInternal::Pending,
            JobResult::RetryAt(_) => JobResultInternal::Pending,
            JobResult::RescheduleAt(_) => JobResultInternal::Pending,
            JobResult::InternalError => JobResultInternal::Pending,
            JobResult::HandlerMissing => JobResultInternal::Unprocessable,
            JobResult::Unprocessable => JobResultInternal::Unprocessable,
            JobResult::Critical => JobResultInternal::Critical,
            JobResult::Cancel => JobResultInternal::Cancelled,
        }
    }
}

#[derive(Debug)]
pub(crate) enum JobResultInternal {
    Pending,
    Failed,
    Completed,
    Unprocessable,
    Cancelled,
    Critical,
    Running,
    BadJob,
}
impl std::fmt::Display for JobResultInternal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobResultInternal::Pending => write!(f, "pending"),
            JobResultInternal::Failed => write!(f, "failed"),
            JobResultInternal::Completed => write!(f, "completed"),
            JobResultInternal::Unprocessable => write!(f, "unprocessable"),
            JobResultInternal::Cancelled => write!(f, "cancelled"),
            JobResultInternal::Critical => write!(f, "critical_failure"),
            JobResultInternal::Running => write!(f, "running"),
            JobResultInternal::BadJob => write!(f, "bad_job"),
        }
    }
}

pub(crate) enum AnyJobResult {
    Internal(JobResultInternal),
    Public(JobResult),
}

impl From<JobResult> for AnyJobResult {
    fn from(result: JobResult) -> Self {
        Self::Public(result)
    }
}

impl From<JobResultInternal> for AnyJobResult {
    fn from(result: JobResultInternal) -> Self {
        Self::Internal(result)
    }
}

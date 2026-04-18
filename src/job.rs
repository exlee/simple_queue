use super::*;

/// Represents a job processed/processing by the queue.
pub struct Job {
    /// Job ID
    pub id: uuid::Uuid,
    /// Fingerprint, made as a soft key for deduplication purposes or cancelling.
    /// Does not violate any database constraints.
    pub fingerprint: Option<String>,
    /// Unique key - database constraint on `unique_key` jobs in `pending` and `running` states.
    /// Repeated inserts with the same `unique_key` will be rejected by the database.
    pub unique_key: Option<String>,
    /// Name of the queue job belongs to.
    pub queue: String,
    /// Payload - `serde_json::Value`,
    pub job_data: serde_json::Value,
    /// Current status of the job in string form
    pub status: String,
    /// Timestamp when the job was created.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Timestamp when the job is scheduled to run.
    pub run_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Timestamp when the job was last updated (for use by heartbeat)
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Attempt count of the job
    pub attempt: i32,
    /// Maximum number of attempts for the job
    pub max_attempts: i32,
    pub(crate) reprocess_count: i32,
}
impl std::fmt::Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("id", &self.id)
            .field("fingerprint", &self.fingerprint)
            .field("unique_key", &self.unique_key)
            .field("queue", &self.queue)
            .field("status", &self.status)
            .field("created_at", &self.created_at)
            .field("attempt", &self.attempt)
            .field("max_attempts", &self.max_attempts)
            .finish()
    }
}
impl Default for Job {
    fn default() -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            fingerprint: None,
            unique_key: None,
            queue: "default".into(),
            job_data: serde_json::Value::default(),
            status: result::JobResultInternal::Pending.to_string(),
            created_at: chrono::Utc::now(),
            run_at: None,
            updated_at: None,
            attempt: 0,
            max_attempts: 3,
            reprocess_count: 0,
        }
    }
}

impl Job {
    /// Create a new [`Job`] instance with the given queue name and job payload.
    pub fn new<T: serde::Serialize + serde::de::DeserializeOwned>(
        queue: &'static str,
        job_data: T,
    ) -> Self {
        Self {
            queue: queue.to_string(),
            job_data: serde_json::to_value(job_data).unwrap_or_default(),
            ..Default::default()
        }
    }
    /// Builder method: sets [`Job::unique_key`]
    pub fn with_unique_key(self, unique_key: impl Into<String>) -> Self {
        Self {
            unique_key: Some(unique_key.into()),
            ..self
        }
    }
    /// Builder method: sets [`Job::run_at`]
    pub fn with_run_at(self, run_at: chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            run_at: Some(run_at),
            ..self
        }
    }
    /// Builder method: sets [`Job::max_attempts`]
    pub fn with_max_attempts(self, max_attempts: i32) -> Self {
        Self {
            max_attempts,
            ..self
        }
    }
    /// Builder method: sets [`Job::fingerprint`]
    pub fn with_fingerprint(self, fingerprint: impl Into<String>) -> Self {
        Self {
            fingerprint: Some(fingerprint.into()),
            ..self
        }
    }
}

pub trait JobExt<T> {
    fn into_job(self, queue: &'static str) -> Job;
}

impl<T: serde::Serialize + serde::de::DeserializeOwned> JobExt<T> for T {
    fn into_job(self, queue: &'static str) -> Job {
        Job::new(queue, self)
    }
}

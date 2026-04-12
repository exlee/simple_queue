use super::*;

pub struct Job {
    pub id: uuid::Uuid,
    pub fingerprint: Option<String>,
    pub unique_key: Option<String>,
    pub queue: String,
    pub job_data: serde_json::Value,
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub run_at: Option<chrono::DateTime<chrono::Utc>>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub attempt: i32,
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
    pub fn with_unique_key(self, unique_key: impl Into<String>) -> Self {
        Self {
            unique_key: Some(unique_key.into()),
            ..self
        }
    }
    pub fn with_run_at(self, run_at: chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            run_at: Some(run_at),
            ..self
        }
    }
    pub fn with_max_attempts(self, max_attempts: i32) -> Self {
        Self {
            max_attempts: max_attempts,
            ..self
        }
    }
    pub fn with_fingerprint(self, fingerprint: impl Into<String>) -> Self {
        Self {
            fingerprint: Some(fingerprint.into()),
            ..self
        }
    }
}

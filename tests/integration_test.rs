// simple_queue/tests/integration_test.rs
mod setup;
use setup::*;
use simple_queue::prelude::*;
use sqlx::PgPool;
use std::time::Duration;

struct SuccessHandler;
impl JobHandler for SuccessHandler {
    const QUEUE: &'static str = "test-success";
    async fn process(&self, _job: &Job) -> Result<JobResult, BoxDynError> {
        Ok(JobResult::Success)
    }
}

struct FailHandler;
impl JobHandler for FailHandler {
    const QUEUE: &'static str = "test-fail";
    async fn process(&self, _job: &Job) -> Result<JobResult, BoxDynError> {
        Ok(JobResult::Failed)
    }
}

struct MaxAttemptsFailHandler;
impl JobHandler for MaxAttemptsFailHandler {
    const QUEUE: &'static str = "test-max-attempts-fail";
    async fn process(&self, _job: &Job) -> Result<JobResult, BoxDynError> {
        Ok(JobResult::Failed)
    }
}

struct CancelHandler;
impl JobHandler for CancelHandler {
    const QUEUE: &'static str = "test-cancel";
    async fn process(&self, _job: &Job) -> Result<JobResult, BoxDynError> {
        Ok(JobResult::Cancel)
    }
}

struct CriticalHandler;
impl JobHandler for CriticalHandler {
    const QUEUE: &'static str = "test-critical";
    async fn process(&self, _job: &Job) -> Result<JobResult, BoxDynError> {
        Ok(JobResult::Critical)
    }
}

struct UnprocessableHandler;
impl JobHandler for UnprocessableHandler {
    const QUEUE: &'static str = "test-unprocessable";
    async fn process(&self, _job: &Job) -> Result<JobResult, BoxDynError> {
        Ok(JobResult::Unprocessable)
    }
}

struct RetryAtHandler;
impl JobHandler for RetryAtHandler {
    const QUEUE: &'static str = "test-retry-at";
    async fn process(&self, _job: &Job) -> Result<JobResult, BoxDynError> {
        Ok(JobResult::RetryAt(
            chrono::Utc::now() + chrono::Duration::seconds(5),
        ))
    }
}

struct RescheduleHandler;
impl JobHandler for RescheduleHandler {
    const QUEUE: &'static str = "test-reschedule";
    async fn process(&self, _job: &Job) -> Result<JobResult, BoxDynError> {
        Ok(JobResult::RescheduleAt(
            chrono::Utc::now() + chrono::Duration::seconds(5),
        ))
    }
}
struct RescheduleImmediateHandler;
impl JobHandler for RescheduleImmediateHandler {
    const QUEUE: &'static str = "test-reschedule-immediate";
    async fn process(&self, _job: &Job) -> Result<JobResult, BoxDynError> {
        Ok(JobResult::RescheduleAt(
            chrono::Utc::now() + chrono::Duration::milliseconds(10),
        ))
    }
}

struct WorkHandler;
impl JobHandler for WorkHandler {
    const QUEUE: &'static str = "test-work";
    async fn process(&self, _job: &Job) -> Result<JobResult, BoxDynError> {
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(JobResult::Success)
    }
}

async fn wait_for_status(
    pool: &PgPool,
    queue: &str,
    expected: &str,
    timeout_secs: u64,
) -> Result<String, String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
    while std::time::Instant::now() < deadline {
        let result: Result<(String,), _> = sqlx::query_as(&format!(
            "SELECT status FROM job_queue WHERE queue = '{}'",
            queue
        ))
        .fetch_one(pool)
        .await;
        if let Ok((status,)) = result {
            if status == expected {
                return Ok(status);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(format!(
        "Timeout waiting for status {} on queue {}",
        expected, queue
    ))
}

async fn wait_for_count(
    pool: &PgPool,
    query: &str,
    expected: i64,
    timeout_secs: u64,
) -> Result<i64, String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
    while std::time::Instant::now() < deadline {
        let result: Result<(i64,), _> = sqlx::query_as(query).fetch_one(pool).await;
        if let Ok((count,)) = result {
            if count == expected {
                return Ok(count);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(format!("Timeout waiting for count {}", expected))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_job_creation() {
    let ctx = TestContext::new().await;
    let job = Job::new("test", serde_json::json!({"key": "value"}));
    assert_eq!(job.queue, "test");
    assert_eq!(job.status, "pending");
    assert_eq!(job.attempt, 0);
    assert_eq!(job.max_attempts, 3);
    ctx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_insert_and_process_success() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(SuccessHandler);
    queue
        .insert_job(Job::new("test-success", serde_json::json!({})))
        .await
        .unwrap();
    let result = wait_for_status(&ctx.pool, "test-success", "completed", 5).await;
    assert_eq!(result.unwrap(), "completed");
    ctx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cancel_result() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(CancelHandler);
    queue
        .insert_job(Job::new("test-cancel", serde_json::json!({})))
        .await
        .unwrap();
    let result = wait_for_status(&ctx.pool, "test-cancel", "cancelled", 5).await;
    assert_eq!(result.unwrap(), "cancelled");
    ctx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failed_result() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(FailHandler);
    queue
        .insert_job(Job::new("test-fail", serde_json::json!({})))
        .await
        .unwrap();
    let result = wait_for_status(&ctx.pool, "test-fail", "pending", 5).await;
    // Failed jobs become pending with a future run_at (retry mechanism)
    assert_eq!(result.unwrap(), "pending");
    ctx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_critical_result() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(CriticalHandler);
    queue
        .insert_job(Job::new("test-critical", serde_json::json!({})))
        .await
        .unwrap();
    let result = wait_for_status(&ctx.pool, "test-critical", "critical_failure", 5).await;
    assert_eq!(result.unwrap(), "critical_failure");
    ctx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unprocessable_result() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(UnprocessableHandler);
    queue
        .insert_job(Job::new("test-unprocessable", serde_json::json!({})))
        .await
        .unwrap();
    let result = wait_for_status(&ctx.pool, "test-unprocessable", "unprocessable", 5).await;
    assert_eq!(result.unwrap(), "unprocessable");
    ctx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_retry_at_result() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(RetryAtHandler);
    queue
        .insert_job(Job::new("test-retry-at", serde_json::json!({})))
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut success = false;
    while std::time::Instant::now() < deadline {
        let result: Result<(String, Option<chrono::DateTime<chrono::Utc>>), _> =
            sqlx::query_as("SELECT status, run_at FROM job_queue WHERE queue = 'test-retry-at'")
                .fetch_one(&ctx.pool)
                .await;
        if let Ok((status, run_at)) = result {
            if status == "pending" && run_at.is_some() && run_at.unwrap() > chrono::Utc::now() {
                success = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(success, "Expected pending status with future run_at");
    ctx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reschedule_at_result() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(RescheduleHandler);
    queue
        .insert_job(Job::new("test-reschedule", serde_json::json!({})))
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut success = false;
    while std::time::Instant::now() < deadline {
        let result: Result<(String, i32, Option<chrono::DateTime<chrono::Utc>>), _> =
            sqlx::query_as(
                "SELECT status, attempt, run_at FROM job_queue WHERE queue = 'test-reschedule'",
            )
            .fetch_one(&ctx.pool)
            .await;
        if let Ok((status, attempt, run_at)) = result {
            if status == "pending" && attempt == 0 && run_at.is_some() {
                success = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        success,
        "Expected pending status with attempt=0 and future run_at"
    );
    ctx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unique_key_no_duplicate() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(SuccessHandler);
    queue
        .insert_job(Job::new("test-unique-key", serde_json::json!({})).with_unique_key("unique-1"))
        .await
        .unwrap();
    queue
        .insert_job(Job::new("test-unique-key", serde_json::json!({})).with_unique_key("unique-1"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM job_queue WHERE queue = 'test-unique-key'")
            .fetch_one(&ctx.pool)
            .await
            .unwrap();
    assert_eq!(count.0, 1);
    ctx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_missing_handler() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue
        .insert_job(Job::new("unregistered-queue", serde_json::json!({})))
        .await
        .unwrap();
    let result = wait_for_status(&ctx.pool, "unregistered-queue", "critical_failure", 5).await;
    assert_eq!(result.unwrap(), "critical_failure");
    ctx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_max_attempts() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(MaxAttemptsFailHandler);
    queue
        .insert_job(Job::new("test-max-attempts-fail", serde_json::json!({})).with_max_attempts(5))
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let mut success = false;
    while std::time::Instant::now() < deadline {
        let result: Result<(String, i32, chrono::DateTime<chrono::Utc>), _> = sqlx::query_as(
            "SELECT status, attempt, run_at FROM job_queue WHERE queue = 'test-max-attempts-fail'",
        )
        .fetch_one(&ctx.pool)
        .await;
        if let Ok((status, attempt, run_at)) = result {
            tracing::info!(
                "status: {}, attempt: {}, run_at: {}",
                status,
                attempt,
                run_at
            );
            if status == "failed" && attempt == 5 {
                success = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(success, "Expected failed status with attempt=5");
    ctx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_processing() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(WorkHandler);

    for i in 0..20 {
        queue
            .insert_job(Job::new("test-work", serde_json::json!({"index": i})))
            .await
            .unwrap();
    }

    let result = wait_for_count(
        &ctx.pool,
        "SELECT COUNT(*) FROM job_queue WHERE status = 'completed' AND queue = 'test-work'",
        20,
        10,
    )
    .await;
    assert_eq!(result.unwrap(), 20);
    ctx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_job_with_fingerprint() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(SuccessHandler);
    queue
        .insert_job(Job::new("test-success", serde_json::json!({})).with_fingerprint("fp-123"))
        .await
        .unwrap();
    let result = wait_for_status(&ctx.pool, "test-success", "completed", 5).await;
    assert_eq!(result.unwrap(), "completed");

    let row: (Option<String>,) =
        sqlx::query_as("SELECT fingerprint FROM job_queue WHERE fingerprint = 'fp-123'")
            .fetch_one(&ctx.pool)
            .await
            .unwrap();
    assert_eq!(row.0, Some("fp-123".to_string()));
    ctx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_poison_job() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(RescheduleImmediateHandler);
    let queue_name = RescheduleImmediateHandler::QUEUE;
    queue
        .insert_job(Job::new(queue_name, serde_json::json!({})))
        .await
        .unwrap();

    tracing::info!("test");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut success = false;
    while std::time::Instant::now() < deadline {
        let result: Result<(String, i32, chrono::DateTime<chrono::Utc>, i32), _> = sqlx::query_as(
            "SELECT status, attempt, run_at, reprocess_count FROM job_queue WHERE queue = $1",
        )
        .bind(queue_name)
        .fetch_one(&ctx.pool)
        .await;
        if let Ok((status, attempt, run_at, reprocess_count)) = result {
            tracing::info!(
                "status: {}, attempt: {}, run_at: {}, reprocess_count: {}",
                status,
                attempt,
                run_at,
                reprocess_count
            );
            if status == "bad_job" {
                success = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(success, "Expected failed status with attempt=5");
    ctx.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_queue_isolation() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;

    // Register slow handler with semaphore limit of 10
    struct SlowHandler;
    impl JobHandler for SlowHandler {
        const QUEUE: &'static str = "slow-queue";
        async fn process(&self, _job: &Job) -> Result<JobResult, BoxDynError> {
            // Simulate slow work
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok(JobResult::Success)
        }
    }

    // Register fast handler
    struct FastHandler;
    impl JobHandler for FastHandler {
        const QUEUE: &'static str = "fast-queue";
        async fn process(&self, _job: &Job) -> Result<JobResult, BoxDynError> {
            // Simulate fast work
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(JobResult::Success)
        }
    }

    queue.register_handler(SlowHandler);
    queue.register_handler(FastHandler);

    let handle = {
        let queue = queue.clone();
        // Add many jobs to slow queue (exceeding semaphore limit)

        tokio::spawn(async move {
            for i in 0..1000 {
                queue
                    .insert_job(Job::new("slow-queue", serde_json::json!({"index": i})))
                    .await
                    .unwrap();
            }

            // Add few jobs to fast queue
            for i in 0..500 {
                queue
                    .insert_job(Job::new("fast-queue", serde_json::json!({"index": i})))
                    .await
                    .unwrap();
            }
        })
        .abort_handle()
    };
    // Wait for fast queue jobs to complete
    let result = wait_for_count(
        &ctx.pool,
        "SELECT COUNT(*) FROM job_queue WHERE status = 'completed' AND queue = 'fast-queue'",
        500,
        1000,
    )
    .await;

    handle.abort();
    assert_eq!(result.unwrap(), 500);

    // Verify slow queue jobs are still processing (not blocked by fast queue)
    let unprocessed_slow_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM job_queue WHERE queue = 'slow-queue' AND status != 'completed'",
    )
    .fetch_one(&ctx.pool)
    .await
    .unwrap();

    // At least some slow jobs should still be processing (not all completed due to semaphore limit)
    tracing::info!("unprocessed_slow_count: {}", unprocessed_slow_count.0);
    assert!(unprocessed_slow_count.0 > 200);

    ctx.cleanup().await;
}

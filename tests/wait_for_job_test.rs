#![cfg(feature = "wait-for-job")]

mod setup;
use setup::*;
use simple_queue::prelude::*;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

struct WaitForSuccessHandler;
impl Handler for WaitForSuccessHandler {
    const QUEUE: &'static str = "wfj-success";
    async fn process(&self, _queue: &SimpleQueue, _job: &Job) -> Result<JobResult, BoxDynError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(JobResult::Success)
    }
}

struct WaitForFailHandler;
impl Handler for WaitForFailHandler {
    const QUEUE: &'static str = "wfj-fail";
    async fn process(&self, _queue: &SimpleQueue, _job: &Job) -> Result<JobResult, BoxDynError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(JobResult::Failed)
    }
}

struct WaitForCriticalHandler;
impl Handler for WaitForCriticalHandler {
    const QUEUE: &'static str = "wfj-critical";
    async fn process(&self, _queue: &SimpleQueue, _job: &Job) -> Result<JobResult, BoxDynError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(JobResult::Critical)
    }
}

struct WaitForUnprocessableHandler;
impl Handler for WaitForUnprocessableHandler {
    const QUEUE: &'static str = "wfj-unprocessable";
    async fn process(&self, _queue: &SimpleQueue, _job: &Job) -> Result<JobResult, BoxDynError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(JobResult::Unprocessable)
    }
}

struct WaitForCancelHandler;
impl Handler for WaitForCancelHandler {
    const QUEUE: &'static str = "wfj-cancel";
    async fn process(&self, _queue: &SimpleQueue, _job: &Job) -> Result<JobResult, BoxDynError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(JobResult::Cancel)
    }
}

struct NoWaiterSuccessHandler;
impl Handler for NoWaiterSuccessHandler {
    const QUEUE: &'static str = "wfj-no-waiter-success";
    async fn process(&self, _queue: &SimpleQueue, _job: &Job) -> Result<JobResult, BoxDynError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(JobResult::Success)
    }
}

struct NoWaiterFailHandler;
impl Handler for NoWaiterFailHandler {
    const QUEUE: &'static str = "wfj-no-waiter-fail";
    async fn process(&self, _queue: &SimpleQueue, _job: &Job) -> Result<JobResult, BoxDynError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(JobResult::Failed)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_wait_for_job_success() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(WaitForSuccessHandler);

    let result = queue
        .insert_job_and_wait(Job::new("wfj-success", serde_json::json!({})))
        .await
        .unwrap();

    let (rx, _id) = result.expect("job should be inserted and return a waiter");

    // The receiver should resolve when the job completes successfully
    let outcome = tokio::time::timeout(Duration::from_secs(5), rx).await;
    assert!(outcome.is_ok(), "rx should resolve within timeout");
    assert!(
        outcome.unwrap().is_ok(),
        "rx should not be dropped without sending"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_wait_for_job_success_db_state() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(WaitForSuccessHandler);

    let result = queue
        .insert_job_and_wait(Job::new("wfj-success", serde_json::json!({})))
        .await
        .unwrap();

    let (rx, job_id) = result.expect("job should be inserted and return a waiter");

    // Wait for the rx to resolve
    let outcome = tokio::time::timeout(Duration::from_secs(5), rx).await;
    assert!(outcome.is_ok(), "rx should resolve within timeout");
    outcome.unwrap().expect("rx should receive value");

    // Immediately verify the job is marked completed in the database
    let row: (String,) = sqlx::query_as("SELECT status FROM job_queue WHERE id = $1")
        .bind(job_id)
        .fetch_one(&ctx.pool)
        .await
        .expect("job should exist in database");

    assert_eq!(
        row.0, "completed",
        "job must be marked as completed in DB when rx signal is received"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_wait_for_job_first_failure() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(WaitForFailHandler);

    let result = queue
        .insert_job_and_wait(Job::new("wfj-fail", serde_json::json!({})))
        .await
        .unwrap();

    let (rx, _id) = result.expect("job should be inserted and return a waiter");

    // The receiver should resolve on the first failure (not wait for retries)
    let outcome = tokio::time::timeout(Duration::from_secs(5), rx).await;
    assert!(
        outcome.is_ok(),
        "rx should resolve within timeout on first failure"
    );
    assert!(
        outcome.unwrap().is_ok(),
        "rx should not be dropped without sending"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_wait_for_job_critical_failure() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(WaitForCriticalHandler);

    let result = queue
        .insert_job_and_wait(Job::new("wfj-critical", serde_json::json!({})))
        .await
        .unwrap();

    let (rx, _id) = result.expect("job should be inserted and return a waiter");

    let outcome = tokio::time::timeout(Duration::from_secs(5), rx).await;
    assert!(
        outcome.is_ok(),
        "rx should resolve within timeout on critical failure"
    );
    assert!(
        outcome.unwrap().is_ok(),
        "rx should not be dropped without sending"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_wait_for_job_unprocessable() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(WaitForUnprocessableHandler);

    let result = queue
        .insert_job_and_wait(Job::new("wfj-unprocessable", serde_json::json!({})))
        .await
        .unwrap();

    let (rx, _id) = result.expect("job should be inserted and return a waiter");

    let outcome = tokio::time::timeout(Duration::from_secs(5), rx).await;
    assert!(
        outcome.is_ok(),
        "rx should resolve within timeout on unprocessable"
    );
    assert!(
        outcome.unwrap().is_ok(),
        "rx should not be dropped without sending"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_wait_for_job_cancel() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(WaitForCancelHandler);

    let result = queue
        .insert_job_and_wait(Job::new("wfj-cancel", serde_json::json!({})))
        .await
        .unwrap();

    let (rx, _id) = result.expect("job should be inserted and return a waiter");

    let outcome = tokio::time::timeout(Duration::from_secs(5), rx).await;
    assert!(
        outcome.is_ok(),
        "rx should resolve within timeout on cancel"
    );
    assert!(
        outcome.unwrap().is_ok(),
        "rx should not be dropped without sending"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_no_waiter_success_does_not_fail() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(NoWaiterSuccessHandler);

    // Insert a job without waiting — no waiter is registered
    queue
        .insert_job(Job::new("wfj-no-waiter-success", serde_json::json!({})))
        .await
        .unwrap();

    // Wait for the job to complete via DB polling to confirm processing didn't panic
    let result = wait_for_status(&ctx.pool, "wfj-no-waiter-success", "completed", 5).await;

    assert_eq!(result.unwrap(), "completed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_no_waiter_fail_does_not_fail() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(NoWaiterFailHandler);

    // Insert a job without waiting — no waiter is registered
    queue
        .insert_job(Job::new("wfj-no-waiter-fail", serde_json::json!({})))
        .await
        .unwrap();

    // A failed job goes back to pending with a future run_at (retry mechanism)
    let result = wait_for_status(&ctx.pool, "wfj-no-waiter-fail", "pending", 5).await;

    assert_eq!(result.unwrap(), "pending");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_insert_jobs_and_wait_all_resolve() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;
    queue.register_handler(WaitForSuccessHandler);

    let jobs = vec![
        Job::new("wfj-success", serde_json::json!({"idx": 0})),
        Job::new("wfj-success", serde_json::json!({"idx": 1})),
        Job::new("wfj-success", serde_json::json!({"idx": 2})),
    ];

    let waiters = queue.insert_jobs_and_wait(jobs).await.unwrap();
    assert_eq!(waiters.len(), 3);

    // All receivers should resolve as each job completes
    for (i, (rx, _id)) in waiters.into_iter().enumerate() {
        let outcome = tokio::time::timeout(Duration::from_secs(10), rx).await;
        assert!(
            outcome.is_ok(),
            "rx for job index {} should resolve within timeout",
            i
        );
        assert!(
            outcome.unwrap().is_ok(),
            "rx for job index {} should not be dropped without sending",
            i
        );
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn wait_for_status(
    pool: &sqlx::PgPool,
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

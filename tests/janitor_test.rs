#![cfg(feature = "janitor")]

mod setup;
use setup::*;
use simple_queue::prelude::*;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Insert a job row directly with the given status, bypassing the queue logic.
async fn insert_job_with_status(pool: &sqlx::PgPool, queue: &str, status: &str) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    sqlx::query!(
        r#"
        INSERT INTO job_queue (id, queue, job_data, status, attempt, max_attempts)
        VALUES ($1, $2, '{}', $3, 0, 3)
        "#,
        id,
        queue,
        status,
    )
    .execute(pool)
    .await
    .expect("failed to insert job");
    id
}

/// Count rows in a table matching a queue and optional status.
async fn count_in(pool: &sqlx::PgPool, table: &str, id: uuid::Uuid) -> i64 {
    let row: (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {} WHERE id = $1", table))
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("count query failed");
    row.0
}

/// Spawn a queue with an unreachably long janitor interval so the background
/// janitor task never fires during a test – we call the methods manually.
async fn spawn_queue_no_janitor(pool: &sqlx::PgPool) -> std::sync::Arc<SimpleQueue> {
    spawn_queue_with(pool, |q| {
        q.with_janitor_interval(tokio::time::Duration::from_secs(3600))
            .with_empty_poll_sleep(tokio::time::Duration::from_millis(100))
            .with_heartbeat_interval(tokio::time::Duration::from_millis(500))
            .with_default_backoff_strategy(simple_queue::sync::BackoffStrategy::Linear {
                delay: chrono::Duration::milliseconds(100),
            })
            .with_max_reprocess_count(10)
    })
    .await
}

// ---------------------------------------------------------------------------
// run_archiver tests
// ---------------------------------------------------------------------------

/// A completed job must be removed from job_queue and appear in job_queue_archive.
#[tokio::test(flavor = "multi_thread")]
async fn test_archiver_moves_completed_job() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue_no_janitor(&ctx.pool).await;

    let id = insert_job_with_status(&ctx.pool, "janitor-archive-completed", "completed").await;

    let janitor = queue.janitor().await;
    janitor.run_archiver().await.expect("run_archiver failed");

    assert_eq!(
        count_in(&ctx.pool, "job_queue", id).await,
        0,
        "completed job should be removed from job_queue"
    );
    assert_eq!(
        count_in(&ctx.pool, "job_queue_archive", id).await,
        1,
        "completed job should appear in job_queue_archive"
    );
}

/// All completed jobs in one run are archived together.
#[tokio::test(flavor = "multi_thread")]
async fn test_archiver_moves_multiple_completed_jobs() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue_no_janitor(&ctx.pool).await;

    let mut inserted = Vec::new();
    for _ in 0..5 {
        let id = insert_job_with_status(&ctx.pool, "janitor-archive-multi", "completed").await;
        inserted.push(id);
    }

    let janitor = queue.janitor().await;
    janitor.run_archiver().await.expect("run_archiver failed");

    for id in &inserted {
        assert_eq!(
            count_in(&ctx.pool, "job_queue", *id).await,
            0,
            "job {id} should be gone from job_queue"
        );
        assert_eq!(
            count_in(&ctx.pool, "job_queue_archive", *id).await,
            1,
            "job {id} should be in job_queue_archive"
        );
    }
}

/// The archiver must not touch jobs whose status is not `completed`.
#[tokio::test(flavor = "multi_thread")]
async fn test_archiver_ignores_non_completed_statuses() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue_no_janitor(&ctx.pool).await;

    let untouched_statuses = [
        "pending",
        "running",
        "failed",
        "cancelled",
        "critical_failure",
        "unprocessable",
        "bad_job",
    ];

    let mut ids = Vec::new();
    for status in &untouched_statuses {
        let id = insert_job_with_status(&ctx.pool, "janitor-archive-ignore", status).await;
        ids.push((*status, id));
    }

    let janitor = queue.janitor().await;
    janitor.run_archiver().await.expect("run_archiver failed");

    for (status, id) in &ids {
        assert_eq!(
            count_in(&ctx.pool, "job_queue", *id).await,
            1,
            "status={status}: job should still be in job_queue"
        );
        assert_eq!(
            count_in(&ctx.pool, "job_queue_archive", *id).await,
            0,
            "status={status}: job must NOT appear in job_queue_archive"
        );
    }
}

/// Archiver on an empty queue must succeed without errors.
#[tokio::test(flavor = "multi_thread")]
async fn test_archiver_empty_queue_is_noop() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue_no_janitor(&ctx.pool).await;

    let janitor = queue.janitor().await;
    janitor
        .run_archiver()
        .await
        .expect("run_archiver should not fail on empty queue");
}

// ---------------------------------------------------------------------------
// run_dlq tests
// ---------------------------------------------------------------------------

/// Every status that the DLQ handles must be moved to job_queue_archive.
#[tokio::test(flavor = "multi_thread")]
async fn test_dlq_moves_all_terminal_statuses() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue_no_janitor(&ctx.pool).await;

    // Statuses that run_dlq is responsible for
    let dlq_statuses = ["unprocessable", "cancelled", "critical_failure", "bad_job"];

    let mut ids = Vec::new();
    for status in &dlq_statuses {
        let id = insert_job_with_status(&ctx.pool, "janitor-dlq-terminal", status).await;
        ids.push((*status, id));
    }

    let janitor = queue.janitor().await;
    janitor.run_dlq().await.expect("run_dlq failed");

    for (status, id) in &ids {
        assert_eq!(
            count_in(&ctx.pool, "job_queue", *id).await,
            0,
            "status={status}: job should be removed from job_queue"
        );
        assert_eq!(
            count_in(&ctx.pool, "job_queue_dlq", *id).await,
            1,
            "status={status}: job should appear in job_queue_dlq"
        );
    }
}

/// DLQ must not touch `running` or `completed` jobs; those belong to other paths.
#[tokio::test(flavor = "multi_thread")]
async fn test_dlq_ignores_running_and_completed() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;

    let ignored_statuses = ["running", "completed"];

    let mut ids = Vec::new();
    for status in &ignored_statuses {
        let id = insert_job_with_status(&ctx.pool, "janitor-dlq-ignore", status).await;
        ids.push((*status, id));
    }

    let janitor = queue.janitor().await;
    janitor.run_dlq().await.expect("run_dlq failed");

    for (status, id) in &ids {
        assert_eq!(
            count_in(&ctx.pool, "job_queue", *id).await,
            1,
            "status={status}: job should still be in job_queue"
        );
        assert_eq!(
            count_in(&ctx.pool, "job_queue_archive", *id).await,
            0,
            "status={status}: job must NOT appear in job_queue_archive"
        );
    }
}

/// DLQ on an empty queue must succeed without errors.
#[tokio::test(flavor = "multi_thread")]
async fn test_dlq_empty_queue_is_noop() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue_no_janitor(&ctx.pool).await;

    let janitor = queue.janitor().await;
    janitor
        .run_dlq()
        .await
        .expect("run_dlq should not fail on empty queue");
}

// ---------------------------------------------------------------------------
// Cross-method isolation tests
// ---------------------------------------------------------------------------

/// run_archiver must not accidentally process DLQ statuses and vice-versa:
/// run_archiver leaves DLQ statuses untouched, run_dlq leaves completed untouched.
#[tokio::test(flavor = "multi_thread")]
async fn test_archiver_and_dlq_do_not_overlap() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue_no_janitor(&ctx.pool).await;

    let completed_id = insert_job_with_status(&ctx.pool, "janitor-overlap", "completed").await;
    let cancelled_id = insert_job_with_status(&ctx.pool, "janitor-overlap", "cancelled").await;

    // Running only the archiver should leave the cancelled job alone.
    {
        let janitor = queue.janitor().await;
        janitor.run_archiver().await.expect("run_archiver failed");
    }

    assert_eq!(
        count_in(&ctx.pool, "job_queue", completed_id).await,
        0,
        "completed job should have been archived"
    );
    assert_eq!(
        count_in(&ctx.pool, "job_queue", cancelled_id).await,
        1,
        "cancelled job should still be in job_queue after archiver"
    );

    // Now run the DLQ; it should move the remaining cancelled job.
    {
        let janitor = queue.janitor().await;
        janitor.run_dlq().await.expect("run_dlq failed");
    }

    assert_eq!(
        count_in(&ctx.pool, "job_queue", cancelled_id).await,
        0,
        "cancelled job should be removed from job_queue after dlq"
    );
    assert_eq!(
        count_in(&ctx.pool, "job_queue_dlq", cancelled_id).await,
        1,
        "cancelled job should be in job_queue_dlq after dlq"
    );
}

/// Run both archiver and DLQ in sequence and verify all eligible jobs move
/// while ineligible ones remain.
#[tokio::test(flavor = "multi_thread")]
async fn test_full_janitor_cycle_moves_correct_jobs() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue_no_janitor(&ctx.pool).await;

    // Jobs that should be archived by run_archiver
    let to_archive = [(
        "completed",
        insert_job_with_status(&ctx.pool, "janitor-full", "completed").await,
    )];

    // Jobs that should be moved to DLQ by run_dlq
    let to_dlq = [
        (
            "cancelled",
            insert_job_with_status(&ctx.pool, "janitor-full", "cancelled").await,
        ),
        (
            "critical_failure",
            insert_job_with_status(&ctx.pool, "janitor-full", "critical_failure").await,
        ),
        (
            "unprocessable",
            insert_job_with_status(&ctx.pool, "janitor-full", "unprocessable").await,
        ),
        (
            "bad_job",
            insert_job_with_status(&ctx.pool, "janitor-full", "bad_job").await,
        ),
    ];

    // Jobs that should remain in job_queue
    let to_keep = [(
        "running",
        insert_job_with_status(&ctx.pool, "janitor-full", "running").await,
    )];

    let janitor = queue.janitor().await;
    janitor.run_archiver().await.expect("run_archiver failed");
    janitor.run_dlq().await.expect("run_dlq failed");

    for (status, id) in &to_archive {
        assert_eq!(
            count_in(&ctx.pool, "job_queue", *id).await,
            0,
            "status={status}: should be gone from job_queue"
        );
        assert_eq!(
            count_in(&ctx.pool, "job_queue_archive", *id).await,
            1,
            "status={status}: should be in job_queue_archive"
        );
    }

    for (status, id) in &to_dlq {
        assert_eq!(
            count_in(&ctx.pool, "job_queue", *id).await,
            0,
            "status={status}: should be gone from job_queue"
        );
        assert_eq!(
            count_in(&ctx.pool, "job_queue_dlq", *id).await,
            1,
            "status={status}: should be in job_queue_dlq"
        );
    }

    for (status, id) in &to_keep {
        assert_eq!(
            count_in(&ctx.pool, "job_queue", *id).await,
            1,
            "status={status}: should stay in job_queue"
        );
        assert_eq!(
            count_in(&ctx.pool, "job_queue_archive", *id).await,
            0,
            "status={status}: must NOT be in job_queue_archive"
        );
    }
}

// ---------------------------------------------------------------------------
// End-to-end: queue processes a job to completion, janitor archives it
// ---------------------------------------------------------------------------

struct SuccessHandler;
impl Handler for SuccessHandler {
    const QUEUE: &'static str = "janitor-e2e-success";
    async fn process(&self, _queue: &SimpleQueue, _job: &Job) -> Result<JobResult, BoxDynError> {
        Ok(JobResult::Success)
    }
}

struct CancelHandler;
impl Handler for CancelHandler {
    const QUEUE: &'static str = "janitor-e2e-cancel";
    async fn process(&self, _queue: &SimpleQueue, _job: &Job) -> Result<JobResult, BoxDynError> {
        Ok(JobResult::Cancel)
    }
}

struct CriticalHandler;
impl Handler for CriticalHandler {
    const QUEUE: &'static str = "janitor-e2e-critical";
    async fn process(&self, _queue: &SimpleQueue, _job: &Job) -> Result<JobResult, BoxDynError> {
        Ok(JobResult::Critical)
    }
}

struct UnprocessableHandler;
impl Handler for UnprocessableHandler {
    const QUEUE: &'static str = "janitor-e2e-unprocessable";
    async fn process(&self, _queue: &SimpleQueue, _job: &Job) -> Result<JobResult, BoxDynError> {
        Ok(JobResult::Unprocessable)
    }
}
/// Wait until a job lands in the given table (i.e. after janitor sweeps it).
async fn wait_for_table(
    pool: &sqlx::PgPool,
    id: uuid::Uuid,
    timeout_secs: u64,
    table: &str,
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
    while std::time::Instant::now() < deadline {
        if count_in(pool, table, id).await == 1 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(format!(
        "Timeout waiting for job {id} to appear in {table}"
    ))
}

/// Wait until the job_queue row for `id` disappears.
#[allow(dead_code)]
async fn wait_for_removal(
    pool: &sqlx::PgPool,
    id: uuid::Uuid,
    timeout_secs: u64,
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
    while std::time::Instant::now() < deadline {
        if count_in(pool, "job_queue", id).await == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(format!(
        "Timeout waiting for job {id} to be removed from job_queue"
    ))
}

/// Full round-trip: queue processes the job to `completed`, then the archiver
/// sweeps it into job_queue_archive.
#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_completed_job_is_archived() {
    let ctx = TestContext::new().await;

    // Use a short janitor interval so the background task fires quickly.
    let queue = spawn_queue_with(&ctx.pool, |q| {
        q.with_janitor_interval(tokio::time::Duration::from_millis(200))
            .with_empty_poll_sleep(tokio::time::Duration::from_millis(100))
            .with_heartbeat_interval(tokio::time::Duration::from_millis(500))
            .with_default_backoff_strategy(simple_queue::sync::BackoffStrategy::Linear {
                delay: chrono::Duration::milliseconds(100),
            })
            .with_max_reprocess_count(10)
    })
    .await;

    queue.register_handler(SuccessHandler);

    let job = Job::new("janitor-e2e-success", serde_json::json!({}));
    let id = job.id;
    queue.insert_job(job).await.unwrap();

    wait_for_table(&ctx.pool, id, 10, "job_queue_archive")
        .await
        .expect("completed job should reach job_queue_archive");

    assert_eq!(
        count_in(&ctx.pool, "job_queue", id).await,
        0,
        "completed job must not remain in job_queue"
    );
}

/// Full round-trip: queue sets status to `cancelled`, DLQ sweeps it to archive.
#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_cancelled_job_is_archived_by_dlq() {
    let ctx = TestContext::new().await;

    let queue = spawn_queue_with(&ctx.pool, |q| {
        q.with_janitor_interval(tokio::time::Duration::from_millis(200))
            .with_empty_poll_sleep(tokio::time::Duration::from_millis(100))
            .with_heartbeat_interval(tokio::time::Duration::from_millis(500))
            .with_default_backoff_strategy(simple_queue::sync::BackoffStrategy::Linear {
                delay: chrono::Duration::milliseconds(100),
            })
            .with_max_reprocess_count(10)
    })
    .await;

    queue.register_handler(CancelHandler);

    let job = Job::new("janitor-e2e-cancel", serde_json::json!({}));
    let id = job.id;
    queue.insert_job(job).await.unwrap();

    wait_for_table(&ctx.pool, id, 10, "job_queue_dlq")
        .await
        .expect("cancelled job should reach job_queue_dlq via DLQ");

    assert_eq!(
        count_in(&ctx.pool, "job_queue", id).await,
        0,
        "cancelled job must not remain in job_queue"
    );
}

/// Full round-trip: critical_failure status is swept by the DLQ.
#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_critical_job_is_archived_by_dlq() {
    let ctx = TestContext::new().await;

    let queue = spawn_queue_with(&ctx.pool, |q| {
        q.with_janitor_interval(tokio::time::Duration::from_millis(200))
            .with_empty_poll_sleep(tokio::time::Duration::from_millis(100))
            .with_heartbeat_interval(tokio::time::Duration::from_millis(500))
            .with_default_backoff_strategy(simple_queue::sync::BackoffStrategy::Linear {
                delay: chrono::Duration::milliseconds(100),
            })
            .with_max_reprocess_count(10)
    })
    .await;

    queue.register_handler(CriticalHandler);

    let job = Job::new("janitor-e2e-critical", serde_json::json!({}));
    let id = job.id;
    queue.insert_job(job).await.unwrap();

    wait_for_table(&ctx.pool, id, 10, "job_queue_dlq")
        .await
        .expect("critical_failure job should reach job_queue_dlq via DLQ");

    assert_eq!(
        count_in(&ctx.pool, "job_queue", id).await,
        0,
        "critical_failure job must not remain in job_queue"
    );
}

/// Full round-trip: unprocessable status is swept by the DLQ.
#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_unprocessable_job_is_archived_by_dlq() {
    let ctx = TestContext::new().await;

    let queue = spawn_queue_with(&ctx.pool, |q| {
        q.with_janitor_interval(tokio::time::Duration::from_millis(200))
            .with_empty_poll_sleep(tokio::time::Duration::from_millis(100))
            .with_heartbeat_interval(tokio::time::Duration::from_millis(500))
            .with_default_backoff_strategy(simple_queue::sync::BackoffStrategy::Linear {
                delay: chrono::Duration::milliseconds(100),
            })
            .with_max_reprocess_count(10)
    })
    .await;

    queue.register_handler(UnprocessableHandler);

    let job = Job::new("janitor-e2e-unprocessable", serde_json::json!({}));
    let id = job.id;
    queue.insert_job(job).await.unwrap();

    wait_for_table(&ctx.pool, id, 10, "job_queue_dlq")
        .await
        .expect("unprocessable job should reach job_queue_dlq via DLQ");

    assert_eq!(
        count_in(&ctx.pool, "job_queue", id).await,
        0,
        "unprocessable job must not remain in job_queue"
    );
}

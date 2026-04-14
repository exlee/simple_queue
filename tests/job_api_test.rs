mod setup;
use serde_json::json;
use setup::*;
use simple_queue::prelude::*;

#[tokio::test]
async fn test_insert_job() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;

    let job = Job::new("test_queue", json!({"key": "value"}));

    let result = queue.insert_job(job).await.unwrap();

    assert!(result.is_some());

    // Verify the job was inserted
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_queue WHERE id = $1")
        .bind(result.unwrap())
        .fetch_one(&ctx.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);

    ctx.cleanup().await;
}

#[tokio::test]
async fn test_insert_job_with_unique_key() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;

    let job = Job::new("test_queue", json!({"key": "value"})).with_unique_key("unique_key_123");

    let result = queue.insert_job(job).await.unwrap();

    assert!(result.is_some());

    // Verify the job was inserted with unique key
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_queue WHERE unique_key = $1")
        .bind("unique_key_123")
        .fetch_one(&ctx.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);

    ctx.cleanup().await;
}

#[tokio::test]
async fn test_insert_job_duplicate_unique_key() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;

    // Insert first job with unique key
    let job1 = Job::new("test_queue", json!({"key": "value1"})).with_unique_key("unique_key_123");

    let result1 = queue.insert_job(job1).await.unwrap();
    assert!(result1.is_some());

    // Try to insert a second job with same unique key (should be a no-op)
    let job2 = Job::new("test_queue", json!({"key": "value2"})).with_unique_key("unique_key_123");

    let result2 = queue.insert_job(job2).await.unwrap();
    assert!(result2.is_none()); // Should be None since duplicate

    // Verify only one job exists
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_queue WHERE unique_key = $1")
        .bind("unique_key_123")
        .fetch_one(&ctx.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);

    ctx.cleanup().await;
}

#[tokio::test]
async fn test_insert_job_duplicate_unique_key_but_cancelled() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;

    // Insert first job with unique key and set to cancelled
    let job1 = Job::new("test_queue", json!({"key": "value1"})).with_unique_key("unique_key_123");

    queue.insert_job(job1).await.unwrap();

    // Cancel the job
    queue
        .cancel_job_by_unique_key("unique_key_123")
        .await
        .unwrap();

    // Now try to insert a new job with same unique key (should succeed)
    let job2 = Job::new("test_queue", json!({"key": "value2"})).with_unique_key("unique_key_123");

    let result = queue.insert_job(job2).await.unwrap();
    assert!(result.is_some()); // Should succeed since previous job was cancelled

    // Verify two jobs exist (one cancelled, one new)
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_queue WHERE unique_key = $1")
        .bind("unique_key_123")
        .fetch_one(&ctx.pool)
        .await
        .unwrap();
    assert_eq!(count, 2);

    ctx.cleanup().await;
}

#[tokio::test]
async fn test_cancel_job_by_unique_key() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;

    // Insert a job with unique key
    let job = Job::new("test_queue", json!({"key": "value"}))
        .with_unique_key("unique_key_123")
        .with_run_at(chrono::Utc::now() + chrono::Duration::seconds(1));

    queue.insert_job(job).await.unwrap();

    // Verify job is pending
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM job_queue WHERE unique_key = $1")
            .bind("unique_key_123")
            .fetch_optional(&ctx.pool)
            .await
            .unwrap();
    assert_eq!(status, Some("pending".to_string()));

    // Cancel the job
    queue
        .cancel_job_by_unique_key("unique_key_123")
        .await
        .unwrap();

    // Verify job is now cancelled
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM job_queue WHERE unique_key = $1")
            .bind("unique_key_123")
            .fetch_optional(&ctx.pool)
            .await
            .unwrap();
    assert_eq!(status, Some("cancelled".to_string()));

    ctx.cleanup().await;
}

#[tokio::test]
async fn test_cancel_all_jobs_by_fingerprint() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;

    // Insert jobs with same fingerprint
    let set_job = |job: Job| {
        job.with_fingerprint("fingerprint_123")
            .with_run_at(chrono::Utc::now() + chrono::Duration::seconds(1))
    };
    let job1 = set_job(Job::new("test_queue", json!({"key": "value1"})));
    let job2 = set_job(Job::new("test_queue", json!({"key": "value2"})));

    queue.insert_job(job1).await.unwrap();
    queue.insert_job(job2).await.unwrap();

    // Verify both jobs are pending
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM job_queue WHERE fingerprint = $1 AND status = 'pending'",
    )
    .bind("fingerprint_123")
    .fetch_one(&ctx.pool)
    .await
    .unwrap();
    assert_eq!(count, 2);

    // Cancel all jobs with this fingerprint
    queue
        .cancel_all_jobs_by_fingerprint("fingerprint_123")
        .await
        .unwrap();

    // Verify both jobs are now cancelled
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM job_queue WHERE fingerprint = $1 AND status = 'cancelled'",
    )
    .bind("fingerprint_123")
    .fetch_one(&ctx.pool)
    .await
    .unwrap();
    assert_eq!(count, 2);

    ctx.cleanup().await;
}
#[tokio::test]
async fn test_insert_jobs() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;

    let jobs = vec![
        Job::new("test_queue", json!({"key": "value1"})),
        Job::new("test_queue", json!({"key": "value2"})),
    ];

    let result = queue.insert_jobs(jobs).await.unwrap();

    assert_eq!(result.len(), 2);

    // Verify both jobs were inserted
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_queue WHERE queue = $1")
        .bind("test_queue")
        .fetch_one(&ctx.pool)
        .await
        .unwrap();
    assert_eq!(count, 2);

    ctx.cleanup().await;
}

#[tokio::test]
async fn test_insert_jobs_conflicting() {
    let ctx = TestContext::new().await;
    let queue = spawn_queue(&ctx.pool).await;

    let jobs = vec![
        Job::new("test_queue_conflicting", json!({"key": "value1"})).with_unique_key("key"),
        Job::new("test_queue_conflicting", json!({"key": "value2"})).with_unique_key("key"),
    ];

    let result = queue.insert_jobs(jobs).await;

    assert!(result.is_err());

    // Verify both jobs were inserted
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_queue")
        .fetch_one(&ctx.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);

    ctx.cleanup().await;
}

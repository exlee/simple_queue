use simple_queue::prelude::*;
use sqlx::PgPool;
use std::sync::Arc;

pub struct ProducerHandler {
    producer_id: usize,
}

impl Handler for ProducerHandler {
    const QUEUE: &'static str = "producer-queue";

    async fn process(&self, _queue: &SimpleQueue, job: &Job) -> Result<JobResult, BoxDynError> {
        let data: serde_json::Value = job.job_data.clone();
        println!(
            "Producer {} processed job {:?}: {:?}",
            self.producer_id, job.id, data
        );
        // Simulate some work
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        Ok(JobResult::Success)
    }
}

#[tokio::main]
pub async fn main() {
    let pool = PgPool::connect(std::env::var("DATABASE_URL").unwrap().as_str())
        .await
        .unwrap();

    // Run migrations to set up the database
    simple_queue::setup(&pool).await.unwrap();

    let queue = Arc::new(
        SimpleQueue::new(pool.clone())
            .with_heartbeat_interval(tokio::time::Duration::from_millis(100)),
    );

    // Register the job handler
    let handler = ProducerHandler { producer_id: 0 };
    queue.register_handler(handler);

    // Spawn the queue processor
    let queue_handle = tokio::spawn({
        let q = queue.clone();
        async move {
            q.run(None).await.unwrap();
        }
    });

    // Spawn 10 producer tasks that continuously insert jobs
    let mut producer_handles = Vec::new();
    for producer_id in 0..10 {
        let q = queue.clone();
        let handle = tokio::spawn(async move {
            let mut count = 0usize;
            loop {
                let job = Job::new(
                    "producer-queue",
                    serde_json::json!({
                        "producer_id": producer_id,
                        "sequence": count,
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    }),
                );
                if let Err(e) = q.insert_job(job).await {
                    eprintln!("Producer {} failed to insert job: {}", producer_id, e);
                } else {
                    println!("Producer {} inserted job #{}", producer_id, count);
                }
                count += 1;
                // Small random-ish delay to create natural dynamics
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        });
        producer_handles.push(handle);
    }

    // Let it run for a while then exit
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    println!("Shutting down after 30 seconds...");

    // Abort all producers
    for handle in producer_handles {
        handle.abort();
    }
    queue_handle.abort();

    println!("Done");
}

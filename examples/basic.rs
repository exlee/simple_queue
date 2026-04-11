use simple_queue::prelude::*;
use sqlx::{PgPool, error::BoxDynError};

pub struct Sth {}
// pub struct Sth2 {}
impl JobHandler for Sth {
    const QUEUE: &'static str = "normal-queue-j1";
    async fn process(&self, _job: &Job) -> Result<JobResult, BoxDynError> {
        Ok(JobResult::Success)
    }
}
#[tokio::main]
pub async fn main() {
    let pool = PgPool::connect(std::env::var("DATABASE_URL").unwrap().as_str())
        .await
        .unwrap();
    let jq = Queue::new(pool.clone());
    let j = Sth {};
    //let j2 = Sth2 {};
    let job = Job::new("normal-queue", String::from(""));
    let job2 = Job::new("normal-queue2", String::from(""));
    jq.register_handler(j);
    //jq.register_handler("normal-queue2", j2);
    jq.insert_job(job).await.unwrap();
    jq.insert_job(job2).await.unwrap();
    jq.run().await.unwrap();
}

use std::sync::{Arc, OnceLock};

use simple_queue::SimpleQueue;
use sqlx::{PgPool, postgres::PgPoolOptions};

const DB_URL: &str = "postgres://postgres@localhost:5432/simple_queue";

pub async fn spawn_queue_with(
    pool: &PgPool,
    config_fn: impl FnOnce(SimpleQueue) -> SimpleQueue,
) -> Arc<SimpleQueue> {
    let queue = Arc::new(config_fn(SimpleQueue::new(pool.clone())));
    let queue2 = queue.clone();
    tokio::spawn(async move {
        let joinset = simple_queue::start(queue2.clone()).await;
        std::future::pending::<()>().await;
        let _ = joinset;
    });
    queue
}

#[allow(dead_code)]
pub async fn spawn_queue(pool: &PgPool) -> Arc<SimpleQueue> {
    let default_queue = |queue: SimpleQueue| {
        queue
            .with_default_backoff_strategy(simple_queue::sync::BackoffStrategy::Linear {
                delay: chrono::Duration::milliseconds(100),
            })
            .with_max_reprocess_count(10)
            .with_empty_poll_sleep(tokio::time::Duration::from_millis(100))
            .with_heartbeat_interval(tokio::time::Duration::from_millis(500))
    };
    return spawn_queue_with(pool, default_queue).await;
}

pub struct TestContext {
    pub pool: PgPool,
    pub admin_pool: PgPool,
    pub schema: String,
}

impl TestContext {
    pub async fn new() -> Self {
        init_tracing();
        let schema = format!(
            "test_schema_{}",
            uuid::Uuid::new_v4().to_string().replace("-", "")
        );
        tracing::info!("schema: {}", schema);

        let admin_pool = PgPool::connect(DB_URL).await.unwrap();

        sqlx::raw_sql(&format!("CREATE SCHEMA \"{}\"", schema))
            .execute(&admin_pool)
            .await
            .unwrap();

        let pool = PgPoolOptions::new()
            .max_connections(20)
            .min_connections(5)
            .connect(&format!("{}?options=--search_path%3D{}", DB_URL, schema))
            .await
            .unwrap();

        simple_queue::setup(&pool).await.unwrap();

        Self {
            pool,
            admin_pool,
            schema,
        }
    }

    pub async fn cleanup(self) {
        let _ = sqlx::raw_sql(&format!("DROP SCHEMA \"{}\" CASCADE", self.schema))
            .execute(&self.admin_pool)
            .await;
        self.admin_pool.close().await;
    }
}

pub fn init_tracing() {
    static TRACING: OnceLock<()> = OnceLock::new();
    TRACING.get_or_init(|| {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer() // routes to cargo test output
            .init();
    });
}

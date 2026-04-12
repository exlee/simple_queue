use super::JobsQueue;
use crate::janitor;
use crate::reaper;

// Main loop
impl JobsQueue {
    /// Return a reaper instance that fixes stale jobs
    pub async fn reaper(&self) -> reaper::Reaper {
        let pool = self.pool.clone();
        let heartbeat_interval = tokio::time::interval(self.heartbeat_interval);
        reaper::Reaper {
            pool,
            heartbeat_interval,
        }
    }
    pub async fn janitor(&self) -> janitor::Janitor {
        let pool = self.pool.clone();
        let interval = tokio::time::interval(self.janitor_interval);
        janitor::Janitor { pool, interval }
    }
}

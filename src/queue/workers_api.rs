#[cfg(feature = "janitor")]
use crate::janitor;
use crate::reaper;
use crate::*;

// Main loop
impl SimpleQueue {
    /// Return a [`reaper::Reaper`] instance that fixes stale jobs
    ///
    /// Useful when you want to control [`reaper::Reaper`] thread yourself.
    pub async fn reaper(&self) -> reaper::Reaper {
        let pool = self.pool.clone();
        let heartbeat_interval = tokio::time::interval(self.heartbeat_interval);
        reaper::Reaper {
            pool,
            heartbeat_interval,
        }
    }
    #[cfg(feature = "janitor")]
    pub async fn janitor(&self) -> janitor::Janitor {
        let pool = self.pool.clone();
        let interval = tokio::time::interval(self.janitor_interval);
        janitor::Janitor { pool, interval }
    }
}

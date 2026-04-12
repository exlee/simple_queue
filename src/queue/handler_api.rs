use std::sync::Arc;

use super::{JobHandler, JobsQueue};
impl JobsQueue {
    /// Register a job handler for a specific queue.
    pub fn register_handler(&self, handler: impl JobHandler + 'static) {
        let queue = handler.queue();
        self.job_registry.insert(queue, Arc::new(handler));
    }
}

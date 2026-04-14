use std::sync::Arc;

use crate::*;
impl SimpleQueue {
    /// Register a job handler for a specific queue.
    pub fn register_handler(&self, handler: impl Handler + 'static) -> &Self {
        let queue = handler.queue();
        self.job_registry.insert(queue, Arc::new(handler));
        self
    }
}

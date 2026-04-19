use std::sync::OnceLock;

use dashmap::DashMap;

use crate::{BoxDynError, Job};

static WAITING_CHANNELS: OnceLock<DashMap<uuid::Uuid, tokio::sync::oneshot::Sender<()>>> =
    OnceLock::new();

fn get_waiting_channels() -> &'static DashMap<uuid::Uuid, tokio::sync::oneshot::Sender<()>> {
    WAITING_CHANNELS.get_or_init(|| DashMap::new())
}

/// Register a oneshot channel for the given job ID and return the receiver.
/// When the job completes (i.e., the associated [`Guard`] is dropped), the receiver
/// will be signaled with `Ok(())`.
pub fn wait_for_job(id: uuid::Uuid) -> tokio::sync::oneshot::Receiver<()> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let dm = get_waiting_channels();
    dm.insert(id, tx);
    rx
}

/// A RAII guard that signals job completion when dropped.
/// If a sender is held, dropping this guard will send `()` to the paired receiver,
/// unblocking any caller awaiting [`wait_for_job`].
pub struct Guard(Option<tokio::sync::oneshot::Sender<()>>);
/// Remove the waiting channel for the given job ID and return a [`Guard`].
/// Returns `None` if no waiter was registered for this job ID.
/// When the returned guard is dropped, the paired receiver will be signaled.
pub fn get_waiting_guard(id: uuid::Uuid) -> Option<Guard> {
    let dm = get_waiting_channels();
    if let Some((_id, ch)) = dm.remove(&id) {
        Some(Guard(Some(ch)))
    } else {
        None
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(ch) = self.0.take() {
            let _ = ch.send(());
        }
    }
}

type WaitingRx = tokio::sync::oneshot::Receiver<()>;
impl super::SimpleQueue {
    /// Insert a single job into the queue and return a receiver that will be
    /// signaled when the job completes. If the insert was a no-op (e.g., due to
    /// a duplicate unique key), `Ok(None)` is returned.
    pub async fn insert_job_and_wait(
        &self,
        job: Job,
    ) -> Result<Option<(WaitingRx, uuid::Uuid)>, BoxDynError> {
        self.insert_job(job).await.map(|res| {
            if let Some(id) = res {
                let rx = wait_for_job(id);
                Some((rx, id))
            } else {
                None
            }
        })
    }
    /// Insert multiple jobs into the queue and return a receiver for each job
    /// that will be signaled when the corresponding job completes.
    pub async fn insert_jobs_and_wait(
        &self,
        jobs: Vec<Job>,
    ) -> Result<Vec<(WaitingRx, uuid::Uuid)>, BoxDynError> {
        self.insert_jobs(jobs).await.map(|vec| {
            vec.into_iter()
                .map(|id| {
                    let rx = wait_for_job(id);
                    (rx, id)
                })
                .collect()
        })
    }
}

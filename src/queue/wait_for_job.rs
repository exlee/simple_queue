use std::sync::OnceLock;

use dashmap::DashMap;

use crate::{BoxDynError, Job};

static WAITING_CHANNELS: OnceLock<DashMap<uuid::Uuid, tokio::sync::oneshot::Sender<()>>> =
    OnceLock::new();

fn get_waiting_channels() -> &'static DashMap<uuid::Uuid, tokio::sync::oneshot::Sender<()>> {
    WAITING_CHANNELS.get_or_init(|| DashMap::new())
}

pub fn wait_for_job(id: uuid::Uuid) -> tokio::sync::oneshot::Receiver<()> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let dm = get_waiting_channels();
    dm.insert(id, tx);
    rx
}

pub struct Guard(Option<tokio::sync::oneshot::Sender<()>>);
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

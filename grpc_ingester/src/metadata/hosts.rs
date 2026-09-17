//! Per-host concurrency, and backing off a host that says it is overloaded.

use {
    std::{
        collections::HashMap,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    },
    tokio::sync::{OwnedSemaphorePermit, Semaphore},
    tracing::info,
};

const MIN_PAUSE: Duration = Duration::from_secs(5);
const MAX_PAUSE: Duration = Duration::from_secs(600);

struct Host {
    permits: Arc<Semaphore>,
    paused_until: Option<Instant>,
    last_pause: Duration,
}

pub enum Slot {
    Go(OwnedSemaphorePermit),
    /// The host is backing off; try again after this long.
    Wait(Duration),
}

pub struct Hosts {
    per_host: usize,
    hosts: Mutex<HashMap<String, Host>>,
}

impl Hosts {
    pub fn new(per_host: usize) -> Self {
        Self {
            per_host: per_host.max(1),
            hosts: Mutex::new(HashMap::new()),
        }
    }

    pub async fn acquire(&self, host: &str) -> Slot {
        let permits = {
            let mut hosts = self.hosts.lock().unwrap_or_else(|p| p.into_inner());
            let entry = hosts.entry(host.to_owned()).or_insert_with(|| Host {
                permits: Arc::new(Semaphore::new(self.per_host)),
                paused_until: None,
                last_pause: Duration::ZERO,
            });
            if let Some(until) = entry.paused_until {
                let now = Instant::now();
                if until > now {
                    return Slot::Wait(until - now);
                }
                entry.paused_until = None;
            }
            Arc::clone(&entry.permits)
        };
        Slot::Go(permits.acquire_owned().await.expect("host semaphore is never closed"))
    }

    /// Pause `host`: for `retry_after` if it said so, otherwise doubling from 5s to 10min.
    pub fn back_off(&self, host: &str, retry_after: Option<Duration>) -> Duration {
        let mut hosts = self.hosts.lock().unwrap_or_else(|p| p.into_inner());
        let Some(entry) = hosts.get_mut(host) else {
            return MIN_PAUSE;
        };
        let now = Instant::now();
        if entry.paused_until.is_some_and(|until| until > now) {
            return entry.paused_until.map_or(MIN_PAUSE, |until| until - now);
        }
        let pause = retry_after
            .unwrap_or_else(|| (entry.last_pause * 2).max(MIN_PAUSE))
            .clamp(MIN_PAUSE, MAX_PAUSE);
        entry.last_pause = pause;
        entry.paused_until = Some(now + pause);
        info!(host, pause_secs = pause.as_secs(), "metadata host is rate limiting; pausing it");
        pause
    }

    pub fn succeeded(&self, host: &str) {
        let mut hosts = self.hosts.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = hosts.get_mut(host) {
            entry.last_pause = Duration::ZERO;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pauses_then_escalates_then_resets() {
        let hosts = Hosts::new(2);
        assert!(matches!(hosts.acquire("h").await, Slot::Go(_)));
        assert_eq!(hosts.back_off("h", None), MIN_PAUSE);
        assert!(matches!(hosts.acquire("h").await, Slot::Wait(_)));
        // a second 429 while already paused doesn't extend or escalate the pause
        assert!(hosts.back_off("h", None) <= MIN_PAUSE);
        hosts.hosts.lock().unwrap().get_mut("h").unwrap().paused_until = None;
        assert_eq!(hosts.back_off("h", None), MIN_PAUSE * 2);
        hosts.hosts.lock().unwrap().get_mut("h").unwrap().paused_until = None;
        assert_eq!(hosts.back_off("h", Some(Duration::from_secs(90))), Duration::from_secs(90));
        hosts.succeeded("h");
        hosts.hosts.lock().unwrap().get_mut("h").unwrap().paused_until = None;
        assert_eq!(hosts.back_off("h", None), MIN_PAUSE);
    }
}

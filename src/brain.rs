use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::{Instant, sleep_until};

/// The arm's shared state: the rate limiter and the lane-wide pause.
///
/// The pause ("penalty box") is what makes one 429 with `Retry-After`
/// stop *every* request on the arm, not just the one that saw it.
pub(crate) struct Brain {
    limiter: Option<DefaultDirectRateLimiter>,
    pause_until: Mutex<Option<Instant>>,
}

impl Brain {
    pub(crate) fn new(quota: Option<Quota>) -> Self {
        Self {
            limiter: quota.map(RateLimiter::direct),
            pause_until: Mutex::new(None),
        }
    }

    /// Wait out any lane-wide pause, then take a rate-limit token.
    pub(crate) async fn wait_for_turn(&self) {
        self.wait_out_pause().await;
        if let Some(limiter) = &self.limiter {
            limiter.until_ready().await;
        }
    }

    /// Pause the whole arm for `d` (never shortens an existing pause).
    pub(crate) fn pause_for(&self, d: Duration) {
        let until = Instant::now() + d;
        let mut guard = self.pause_until.lock().unwrap_or_else(|e| e.into_inner());
        if guard.is_none_or(|current| until > current) {
            *guard = Some(until);
        }
    }

    async fn wait_out_pause(&self) {
        loop {
            // Copy the instant out; never hold the lock across an await.
            let until = *self.pause_until.lock().unwrap_or_else(|e| e.into_inner());
            match until {
                Some(t) if t > Instant::now() => sleep_until(t).await, // may have been extended: re-check
                _ => return,
            }
        }
    }
}

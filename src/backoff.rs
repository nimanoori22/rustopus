use std::time::Duration;

/// How long to wait between retries when the server didn't say.
///
/// Exponential with a cap. Jitter is "equal jitter": the delay is drawn
/// from `[d/2, d]`, so retries spread out without ever collapsing to ~0.
#[derive(Debug, Clone)]
pub struct Backoff {
    base: Duration,
    factor: f64,
    cap: Duration,
    jitter: bool,
}

impl Backoff {
    /// `base`, `base*2`, `base*4`, ... capped at 30s, with jitter.
    pub fn exponential(base: Duration) -> Self {
        Self { base, factor: 2.0, cap: Duration::from_secs(30), jitter: true }
    }

    /// Always `delay`, no jitter.
    pub fn constant(delay: Duration) -> Self {
        Self { base: delay, factor: 1.0, cap: delay, jitter: false }
    }

    pub fn factor(mut self, factor: f64) -> Self {
        self.factor = factor;
        self
    }

    pub fn cap(mut self, cap: Duration) -> Self {
        self.cap = cap;
        self
    }

    pub fn jitter(mut self, on: bool) -> Self {
        self.jitter = on;
        self
    }

    /// Delay before retry number `attempt` (1 = the wait after the first failure).
    pub fn delay(&self, attempt: u32) -> Duration {
        let exp = attempt.saturating_sub(1).min(32) as i32;
        let raw = self.base.as_secs_f64() * self.factor.powi(exp);
        let cap = self.cap.as_secs_f64();
        let capped = if raw.is_finite() { raw.min(cap) } else { cap }.max(0.0);
        let secs = if self.jitter {
            capped / 2.0 + capped / 2.0 * rand::random::<f64>()
        } else {
            capped
        };
        Duration::from_secs_f64(secs)
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::exponential(Duration::from_millis(500))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_without_jitter_doubles_then_caps() {
        let b = Backoff::exponential(Duration::from_millis(100))
            .cap(Duration::from_secs(1))
            .jitter(false);
        let ms: Vec<u128> = (1..=5).map(|a| b.delay(a).as_millis()).collect();
        assert_eq!(ms, [100, 200, 400, 800, 1000]);
    }

    #[test]
    fn jitter_stays_within_half_to_full() {
        let b = Backoff::exponential(Duration::from_millis(100)).jitter(true);
        for _ in 0..200 {
            let d = b.delay(3); // un-jittered: 400ms
            assert!(d >= Duration::from_millis(200) && d <= Duration::from_millis(400), "{d:?}");
        }
    }

    #[test]
    fn huge_attempt_numbers_do_not_overflow() {
        let b = Backoff::exponential(Duration::from_secs(1)).jitter(false);
        assert_eq!(b.delay(u32::MAX), Duration::from_secs(30));
    }

    #[test]
    fn constant_is_constant() {
        let b = Backoff::constant(Duration::from_millis(7));
        assert_eq!(b.delay(1), b.delay(9));
    }
}

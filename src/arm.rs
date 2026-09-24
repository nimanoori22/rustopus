use crate::{
    backoff::Backoff,
    brain::Brain,
    error::{Error, TransportError},
    outcome::Outcome,
    policy::{Decision, Policy, StatusPolicy},
    transport::{ReqwestTransport, Transport},
};
use governor::Quota;
use std::{num::NonZeroU32, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tracing::Instrument;

/// One arm per API: its own rate limit, its own suckers (concurrency slots),
/// its own [`Policy`].
///
/// `Arm` is a cheap `Clone` (an `Arc` inside) and `Send + Sync`, so hand
/// clones to as many Tokio tasks as you like; they all share the same limits.
/// Nothing is spawned: `send` runs entirely inside the caller's future, so
/// dropping that future cancels the request, its backoff sleep and its place
/// in the queue.
pub struct Arm<P, T = ReqwestTransport> {
    inner: Arc<Inner<P, T>>,
}

impl<P, T> Clone for Arm<P, T> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

struct Inner<P, T> {
    name: String,
    transport: T,
    policy: P,
    suckers: Semaphore,
    brain: Brain,
    backoff: Backoff,
    max_attempts: u32,
    total_timeout: Option<Duration>,
}

impl Arm<StatusPolicy, ReqwestTransport> {
    /// Start building an arm. Defaults: [`StatusPolicy`], 8 suckers, no rate
    /// limit, 4 attempts, default [`Backoff`], no total deadline.
    pub fn builder() -> ArmBuilder<StatusPolicy> {
        ArmBuilder {
            name: "arm".to_owned(),
            quota: None,
            suckers: 8,
            backoff: Backoff::default(),
            max_attempts: 4,
            total_timeout: None,
            policy: StatusPolicy::new(),
        }
    }
}

/// Builder for [`Arm`]. See [`Arm::builder`].
pub struct ArmBuilder<P> {
    name: String,
    quota: Option<Quota>,
    suckers: usize,
    backoff: Backoff,
    max_attempts: u32,
    total_timeout: Option<Duration>,
    policy: P,
}

impl<P> ArmBuilder<P> {
    /// Label used in tracing spans.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Full control over the rate limit, e.g.
    /// `Quota::per_second(n).allow_burst(m)`.
    pub fn rate(mut self, quota: Quota) -> Self {
        self.quota = Some(quota);
        self
    }

    /// Sugar for `rate(Quota::per_second(n))`. Note that governor allows a
    /// burst of `n` up front; use [`rate`](Self::rate) with `allow_burst` to
    /// smooth it out. Panics if `n == 0`.
    pub fn per_second(self, n: u32) -> Self {
        let n = NonZeroU32::new(n).expect("per_second(0) would never send anything");
        self.rate(Quota::per_second(n))
    }

    /// How many requests may be in flight at once on this arm (min 1).
    pub fn suckers(mut self, n: usize) -> Self {
        self.suckers = n.max(1);
        self
    }

    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// Maximum attempts per request, including the first (min 1).
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n.max(1);
        self
    }

    /// Deadline for one `send`, covering *all* attempts, waiting and backoff.
    pub fn total_timeout(mut self, d: Duration) -> Self {
        self.total_timeout = Some(d);
        self
    }

    /// Swap in this API's own policy.
    pub fn policy<Q: Policy>(self, policy: Q) -> ArmBuilder<Q> {
        ArmBuilder {
            name: self.name,
            quota: self.quota,
            suckers: self.suckers,
            backoff: self.backoff,
            max_attempts: self.max_attempts,
            total_timeout: self.total_timeout,
            policy,
        }
    }

    /// Build an arm that talks to the network through `client`.
    pub fn build(self, client: reqwest::Client) -> Arm<P, ReqwestTransport> {
        self.build_with(ReqwestTransport::new(client))
    }

    /// Build an arm over any [`Transport`] (mainly for tests).
    pub fn build_with<T: Transport>(self, transport: T) -> Arm<P, T> {
        Arm {
            inner: Arc::new(Inner {
                name: self.name,
                transport,
                policy: self.policy,
                suckers: Semaphore::new(self.suckers),
                brain: Brain::new(self.quota),
                backoff: self.backoff,
                max_attempts: self.max_attempts,
                total_timeout: self.total_timeout,
            }),
        }
    }
}

impl<P: Policy, T: Transport> Arm<P, T> {
    /// Send `request`, retrying per the policy while respecting this arm's limits.
    ///
    /// The request must be cloneable (any normal GET is), because each attempt
    /// sends a fresh clone.
    pub async fn send(&self, request: reqwest::Request) -> Result<Outcome, Error<P::Error>> {
        // Log host + path only: query strings often carry API keys.
        let span = tracing::info_span!(
            "arm.send",
            arm = %self.inner.name,
            method = %request.method(),
            host = request.url().host_str().unwrap_or(""),
            path = request.url().path(),
        );
        let attempts = self.send_loop(request).instrument(span);
        match self.inner.total_timeout {
            Some(limit) => tokio::time::timeout(limit, attempts)
                .await
                .unwrap_or_else(|_| Err(Error::Deadline)),
            None => attempts.await,
        }
    }

    async fn send_loop(&self, request: reqwest::Request) -> Result<Outcome, Error<P::Error>> {
        let inner = &*self.inner;
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let req = request.try_clone().ok_or(Error::NotCloneable)?;

            let result = {
                // Take a sucker first so a rate token is never spent while
                // waiting for a slot. It is released before any backoff sleep.
                let _sucker = inner
                    .suckers
                    .acquire()
                    .await
                    .expect("the semaphore is never closed");
                inner.brain.wait_for_turn().await;
                inner.transport.send(req).await
            };

            tracing::debug!(attempt, result = %describe(&result), "attempt finished");

            match inner.policy.decide(attempt, &result) {
                Decision::Accept => return result.map_err(Error::Transport),
                Decision::Fail(e) => return Err(Error::Policy(e)),
                Decision::Retry { after } => {
                    if attempt >= inner.max_attempts {
                        return Err(Error::Exhausted { attempts: attempt, last: Box::new(result) });
                    }
                    drop(result);
                    match after {
                        // Whole arm pauses; the next loop iteration waits it out.
                        Some(d) => {
                            tracing::debug!(attempt, delay = ?d, "server-directed pause");
                            inner.brain.pause_for(d);
                        }
                        None => {
                            let d = inner.backoff.delay(attempt);
                            tracing::debug!(attempt, delay = ?d, "backing off");
                            tokio::time::sleep(d).await;
                        }
                    }
                }
            }
        }
    }
}

fn describe(result: &Result<Outcome, TransportError>) -> String {
    match result {
        Ok(o) => format!("status {}", o.status.as_u16()),
        Err(e) => format!("transport error {:?}", e.kind()),
    }
}

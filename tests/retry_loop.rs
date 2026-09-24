mod common;
use common::*;
use rustopus::{Arm, Backoff, Decision, Error, Outcome, Policy, TransportError, TransportErrorKind};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::time::Instant;

fn fast() -> Backoff {
    Backoff::constant(Duration::from_millis(1))
}

/// Arm over a transport we keep a handle to, so we can read its counters.
fn arm_over<T: rustopus::Transport>(
    t: Arc<T>,
    f: impl FnOnce(rustopus::ArmBuilder<rustopus::StatusPolicy>) -> rustopus::ArmBuilder<rustopus::StatusPolicy>,
) -> Arm<rustopus::StatusPolicy, ArcT<T>> {
    f(Arm::builder().backoff(fast())).build_with(ArcT(t))
}

struct ArcT<T>(Arc<T>);
impl<T: rustopus::Transport> rustopus::Transport for ArcT<T> {
    fn send(&self, r: reqwest::Request) -> impl Future<Output = Result<Outcome, TransportError>> + Send {
        self.0.send(r)
    }
}

#[tokio::test]
async fn retries_429_429_then_succeeds() {
    let t = Arc::new(Scripted::new(vec![resp(429), resp(429), resp(200)]));
    let arm = arm_over(t.clone(), |b| b);
    let out = arm.send(request()).await.unwrap();
    assert_eq!(out.status, 200);
    assert_eq!(t.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn client_error_fails_without_retrying() {
    let t = Arc::new(Scripted::new(vec![resp(400)]));
    let arm = arm_over(t.clone(), |b| b);
    match arm.send(request()).await {
        Err(Error::Policy(e)) => assert_eq!(e.status, 400),
        other => panic!("{other:?}"),
    }
    assert_eq!(t.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn gives_up_after_max_attempts() {
    let t = Arc::new(Scripted::new(vec![resp(503), resp(503), resp(503)]));
    let arm = arm_over(t.clone(), |b| b.max_attempts(3));
    match arm.send(request()).await {
        Err(Error::Exhausted { attempts, last }) => {
            assert_eq!(attempts, 3);
            assert_eq!(last.as_ref().as_ref().unwrap().status, 503);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(t.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn retries_transient_transport_errors() {
    let timeout = Err(TransportError::new(TransportErrorKind::Timeout, "slow"));
    let t = Arc::new(Scripted::new(vec![timeout, resp(200)]));
    let arm = arm_over(t.clone(), |b| b);
    assert_eq!(arm.send(request()).await.unwrap().status, 200);
    assert_eq!(t.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn non_retryable_transport_error_surfaces_as_transport() {
    let bad = Err(TransportError::new(TransportErrorKind::Request, "malformed"));
    let t = Arc::new(Scripted::new(vec![bad]));
    let arm = arm_over(t.clone(), |b| b);
    assert!(matches!(arm.send(request()).await, Err(Error::Transport(_))));
    assert_eq!(t.calls.load(Ordering::SeqCst), 1);
}

/// A custom policy: this API answers 200 with `{"error": ...}` in the body.
struct BodyErrorPolicy;
impl Policy for BodyErrorPolicy {
    type Error = String;
    fn decide(&self, _a: u32, r: &Result<Outcome, TransportError>) -> Decision<String> {
        match r {
            Ok(o) if o.text_lossy().contains("rate_limited") => Decision::Retry { after: None },
            Ok(o) if o.text_lossy().contains("error") => Decision::Fail(o.text_lossy()),
            _ => Decision::Accept,
        }
    }
}

#[tokio::test]
async fn custom_policy_can_read_the_body() {
    use bytes::Bytes;
    let mut limited = resp(200).unwrap();
    limited.body = Bytes::from_static(br#"{"error":"rate_limited"}"#);
    let mut broken = resp(200).unwrap();
    broken.body = Bytes::from_static(br#"{"error":"bad series"}"#);

    let t = Arc::new(Scripted::new(vec![Ok(limited), Ok(broken)]));
    let arm = Arm::builder()
        .policy(BodyErrorPolicy)
        .backoff(fast())
        .build_with(ArcT(t.clone()));
    match arm.send(request()).await {
        Err(Error::Policy(msg)) => assert!(msg.contains("bad series")),
        other => panic!("{other:?}"),
    }
    assert_eq!(t.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn suckers_cap_concurrency() {
    let t = Arc::new(Slow::new(Duration::from_millis(50)));
    let arm = arm_over(t.clone(), |b| b.suckers(4));

    let mut set = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let arm = arm.clone();
        set.spawn(async move { arm.send(request()).await.map(|o| o.status) });
    }
    while let Some(r) = set.join_next().await {
        assert_eq!(r.unwrap().unwrap(), 200);
    }
    assert_eq!(t.peak.load(Ordering::SeqCst), 4);
}

#[tokio::test(start_paused = true)]
async fn retry_after_pauses_the_whole_arm() {
    // Request A gets a 429 (Retry-After: 5). Request B starts 100ms later and
    // must also wait for the pause, even though B itself never saw a 429.
    let t = Arc::new(Scripted::new(vec![resp_retry_after(429, 5), resp(200), resp(200)]));
    let arm = arm_over(t.clone(), |b| b);
    let start = Instant::now();

    let a = arm.clone();
    let first = tokio::spawn(async move { a.send(request()).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let second = arm.send(request()).await.unwrap();
    assert_eq!(second.status, 200);
    assert!(start.elapsed() >= Duration::from_millis(4900), "B ran during the pause: {:?}", start.elapsed());
    assert_eq!(first.await.unwrap().unwrap().status, 200);
    assert_eq!(t.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn total_timeout_covers_all_attempts() {
    let t = Arc::new(Scripted::new(vec![resp_retry_after(429, 30), resp(200)]));
    let arm = arm_over(t.clone(), |b| b.total_timeout(Duration::from_secs(2)));
    assert!(matches!(arm.send(request()).await, Err(Error::Deadline)));
}

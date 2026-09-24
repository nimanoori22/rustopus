//! End-to-end through real reqwest against a local mock server.
use rustopus::{Arm, Backoff, Error, Quota};
use std::{num::NonZeroU32, time::{Duration, Instant}};
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::{method, path}};

fn get(server: &MockServer, p: &str) -> reqwest::Request {
    reqwest::Client::new().get(format!("{}{p}", server.uri())).build().unwrap()
}

#[tokio::test]
async fn honours_retry_after_header_over_real_http() {
    let server = MockServer::start().await;
    Mock::given(method("GET")).and(path("/limited"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "1"))
        .up_to_n_times(1)
        .mount(&server).await;
    Mock::given(method("GET")).and(path("/limited"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"ok":true}"#))
        .mount(&server).await;

    let arm = Arm::builder().build(reqwest::Client::new());
    let start = Instant::now();
    let out = arm.send(get(&server, "/limited")).await.unwrap();

    assert_eq!(out.status, 200);
    assert_eq!(out.text_lossy(), r#"{"ok":true}"#);
    assert!(start.elapsed() >= Duration::from_millis(950), "did not wait: {:?}", start.elapsed());
}

#[tokio::test]
async fn non_retryable_status_carries_the_body() {
    let server = MockServer::start().await;
    Mock::given(method("GET")).and(path("/bad"))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad series id"))
        .expect(1)
        .mount(&server).await;

    let arm = Arm::builder().build(reqwest::Client::new());
    match arm.send(get(&server, "/bad")).await {
        Err(Error::Policy(e)) => {
            assert_eq!(e.status, 400);
            assert_eq!(&e.body[..], b"bad series id");
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn rate_limit_spaces_requests_out() {
    let server = MockServer::start().await;
    Mock::given(method("GET")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

    // 2/sec with no burst allowance beyond 1: 5 requests need >= ~2s.
    let quota = Quota::per_second(NonZeroU32::new(2).unwrap()).allow_burst(NonZeroU32::new(1).unwrap());
    let arm = Arm::builder()
        .rate(quota)
        .suckers(10)
        .backoff(Backoff::constant(Duration::from_millis(1)))
        .build(reqwest::Client::new());

    let start = Instant::now();
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..5 {
        let (arm, req) = (arm.clone(), get(&server, "/"));
        set.spawn(async move { arm.send(req).await.map(|o| o.status) });
    }
    while let Some(r) = set.join_next().await {
        assert_eq!(r.unwrap().unwrap(), 200);
    }
    let took = start.elapsed();
    assert!(took >= Duration::from_millis(1900), "too fast: {took:?}");
    assert!(took < Duration::from_secs(4), "too slow: {took:?}");
}

//! Two APIs, two arms, two policies, one runtime. Runs offline against mock servers.
//!
//!     cargo run --example two_arms
use rustopus::{Arm, Backoff, Decision, Outcome, Policy, TransportError};
use std::time::Duration;
use tracing_subscriber::{filter::Targets, prelude::*};
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

/// "Other API": always answers 200, and signals problems inside the JSON body.
struct BodyErrorPolicy;

impl Policy for BodyErrorPolicy {
    type Error = String;

    fn decide(&self, _attempt: u32, result: &Result<Outcome, TransportError>) -> Decision<String> {
        match result {
            Ok(o) if o.text_lossy().contains(r#""error":"busy""#) => Decision::Retry { after: None },
            Ok(o) if o.text_lossy().contains(r#""error""#) => Decision::Fail(o.text_lossy()),
            _ => Decision::Accept,
        }
    }
}

#[tokio::main]
async fn main() {
    // Show only rustopus's own events (attempts, backoff, pauses).
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().compact().with_ansi(false))
        .with(Targets::new().with_target("rustopus", tracing::Level::DEBUG))
        .init();

    // Mock "FRED": 429 with Retry-After once, then fine.
    let fred_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "1"))
        .up_to_n_times(1)
        .mount(&fred_server).await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"observations":[]}"#))
        .mount(&fred_server).await;

    // Mock "other API": says "busy" inside a 200 once, then fine.
    let other_server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"error":"busy"}"#))
        .up_to_n_times(1)
        .mount(&other_server).await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"data":[1,2,3]}"#))
        .mount(&other_server).await;

    let client = reqwest::Client::new();

    // Arm 1: default StatusPolicy, 2 req/s, 4 suckers.
    let fred = Arm::builder()
        .name("fred")
        .per_second(2)
        .suckers(4)
        .build(client.clone());

    // Arm 2: its own policy, more suckers, its own backoff.
    let other = Arm::builder()
        .name("other")
        .policy(BodyErrorPolicy)
        .suckers(16)
        .backoff(Backoff::exponential(Duration::from_millis(100)))
        .build(client.clone());

    let fred_req = client.get(format!("{}/series", fred_server.uri())).build().unwrap();
    let other_req = client.get(format!("{}/data", other_server.uri())).build().unwrap();

    let (a, b) = tokio::join!(fred.send(fred_req), other.send(other_req));
    println!("fred  -> {}", a.unwrap().text_lossy());
    println!("other -> {}", b.unwrap().text_lossy());
}

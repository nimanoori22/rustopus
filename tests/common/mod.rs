#![allow(dead_code)]
use bytes::Bytes;
use http::{HeaderMap, StatusCode, header::RETRY_AFTER};
use rustopus::{Outcome, Transport, TransportError};
use std::{
    collections::VecDeque,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

pub fn resp(status: u16) -> Result<Outcome, TransportError> {
    Ok(Outcome {
        status: StatusCode::from_u16(status).unwrap(),
        headers: HeaderMap::new(),
        body: Bytes::from_static(b"{}"),
    })
}

pub fn resp_retry_after(status: u16, secs: u64) -> Result<Outcome, TransportError> {
    let mut o = resp(status).unwrap();
    o.headers.insert(RETRY_AFTER, secs.to_string().parse().unwrap());
    Ok(o)
}

pub fn request() -> reqwest::Request {
    reqwest::Client::new().get("http://example.invalid/x").build().unwrap()
}

/// Replays scripted results in call order and counts calls.
pub struct Scripted {
    script: Mutex<VecDeque<Result<Outcome, TransportError>>>,
    pub calls: AtomicUsize,
}

impl Scripted {
    pub fn new(script: Vec<Result<Outcome, TransportError>>) -> Self {
        Self { script: Mutex::new(script.into()), calls: AtomicUsize::new(0) }
    }
}

impl Transport for Scripted {
    async fn send(&self, _r: reqwest::Request) -> Result<Outcome, TransportError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.script.lock().unwrap().pop_front().expect("script exhausted")
    }
}

/// Always 200 after `latency`; records the peak number of concurrent calls.
pub struct Slow {
    pub latency: Duration,
    pub in_flight: AtomicUsize,
    pub peak: AtomicUsize,
}

impl Slow {
    pub fn new(latency: Duration) -> Self {
        Self { latency, in_flight: AtomicUsize::new(0), peak: AtomicUsize::new(0) }
    }
}

impl Transport for Slow {
    async fn send(&self, _r: reqwest::Request) -> Result<Outcome, TransportError> {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(self.latency).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        resp(200)
    }
}

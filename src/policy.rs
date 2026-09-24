use crate::{error::{TransportError, TransportErrorKind}, outcome::Outcome};
use bytes::Bytes;
use http::{HeaderMap, StatusCode, header::RETRY_AFTER};
use std::time::Duration;

/// What the arm should do with the result of one attempt.
#[derive(Debug)]
#[non_exhaustive]
pub enum Decision<E> {
    /// Stop and hand the result back as-is (`Ok(outcome)`, or
    /// `Error::Transport` if the attempt was a transport failure).
    Accept,
    /// Try again.
    ///
    /// * `after: None`: wait per the arm's [`Backoff`](crate::Backoff); only this request waits.
    /// * `after: Some(d)`: the server told us how long. The *whole arm* pauses for
    ///   `d`, so the other in-flight requests don't each rediscover the 429.
    Retry { after: Option<Duration> },
    /// Stop and fail with the API's own error.
    Fail(E),
}

/// What a response means for one particular API.
///
/// The arm supplies the mechanics (limits, waiting, the retry loop); the
/// policy only answers "accept, retry, or fail?". It is synchronous on
/// purpose: it sees a fully buffered [`Outcome`], so it can look at status,
/// headers *and* body.
pub trait Policy: Send + Sync + 'static {
    /// The API-specific error returned via [`Decision::Fail`].
    type Error: Send + 'static;

    /// `attempt` starts at 1. `result` is `Err` if no full response arrived.
    fn decide(
        &self,
        attempt: u32,
        result: &Result<Outcome, TransportError>,
    ) -> Decision<Self::Error>;
}

/// Error produced by [`StatusPolicy`] for non-retryable HTTP failures.
#[derive(Debug, Clone, thiserror::Error)]
#[error("HTTP {status}: {}", preview(.body))]
pub struct HttpStatusError {
    pub status: StatusCode,
    pub body: Bytes,
}

fn preview(body: &Bytes) -> String {
    let cut = &body[..body.len().min(200)];
    String::from_utf8_lossy(cut).into_owned()
}

/// Sensible default for plain REST APIs, so a new wrapper needs no policy code.
///
/// * 2xx → accept
/// * timeout / connect / body errors → retry with backoff
/// * 408, 429, 500, 502, 503, 504 → retry, honouring `Retry-After` (in seconds)
/// * any other status → fail with [`HttpStatusError`]
#[derive(Debug, Clone)]
pub struct StatusPolicy {
    max_retry_after: Duration,
}

impl StatusPolicy {
    pub fn new() -> Self {
        Self { max_retry_after: Duration::from_secs(60) }
    }

    /// Upper bound on how long a `Retry-After` may stall the arm (default 60s).
    pub fn max_retry_after(mut self, max: Duration) -> Self {
        self.max_retry_after = max;
        self
    }
}

impl Default for StatusPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl Policy for StatusPolicy {
    type Error = HttpStatusError;

    fn decide(
        &self,
        _attempt: u32,
        result: &Result<Outcome, TransportError>,
    ) -> Decision<HttpStatusError> {
        match result {
            Err(e) => match e.kind() {
                TransportErrorKind::Timeout
                | TransportErrorKind::Connect
                | TransportErrorKind::Body => Decision::Retry { after: None },
                _ => Decision::Accept, // surfaces as Error::Transport
            },
            Ok(o) if o.status.is_success() => Decision::Accept,
            Ok(o) if matches!(o.status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504) => {
                Decision::Retry { after: retry_after(&o.headers, self.max_retry_after) }
            }
            Ok(o) => Decision::Fail(HttpStatusError { status: o.status, body: o.body.clone() }),
        }
    }
}

/// Parses `Retry-After: <seconds>`. The HTTP-date form is not supported yet
/// and is treated as absent (the arm's backoff is used instead).
pub fn retry_after(headers: &HeaderMap, max: Duration) -> Option<Duration> {
    let secs: u64 = headers.get(RETRY_AFTER)?.to_str().ok()?.trim().parse().ok()?;
    Some(Duration::from_secs(secs).min(max))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(status: u16, retry_after: Option<&str>) -> Result<Outcome, TransportError> {
        let mut headers = HeaderMap::new();
        if let Some(v) = retry_after {
            headers.insert(RETRY_AFTER, v.parse().unwrap());
        }
        Ok(Outcome {
            status: StatusCode::from_u16(status).unwrap(),
            headers,
            body: Bytes::from_static(b"body"),
        })
    }

    #[test]
    fn classifies_statuses() {
        let p = StatusPolicy::new();
        assert!(matches!(p.decide(1, &outcome(200, None)), Decision::Accept));
        assert!(matches!(p.decide(1, &outcome(503, None)), Decision::Retry { after: None }));
        assert!(matches!(p.decide(1, &outcome(400, None)), Decision::Fail(_)));
        assert!(matches!(p.decide(1, &outcome(404, None)), Decision::Fail(_)));
    }

    #[test]
    fn honours_and_caps_retry_after() {
        let p = StatusPolicy::new();
        match p.decide(1, &outcome(429, Some("3"))) {
            Decision::Retry { after: Some(d) } => assert_eq!(d, Duration::from_secs(3)),
            other => panic!("{other:?}"),
        }
        match p.decide(1, &outcome(429, Some("99999"))) {
            Decision::Retry { after: Some(d) } => assert_eq!(d, Duration::from_secs(60)),
            other => panic!("{other:?}"),
        }
        // HTTP-date form is ignored for now.
        assert!(matches!(
            p.decide(1, &outcome(429, Some("Wed, 21 Oct 2026 07:28:00 GMT"))),
            Decision::Retry { after: None }
        ));
    }

    #[test]
    fn retries_transient_transport_errors_only() {
        let p = StatusPolicy::new();
        let t = Err(TransportError::new(TransportErrorKind::Timeout, "t"));
        let r = Err(TransportError::new(TransportErrorKind::Request, "bad"));
        assert!(matches!(p.decide(1, &t), Decision::Retry { after: None }));
        assert!(matches!(p.decide(1, &r), Decision::Accept));
    }
}

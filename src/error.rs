use crate::outcome::Outcome;
use std::error::Error as StdError;

/// Broad category of a failure that happened before a full response arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransportErrorKind {
    /// The request or the body read timed out.
    Timeout,
    /// DNS, TCP or TLS connection failure.
    Connect,
    /// The connection broke while the body was being read.
    Body,
    /// The request itself was malformed or could not be sent.
    Request,
    /// Anything else.
    Other,
}

/// A failure that happened before a full response was received.
///
/// Wraps the underlying error (usually `reqwest::Error`) behind a small
/// `kind`, so policies don't depend on reqwest internals and tests can
/// fabricate transport failures.
#[derive(Debug, thiserror::Error)]
#[error("transport error ({kind:?}): {source}")]
pub struct TransportError {
    kind: TransportErrorKind,
    #[source]
    source: Box<dyn StdError + Send + Sync>,
}

impl TransportError {
    pub fn new(
        kind: TransportErrorKind,
        source: impl Into<Box<dyn StdError + Send + Sync>>,
    ) -> Self {
        Self { kind, source: source.into() }
    }

    pub fn kind(&self) -> TransportErrorKind {
        self.kind
    }
}

impl From<reqwest::Error> for TransportError {
    fn from(e: reqwest::Error) -> Self {
        let kind = if e.is_timeout() {
            TransportErrorKind::Timeout
        } else if e.is_connect() {
            TransportErrorKind::Connect
        } else if e.is_body() || e.is_decode() {
            TransportErrorKind::Body
        } else if e.is_request() || e.is_builder() {
            TransportErrorKind::Request
        } else {
            TransportErrorKind::Other
        };
        Self::new(kind, e)
    }
}

/// Why [`Arm::send`](crate::Arm::send) failed. `E` is the policy's own error type.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error<E> {
    /// The request body can't be cloned (e.g. a stream), so it can't be retried.
    #[error("request cannot be retried: its body is not cloneable")]
    NotCloneable,

    /// A transport failure the policy chose not to retry.
    #[error(transparent)]
    Transport(TransportError),

    /// Every allowed attempt failed. `last` is what the final attempt produced.
    #[error("gave up after {attempts} attempts")]
    Exhausted {
        attempts: u32,
        last: Box<Result<Outcome, TransportError>>,
    },

    /// The arm's total deadline (across all attempts) elapsed.
    #[error("total deadline exceeded")]
    Deadline,

    /// The policy rejected the response.
    #[error("policy rejected the response: {0}")]
    Policy(E),
}

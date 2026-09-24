use bytes::Bytes;
use http::{HeaderMap, StatusCode};

/// A fully-received HTTP response: status, headers and the whole body.
///
/// The body is buffered so a [`Policy`](crate::Policy) can inspect it
/// (many APIs put the real error in the JSON) and so a body-read failure
/// can be retried like any other transport failure.
#[derive(Debug, Clone)]
pub struct Outcome {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl Outcome {
    /// The body as text, replacing invalid UTF-8 sequences.
    pub fn text_lossy(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

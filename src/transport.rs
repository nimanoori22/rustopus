use crate::{error::TransportError, outcome::Outcome};
use std::future::Future;

/// Sends one request and returns the fully buffered response.
///
/// The arm talks to the network only through this trait, which is what lets
/// tests replay scripted responses without any I/O.
pub trait Transport: Send + Sync + 'static {
    fn send(
        &self,
        request: reqwest::Request,
    ) -> impl Future<Output = Result<Outcome, TransportError>> + Send;
}

/// The real thing: `reqwest::Client::execute` + reading the whole body.
#[derive(Debug, Clone)]
pub struct ReqwestTransport {
    client: reqwest::Client,
}

impl ReqwestTransport {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl Transport for ReqwestTransport {
    async fn send(&self, request: reqwest::Request) -> Result<Outcome, TransportError> {
        let response = self.client.execute(request).await?;
        let status = response.status();
        let headers = response.headers().clone();
        // A failure while reading the body is a transport error too, so it can be retried.
        let body = response.bytes().await?;
        Ok(Outcome { status, headers, body })
    }
}

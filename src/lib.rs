//! # rustopus
//!
//! Async HTTP request orchestration for API wrapper crates: per-API rate
//! limiting, bounded concurrency, and retry/backoff driven by a pluggable
//! [`Policy`]. Built on `reqwest` + Tokio, and safe to share across a
//! multi-threaded runtime.
//!
//! ## Anatomy
//!
//! | Octopus       | In the code                                                        |
//! |---------------|--------------------------------------------------------------------|
//! | **Arm**       | [`Arm`]: one per API. Cheap to clone, `Send + Sync`.               |
//! | **Suckers**   | The concurrency slots of an arm (`ArmBuilder::suckers`).           |
//! | **Brain**     | Private per-arm state: the rate limiter + the lane-wide pause.     |
//! | **Tentacles** | Your own futures calling `arm.send(..).await`. Nothing is spawned. |
//! | **Policies**  | [`Policy`]: what a response means for *this* API.                  |
//!
//! ```no_run
//! use rustopus::{Arm, Backoff};
//! use std::time::Duration;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let client = reqwest::Client::new();
//! let fred = Arm::builder()
//!     .name("fred")
//!     .per_second(2)
//!     .suckers(4)
//!     .max_attempts(5)
//!     .backoff(Backoff::exponential(Duration::from_millis(500)))
//!     .build(client.clone());
//!
//! let request = client.get("https://api.stlouisfed.org/fred/series").build()?;
//! let outcome = fred.send(request).await?;
//! println!("{} bytes", outcome.body.len());
//! # Ok(()) }
//! ```

mod arm;
mod backoff;
mod brain;
mod error;
mod outcome;
mod policy;
mod transport;

pub use arm::{Arm, ArmBuilder};
pub use backoff::Backoff;
pub use error::{Error, TransportError, TransportErrorKind};
pub use outcome::Outcome;
pub use policy::{Decision, HttpStatusError, Policy, StatusPolicy};
pub use transport::{ReqwestTransport, Transport};

/// Re-exported so wrapper crates use the exact same versions rustopus does.
pub use governor::Quota;
pub use reqwest;

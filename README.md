# rustopus

Async HTTP request orchestration for API wrapper crates: **per-API rate limiting, bounded concurrency, and retry/backoff driven by a pluggable policy.** Built on `reqwest` and Tokio, and safe to share across a multi-threaded runtime.

Write your API wrapper once, hand it an `Arm`, and stop re-implementing rate limiters and retry loops in every crate.

## Features

- **Rate limiting** per API, powered by [`governor`](https://crates.io/crates/governor).
- **Bounded concurrency** via a fixed number of in-flight slots ("suckers").
- **Retry with exponential backoff**, with jitter, driven by a `Policy` you can replace per API.
- **Lane-wide pause**: one `429` with `Retry-After` pauses *every* request on the arm, so concurrent requests don't each rediscover the limit.
- **Total deadline** covering all attempts, waiting and backoff.
- **No background tasks.** `send` runs entirely inside the caller's future. Dropping that future cancels the request, its backoff sleep and its place in the queue.
- **Testable.** The network sits behind a `Transport` trait, so tests can replay scripted responses without any I/O.
- **Key-safe tracing.** Spans log host and path only, never the query string.

## Anatomy

| Octopus       | In the code                                                          |
|---------------|----------------------------------------------------------------------|
| **Arm**       | `Arm`: one per API. Cheap to clone, `Send + Sync`.                   |
| **Suckers**   | The concurrency slots of an arm (`ArmBuilder::suckers`).             |
| **Brain**     | Private per-arm state: the rate limiter and the lane-wide pause.     |
| **Tentacles** | Your own futures calling `arm.send(..).await`. Nothing is spawned.   |
| **Policies**  | `Policy`: what a response means for *this* API.                      |

## Installation

```toml
[dependencies]
rustopus = { git = "https://github.com/nimanoori22/rustopus" }
```

rustopus re-exports `reqwest` and `governor::Quota`. **Use the re-exported `reqwest`** in wrapper crates so your `Client`, `Request` and `Error` types are the exact same ones rustopus uses:

```rust
use rustopus::reqwest::{self, Client};
```

If your crate depends on `reqwest` directly with a different version, Cargo builds two copies and you get confusing "mismatched types" errors. Keep the versions aligned, or use the re-export.

## Quick start

```rust
use rustopus::{Arm, Backoff};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();

    let fred = Arm::builder()
        .name("fred")
        .per_second(2)          // at most 2 requests per second
        .suckers(4)             // at most 4 requests in flight
        .max_attempts(5)        // first try + 4 retries
        .backoff(Backoff::exponential(Duration::from_millis(500)))
        .build(client.clone());

    let request = client.get("https://api.stlouisfed.org/fred/series").build()?;
    let outcome = fred.send(request).await?;

    println!("{} bytes, status {}", outcome.body.len(), outcome.status);
    Ok(())
}
```

`Arm` is a cheap `Clone`. Give a clone to every task and they all share the same limits:

```rust
let arm = fred.clone();
tokio::spawn(async move { arm.send(request).await });
```

## Configuration

`Arm::builder()` defaults: `StatusPolicy`, 8 suckers, no rate limit, 4 attempts, default `Backoff`, no total deadline.

| Method                | Meaning                                                                          |
|-----------------------|----------------------------------------------------------------------------------|
| `name(..)`            | Label used in tracing spans.                                                     |
| `per_second(n)`       | Sugar for `rate(Quota::per_second(n))`. Panics if `n == 0`.                      |
| `rate(quota)`         | Full control, e.g. `Quota::per_second(n).allow_burst(m)`.                        |
| `suckers(n)`          | Max requests in flight at once (minimum 1).                                      |
| `max_attempts(n)`     | Max attempts per request, including the first (minimum 1).                       |
| `backoff(b)`          | Delay strategy between retries when the server didn't say.                       |
| `total_timeout(d)`    | Deadline for one `send`, covering all attempts, waiting and backoff.             |
| `policy(p)`           | Swap in this API's own `Policy`.                                                 |
| `build(client)`       | Build an arm over a `reqwest::Client`.                                           |
| `build_with(t)`       | Build an arm over any `Transport` (mainly for tests).                            |

**Burst note:** `per_second(n)` lets governor allow a burst of `n` requests up front. For evenly spaced requests, use:

```rust
use std::num::NonZeroU32;
use rustopus::Quota;

.rate(Quota::per_second(NonZeroU32::new(2).unwrap()).allow_burst(NonZeroU32::MIN))
```

### Backoff

```rust
Backoff::exponential(Duration::from_millis(500))   // base, base*2, base*4, ... capped at 30s, with jitter
    .factor(2.0)
    .cap(Duration::from_secs(10))
    .jitter(true);

Backoff::constant(Duration::from_secs(1));         // always 1s, no jitter
```

Jitter is "equal jitter": the delay is drawn from `[d/2, d]`, so retries spread out without collapsing to near zero.

## Policies

The arm supplies the mechanics (limits, waiting, the retry loop). A `Policy` answers one question per attempt: **accept, retry, or fail?**

```rust
pub trait Policy: Send + Sync + 'static {
    type Error: Send + 'static;

    fn decide(
        &self,
        attempt: u32,                                   // starts at 1
        result: &Result<Outcome, TransportError>,       // Err if no full response arrived
    ) -> Decision<Self::Error>;
}

pub enum Decision<E> {
    Accept,                              // hand the result back as-is
    Retry { after: Option<Duration> },   // None: per-request backoff; Some(d): pause the whole arm for d
    Fail(E),                             // stop with the API's own error
}
```

The policy is synchronous on purpose. It sees a fully buffered `Outcome`, so it can look at the status, headers *and* body, which matters because many APIs put the real error in the JSON.

### The default: `StatusPolicy`

Sensible for plain REST APIs, so a new wrapper needs no policy code:

| Result                                         | Decision                                     |
|------------------------------------------------|----------------------------------------------|
| 2xx                                            | Accept                                       |
| Timeout, connect or body-read errors           | Retry with backoff                           |
| 408, 429, 500, 502, 503, 504                   | Retry, honouring `Retry-After` (seconds)     |
| Any other status                               | Fail with `HttpStatusError { status, body }` |

`Retry-After` is capped at 60 seconds by default (`StatusPolicy::new().max_retry_after(..)`). The HTTP-date form of the header is not supported yet and is treated as absent.

### A custom policy

Say your API returns `200` with an error inside the JSON body, or uses a status code the default treats as fatal:

```rust
use rustopus::{Arm, Decision, Outcome, Policy, TransportError, TransportErrorKind};

struct MyApiPolicy;

impl Policy for MyApiPolicy {
    type Error = String;

    fn decide(
        &self,
        _attempt: u32,
        result: &Result<Outcome, TransportError>,
    ) -> Decision<String> {
        match result {
            Err(e) => match e.kind() {
                TransportErrorKind::Timeout
                | TransportErrorKind::Connect
                | TransportErrorKind::Body => Decision::Retry { after: None },
                _ => Decision::Accept,
            },
            Ok(o) if o.status.is_success() => Decision::Accept,
            Ok(o) if o.status.is_server_error() => Decision::Retry { after: None },
            Ok(o) => Decision::Fail(o.text_lossy()),
        }
    }
}

let arm = Arm::builder()
    .policy(MyApiPolicy)
    .per_second(5)
    .build(client);
```

`send` then returns `Result<Outcome, Error<String>>`, where `Error::Policy(String)` carries your error.

## Errors

`Arm::send` returns `Result<Outcome, Error<P::Error>>`:

| Variant                    | Meaning                                                                    |
|----------------------------|----------------------------------------------------------------------------|
| `Error::Policy(e)`         | The policy rejected the response (`Decision::Fail`).                       |
| `Error::Transport(e)`      | A transport failure the policy chose not to retry.                         |
| `Error::Exhausted { attempts, last }` | Every allowed attempt failed; `last` holds the final result.    |
| `Error::Deadline`          | The total deadline elapsed.                                                |
| `Error::NotCloneable`      | The request body can't be cloned (e.g. a stream), so it can't be retried. |

`Error` is `#[non_exhaustive]`, so keep a wildcard arm when matching.

`TransportError` wraps the underlying error behind a small `TransportErrorKind` (`Timeout`, `Connect`, `Body`, `Request`, `Other`), so policies don't depend on reqwest internals.

## Using it in a wrapper crate

A typical wrapper holds one `Arm` and builds requests with its own `reqwest::Client`:

```rust
#[derive(Clone)]
pub struct MyClient {
    http: reqwest::Client,
    arm: Arm<StatusPolicy>,
}

impl MyClient {
    async fn get<T: serde::de::DeserializeOwned>(&self, url: url::Url) -> Result<T, MyError> {
        let request = self.http.get(url).build()?;
        let outcome = self.arm.send(request).await.map_err(map_arm_error)?;
        Ok(serde_json::from_slice(&outcome.body)?)
    }
}
```

Every clone of `MyClient` shares the same `Arm`, so rate limits and concurrency caps hold across all tasks.

## Testing

Implement `Transport` to script responses with no network:

```rust
pub trait Transport: Send + Sync + 'static {
    fn send(&self, request: reqwest::Request)
        -> impl Future<Output = Result<Outcome, TransportError>> + Send;
}
```

Then build the arm with `.build_with(my_fake_transport)`.

## Things to know

- **Requests must be cloneable.** Each attempt sends a fresh clone. Any normal GET is fine.
- **Cancellation is free.** Nothing is spawned, so dropping the `send` future cancels everything it was doing.
- **Slots are released before backoff sleeps,** and a rate token is never spent while waiting for a slot.
- **Proxies.** `reqwest::Client::new()` reads `ALL_PROXY`, `HTTPS_PROXY` and friends from the environment. If requests fail instantly with a connect error, check for a stale proxy variable. Build your client with `Client::builder().no_proxy()` if you never want that.
- **Secrets in errors.** `reqwest::Error` display includes the request URL, query string and all. If your API key travels in the query, strip it with `e.without_url()` when converting to `TransportError`.
- **reqwest features.** rustopus does not enable optional reqwest features such as `query`. If your code needs `RequestBuilder::query`, enable the feature in your own `Cargo.toml`. Cargo unifies features across the same reqwest version.

## License

Licensed under the [MIT license](LICENSE).

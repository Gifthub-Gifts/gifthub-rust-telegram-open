//! Send-failure classification (house style: hand-written enum, no anyhow/thiserror).
//!
//! Telegram's failure modes for `sendMessage` collapse into three actions:
//!   `RetryAfter(secs)` — 429; sleep the server-supplied delay (clamped, never trusted raw —
//!                        same rule as gifthub-rust-ai's provider backoff) and retry;
//!   `Transient`        — network/IO/decoding trouble; short doubling backoff and retry;
//!   `Permanent`        — the API understood us and said no (bad token, unknown chat, bot
//!                        blocked, bad request); retrying can only repeat the answer — drop.

use teloxide::RequestError;

#[derive(Debug)]
pub enum SendError {
    RetryAfter(u64),
    Transient(String),
    Permanent(String),
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::RetryAfter(s) => write!(f, "rate limited, retry after {s}s"),
            SendError::Transient(e) => write!(f, "transient send error: {e}"),
            SendError::Permanent(e) => write!(f, "permanent send error: {e}"),
        }
    }
}

impl std::error::Error for SendError {}

impl SendError {
    pub fn is_transient(&self) -> bool {
        !matches!(self, SendError::Permanent(_))
    }
}

pub fn classify(err: RequestError) -> SendError {
    match err {
        RequestError::RetryAfter(secs) => SendError::RetryAfter(secs.seconds() as u64),
        RequestError::Api(api) => SendError::Permanent(api.to_string()),
        // Network hiccups, IO trouble, and undecodable responses are all worth another try.
        other => SendError::Transient(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use teloxide::types::Seconds;
    use teloxide::ApiError;

    #[test]
    fn retry_after_carries_seconds() {
        match classify(RequestError::RetryAfter(Seconds::from_seconds(17))) {
            SendError::RetryAfter(17) => {}
            other => panic!("expected RetryAfter(17), got {other:?}"),
        }
    }

    #[test]
    fn api_errors_are_permanent() {
        let err = classify(RequestError::Api(ApiError::BotBlocked));
        assert!(!err.is_transient());
        assert!(matches!(err, SendError::Permanent(_)));
    }

    #[test]
    fn retry_after_and_transient_are_transient() {
        assert!(classify(RequestError::RetryAfter(Seconds::from_seconds(1))).is_transient());
        let io = RequestError::Io(std::sync::Arc::new(std::io::Error::other("boom")));
        assert!(classify(io).is_transient());
    }

    #[test]
    fn display_is_informative() {
        let s = classify(RequestError::RetryAfter(Seconds::from_seconds(5))).to_string();
        assert!(s.contains("retry after 5s"));
    }
}

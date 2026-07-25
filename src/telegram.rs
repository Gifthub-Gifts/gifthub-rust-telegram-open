//! Telegram delivery: split → send with retries. Plain text, no parse_mode — a MarkdownV2
//! escaping slip turns into a 400 and a LOST alert, and formatting is worth less than
//! delivery certainty here (deliberate deviation from the first design draft).
//!
//! Retry semantics fix the gap in the reference bots (vpn-watchdog drops on any send error):
//! 429 sleeps the server-supplied delay clamped to [1, 90]s (never trust provider delays raw —
//! gifthub-rust-ai's clamp rule), transient errors take a 2s→30s doubling backoff, permanent
//! API answers fail immediately. All sleeps abort on shutdown.

use teloxide::prelude::Requester;
use teloxide::types::ChatId;
use teloxide::Bot;
use tokio::sync::watch;

use crate::error::{classify, SendError};

/// 4000-byte chunks stay under Telegram's 4096 limit (which counts UTF-16 code units; UTF-16
/// units never exceed UTF-8 bytes for any string, so a byte bound is always safe).
pub const CHUNK_LIMIT: usize = 4000;

pub fn build_bot(token: &str) -> Bot {
    Bot::new(token)
}

/// Fan one alert out to every admin chat. One admin failing (blocked bot, never-started chat,
/// bad id) must not block the others — each gets its own send+retry, failures are logged per
/// admin (watchdog-open's fan-out rule). `Ok` when at least one admin received the alert;
/// otherwise the merged error decides requeue-vs-drop (any transient failure wins, because a
/// retry might still reach someone).
pub async fn send_to_admins(
    bot: &Bot,
    chat_ids: &[i64],
    text: &str,
    max_attempts: u32,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), SendError> {
    let mut outcomes = Vec::with_capacity(chat_ids.len());
    for &chat_id in chat_ids {
        let res = send_text(bot, chat_id, text, max_attempts, shutdown).await;
        if let Err(e) = &res {
            log::warn!("delivery to admin {chat_id} failed: {e}");
        }
        outcomes.push(res);
    }
    merge_outcomes(outcomes)
}

/// Send one alert text to one chat (splitting if oversized). Stops at the first failed chunk.
async fn send_text(
    bot: &Bot,
    chat_id: i64,
    text: &str,
    max_attempts: u32,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), SendError> {
    for chunk in split_message(text, CHUNK_LIMIT) {
        send_with_retry(bot, chat_id, &chunk, max_attempts, shutdown).await?;
    }
    Ok(())
}

/// Any success → success. All failed → prefer the transient error (job stays retryable);
/// only when every admin failed permanently is the job a permanent drop.
fn merge_outcomes(outcomes: Vec<Result<(), SendError>>) -> Result<(), SendError> {
    if outcomes.iter().any(|o| o.is_ok()) {
        return Ok(());
    }
    let mut permanent = None;
    for o in outcomes {
        match o {
            Err(e) if e.is_transient() => return Err(e),
            Err(e) => permanent = Some(e),
            Ok(()) => unreachable!("handled above"),
        }
    }
    Err(permanent.unwrap_or_else(|| SendError::Transient("no admin chats configured".into())))
}

async fn send_with_retry(
    bot: &Bot,
    chat_id: i64,
    chunk: &str,
    max_attempts: u32,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), SendError> {
    let mut backoff = 2u64;
    let mut last = SendError::Transient("no attempts made".into());
    for _ in 0..max_attempts.max(1) {
        match bot.send_message(ChatId(chat_id), chunk).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                let err = classify(e);
                match &err {
                    SendError::Permanent(_) => return Err(err),
                    SendError::RetryAfter(secs) => {
                        let delay = (*secs).clamp(1, 90);
                        log::warn!("telegram rate limit; sleeping {delay}s");
                        if sleep_or_shutdown(delay, shutdown).await {
                            return Err(err);
                        }
                    }
                    SendError::Transient(msg) => {
                        log::warn!("transient telegram error: {msg}; retrying in {backoff}s");
                        if sleep_or_shutdown(backoff, shutdown).await {
                            return Err(err);
                        }
                        backoff = next_backoff(backoff);
                    }
                }
                last = err;
            }
        }
    }
    Err(last)
}

/// `true` = shutdown fired during the sleep; abandon the retry loop.
async fn sleep_or_shutdown(secs: u64, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = shutdown.changed() => true,
        _ = tokio::time::sleep(std::time::Duration::from_secs(secs)) => false,
    }
}

fn next_backoff(current: u64) -> u64 {
    (current * 2).min(30)
}

/// Split into ≤`limit`-byte chunks, preferring the last newline in the window, otherwise
/// backing off to the nearest char boundary. The reference implementation
/// (telegram-happ-rust `split_message`) slices at raw byte indexes and panics mid-UTF-8-char —
/// alert text carries emoji, so the boundary walk here is load-bearing.
pub fn split_message(text: &str, limit: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut rest = text;
    while rest.len() > limit {
        let mut end = limit;
        while !rest.is_char_boundary(end) {
            end -= 1;
        }
        let cut = rest[..end].rfind('\n').map(|i| i + 1).unwrap_or(end);
        chunks.push(rest[..cut].to_string());
        rest = &rest[cut..];
    }
    if !rest.is_empty() {
        chunks.push(rest.to_string());
    }
    chunks
}

/// "(+N suppressed …)" suffix for the first send after a collapse window. The period phrase is
/// derived from the window so the message never lies when DEDUPE_WINDOW_SECS is tuned.
pub fn suppressed_suffix(n: u64, window_secs: u64) -> String {
    if n == 0 {
        return String::new();
    }
    let period = if window_secs == 3_600 {
        "in the last hour".to_string()
    } else {
        format!("in the last {} min", window_secs.div_ceil(60))
    };
    format!("\n(+{n} suppressed {period})")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_text_is_one_chunk() {
        assert_eq!(split_message("hello", CHUNK_LIMIT), vec!["hello"]);
    }

    #[test]
    fn splits_at_newline_and_round_trips() {
        let text = format!("{}\n{}", "a".repeat(3_900), "b".repeat(3_900));
        let chunks = split_message(&text, CHUNK_LIMIT);
        assert_eq!(chunks.len(), 2);
        assert!(chunks.iter().all(|c| c.len() <= CHUNK_LIMIT));
        assert_eq!(chunks.concat(), text);
    }

    #[test]
    fn emoji_dense_text_never_splits_mid_char() {
        // Regression vs the reference bot's byte-index splitter: 🚀 is 4 bytes, and 4000 % 4 == 0,
        // so shift by one with a leading char to force a mid-char boundary at the limit.
        let text = format!("x{}", "🚀".repeat(2_500));
        let chunks = split_message(&text, CHUNK_LIMIT);
        assert!(chunks.len() > 1);
        assert!(chunks.iter().all(|c| c.len() <= CHUNK_LIMIT));
        assert_eq!(chunks.concat(), text);
    }

    #[test]
    fn suffix_zero_is_empty() {
        assert_eq!(suppressed_suffix(0, 3_600), "");
    }

    #[test]
    fn suffix_hour_phrasing() {
        assert_eq!(
            suppressed_suffix(5, 3_600),
            "\n(+5 suppressed in the last hour)"
        );
    }

    #[test]
    fn suffix_non_default_window_phrasing() {
        assert_eq!(
            suppressed_suffix(2, 600),
            "\n(+2 suppressed in the last 10 min)"
        );
    }

    #[test]
    fn merge_any_success_wins() {
        let outcomes = vec![Err(SendError::Permanent("chat not found".into())), Ok(())];
        assert!(merge_outcomes(outcomes).is_ok());
    }

    #[test]
    fn merge_prefers_transient_when_all_fail() {
        let outcomes = vec![
            Err(SendError::Permanent("chat not found".into())),
            Err(SendError::Transient("network".into())),
        ];
        assert!(merge_outcomes(outcomes).unwrap_err().is_transient());
    }

    #[test]
    fn merge_all_permanent_is_permanent() {
        let outcomes = vec![
            Err(SendError::Permanent("a".into())),
            Err(SendError::Permanent("b".into())),
        ];
        assert!(!merge_outcomes(outcomes).unwrap_err().is_transient());
    }

    #[test]
    fn backoff_progression_caps_at_30() {
        let mut b = 2;
        let mut seen = Vec::new();
        for _ in 0..6 {
            seen.push(b);
            b = next_backoff(b);
        }
        assert_eq!(seen, vec![2, 4, 8, 16, 30, 30]);
    }
}

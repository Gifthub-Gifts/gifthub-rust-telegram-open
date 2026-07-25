//! Configuration: everything from the environment (`.env` supported, same as the sibling
//! gateways). No registry file — this service has exactly one producer (gifthub's Django) and
//! one destination chat, so plain env vars cover it. Required vars fail loudly at boot
//! (`expect`), never per job; the deploy health check turns a boot panic into a rollback.

use std::sync::OnceLock;

#[derive(Debug)]
pub struct Config {
    /// Must point at the SAME Redis + DB Django's `apps/telegram/gateway.py` pushes to (db 6).
    pub redis_url: String,
    pub jobs_key: String,
    pub heartbeat_key: String,

    pub bot_token: String,
    /// The ops chats every alert fans out to (comma/space-separated env, deduped, order kept).
    /// One admin failing never blocks the others — watchdog-open's fan-out rule.
    pub admin_chat_ids: Vec<i64>,

    /// Jobs older than this are dropped on arrival — after an outage the backlog is history,
    /// not news, and must not spam the chat (Django also LTRIM-caps the list producer-side).
    pub stale_after_secs: u64,
    /// Per-dedupe_key collapse window: first alert sends, repeats inside the window are counted
    /// and suppressed, the next send reports the count.
    pub dedupe_window_secs: u64,

    /// Default 1: alerts stay FIFO and inside Telegram's ~1 msg/sec/chat budget. Raising it
    /// sacrifices ordering; the loop machinery supports it either way.
    pub max_concurrency: usize,
    pub pop_timeout_secs: u64,
    pub request_timeout_secs: u64,
    /// In-service attempts for TRANSIENT send failures before the job is requeued.
    pub send_max_attempts: u32,
}

static CONFIG: OnceLock<Config> = OnceLock::new();

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Comma/space-separated chat ids → deduped Vec, first-seen order kept (watchdog-open's
/// `ADMIN_TELEGRAM_ID` parsing). Garbage entries are skipped with a loud log, not fatal —
/// one typo must not take down alerting for the valid admins.
fn parse_chat_ids(raw: &str) -> Vec<i64> {
    let mut ids = Vec::new();
    for part in raw.split([',', ' ', '\t']).filter(|p| !p.trim().is_empty()) {
        match part.trim().parse::<i64>() {
            Ok(id) => {
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
            Err(_) => log::error!("ignoring invalid chat id '{part}' in TELEGRAM_ADMIN_CHAT_IDS"),
        }
    }
    ids
}

pub fn get_config() -> &'static Config {
    CONFIG.get_or_init(|| {
        dotenv::dotenv().ok();

        let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL must be set (redis db 6)");
        let bot_token = std::env::var("TELOXIDE_TOKEN").expect("TELOXIDE_TOKEN is required");
        let raw_ids = std::env::var("TELEGRAM_ADMIN_CHAT_IDS")
            .or_else(|_| std::env::var("TELEGRAM_ADMIN_CHAT_ID"))
            .expect("TELEGRAM_ADMIN_CHAT_IDS is required (comma/space-separated chat ids)");
        let admin_chat_ids = parse_chat_ids(&raw_ids);
        if admin_chat_ids.is_empty() {
            panic!("TELEGRAM_ADMIN_CHAT_IDS contains no valid chat ids: '{raw_ids}'");
        }

        Config {
            redis_url,
            jobs_key: env_or("TELEGRAM_JOBS_KEY", "telegram_jobs"),
            heartbeat_key: env_or("TELEGRAM_HEARTBEAT_KEY", "telegram_heartbeat"),
            bot_token,
            admin_chat_ids,
            stale_after_secs: env_parse("STALE_AFTER_SECS", 21_600),
            dedupe_window_secs: env_parse("DEDUPE_WINDOW_SECS", 3_600),
            max_concurrency: env_parse("MAX_CONCURRENCY", 1),
            pop_timeout_secs: env_parse("POP_TIMEOUT_SECS", 5),
            request_timeout_secs: env_parse("REQUEST_TIMEOUT_SECS", 30),
            send_max_attempts: env_parse("SEND_MAX_ATTEMPTS", 3),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_parse_falls_back_on_garbage() {
        std::env::set_var("TG_TEST_GARBAGE", "not-a-number");
        assert_eq!(env_parse("TG_TEST_GARBAGE", 42u64), 42);
        std::env::remove_var("TG_TEST_GARBAGE");
    }

    #[test]
    fn chat_ids_parse_dedupe_and_keep_order() {
        assert_eq!(
            parse_chat_ids("1000000001, 1000000002 1000000001"),
            vec![1000000001, 1000000002]
        );
    }

    #[test]
    fn chat_ids_skip_garbage_keep_valid() {
        assert_eq!(parse_chat_ids("nope, 42,"), vec![42]);
        assert!(parse_chat_ids("  ,nope ").is_empty());
    }

    #[test]
    fn env_or_prefers_env() {
        std::env::set_var("TG_TEST_KEY", "custom");
        assert_eq!(env_or("TG_TEST_KEY", "default"), "custom");
        std::env::remove_var("TG_TEST_KEY");
        assert_eq!(env_or("TG_TEST_KEY", "default"), "default");
    }
}

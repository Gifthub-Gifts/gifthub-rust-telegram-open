//! All Redis interaction: job consumption plus the dedupe/heartbeat side-channel.
//!
//! Keys (db 6, alongside Django's `telegram_jobs` producer):
//!   `telegram_jobs`           — the job list (Django RPUSHes the tail, we BLPOP the head:
//!                               opposite ends = FIFO. Mail's BRPOP is acknowledged drift —
//!                               follow gifthub-rust-ai's documented rule, not mail, or a
//!                               backlog starves the oldest alerts);
//!   `tg_dedupe:{key}`         — collapse gate (SET NX EX window): holder of the gate is the
//!                               alert that was sent; repeats inside the TTL are suppressed;
//!   `tg_dedupe_count:{key}`   — how many repeats were suppressed while the gate was held;
//!                               harvested atomically (GET+DEL pipeline — NOT GETDEL, prod
//!                               runs Redis 6.0 and GETDEL needs 6.2) on the next send;
//!   `telegram_heartbeat`      — liveness timestamp with TTL, alarms without an inbound port.
//!
//! Side-channel helpers are fallible-but-forgiving (log, never propagate): dedupe fails OPEN —
//! a duplicate alert beats a lost one.

use redis::aio::ConnectionManager;
use redis::AsyncCommands;

pub async fn init_redis(url: &str) -> Result<ConnectionManager, redis::RedisError> {
    let client = redis::Client::open(url)?;
    ConnectionManager::new(client).await
}

/// Blocking-pop one raw job from the HEAD (BLPOP). `Ok(None)` = timeout, loop again.
pub async fn pop_job(
    conn: &mut ConnectionManager,
    key: &str,
    timeout_secs: u64,
) -> Result<Option<String>, redis::RedisError> {
    let res: Option<(String, String)> = redis::cmd("BLPOP")
        .arg(key)
        .arg(timeout_secs)
        .query_async(conn)
        .await?;
    Ok(res.map(|(_key, payload)| payload))
}

/// Put a raw job back on the tail (transient-exhausted deferral). The 6h stale-drop bounds how
/// long a job can cycle through here during a Telegram outage.
pub async fn requeue(conn: &ConnectionManager, key: &str, raw: &str) {
    let mut conn = conn.clone();
    if let Err(e) = conn.rpush::<_, _, ()>(key, raw).await {
        log::error!("failed to requeue job: {e}");
    }
}

pub async fn heartbeat(conn: &ConnectionManager, key: &str, ttl_secs: u64) {
    let mut conn = conn.clone();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if let Err(e) = conn.set_ex::<_, _, ()>(key, now, ttl_secs).await {
        log::warn!("heartbeat write failed: {e}");
    }
}

/// Try to claim the collapse gate for `dedupe_key` (SET NX EX). `true` = we send; `false` = a
/// send already happened inside the window, suppress. Redis errors fail OPEN (`true`).
pub async fn claim_dedupe_gate(
    conn: &ConnectionManager,
    dedupe_key: &str,
    window_secs: u64,
) -> bool {
    let mut conn = conn.clone();
    let opts = redis::SetOptions::default()
        .conditional_set(redis::ExistenceCheck::NX)
        .with_expiration(redis::SetExpiry::EX(window_secs));
    match conn
        .set_options::<_, _, Option<String>>(format!("tg_dedupe:{dedupe_key}"), 1, opts)
        .await
    {
        Ok(res) => res.is_some(),
        Err(e) => {
            log::warn!("dedupe gate check failed for '{dedupe_key}': {e} (sending anyway)");
            true
        }
    }
}

/// Release a gate we claimed but whose send did not go out — the next alert for this key must
/// not be suppressed behind a message nobody received.
pub async fn release_dedupe_gate(conn: &ConnectionManager, dedupe_key: &str) {
    let mut conn = conn.clone();
    if let Err(e) = conn.del::<_, ()>(format!("tg_dedupe:{dedupe_key}")).await {
        log::warn!("failed to release dedupe gate for '{dedupe_key}': {e}");
    }
}

/// Count one suppressed repeat. The counter outlives the gate (2× window) so the next send can
/// still harvest it after the gate expires.
pub async fn incr_suppressed(conn: &ConnectionManager, dedupe_key: &str, window_secs: u64) -> u64 {
    let mut conn = conn.clone();
    let key = format!("tg_dedupe_count:{dedupe_key}");
    match redis::pipe()
        .atomic()
        .incr(&key, 1)
        .expire(&key, (window_secs * 2) as i64)
        .ignore()
        .query_async::<(u64,)>(&mut conn)
        .await
    {
        Ok((n,)) => n,
        Err(e) => {
            log::warn!("suppressed-counter incr failed for '{dedupe_key}': {e}");
            0
        }
    }
}

/// Atomically read-and-reset the suppressed counter (MULTI GET+DEL — Redis 6.0-safe).
pub async fn harvest_suppressed(conn: &ConnectionManager, dedupe_key: &str) -> u64 {
    let mut conn = conn.clone();
    let key = format!("tg_dedupe_count:{dedupe_key}");
    match redis::pipe()
        .atomic()
        .get(&key)
        .del(&key)
        .ignore()
        .query_async::<(Option<u64>,)>(&mut conn)
        .await
    {
        Ok((n,)) => n.unwrap_or(0),
        Err(e) => {
            log::warn!("suppressed-counter harvest failed for '{dedupe_key}': {e}");
            0
        }
    }
}

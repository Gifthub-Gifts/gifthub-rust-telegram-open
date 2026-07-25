//! gifthub-rust-telegram — one-way ops alerts from the GiftHub apps to the operator's chat.
//!
//! Fourth gateway in the family (-ai, -payments, -mail), in its simplest form: Django's
//! `apps.telegram.services.notify_ops()` RPUSHes `{job_id, event, text, dedupe_key, created_at}`
//! onto the `telegram_jobs` list (redis db 6, LTRIM-capped producer-side); this binary BLPOPs,
//! collapses repeats per `dedupe_key`, and delivers to the single admin chat. No callback, no
//! outbox, no inbound port — a dropped alert is acceptable, a blocked producer is not, and
//! journald plus the heartbeat key are the only ops surfaces.
//!
//! Per-job outcomes:
//!   sent        — delivered (possibly split into chunks), one greppable info line;
//!   suppressed  — a send for the same dedupe_key already happened inside the window; counted,
//!                 reported as "(+N suppressed…)" on the window's next send;
//!   stale       — older than STALE_AFTER_SECS on arrival (outage backlog is history, not news);
//!   dropped     — malformed, or Telegram answered with a permanent API error;
//!   requeued    — transient failures exhausted the retry budget; job returns to the tail and
//!                 the stale cutoff bounds its lifetime.

mod config;
mod error;
mod job;
mod redis_io;
mod telegram;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::sync::Semaphore;

use config::Config;
use error::SendError;

#[tokio::main]
async fn main() {
    env_logger::init();
    let cfg = config::get_config();
    log::info!(
        "gifthub-rust-telegram starting (chat {})",
        cfg.admin_chat_id
    );

    let mut consumer = match redis_io::init_redis(&cfg.redis_url).await {
        Ok(c) => c,
        Err(e) => {
            log::error!("❌ Redis connection failed: {e}");
            return;
        }
    };
    // Second connection for the side-channel (dedupe gates, counters, heartbeat) so the
    // blocking BLPOP on the consumer connection never delays a state lookup.
    let state = match redis_io::init_redis(&cfg.redis_url).await {
        Ok(c) => c,
        Err(e) => {
            log::error!("❌ Redis state connection failed: {e}");
            return;
        }
    };
    log::info!("✅ connected to Redis");

    let bot = telegram::build_bot(&cfg.bot_token);
    let sem = Arc::new(Semaphore::new(cfg.max_concurrency));

    // Unified shutdown bus: one watch sender flipped by SIGINT or SIGTERM (systemctl stop / deploy).
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let mut sigterm =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    log::error!("failed to install SIGTERM handler: {e}");
                    return;
                }
            };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => log::info!("SIGINT received, shutting down…"),
            _ = sigterm.recv() => log::info!("SIGTERM received, shutting down…"),
        }
        shutdown_tx.send(true).ok();
    });

    // Startup smoke message: proves token + chat id before we consume anything. A permanent
    // API answer (bad token / unknown chat / bot blocked) is a config error — exit non-zero so
    // the deploy health check reddens (no ready marker, unit flaps visibly). A transient
    // failure must NOT stop the consumer: Telegram being down is exactly when jobs should
    // keep queueing here rather than piling into Redis past the stale cutoff.
    {
        let mut rx = shutdown_rx.clone();
        match telegram::send_text(
            &bot,
            cfg.admin_chat_id,
            "🚀 gifthub-rust-telegram started",
            cfg.send_max_attempts,
            &mut rx,
        )
        .await
        {
            Ok(()) => log::info!("✅ startup message delivered"),
            Err(SendError::Permanent(e)) => {
                log::error!("❌ startup message permanently rejected ({e}) — check TELOXIDE_TOKEN / TELEGRAM_ADMIN_CHAT_ID");
                std::process::exit(1);
            }
            Err(e) => log::warn!("startup message not delivered ({e}); continuing"),
        }
    }

    // Liveness heartbeat: alarms can watch this key going stale — same "publish state to Redis
    // instead of opening an inbound port" trick as the sibling gateways. TTL 3× the interval.
    {
        let state = state.clone();
        let key = cfg.heartbeat_key.clone();
        let mut hb_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            loop {
                redis_io::heartbeat(&state, &key, 90).await;
                tokio::select! {
                    _ = hb_shutdown.changed() => break,
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                }
            }
        });
    }

    // Greppable ready marker (the deploy health check looks for this line in journald).
    log::info!(
        "🚀 gifthub-rust-telegram started: consuming Redis list '{}'",
        cfg.jobs_key
    );

    loop {
        // Backpressure: take a permit BEFORE popping, so we never pop more jobs than we can run.
        // With MAX_CONCURRENCY=1 (the default) this makes delivery strictly sequential/FIFO.
        let permit = tokio::select! {
            _ = shutdown_rx.changed() => break,
            p = Arc::clone(&sem).acquire_owned() => p.expect("semaphore should not be closed"),
        };

        let raw = tokio::select! {
            _ = shutdown_rx.changed() => break, // permit dropped here, returns to the pool
            res = redis_io::pop_job(&mut consumer, &cfg.jobs_key, cfg.pop_timeout_secs) => match res {
                Ok(Some(raw)) => raw,
                Ok(None) => continue, // BLPOP timeout — loop for the shutdown check
                Err(e) => {
                    log::error!("Redis pop failed: {e}; reconnecting after backoff");
                    tokio::select! {
                        _ = shutdown_rx.changed() => break,
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                    continue;
                }
            }
        };

        let bot = bot.clone();
        let state = state.clone();
        let shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            handle_job(cfg, bot, state, shutdown, raw).await;
        });
    }

    log::info!("shutdown signal received; draining in-flight jobs…");
    let drain = Arc::clone(&sem).acquire_many_owned(cfg.max_concurrency as u32);
    match tokio::time::timeout(Duration::from_secs(cfg.request_timeout_secs + 5), drain).await {
        Ok(_) => log::info!("all in-flight jobs finished"),
        Err(_) => log::warn!("shutdown grace expired with jobs still in flight"),
    }

    log::info!("gifthub-rust-telegram stopped");
}

/// Process one raw job: parse → stale check → dedupe gate → send. Stale runs BEFORE the gate
/// so an outage backlog neither arms the window nor counts as suppressed — the first fresh
/// alert after recovery always sends.
async fn handle_job(
    cfg: &'static Config,
    bot: teloxide::Bot,
    state: redis::aio::ConnectionManager,
    mut shutdown: watch::Receiver<bool>,
    raw: String,
) {
    let job = match job::TelegramJob::parse(&raw) {
        Ok(j) => j,
        Err(e) => {
            // Nobody to report to (fire-and-forget contract) — log loudly and drop.
            log::error!(
                "dropping malformed telegram job: {e}; raw={}",
                &raw[..raw.len().min(300)]
            );
            return;
        }
    };

    if job.is_stale(time::OffsetDateTime::now_utc(), cfg.stale_after_secs) {
        log::warn!(
            "dropping stale job {} ({}): age exceeds {}s",
            job.job_id,
            job.event,
            cfg.stale_after_secs
        );
        return;
    }

    // Dedupe collapse (skipped entirely for jobs without a key).
    let gated = !job.dedupe_key.is_empty();
    let mut text = job.text.clone();
    if gated {
        if !redis_io::claim_dedupe_gate(&state, &job.dedupe_key, cfg.dedupe_window_secs).await {
            let n =
                redis_io::incr_suppressed(&state, &job.dedupe_key, cfg.dedupe_window_secs).await;
            log::info!(
                "suppressed duplicate {} ('{}', count {n})",
                job.event,
                job.dedupe_key
            );
            return;
        }
        let suppressed = redis_io::harvest_suppressed(&state, &job.dedupe_key).await;
        text.push_str(&telegram::suppressed_suffix(
            suppressed,
            cfg.dedupe_window_secs,
        ));
    }

    match telegram::send_text(
        &bot,
        cfg.admin_chat_id,
        &text,
        cfg.send_max_attempts,
        &mut shutdown,
    )
    .await
    {
        Ok(()) => {
            // The one greppable line per delivered alert (journald is the ops surface).
            log::info!("job {} sent ({})", job.job_id, job.event);
        }
        Err(e) => {
            // Either way nobody received this alert — free the gate so the next one for this
            // key isn't suppressed behind a message that never landed.
            if gated {
                redis_io::release_dedupe_gate(&state, &job.dedupe_key).await;
            }
            if e.is_transient() {
                // Retry budget exhausted on a recoverable error: back on the tail — the stale
                // cutoff bounds the job's total lifetime through here.
                log::warn!("job {} failed after retries ({e}); requeueing", job.job_id);
                redis_io::requeue(&state, &cfg.jobs_key, &raw).await;
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else {
                log::error!("❌ job {} permanently rejected: {e}", job.job_id);
            }
        }
    }
}

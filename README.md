# gifthub-rust-telegram

One-way ops alerts from the GiftHub apps to the operator's Telegram chat. Fourth gateway in the
family (`gifthub-rust-ai`, `gifthub-rust-payments`, `gifthub-rust-mail`), and the simplest:
Django's `apps.telegram.services.notify_ops()` RPUSHes a job onto a Redis list; this binary
BLPOPs it, collapses repeats, and sends it to the admin chats. No callback, no outbox, no
inbound port — a dropped alert is acceptable, a blocked producer is not. Design doc lives
Django-side: `gifthub/docs/telegram-service-design.md`.

## Contract

Job (RPUSHed onto `telegram_jobs`, redis **db 6**, list LTRIM-capped at 500 producer-side):

```json
{"job_id": "uuid", "event": "order_fulfillment_failed", "text": "🔴 …", "dedupe_key": "order_fulfillment_failed:98", "created_at": "2026-07-25T12:00:00+00:00"}
```

- `text` is final **plain text**, sent verbatim (no parse_mode → no escaping failure can lose
  an alert). The producer must not pre-escape anything.
- `dedupe_key` scopes collapse; empty/missing = always send.
- Jobs older than `STALE_AFTER_SECS` (6h) are dropped on arrival: after an outage the backlog
  is history, not news.

Outcomes (journald is the only report channel — there is no callback by design):

| outcome | meaning | terminal? |
|---|---|---|
| `sent` | delivered (split into ≤4000-byte chunks if needed) | yes |
| suppressed | same `dedupe_key` already alerted inside the window; counted | yes |
| stale | older than the cutoff on arrival | yes |
| dropped | malformed job, or Telegram answered a permanent API error | yes |
| requeued | transient failures exhausted retries; back on the tail (stale cutoff bounds it) | no |

## Dedupe / collapse

First alert for a `dedupe_key` claims a Redis gate (`SET tg_dedupe:{key} NX EX window`) and
sends; repeats inside the window increment `tg_dedupe_count:{key}` and stay silent; the next
send after the window appends `(+N suppressed in the last hour)`. State lives in Redis so a
deploy-restart mid-incident-storm neither re-alerts the storm nor loses the count. All dedupe
failures fail **open** — a duplicate alert beats a lost one. Counter harvest is a MULTI
GET+DEL pipeline (not GETDEL: prod runs Redis 6.0).

## Retries

- 429 → sleep the server-supplied `retry_after`, clamped to [1, 90]s, then retry.
- Transient network/IO errors → 2s doubling backoff capped at 30s.
- Permanent API answers (bad token, unknown chat, bot blocked) → drop immediately.
- Budget: `SEND_MAX_ATTEMPTS` (3) per chunk; exhausted transient → job requeued to the tail.
- All retry sleeps abort on shutdown (SIGTERM-aware — `systemctl stop` drains cleanly).

## Ops

- Liveness: `telegram_heartbeat` key in db 6 (epoch seconds, TTL 90, written every 30s) —
  `redis-cli -n 6 TTL telegram_heartbeat` should never be negative while the unit runs.
- Boot sends "🚀 gifthub-rust-telegram started" to the chat — a permanent rejection there
  means a bad token/chat id and exits non-zero (the deploy health check catches it).
- Ready marker in journald: `gifthub-rust-telegram started` (deploy health check greps it).
- Manual smoke:
  `redis-cli -n 6 RPUSH telegram_jobs '{"job_id":"t1","event":"test","text":"hello 🚀","dedupe_key":"t","created_at":"'"$(date -u +%Y-%m-%dT%H:%M:%S+00:00)"'"}'`
  → message arrives; push it again → suppressed (journald line); after the window the next one
  reports the suppressed count.
- Config: `/srv/gifthub-rust-telegram/.env` (see `.env.example`), unit
  `deploy/gifthub-rust-telegram.service`, deploy via `.github/workflows/deploy.yml`
  (build → scp → restart → health check → auto-rollback), same shape as the siblings.

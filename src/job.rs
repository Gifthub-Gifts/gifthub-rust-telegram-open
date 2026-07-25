//! The alert job contract, as pushed by Django's `apps/telegram/gateway.py`:
//! `{job_id, event, text, dedupe_key, created_at}`. `text` arrives final and is sent verbatim
//! (plain text, no parse_mode — no escaping failure mode can lose an alert). Semantic
//! validation lives here so the consume loop stays dumb; malformed jobs are dropped loudly by
//! the caller, never requeued.

use serde::Deserialize;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

#[derive(Debug, Deserialize)]
pub struct TelegramJob {
    pub job_id: String,
    pub event: String,
    pub text: String,
    #[serde(default)]
    pub dedupe_key: String,
    #[serde(default)]
    pub created_at: String,
}

impl TelegramJob {
    pub fn parse(raw: &str) -> Result<Self, String> {
        let job: TelegramJob = serde_json::from_str(raw).map_err(|e| format!("bad JSON: {e}"))?;
        if job.job_id.is_empty() {
            return Err("empty job_id".into());
        }
        if job.text.trim().is_empty() {
            return Err(format!("job {} has empty text", job.job_id));
        }
        Ok(job)
    }

    /// Age in seconds, or `None` when `created_at` is missing/unparseable — the caller treats
    /// that as fresh (fail-open: never lose an alert to a timestamp formatting bug).
    pub fn age_secs(&self, now: OffsetDateTime) -> Option<u64> {
        let created = OffsetDateTime::parse(&self.created_at, &Rfc3339).ok()?;
        let age = (now - created).whole_seconds();
        Some(age.max(0) as u64)
    }

    pub fn is_stale(&self, now: OffsetDateTime, stale_after_secs: u64) -> bool {
        match self.age_secs(now) {
            Some(age) => age > stale_after_secs,
            None => {
                if !self.created_at.is_empty() {
                    log::warn!(
                        "job {} has unparseable created_at '{}'; treating as fresh",
                        self.job_id,
                        self.created_at
                    );
                }
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job_json(created_at: &str) -> String {
        format!(
            r#"{{"job_id":"j1","event":"order_fulfillment_failed","text":"🔴 boom","dedupe_key":"k","created_at":"{created_at}"}}"#
        )
    }

    #[test]
    fn parses_well_formed_job() {
        let job = TelegramJob::parse(&job_json("2026-07-25T12:00:00+00:00")).unwrap();
        assert_eq!(job.job_id, "j1");
        assert_eq!(job.event, "order_fulfillment_failed");
        assert_eq!(job.dedupe_key, "k");
    }

    #[test]
    fn rejects_malformed_json_and_missing_fields() {
        assert!(TelegramJob::parse("{not json").is_err());
        assert!(TelegramJob::parse(r#"{"event":"e","text":"t"}"#).is_err()); // no job_id
        assert!(TelegramJob::parse(r#"{"job_id":"j","event":"e","text":"  "}"#).is_err());
    }

    #[test]
    fn missing_dedupe_key_is_tolerated() {
        let job = TelegramJob::parse(r#"{"job_id":"j","event":"e","text":"t"}"#).unwrap();
        assert!(job.dedupe_key.is_empty());
    }

    #[test]
    fn stale_boundary() {
        let now = OffsetDateTime::parse("2026-07-25T12:00:00Z", &Rfc3339).unwrap();
        let fresh = TelegramJob::parse(&job_json("2026-07-25T11:00:00+00:00")).unwrap();
        assert!(!fresh.is_stale(now, 21_600));
        let old = TelegramJob::parse(&job_json("2026-07-25T05:59:59+00:00")).unwrap();
        assert!(old.is_stale(now, 21_600));
    }

    #[test]
    fn both_django_offset_and_zulu_parse() {
        let now = OffsetDateTime::parse("2026-07-25T12:00:00Z", &Rfc3339).unwrap();
        let offset = TelegramJob::parse(&job_json("2026-07-25T11:59:00+00:00")).unwrap();
        assert_eq!(offset.age_secs(now), Some(60));
        let zulu = TelegramJob::parse(&job_json("2026-07-25T11:59:00Z")).unwrap();
        assert_eq!(zulu.age_secs(now), Some(60));
    }

    #[test]
    fn unparseable_created_at_is_fresh() {
        let now = OffsetDateTime::now_utc();
        let job = TelegramJob::parse(&job_json("yesterday-ish")).unwrap();
        assert!(job.age_secs(now).is_none());
        assert!(!job.is_stale(now, 0));
    }
}

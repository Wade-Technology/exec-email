-- exec-email — rate-limit ledger (HIGH H1).
--
-- Per-sender, per-role, per-hour bucket. Used to enforce
-- MAX_PER_SENDER_PER_HOUR and MAX_PER_ROLE_PER_DAY (sum across
-- last 24h) before any inbound message triggers a Crew call or
-- SMTP reply.
--
-- The PRIMARY KEY (role, sender_email, bucket_hour) is what the
-- INSERT ... ON CONFLICT DO UPDATE upsert in exec_email.py keys on.
-- Idempotent: safe to re-run.

CREATE TABLE IF NOT EXISTS exec_email_rate (
    role          TEXT NOT NULL,
    sender_email  TEXT NOT NULL,
    bucket_hour   TIMESTAMP NOT NULL,
    count         INT NOT NULL DEFAULT 0,
    PRIMARY KEY (role, sender_email, bucket_hour)
);

CREATE INDEX IF NOT EXISTS idx_exec_email_rate_lookup
    ON exec_email_rate (role, sender_email, bucket_hour);

-- Also indexed by (role, bucket_hour) so the per-role 24h sum
-- can range-scan without touching every sender row.
CREATE INDEX IF NOT EXISTS idx_exec_email_rate_role_hour
    ON exec_email_rate (role, bucket_hour);

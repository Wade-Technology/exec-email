-- exec-email — schema init.
-- Tracks per-thread state so replies thread correctly and we can cap turns.

CREATE TABLE IF NOT EXISTS exec_email_threads (
    thread_id        TEXT NOT NULL,
    role             TEXT NOT NULL,
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_message_id  TEXT,
    message_count    INTEGER NOT NULL DEFAULT 0,
    prior_messages   JSONB NOT NULL DEFAULT '[]'::jsonb,
    handed_off       BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (role, thread_id)
);

CREATE INDEX IF NOT EXISTS idx_exec_email_threads_last_seen
    ON exec_email_threads (last_seen_at DESC);

-- Per-exec runtime state (last_uid we processed, last error, etc.).
CREATE TABLE IF NOT EXISTS exec_email_state (
    role             TEXT PRIMARY KEY,
    last_uid         BIGINT,
    last_message_at  TIMESTAMPTZ,
    last_error       TEXT,
    messages_today   INTEGER NOT NULL DEFAULT 0,
    today_date       DATE NOT NULL DEFAULT CURRENT_DATE,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

# exec-email

Gives each Wade.Technology AI executive a working public mailbox at
`@wade.technology`. The service holds an IMAP IDLE connection to Migadu
for each exec, routes inbound messages through the Aspen Crew
`permanent-message` API as that exec's persona, and replies via SMTPS
threaded into the original conversation. Wade is BCC'd on every outbound
message.

## Execs (all `@wade.technology`)

| Role | Address | Persona |
|------|---------|---------|
| ceo | victor | Victor Kane (CEO) |
| cto | kai | Kai Patel (CTO) |
| coo | marcus | Marcus Rivera (COO) |
| cso | diana | Diana Frost (CSO) |
| cfo | james | James Whitfield (CFO) |
| cro | sophia | Sophia Park (CRO) |
| ccio | leo | Leo Nakamura (CCIO) |
| higginsdev | higginsdev | HigginsDev (Chief of Staff) |

An exec with no `EXEC_EMAIL_{KEY}_PASSWORD` env var set is silently
skipped (same pattern as `exec-telegram`).

## Safety rails

The exec addresses are listed publicly on https://wade.technology, so
inbound traffic is fully untrusted. The following defences run before
any LLM call or SMTP send:

- **Per-sender hourly cap** (`MAX_PER_SENDER_PER_HOUR`, default 5) and
  **per-role 24h hard cap** (`MAX_PER_ROLE_PER_DAY`, default 50). Excess
  inbound is silently `\Seen`-marked. Wade is notified at most once per
  sender per day and once per role per day. Backed by the
  `exec_email_rate` ledger; if the DB is unreachable the service
  **fails closed** (no replies) rather than auto-allow.
- **Reply-loop hardening:** every outbound message carries
  `Auto-Submitted: auto-replied`, `Precedence: bulk`, `X-Exec-Mail: 1`,
  and an `[Exec-Mail]` subject prefix. Inbound is rejected if any of
  those signals come back at us (RFC 3834-aware).
- Bounce / no-reply / postmaster senders are dropped.
- Subjects beginning with `[Exec-Mail]`, `Re: [Exec-Mail]`, `Auto:`,
  `Automatic reply:`, `Out of office:` are dropped.
- `Auto-Submitted != no` / `Precedence in {bulk,list,junk,auto_reply}`
  are dropped.
- A thread caps at `MAX_THREAD_TURNS` (default 3) replies, then sends a
  one-shot "passed to Wade" reply and notifies Wade separately.
- **Crew API retry:** one retry after 5s on 5xx / timeout / connector
  errors; no retry on 4xx — prevents notify-storms during Aspen
  restarts.
- **`\Seen` ordering:** messages are only marked Seen after successful
  processing or an intentional skip (denylist / rate-limit). Parse
  crashes leave the message UNSEEN and notify Wade so silent drops
  cannot happen.
- On any Crew error after retry we send Wade a heads-up and **do not**
  auto-reply.
- Outbound signature includes AI disclosure.
- Bodies in logs / heads-up notifies are capped at 200 chars and email
  addresses are redacted (`<email>`).

## Heartbeat

`GET http://127.0.0.1:8085/healthz` returns

```json
{
  "service": "exec-email",
  "now": "...",
  "limits": {
    "max_per_sender_per_hour": 5,
    "max_per_role_per_day": 50,
    "max_thread_turns": 3
  },
  "execs": [
    {"role": "ceo", "address": "victor@wade.technology",
     "configured": true, "imap_connected": true,
     "last_message_at": "...", "messages_today": 0, "last_error": null,
     "rate_today": 3, "rate_this_hour_total": 1,
     "rate_capped_today": false}, ...
  ]
}
```

Mission Control polls this for the per-exec mailbox status panel.

## Provisioning mailboxes

The Migadu mailboxes themselves are created by
`scripts/provision-exec-mailboxes.sh` in the docker repo (idempotent;
appends `EXEC_EMAIL_{KEY}_PASSWORD=...` to `/opt/wtec/.env`).

```bash
ssh root@hetzner '/opt/exec-email/scripts/provision-exec-mailboxes.sh --dry-run'
ssh root@hetzner '/opt/exec-email/scripts/provision-exec-mailboxes.sh'
ssh root@hetzner 'cd /opt/exec-email && docker compose up -d --force-recreate'
```

## Environment

See `.env.example`. Production values live in `/opt/wtec/.env` on
Hetzner; the deploy script sources that file before `docker compose up`.

Key vars:

| Var | Purpose |
|-----|---------|
| `ASPEN_ADMIN_URL` | Crew API base (default `http://aspen-admin-backend:8001`) |
| `ADMIN_API_KEY` | `X-Admin-Key` header for Crew |
| `WADE_BCC_EMAIL` | BCC on every outbound (also accepts `WADE_EMAIL`) |
| `IMAP_HOST` / `SMTP_HOST` | `imap.migadu.com` / `smtp.migadu.com` |
| `EXEC_EMAIL_{KEY}_PASSWORD` | One per exec; missing => skipped |
| `MAX_THREAD_TURNS` | Hard cap before handoff (default 3) |
| `MAX_PER_SENDER_PER_HOUR` | Per-sender hourly cap (default 5) — excess silently dropped |
| `MAX_PER_ROLE_PER_DAY` | Per-role 24h hard cap (default 50) — DoS amp prevention |
| `POLL_INTERVAL_FALLBACK` | Seconds between IDLE keepalives (default 30) |
| `DB_*` | Reuses the wtec Postgres for thread state + rate-limit ledger |

## Deploy

CI auto-deploys on push to `main` via `.github/workflows/deploy.yml`
(SSH to Hetzner, `git pull`, `docker compose up -d --build`,
verify container Up, hit `/healthz`). Manual force-deploy:
`./deploy-wade-tech.sh exec-email` from the docker repo.

Hetzner pull path: `/opt/exec-email/`.

## Bring-up checklist

1. From the docker repo: `ssh root@hetzner '/opt/exec-email/scripts/provision-exec-mailboxes.sh --dry-run'` — verify it lists 8 mailboxes to create.
2. Drop `--dry-run` to actually provision — passwords are written only to `/opt/wtec/.env`, never to stdout.
3. Push this repo to `main` (or run `./deploy-wade-tech.sh exec-email`) — CI brings the container up and curl-verifies `/healthz`.
4. Confirm at `http://178.156.206.0:8085/healthz` (or via Mission Control) that all 8 execs show `configured: true, imap_connected: true`.

## Mission Control hookup

MC's exec-email status panel polls `http://exec-email:8080/healthz` over the
`wtec_default` Docker network. The same endpoint is exposed at
`127.0.0.1:8085/healthz` on the host for ad-hoc inspection.

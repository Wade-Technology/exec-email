#!/usr/bin/env python3
"""
exec-email — give each Wade.Technology AI executive a working mailbox.

For each exec with EXEC_EMAIL_{KEY}_PASSWORD set, this service:
  1. Holds an IMAP IDLE connection to imap.migadu.com:993
  2. On a new INBOX message: parses it, builds prior_messages from thread
     history in Postgres, posts to {ASPEN_ADMIN_URL}/api/crew/permanent/{role}/message
  3. On 200, sends the response via SMTPS at smtp.migadu.com:465 from the
     exec mailbox, threaded via In-Reply-To/References, BCC Wade.

Skip rules: bounces, no-reply senders, our own forwards, denylisted prefixes.
After MAX_THREAD_TURNS replies in a thread we hand off to Wade.

Heartbeat: aiohttp on :8080 GET /healthz returns per-exec state.
"""

import asyncio
import email
import email.utils
import json
import logging
import os
import re
import signal
import ssl
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import getaddresses, parseaddr
from typing import Optional

import aiohttp
import aioimaplib
import aiosmtplib
import asyncpg
from aiohttp import web

# ───── config ────────────────────────────────────────────────────────────
ASPEN_ADMIN_URL = os.environ.get("ASPEN_ADMIN_URL", "http://aspen-admin-backend:8001")
ADMIN_API_KEY = os.environ.get("ADMIN_API_KEY", "").strip()

WADE_BCC_EMAIL = (
    os.environ.get("WADE_BCC_EMAIL")
    or os.environ.get("WADE_EMAIL")
    or "wadethomaswarren@gmail.com"
).strip()

IMAP_HOST = os.environ.get("IMAP_HOST", "imap.migadu.com")
IMAP_PORT = int(os.environ.get("IMAP_PORT", "993"))
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.migadu.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))

POLL_INTERVAL_FALLBACK = int(os.environ.get("POLL_INTERVAL_FALLBACK", "30"))
MAX_THREAD_TURNS = int(os.environ.get("MAX_THREAD_TURNS", "3"))
MAX_BODY_CHARS = 8000

DB_DSN = (
    f"postgres://{os.environ.get('DB_USER','wtec')}:"
    f"{os.environ.get('DB_PASSWORD','')}"
    f"@{os.environ.get('DB_HOST','wtec-postgres')}:"
    f"{os.environ.get('DB_PORT','5432')}"
    f"/{os.environ.get('DB_NAME','wtec')}"
)

DOMAIN = "wade.technology"

EXECS = [
    {"key": "CEO",        "role": "ceo",        "local": "victor",     "name": "Victor Kane",      "title": "CEO"},
    {"key": "CTO",        "role": "cto",        "local": "kai",        "name": "Kai Patel",        "title": "CTO"},
    {"key": "COO",        "role": "coo",        "local": "marcus",     "name": "Marcus Rivera",    "title": "COO"},
    {"key": "CSO",        "role": "cso",        "local": "diana",      "name": "Diana Frost",      "title": "CSO"},
    {"key": "CFO",        "role": "cfo",        "local": "james",      "name": "James Whitfield",  "title": "CFO"},
    {"key": "CRO",        "role": "cro",        "local": "sophia",     "name": "Sophia Park",      "title": "CRO"},
    {"key": "CCIO",       "role": "ccio",       "local": "leo",        "name": "Leo Nakamura",     "title": "CCIO"},
    {"key": "HIGGINSDEV", "role": "higginsdev", "local": "higginsdev", "name": "HigginsDev",       "title": "Chief of Staff"},
]

DENY_SENDER_PATTERNS = re.compile(
    r"(mailer-daemon|mail-daemon|postmaster|no[-_.]?reply|noreply|do[-_.]?not[-_.]?reply|bounces?@|notifications?@github\.com)",
    re.IGNORECASE,
)
DENY_SUBJECT_PREFIXES = ("re: [exec-mail]", "auto:", "automatic reply:", "out of office:")

logging.basicConfig(
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("exec-email")

# Mutable runtime state, keyed by exec role.
state: dict = {
    e["role"]: {
        "imap_connected": False,
        "last_message_at": None,
        "messages_today": 0,
        "today_date": datetime.now(timezone.utc).date().isoformat(),
        "last_error": None,
        "configured": False,
    }
    for e in EXECS
}

db_pool: Optional[asyncpg.Pool] = None


# ───── helpers ───────────────────────────────────────────────────────────
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def bump_messages_today(role: str) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    s = state[role]
    if s.get("today_date") != today:
        s["today_date"] = today
        s["messages_today"] = 0
    s["messages_today"] += 1
    s["last_message_at"] = now_iso()


def signature(name: str, title: str) -> str:
    return (
        f"\n\n— {name}, {title} · Wade.Technology\n"
        "Note: I'm an AI executive at Wade.Technology — happy to be transparent about that."
    )


def extract_text_body(msg: email.message.Message) -> str:
    """Return first text/plain body, stripping forwarded prior turns crudely."""
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain" and "attachment" not in (part.get("Content-Disposition") or ""):
                try:
                    body = part.get_content()
                except Exception:
                    payload = part.get_payload(decode=True) or b""
                    body = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
                if body:
                    break
        if not body:
            # Fall back to text/html stripped of tags
            for part in msg.walk():
                if part.get_content_type() == "text/html":
                    try:
                        html = part.get_content()
                    except Exception:
                        payload = part.get_payload(decode=True) or b""
                        html = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
                    body = re.sub(r"<[^>]+>", " ", html)
                    body = re.sub(r"\s+", " ", body).strip()
                    break
    else:
        try:
            body = msg.get_content()
        except Exception:
            payload = msg.get_payload(decode=True) or b""
            body = payload.decode(msg.get_content_charset() or "utf-8", errors="replace")

    # Trim quoted history (anything after a typical quote marker line).
    cut_markers = (
        "\n-----Original Message-----",
        "\nOn ", "On ",  # "On Mon, Apr 21, 2026 at 10:00 AM Foo wrote:" — handled below
        "\n>",
    )
    # Specifically chop "On <date> ... wrote:" prefix
    m = re.search(r"\n(On .+wrote:)", body)
    if m:
        body = body[: m.start()]
    if "\n-----Original Message-----" in body:
        body = body.split("\n-----Original Message-----", 1)[0]
    # Strip quoted lines
    body = "\n".join(ln for ln in body.splitlines() if not ln.lstrip().startswith(">"))
    body = body.strip()
    return body[:MAX_BODY_CHARS]


def thread_root_id(msg: email.message.Message) -> str:
    """Return the earliest Message-ID in this thread (References[0] || In-Reply-To || own Message-ID)."""
    refs = msg.get("References", "")
    if refs:
        first = refs.split()[0].strip()
        if first:
            return first
    in_reply = (msg.get("In-Reply-To") or "").strip()
    if in_reply:
        return in_reply
    return (msg.get("Message-ID") or f"no-id-{datetime.now(timezone.utc).timestamp()}").strip()


def should_skip(msg: email.message.Message, exec_email_addr: str) -> Optional[str]:
    sender = parseaddr(msg.get("From", ""))[1].lower()
    if not sender:
        return "no sender"
    if DENY_SENDER_PATTERNS.search(sender):
        return f"deny sender pattern: {sender}"
    if sender == exec_email_addr.lower():
        return "loop: from self"

    subject = (msg.get("Subject") or "").strip().lower()
    for pfx in DENY_SUBJECT_PREFIXES:
        if subject.startswith(pfx):
            return f"deny subject prefix: {pfx}"

    auto_submitted = (msg.get("Auto-Submitted") or "").strip().lower()
    if auto_submitted and auto_submitted != "no":
        return f"auto-submitted: {auto_submitted}"

    precedence = (msg.get("Precedence") or "").strip().lower()
    if precedence in ("bulk", "list", "junk", "auto_reply"):
        return f"precedence: {precedence}"
    return None


# ───── DB ────────────────────────────────────────────────────────────────
async def init_db() -> None:
    global db_pool
    db_pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=4)
    sql_path = os.path.join(os.path.dirname(__file__), "migrations", "001_init.sql")
    if os.path.exists(sql_path):
        with open(sql_path, "r", encoding="utf-8") as f:
            sql = f.read()
        async with db_pool.acquire() as conn:
            await conn.execute(sql)
    logger.info("[db] connected, schema applied")


async def load_thread(role: str, thread_id: str) -> dict:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT message_count, prior_messages, handed_off FROM exec_email_threads "
            "WHERE role=$1 AND thread_id=$2",
            role, thread_id,
        )
    if not row:
        return {"message_count": 0, "prior_messages": [], "handed_off": False}
    pm = row["prior_messages"]
    if isinstance(pm, str):
        pm = json.loads(pm)
    return {"message_count": row["message_count"], "prior_messages": pm or [], "handed_off": row["handed_off"]}


async def save_thread(role: str, thread_id: str, message_id: str, prior_messages: list, handed_off: bool) -> None:
    # Cap prior_messages to last 20 entries (10 turns) for sanity.
    pm = prior_messages[-20:]
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO exec_email_threads (thread_id, role, last_seen_at, last_message_id, message_count, prior_messages, handed_off)
            VALUES ($1, $2, now(), $3, 1, $4::jsonb, $5)
            ON CONFLICT (role, thread_id) DO UPDATE
              SET last_seen_at = now(),
                  last_message_id = EXCLUDED.last_message_id,
                  message_count = exec_email_threads.message_count + 1,
                  prior_messages = EXCLUDED.prior_messages,
                  handed_off = exec_email_threads.handed_off OR EXCLUDED.handed_off
            """,
            thread_id, role, message_id, json.dumps(pm), handed_off,
        )


# ───── Crew API ──────────────────────────────────────────────────────────
async def ask_crew(role: str, body: str, conversation_id: str, prior_messages: list) -> Optional[str]:
    if not ADMIN_API_KEY:
        logger.error("ADMIN_API_KEY not set — cannot call Crew")
        return None
    payload = {
        "message": body,
        "conversation_id": conversation_id,
        "prior_messages": prior_messages[-20:],
        "max_turns": 5,
        "timeout_seconds": 180,
    }
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.post(
                f"{ASPEN_ADMIN_URL}/api/crew/permanent/{role}/message",
                json=payload,
                headers={"X-Admin-Key": ADMIN_API_KEY, "Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=210),
            ) as resp:
                if resp.status != 200:
                    txt = await resp.text()
                    logger.error(f"[{role}] crew api {resp.status}: {txt[:300]}")
                    return None
                data = await resp.json()
                return (data.get("response") or "").strip() or None
    except asyncio.TimeoutError:
        logger.error(f"[{role}] crew api timeout")
        return None
    except Exception as e:
        logger.error(f"[{role}] crew api error: {e}")
        return None


# ───── SMTP ──────────────────────────────────────────────────────────────
async def send_smtp(exec_cfg: dict, to_addr: str, subject: str, body_text: str,
                    in_reply_to: Optional[str], references: Optional[str]) -> str:
    msg = EmailMessage()
    from_addr = f"{exec_cfg['name']} <{exec_cfg['local']}@{DOMAIN}>"
    msg["From"] = from_addr
    msg["To"] = to_addr
    bcc = WADE_BCC_EMAIL
    if bcc and parseaddr(to_addr)[1].lower() != bcc.lower():
        msg["Bcc"] = bcc
    msg["Subject"] = subject
    new_msg_id = email.utils.make_msgid(domain=DOMAIN)
    msg["Message-ID"] = new_msg_id
    msg["Date"] = email.utils.formatdate(localtime=True)
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
    if references:
        msg["References"] = references
    elif in_reply_to:
        msg["References"] = in_reply_to
    msg.set_content(body_text + signature(exec_cfg["name"], exec_cfg["title"]))

    password = os.environ.get(f"EXEC_EMAIL_{exec_cfg['key']}_PASSWORD", "")
    await aiosmtplib.send(
        msg,
        hostname=SMTP_HOST,
        port=SMTP_PORT,
        username=f"{exec_cfg['local']}@{DOMAIN}",
        password=password,
        use_tls=True,
        timeout=60,
    )
    return new_msg_id


async def notify_wade(exec_cfg: dict, subject: str, note: str) -> None:
    """Send a heads-up to Wade only (no reply to the original sender)."""
    try:
        msg = EmailMessage()
        msg["From"] = f"{exec_cfg['name']} <{exec_cfg['local']}@{DOMAIN}>"
        msg["To"] = WADE_BCC_EMAIL
        msg["Subject"] = f"[exec-email/{exec_cfg['role']}] {subject}"
        msg["Date"] = email.utils.formatdate(localtime=True)
        msg.set_content(note)
        password = os.environ.get(f"EXEC_EMAIL_{exec_cfg['key']}_PASSWORD", "")
        await aiosmtplib.send(
            msg, hostname=SMTP_HOST, port=SMTP_PORT,
            username=f"{exec_cfg['local']}@{DOMAIN}", password=password,
            use_tls=True, timeout=60,
        )
    except Exception as e:
        logger.error(f"[{exec_cfg['role']}] notify_wade failed: {e}")


# ───── message processing ───────────────────────────────────────────────
async def process_message(exec_cfg: dict, raw_bytes: bytes) -> None:
    role = exec_cfg["role"]
    exec_addr = f"{exec_cfg['local']}@{DOMAIN}"
    try:
        msg = email.message_from_bytes(raw_bytes, policy=email.policy.default)
    except Exception as e:
        logger.error(f"[{role}] parse error: {e}")
        return

    skip_reason = should_skip(msg, exec_addr)
    if skip_reason:
        logger.info(f"[{role}] skip: {skip_reason}")
        return

    sender_addr = parseaddr(msg.get("From", ""))[1]
    subject = (msg.get("Subject") or "(no subject)").strip()
    body = extract_text_body(msg)
    if not body:
        logger.info(f"[{role}] skip: empty body from {sender_addr}")
        return

    msg_id = (msg.get("Message-ID") or "").strip()
    root_id = thread_root_id(msg)
    refs = msg.get("References") or ""
    new_refs = (refs + " " + msg_id).strip() if msg_id else refs

    thread = await load_thread(role, root_id)

    # Hard cap: hand off after MAX_THREAD_TURNS replies
    if thread["handed_off"]:
        logger.info(f"[{role}] thread {root_id} already handed off — ignoring")
        return
    if thread["message_count"] >= MAX_THREAD_TURNS:
        handoff_text = (
            f"Thanks for the follow-up. I've passed this thread to Wade for human "
            f"follow-up — he'll be in touch directly."
        )
        try:
            reply_subject = subject if subject.lower().startswith("re:") else f"Re: {subject}"
            await send_smtp(exec_cfg, sender_addr, reply_subject, handoff_text,
                            in_reply_to=msg_id or None, references=new_refs or None)
            await notify_wade(exec_cfg, f"thread handoff to you",
                              f"Thread '{subject}' from {sender_addr} hit MAX_THREAD_TURNS={MAX_THREAD_TURNS}.\n"
                              f"Last inbound message:\n\n{body[:1000]}")
            await save_thread(role, root_id, msg_id, thread["prior_messages"], handed_off=True)
            bump_messages_today(role)
            logger.info(f"[{role}] handed off thread {root_id} to Wade")
        except Exception as e:
            logger.error(f"[{role}] handoff send failed: {e}")
        return

    # Build prior_messages with the new inbound appended.
    prior = list(thread["prior_messages"])
    crew_input = f"From {sender_addr} — Subject: {subject}\n\n{body}"
    conversation_id = f"email:{role}:{root_id}"

    reply = await ask_crew(role, crew_input, conversation_id, prior)
    if not reply:
        # Better silence than a wrong reply — notify Wade.
        await notify_wade(
            exec_cfg, "crew error — no reply sent",
            f"Inbound email from {sender_addr}, subject '{subject}', failed Crew call.\n\n"
            f"Body:\n{body[:2000]}"
        )
        state[role]["last_error"] = f"crew error at {now_iso()}"
        return

    reply_subject = subject if subject.lower().startswith("re:") else f"Re: {subject}"
    try:
        await send_smtp(exec_cfg, sender_addr, reply_subject, reply,
                        in_reply_to=msg_id or None, references=new_refs or None)
    except Exception as e:
        logger.error(f"[{role}] SMTP send failed: {e}")
        state[role]["last_error"] = f"smtp error: {e}"
        await notify_wade(exec_cfg, "SMTP send failed",
                          f"To: {sender_addr}\nSubject: {subject}\nError: {e}\n\nReply was:\n{reply}")
        return

    prior.append({"role": "user", "content": crew_input})
    prior.append({"role": "assistant", "content": reply})
    await save_thread(role, root_id, msg_id, prior, handed_off=False)
    bump_messages_today(role)
    logger.info(f"[{role}] replied to {sender_addr} (thread={root_id}, "
                f"turn={thread['message_count']+1}/{MAX_THREAD_TURNS})")


# ───── IMAP loop ─────────────────────────────────────────────────────────
async def imap_loop(exec_cfg: dict) -> None:
    role = exec_cfg["role"]
    key = exec_cfg["key"]
    password = os.environ.get(f"EXEC_EMAIL_{key}_PASSWORD", "").strip()
    if not password:
        logger.info(f"[{role}] no EXEC_EMAIL_{key}_PASSWORD set — skipping")
        return

    state[role]["configured"] = True
    user = f"{exec_cfg['local']}@{DOMAIN}"

    backoff = 5
    while True:
        try:
            ssl_ctx = ssl.create_default_context()
            client = aioimaplib.IMAP4_SSL(host=IMAP_HOST, port=IMAP_PORT, ssl_context=ssl_ctx, timeout=60)
            await client.wait_hello_from_server()
            await client.login(user, password)
            await client.select("INBOX")
            state[role]["imap_connected"] = True
            state[role]["last_error"] = None
            logger.info(f"[{role}] IMAP connected as {user}")
            backoff = 5

            # On startup, scan UNSEEN messages once (catches anything received while we were down)
            await scan_unseen(client, exec_cfg)

            while True:
                if client.has_pending_idle():
                    await client.idle_done()

                idle_task = await client.idle_start(timeout=600)  # ~10 min keepalive
                # Wait for either an event or fallback poll interval
                try:
                    await asyncio.wait_for(client.wait_server_push(), timeout=POLL_INTERVAL_FALLBACK)
                except asyncio.TimeoutError:
                    pass
                client.idle_done()
                try:
                    await asyncio.wait_for(idle_task, timeout=10)
                except asyncio.TimeoutError:
                    pass

                # Check for new messages
                await scan_unseen(client, exec_cfg)

        except Exception as e:
            state[role]["imap_connected"] = False
            state[role]["last_error"] = f"imap loop: {e}"
            logger.error(f"[{role}] IMAP loop error: {e!r} — reconnect in {backoff}s")
            try:
                await client.logout()
            except Exception:
                pass
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 300)


async def scan_unseen(client: "aioimaplib.IMAP4_SSL", exec_cfg: dict) -> None:
    role = exec_cfg["role"]
    try:
        typ, data = await client.search("UNSEEN")
        if typ != "OK" or not data:
            return
        # data[0] is bytes like b"1 2 3"
        ids_blob = data[0]
        if isinstance(ids_blob, (bytes, bytearray)):
            ids = ids_blob.split()
        else:
            ids = str(ids_blob).split()
        if not ids:
            return
        for uid_b in ids:
            uid = uid_b.decode() if isinstance(uid_b, (bytes, bytearray)) else str(uid_b)
            if not uid.strip():
                continue
            typ, msg_data = await client.fetch(uid, "(RFC822)")
            if typ != "OK" or not msg_data:
                continue
            # msg_data is a list; find the bytes blob
            raw = None
            for item in msg_data:
                if isinstance(item, (bytes, bytearray)) and len(item) > 100:
                    raw = bytes(item)
                    break
            if raw is None:
                continue
            try:
                await process_message(exec_cfg, raw)
            except Exception as e:
                logger.error(f"[{role}] process_message error: {e!r}")
            # Mark as seen so we don't reprocess on reconnect.
            try:
                await client.store(uid, "+FLAGS", "(\\Seen)")
            except Exception:
                pass
    except Exception as e:
        logger.error(f"[{role}] scan_unseen error: {e!r}")


# ───── HTTP heartbeat ────────────────────────────────────────────────────
async def healthz(_request: web.Request) -> web.Response:
    out = []
    for e in EXECS:
        s = state[e["role"]]
        out.append({
            "role": e["role"],
            "address": f"{e['local']}@{DOMAIN}",
            "name": e["name"],
            "title": e["title"],
            "configured": s["configured"],
            "imap_connected": s["imap_connected"],
            "last_message_at": s["last_message_at"],
            "messages_today": s["messages_today"],
            "last_error": s["last_error"],
        })
    payload = {
        "service": "exec-email",
        "now": now_iso(),
        "execs": out,
    }
    return web.json_response(payload)


async def start_http() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/", healthz)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    logger.info("[http] listening on :8080/healthz")
    return runner


# ───── main ──────────────────────────────────────────────────────────────
async def main() -> None:
    if not ADMIN_API_KEY:
        logger.warning("ADMIN_API_KEY missing — Crew calls will fail; service still running for heartbeat")
    logger.info(f"Aspen Admin URL: {ASPEN_ADMIN_URL}")
    logger.info(f"BCC Wade at: {WADE_BCC_EMAIL}")
    logger.info(f"MAX_THREAD_TURNS={MAX_THREAD_TURNS}, POLL_INTERVAL_FALLBACK={POLL_INTERVAL_FALLBACK}s")

    try:
        await init_db()
    except Exception as e:
        logger.error(f"[db] init failed: {e!r} — service will degrade (per-thread state lost across restarts)")

    runner = await start_http()

    # One IMAP task per configured exec.
    tasks = []
    for cfg in EXECS:
        if os.environ.get(f"EXEC_EMAIL_{cfg['key']}_PASSWORD", "").strip():
            tasks.append(asyncio.create_task(imap_loop(cfg), name=f"imap-{cfg['role']}"))
        else:
            logger.info(f"[{cfg['role']}] EXEC_EMAIL_{cfg['key']}_PASSWORD not set — skipping IMAP task")

    if not tasks:
        logger.warning("No exec mailboxes configured — heartbeat only.")

    # Wait for SIGTERM/SIGINT
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig_name in ("SIGTERM", "SIGINT"):
        try:
            loop.add_signal_handler(getattr(signal, sig_name), stop_event.set)
        except (NotImplementedError, AttributeError):
            pass  # Windows / no signal support — Ctrl+C will still raise

    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await runner.cleanup()
        if db_pool:
            await db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())

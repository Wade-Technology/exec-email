"""Unit tests for the imap-tools-based IMAP loop in exec-email.

Covers:

- _fetch_unseen_blocking returns (uid, raw_bytes) tuples for UNSEEN messages.
- _flag_seen_blocking calls mb.flag(uids, SEEN, True) on the given UIDs.
- imap_loop processes each fetched message, flags only PROC_OK / PROC_SKIP_SEEN
  results as \\Seen, leaves PROC_SKIP_UNSEEN alone.
- A MailboxLoginError on fetch trips the reconnect/backoff path (does NOT
  crash the loop) — same shape as a generic OSError.
- should_skip rules (deny sender, [Exec-Mail] tag, X-Exec-Mail header,
  Auto-Submitted, Precedence) still cause PROC_SKIP_SEEN even after the
  library swap.
"""

from __future__ import annotations

import asyncio
import sys
import unittest
from email.message import EmailMessage
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import exec_email  # noqa: E402
from imap_tools.errors import MailboxLoginError  # noqa: E402


# ───── helpers ──────────────────────────────────────────────────────────
def _build_msg(
    *,
    sender: str = "jane@example.com",
    to: str = "marcus@wade.technology",
    subject: str = "Partnership inquiry",
    body: str = "Hi Marcus, we'd like to explore a pilot.",
    extra_headers: dict | None = None,
) -> bytes:
    """Build a tiny RFC822 byte blob for tests."""
    m = EmailMessage()
    m["From"] = sender
    m["To"] = to
    m["Subject"] = subject
    m["Message-ID"] = "<test-msg-1@example.com>"
    if extra_headers:
        for k, v in extra_headers.items():
            m[k] = v
    m.set_content(body)
    return bytes(m)


class _FakeImapToolsMessage:
    """Just enough of imap_tools.message.MailMessage for our fetch path."""

    def __init__(self, uid: str, raw: bytes):
        self.uid = uid
        # exec_email pulls .obj.as_bytes() — duck-type with email.message.
        import email as _email
        import email.policy as _policy

        self.obj = _email.message_from_bytes(raw, policy=_policy.default)


class _FakeMailBox:
    """Stand-in for imap_tools.MailBox. Acts as a context manager,
    .login() returns self, .fetch() yields canned messages, .flag()
    records calls. Matches the call shape exec_email uses.
    """

    last_login_args: tuple | None = None
    fail_login: bool = False

    def __init__(self, host=None, port=None, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.flag_calls: list = []
        self._fetch_msgs: list = []

    def login(self, user, password, initial_folder="INBOX"):
        type(self).last_login_args = (user, password, initial_folder)
        if type(self).fail_login:
            raise MailboxLoginError("auth fail (test)", "NO")
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def fetch(self, criteria=None, mark_seen=True, bulk=False, **_):
        # criteria/mark_seen ignored — test injects whatever it wants.
        for m in self._fetch_msgs:
            yield m

    def flag(self, uid_list, flag_set, value, chunks=None):
        # Record exactly what exec_email asked us to flag.
        self.flag_calls.append((tuple(uid_list) if not isinstance(uid_list, str) else (uid_list,), flag_set, value))


# ───── tests ────────────────────────────────────────────────────────────
class FetchUnseenBlockingTests(unittest.TestCase):
    def setUp(self):
        _FakeMailBox.last_login_args = None
        _FakeMailBox.fail_login = False

    def test_returns_uid_and_raw_bytes_tuples(self):
        raw1 = _build_msg(subject="One")
        raw2 = _build_msg(subject="Two")
        fb = _FakeMailBox()
        fb._fetch_msgs = [
            _FakeImapToolsMessage("11", raw1),
            _FakeImapToolsMessage("12", raw2),
        ]

        with patch.object(exec_email, "MailBox", return_value=fb):
            out = exec_email._fetch_unseen_blocking("marcus@wade.technology", "pw")

        self.assertEqual(len(out), 2)
        self.assertEqual(out[0][0], "11")
        self.assertIsInstance(out[0][1], bytes)
        self.assertIn(b"Subject: One", out[0][1])
        self.assertEqual(out[1][0], "12")
        self.assertIn(b"Subject: Two", out[1][1])
        # Confirm we logged in with the right creds.
        self.assertEqual(
            _FakeMailBox.last_login_args,
            ("marcus@wade.technology", "pw", "INBOX"),
        )

    def test_skips_messages_with_no_uid_or_tiny_payload(self):
        fb = _FakeMailBox()
        fb._fetch_msgs = [
            _FakeImapToolsMessage("", _build_msg()),  # no uid
        ]
        # Force tiny payload: zero out obj
        m_tiny = _FakeImapToolsMessage("99", _build_msg())
        m_tiny.obj = None  # exec_email's guard treats this as empty bytes
        fb._fetch_msgs.append(m_tiny)

        with patch.object(exec_email, "MailBox", return_value=fb):
            out = exec_email._fetch_unseen_blocking("u", "p")

        self.assertEqual(out, [])

    def test_login_failure_raises_for_outer_backoff(self):
        _FakeMailBox.fail_login = True
        with patch.object(exec_email, "MailBox", return_value=_FakeMailBox()):
            with self.assertRaises(MailboxLoginError):
                exec_email._fetch_unseen_blocking("u", "wrong-pw")


class FlagSeenBlockingTests(unittest.TestCase):
    def test_flags_uids_with_seen_true(self):
        fb = _FakeMailBox()
        with patch.object(exec_email, "MailBox", return_value=fb):
            exec_email._flag_seen_blocking("u", "p", ["1", "2", "3"])
        self.assertEqual(len(fb.flag_calls), 1)
        uids, flag, value = fb.flag_calls[0]
        self.assertEqual(uids, ("1", "2", "3"))
        self.assertEqual(flag, exec_email.MailMessageFlags.SEEN)
        self.assertTrue(value)

    def test_empty_uid_list_is_noop(self):
        fb = _FakeMailBox()
        with patch.object(exec_email, "MailBox", return_value=fb):
            exec_email._flag_seen_blocking("u", "p", [])
        self.assertEqual(fb.flag_calls, [])


class ImapLoopProcessingTests(unittest.IsolatedAsyncioTestCase):
    """Drive a single iteration of imap_loop and confirm the right
    PROC_OK / PROC_SKIP_SEEN / PROC_SKIP_UNSEEN triage happens."""

    def setUp(self):
        # Reset per-role state.
        exec_email.state["coo"].update({
            "imap_connected": False,
            "last_message_at": None,
            "messages_today": 0,
            "today_date": "2026-04-26",
            "last_error": None,
            "configured": False,
            "rate_today": 0,
            "rate_this_hour_total": 0,
            "rate_capped_today": False,
        })

    async def _run_one_iteration(
        self,
        fetch_return,
        process_results,
    ):
        """Run imap_loop until it either flags or hits its sleep, then
        cancel. Returns the flag_seen UIDs that were called.
        """
        flag_calls: list = []

        async def fake_fetch(user, password):
            # Only return the batch on the first call; second call return []
            # and the test will cancel before the third sleep finishes.
            return fetch_return.pop(0) if fetch_return else []

        async def fake_flag(user, password, uids):
            flag_calls.append(list(uids))

        # process_message returns from a queue per call.
        results_queue = list(process_results)

        async def fake_process(_cfg, _raw):
            return results_queue.pop(0)

        # Patch asyncio.to_thread to call our fakes directly (bypass thread
        # so we can await them).
        async def fake_to_thread(func, *args, **kw):
            if func is exec_email._fetch_unseen_blocking:
                return await fake_fetch(*args, **kw)
            if func is exec_email._flag_seen_blocking:
                return await fake_flag(*args, **kw)
            return func(*args, **kw)

        # Make POLL_INTERVAL_FALLBACK tiny so the loop iterates fast,
        # then we cancel.
        with patch.object(exec_email, "POLL_INTERVAL_FALLBACK", 0.01), \
             patch.object(exec_email, "asyncio", asyncio), \
             patch("asyncio.to_thread", fake_to_thread), \
             patch.object(exec_email, "process_message", fake_process), \
             patch.dict("os.environ", {"EXEC_EMAIL_COO_PASSWORD": "test-pw"}):
            cfg = next(e for e in exec_email.EXECS if e["role"] == "coo")
            task = asyncio.create_task(exec_email.imap_loop(cfg))
            # Let the loop run a few iterations.
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        return flag_calls

    async def test_proc_ok_results_get_flagged_seen(self):
        raw = _build_msg()
        fetch_return = [[("100", raw), ("101", raw)]]
        process_results = [exec_email.PROC_OK, exec_email.PROC_OK]

        flag_calls = await self._run_one_iteration(fetch_return, process_results)
        # Both UIDs should have been flagged in one batch.
        self.assertEqual(len(flag_calls), 1)
        self.assertEqual(set(flag_calls[0]), {"100", "101"})

    async def test_skip_seen_results_get_flagged_seen(self):
        raw = _build_msg()
        fetch_return = [[("200", raw)]]
        process_results = [exec_email.PROC_SKIP_SEEN]

        flag_calls = await self._run_one_iteration(fetch_return, process_results)
        self.assertEqual(flag_calls, [["200"]])

    async def test_skip_unseen_results_NOT_flagged(self):
        raw = _build_msg()
        fetch_return = [[("300", raw), ("301", raw)]]
        process_results = [exec_email.PROC_SKIP_UNSEEN, exec_email.PROC_OK]

        flag_calls = await self._run_one_iteration(fetch_return, process_results)
        # Only the PROC_OK UID gets flagged; the SKIP_UNSEEN one is left alone.
        self.assertEqual(flag_calls, [["301"]])

    async def test_imap_connected_state_announced_after_first_successful_fetch(self):
        raw = _build_msg()
        fetch_return = [[("400", raw)]]
        process_results = [exec_email.PROC_OK]

        await self._run_one_iteration(fetch_return, process_results)
        self.assertTrue(exec_email.state["coo"]["imap_connected"])
        self.assertIsNone(exec_email.state["coo"]["last_error"])

    async def test_login_error_sets_last_error_and_does_not_crash(self):
        """A MailboxLoginError should flow through the outer except,
        set last_error, and trigger the backoff sleep — not crash."""

        async def fake_to_thread(func, *args, **kw):
            if func is exec_email._fetch_unseen_blocking:
                raise MailboxLoginError("auth failed (test)", "NO")
            return func(*args, **kw)

        async def never_called(*a, **kw):
            self.fail("process_message should not be called when fetch fails")

        with patch.object(exec_email, "POLL_INTERVAL_FALLBACK", 0.01), \
             patch("asyncio.to_thread", fake_to_thread), \
             patch.object(exec_email, "process_message", never_called), \
             patch.dict("os.environ", {"EXEC_EMAIL_COO_PASSWORD": "test-pw"}):
            cfg = next(e for e in exec_email.EXECS if e["role"] == "coo")
            task = asyncio.create_task(exec_email.imap_loop(cfg))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.assertFalse(exec_email.state["coo"]["imap_connected"])
        self.assertIsNotNone(exec_email.state["coo"]["last_error"])
        self.assertIn("imap loop:", exec_email.state["coo"]["last_error"])


class SkipRulesStillFireUnderImapTools(unittest.IsolatedAsyncioTestCase):
    """The library swap must NOT change the should_skip semantics: the
    same rules (deny sender, [Exec-Mail] subject, X-Exec-Mail header,
    Auto-Submitted, Precedence) still trip PROC_SKIP_SEEN."""

    async def asyncSetUp(self):
        # Bypass DB / Crew / SMTP — process_message will hit them
        # otherwise. We only want to confirm should_skip cuts in early.
        exec_email.db_pool = None
        exec_email.nesta_db_pool = None
        # Reset state for "coo".
        exec_email.state["coo"].update({
            "imap_connected": False, "last_message_at": None,
            "messages_today": 0, "today_date": "2026-04-26",
            "last_error": None, "configured": True,
            "rate_today": 0, "rate_this_hour_total": 0,
            "rate_capped_today": False,
        })
        self._cfg = next(e for e in exec_email.EXECS if e["role"] == "coo")

    async def test_deny_sender_pattern(self):
        raw = _build_msg(sender="mailer-daemon@example.com")
        result = await exec_email.process_message(self._cfg, raw)
        self.assertEqual(result, exec_email.PROC_SKIP_SEEN)

    async def test_subject_with_exec_mail_tag_is_skipped(self):
        raw = _build_msg(subject="[Exec-Mail] Reply you sent")
        result = await exec_email.process_message(self._cfg, raw)
        self.assertEqual(result, exec_email.PROC_SKIP_SEEN)

    async def test_x_exec_mail_header_is_skipped(self):
        raw = _build_msg(extra_headers={"X-Exec-Mail": "1"})
        result = await exec_email.process_message(self._cfg, raw)
        self.assertEqual(result, exec_email.PROC_SKIP_SEEN)

    async def test_auto_submitted_other_than_no_is_skipped(self):
        raw = _build_msg(extra_headers={"Auto-Submitted": "auto-replied"})
        result = await exec_email.process_message(self._cfg, raw)
        self.assertEqual(result, exec_email.PROC_SKIP_SEEN)

    async def test_precedence_bulk_is_skipped(self):
        raw = _build_msg(extra_headers={"Precedence": "bulk"})
        result = await exec_email.process_message(self._cfg, raw)
        self.assertEqual(result, exec_email.PROC_SKIP_SEEN)


if __name__ == "__main__":
    unittest.main()

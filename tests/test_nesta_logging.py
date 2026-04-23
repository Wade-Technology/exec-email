"""Unit tests for the Nesta conversations double-log in exec-email.

Goals
-----
- Given a fake inbound envelope + mocked Nesta pool, the `conversations`
  row gets the expected tenant_id, mailbox, persona_id, direction,
  metadata, etc.
- Given a Nesta DB failure, the logger returns cleanly with no
  exception propagating — the IMAP loop must never block on Nesta.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

# Make the repo root importable without installing the package.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# The module starts IMAP+SMTP loops inside main(), but simply importing
# it is fine — main() is only invoked when __name__ == "__main__".
import exec_email  # noqa: E402


class _FakeConn:
    """Context-manager-style mock of an asyncpg connection. Collects
    every execute() call so tests can assert on SQL + params.
    """

    def __init__(self, raise_on_execute: Exception | None = None):
        self.executes: list[tuple[str, tuple]] = []
        self.raise_on_execute = raise_on_execute

    async def execute(self, sql: str, *args):
        self.executes.append((sql, args))
        if self.raise_on_execute is not None:
            raise self.raise_on_execute
        return "INSERT 0 1"


class _FakePool:
    """asyncpg.Pool look-alike with an async context-manager acquire()."""

    def __init__(self, conn: _FakeConn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        class _Acquirer:
            async def __aenter__(self_inner):
                return conn

            async def __aexit__(self_inner, exc_type, exc, tb):
                return False

        return _Acquirer()


class NestaLoggingTests(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        # Reset module-level state between tests.
        exec_email.nesta_db_pool = None
        exec_email._nesta_log_state.update({
            "configured": True,
            "connected": False,
            "last_error": None,
            "last_logged_at": None,
            "rows_logged": 0,
            "failures": 0,
        })

    async def test_inbound_insert_uses_expected_tenant_mailbox_persona(self):
        """Happy path: the INSERT gets Wade's tenant, the exec mailbox,
        the role key, and direction='inbound' with rich metadata.
        """
        fake_conn = _FakeConn()
        exec_email.nesta_db_pool = _FakePool(fake_conn)

        await exec_email._insert_nesta_conversation(
            direction="inbound",
            mailbox="marcus@wade.technology",
            persona_id="coo",
            subject="Partnership inquiry",
            body="We want to explore a pilot.",
            body_plain="We want to explore a pilot.",
            metadata={
                "from": "jane@example.com",
                "to": ["marcus@wade.technology"],
                "cc": [],
                "message_id": "<abc@example.com>",
                "thread_root_id": "<abc@example.com>",
                "in_reply_to": "",
                "exec_role": "coo",
                "exec_name": "Marcus Rivera",
            },
            trigger_event="exec_email_autonomous",
            initiated_by="coo",
            visibility_tag="system",
            requires_review=False,
        )

        self.assertEqual(len(fake_conn.executes), 1)
        sql, args = fake_conn.executes[0]

        self.assertIn("INSERT INTO conversations", sql)
        # Positional args we care about.
        # $2 = tenant_id, $3 = direction, $4 = mailbox, $5 = persona_id
        self.assertEqual(args[1], exec_email.WADE_TENANT_ID)
        self.assertEqual(args[2], "inbound")
        self.assertEqual(args[3], "marcus@wade.technology")
        self.assertEqual(args[4], "coo")
        # $6 = subject, $9 = metadata JSON
        self.assertEqual(args[5], "Partnership inquiry")
        meta = json.loads(args[8])
        self.assertEqual(meta["from"], "jane@example.com")
        self.assertEqual(meta["exec_role"], "coo")
        self.assertIn("marcus@wade.technology", meta["to"])
        # $11 = visibility_tag, $12 = trigger_event, $13 = initiated_by,
        # $14 = requires_review
        self.assertEqual(args[10], "system")
        self.assertEqual(args[11], "exec_email_autonomous")
        self.assertEqual(args[12], "coo")
        self.assertFalse(args[13])

        # State counters updated.
        self.assertEqual(exec_email._nesta_log_state["rows_logged"], 1)
        self.assertEqual(exec_email._nesta_log_state["failures"], 0)
        self.assertTrue(exec_email._nesta_log_state["connected"])

    async def test_db_failure_does_not_propagate(self):
        """If the Nesta pool raises on execute, _insert_nesta_conversation
        must swallow the exception, bump the failure counter, and return
        cleanly. The caller (IMAP loop) must not see the error.
        """
        fake_conn = _FakeConn(raise_on_execute=RuntimeError("connection reset"))
        exec_email.nesta_db_pool = _FakePool(fake_conn)

        # MUST NOT raise.
        await exec_email._insert_nesta_conversation(
            direction="outbound",
            mailbox="marcus@wade.technology",
            persona_id="coo",
            subject="Re: anything",
            body="reply body",
            body_plain="reply body",
            metadata={"from": "marcus@wade.technology", "to": ["x@y.com"]},
            trigger_event="exec_email_autonomous",
            initiated_by="coo",
            visibility_tag="system",
            requires_review=False,
        )

        self.assertEqual(exec_email._nesta_log_state["failures"], 1)
        self.assertEqual(exec_email._nesta_log_state["rows_logged"], 0)
        self.assertFalse(exec_email._nesta_log_state["connected"])
        self.assertIsNotNone(exec_email._nesta_log_state["last_error"])

    async def test_pool_not_initialized_is_noop(self):
        """With no pool configured the logger silently returns — not an
        error, just disabled (e.g. NESTA_DB_* unset in dev)."""
        exec_email.nesta_db_pool = None

        await exec_email._insert_nesta_conversation(
            direction="inbound",
            mailbox="victor@wade.technology",
            persona_id="ceo",
            subject="hello",
            body="hi",
            body_plain="hi",
            metadata={},
            trigger_event="exec_email_autonomous",
            initiated_by="ceo",
        )

        self.assertEqual(exec_email._nesta_log_state["rows_logged"], 0)
        self.assertEqual(exec_email._nesta_log_state["failures"], 0)

    async def test_fire_and_forget_schedules_task_and_swallows_pool_errors(self):
        """_log_to_nesta_fire_and_forget must return synchronously and
        the scheduled task must not propagate DB errors either."""
        fake_conn = _FakeConn(raise_on_execute=RuntimeError("nesta down"))
        exec_email.nesta_db_pool = _FakePool(fake_conn)

        # Schedule (returns immediately).
        exec_email._log_to_nesta_fire_and_forget(
            direction="inbound",
            mailbox="leo@wade.technology",
            persona_id="ccio",
            subject="security question",
            body="pls help",
            body_plain="pls help",
            metadata={"from": "curious@example.com"},
            trigger_event="exec_email_autonomous",
            initiated_by="ccio",
        )

        # Give the event loop a tick to actually run the scheduled task.
        await asyncio.sleep(0.05)

        # The scheduled task hit the error path; state reflects it, but
        # nothing raised out of the scheduler.
        self.assertEqual(exec_email._nesta_log_state["failures"], 1)

    async def test_requires_review_true_when_smtp_fails(self):
        """SMTP-failure outbound rows should surface with
        visibility_tag='sensitive' and requires_review=True so Wade
        sees them in /communications."""
        fake_conn = _FakeConn()
        exec_email.nesta_db_pool = _FakePool(fake_conn)

        await exec_email._insert_nesta_conversation(
            direction="outbound",
            mailbox="kai@wade.technology",
            persona_id="cto",
            subject="Re: infra question",
            body="reply that failed to send",
            body_plain="reply that failed to send",
            metadata={"smtp_error": "Connection refused"},
            trigger_event="exec_email_autonomous",
            initiated_by="cto",
            visibility_tag="sensitive",
            requires_review=True,
        )

        self.assertEqual(len(fake_conn.executes), 1)
        _, args = fake_conn.executes[0]
        # $11 = visibility_tag, $14 = requires_review
        self.assertEqual(args[10], "sensitive")
        self.assertTrue(args[13])


if __name__ == "__main__":
    unittest.main()

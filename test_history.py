import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
import tempfile
import sqlite3
import unittest
from unittest.mock import patch

from bot import Bridge
from history import History, ZONE, current_archive, query
from plaintext import plain_reply


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "history.sqlite3"
        self.history = History(self.path)

    def tearDown(self):
        self.tmp.cleanup()

    def message(self, mid, date, user=8, text="今天讨论国庆出游"):
        return {"message_id": mid, "time": datetime.fromisoformat(date).replace(tzinfo=ZONE).timestamp(), "user_id": user, "sender": {"nickname": "小明"}, "message": [{"type": "text", "data": {"text": text}}]}

    def test_date_boundaries_persistence_filters_and_pagination(self):
        self.history.save(self.message(1, "2026-09-30T23:59:59"))
        self.history.save(self.message(2, "2026-10-01T00:00:00"))
        self.history.save(self.message(3, "2026-10-01T23:59:59", user=9))
        self.history.save(self.message(4, "2026-10-02T00:00:00"))
        self.assertFalse(self.history.save(self.message(2, "2026-10-01T00:00:00")))
        reopened = History(self.path)
        page = query(reopened.path, date="2026-10-01", limit=1)
        self.assertEqual(page["total"], 2)
        self.assertEqual(page["messages"][0]["message_id"], "2")
        self.assertEqual(page["next_offset"], 1)
        self.assertEqual(query(self.path, date="2026-10-01", offset=1)["messages"][0]["message_id"], "3")
        self.assertEqual(query(self.path, date="2026-10-01", user=9, contains="国庆")["total"], 1)
        self.assertEqual(query(self.path, contains="' OR 1=1 --")["total"], 0)
        self.assertEqual(query(self.path, message_id="2")["messages"][0]["name"], "小明")
        self.assertEqual(query(self.path, date="2026-09-01")["total"], 0)
        self.assertEqual(query(self.path)["coverage"]["count"], 4)

    def test_groups_do_not_share_archives(self):
        self.history.save(self.message(1, "2026-10-01T12:00:00"))
        other = History(Path(self.tmp.name) / "other" / "history.sqlite3")
        self.assertEqual(query(other.path)["total"], 0)
        with patch("history.Path.cwd", return_value=Path("/state/groups/123456789/workspace")):
            self.assertEqual(current_archive(), Path("/state/groups/123456789/history.sqlite3"))
        with patch("history.Path.cwd", return_value=Path("/etc")):
            self.assertRaises(ValueError, current_archive)

    def test_query_with_read_only_directory_needs_no_sidecar_files(self):
        if os.geteuid() == 0:
            self.skipTest("Permission regression must run as an unprivileged user")
        self.history.save(self.message(1, "2026-10-01T12:00:00"))
        parent = self.path.parent
        before = sorted(p.name for p in parent.iterdir())
        self.path.chmod(0o444)
        parent.chmod(0o555)
        try:
            result = query(self.path, date="2026-10-01")
            self.assertEqual(result["total"], 1)
            self.assertEqual(sorted(p.name for p in parent.iterdir()), before)
        finally:
            parent.chmod(0o700)
            self.path.chmod(0o600)

    def test_existing_wal_archive_migrates_without_losing_records(self):
        self.history.save(self.message(1, "2026-10-01T12:00:00"))
        with self.history.connection() as db:
            self.assertEqual(db.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal")
            db.execute("INSERT OR REPLACE INTO metadata VALUES ('backfill_status','sync_window_reached')")
        reopened = History(self.path)
        with reopened.connection() as db:
            self.assertEqual(db.execute("PRAGMA journal_mode").fetchone()[0], "delete")
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        result = query(self.path)
        self.assertEqual(result["total"], 1)
        self.assertIn("does not establish", result["coverage"]["backfill_status_meaning"])

    def test_markdown_is_plain_text_but_code_and_urls_survive(self):
        result = plain_reply("## 结论\n\n**结果**是 `4`。\n\n- [来源](https://example.com)\n\n```python\nprint(2*2)\n```\n")
        self.assertNotIn("**", result)
        self.assertNotIn("##", result)
        self.assertNotIn("```", result)
        self.assertNotIn("[来源]", result)
        self.assertIn("https://example.com", result)
        self.assertIn("print(2*2)", result)
        self.assertLessEqual(len(plain_reply("长" * 10000)), 1800)
        table = plain_reply("| 名字 | 数量 |\n|---|---|\n| 小明 | 2 |")
        self.assertNotIn("|", table)
        self.assertIn("小明", table)
        self.assertEqual(plain_reply("~~旧说法~~，新说法"), "旧说法，新说法")


class BackfillTests(unittest.IsolatedAsyncioTestCase):
    async def test_history_pages_are_archived_without_triggering_jobs(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps({"bot_qq": 12345, "trusted_groups": [123456789]}))
            bridge = Bridge(path, tmp, "x" * 40)
            calls = []

            async def api(action, params):
                calls.append(params)
                if len(calls) == 1:
                    return {"messages": [{"message_id": 90, "message_seq": 90, "group_id": 123456789, "time": datetime(2026, 10, 1, 10, tzinfo=ZONE).timestamp(), "user_id": 8, "message": [{"type": "at", "data": {"qq": "12345"}}, {"type": "text", "data": {"text": "旧消息"}}]}]}
                return {"messages": []}

            bridge.api = api
            bridge.connected.set()
            task = asyncio.create_task(bridge.sync_history())
            try:
                for _ in range(100):
                    if bridge.archive(123456789).get_metadata("last_sync"):
                        break
                    await asyncio.sleep(.01)
                self.assertEqual(query(bridge.archive(123456789).path, date="2026-10-01")["total"], 1)
                self.assertTrue(bridge.queue.empty())
                if len(calls) > 1:
                    self.assertEqual(calls[1]["message_seq"], "90")
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                bridge.db.close()


if __name__ == "__main__":
    unittest.main()

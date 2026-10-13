import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
import tempfile
import sqlite3
import unittest
from unittest.mock import AsyncMock, patch

from bot import Bridge, load_config
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

    def test_transcription_is_persisted_in_recent_and_query(self):
        voice = self.message("voice", "2026-10-01T12:00:00")
        voice["message"] = [{"type": "record", "data": {"file": "a.silk"}}]
        self.history.save(voice)
        self.history.set_transcription("voice", "请改用 C++", "ready")
        self.assertIn("请改用 C++", self.history.recent()[0]["text"])
        self.assertEqual(self.history.transcription("voice")["status"], "ready")
        self.assertIn("请改用 C++", query(self.path, message_id="voice")["messages"][0]["text"])

    def test_file_attachments_are_indexed_and_exposed_without_downloading(self):
        message = self.message("file", "2026-10-01T12:00:00")
        message["message"] = [{"type": "file", "data": {"file_id": "qq-file-id", "file": "report.pdf", "file_size": "1234"}}]
        self.history.save(message)
        self.assertEqual(self.history.files("file")[0]["name"], "report.pdf")
        result = query(self.path, message_id="file")
        self.assertEqual(result["messages"][0]["files"][0]["status"], "available")
        self.assertEqual(result["messages"][0]["files"][0]["size"], 1234)

    def test_member_statistics_aliases_and_transcript_search(self):
        a = self.message(1, '2026-10-01T12:00:00', user=8)
        a['sender']['nickname'] = 'Old Name'
        self.history.save(a)
        a = self.message(2, '2026-10-02T12:00:00', user=8)
        a['sender']['nickname'] = 'New Name'
        self.history.save(a)
        self.history.save(self.message(3, '2026-10-02T12:01:00', user=9))
        self.history.set_transcription(2, '测试语音 Python')
        self.assertEqual(query(self.path, contains='python')['messages'][0]['message_id'], '2')
        from history import statistics
        result = statistics(self.path, members=True, name='Old', start='2026-10-02')
        self.assertEqual(result['member_count'], 1)
        self.assertEqual(result['members'][0]['names'], ['New Name', 'Old Name'])
        self.assertEqual(result['members'][0]['count'], 1)
        self.assertEqual(statistics(self.path, user=8)['active_days'], 2)
        self.assertEqual(statistics(self.path, exclude_user=[8])['total'], 1)
        self.assertEqual(statistics(self.path, members=True, name="' OR 1=1 --")['total'], 0)

    def test_sampling_days_recent_order_and_context_attribution(self):
        for i in range(30):
            self.history.save(self.message(str(i), f'2026-10-01T12:00:{i:02}', user=8))
        self.history.save(self.message('day2', '2026-10-02T12:00:00', user=8))
        self.history.save(self.message('neighbor', '2026-10-03T11:59:59', user=9))
        reply = self.message('day3', '2026-10-03T12:00:00', user=8)
        reply['message'].insert(0, {'type': 'reply', 'data': {'id': '0'}})
        self.history.save(reply)
        a = query(self.path, user=8, sample=3, seed='stable', context=1)
        b = query(self.path, user=8, sample=3, seed='stable', context=1)
        self.assertEqual(a, b)
        self.assertEqual(len({m['time'][:10] for m in a['messages']}), 3)
        self.assertTrue(all(m['sender_id'] == 8 for m in a['messages']))
        third = next(m for m in a['messages'] if m['message_id'] == 'day3')
        self.assertEqual(third['reply_to'], ['0'])
        self.assertIn('neighbor', third['context_ids'])
        self.assertIn('0', third['context_ids'])
        self.assertTrue(any(m['sender_id'] == 9 for m in a['context_messages']))
        self.assertIsNone(a['next_offset'])
        newest = query(self.path, order='desc', limit=1)
        self.assertEqual(newest['messages'][0]['message_id'], 'day3')
        self.assertEqual(query(self.path, order='desc', limit=1, offset=newest['next_offset'])['messages'][0]['message_id'], 'neighbor')
        with self.assertRaises(ValueError):
            query(self.path, sample=3, offset=1)

    def test_export_scope_no_overwrite_and_evidence_roundtrip(self):
        from history import export_result
        self.history.save(self.message('1', '2026-10-01T12:00:00'))
        workspace = Path(self.tmp.name) / 'workspace'
        workspace.mkdir()
        result = query(self.path, sample=1)
        output = export_result(result, 'evidence.jsonl', workspace)
        rows = [json.loads(line) for line in Path(output['file_path']).read_text().splitlines()]
        self.assertEqual(rows[0]['record_type'], 'metadata')
        self.assertEqual(rows[1]['message_id'], '1')
        self.assertEqual(rows[1]['role'], 'selected')
        self.assertEqual(Path(output['file_path']).stat().st_mode & 0o777, 0o600)
        with self.assertRaises(FileExistsError):
            export_result(result, 'evidence.jsonl', workspace)
        with self.assertRaises(ValueError):
            export_result(result, '../outside.jsonl', workspace)
        (workspace / 'escape').symlink_to(workspace.parent)
        with self.assertRaises(ValueError):
            export_result(result, 'escape/outside.jsonl', workspace)
        export_result(result, 'evidence.txt', workspace, 'text')
        self.assertIn('QQ 8, message 1', (workspace / 'evidence.txt').read_text())

    def test_context_budget_and_missing_reply_are_explicit(self):
        for i in range(40):
            row = self.message(str(i), f'2026-10-01T12:00:{i:02}', text='x' * 4000)
            row['message'].insert(0, {'type': 'reply', 'data': {'id': 'missing'}})
            self.history.save(row)
        result = query(self.path, limit=40, context=5)
        self.assertLess(len(result['messages']), 40)
        self.assertIsNotNone(result['next_offset'])
        self.assertTrue(result['selection']['text_budget_truncated'])
        self.assertTrue(result['context_truncated'])
        self.assertIn('missing', result['messages'][0]['missing_reply_ids'])
        self.assertTrue(result['messages'][0]['text_truncated'])



class BackfillTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def old_message(mid, now, age_days):
        return {"message_id": mid, "group_id": 456, "time": now - age_days * 86400,
                "user_id": 8, "message": [{"type": "text", "data": {"text": f"message {mid}"}}]}

    async def test_year_import_ignores_legacy_checkpoint_and_keeps_older_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.json"
            config.write_text(json.dumps({"bot_qq": 123, "trusted_groups": [456]}))
            bridge = Bridge(config, tmp, "x" * 40)
            now = 1800000000
            archive = bridge.archive(456)
            archive.save(self.old_message(1, now, 500))
            archive.metadata("last_successful_sync", now - 60)
            bridge.api = AsyncMock(side_effect=[
                {"messages": [self.old_message(2, now, 60)]},
                {"messages": [self.old_message(3, now, 366)]},
            ])
            try:
                with patch("bot.time.time", return_value=now), patch("bot.asyncio.sleep", new_callable=AsyncMock):
                    await bridge.sync_group_history(456)
                self.assertEqual(bridge.api.await_count, 2)
                self.assertEqual(archive.get_metadata("backfill_status"), "initial_lookback_limit_reached")
                self.assertEqual(query(archive.path)["total"], 3)
                self.assertEqual(query(archive.path)["coverage"]["history_lookback_days"], 365)
                self.assertTrue(bridge.queue.empty())
            finally:
                bridge.db.close()

    async def test_page_budget_and_process_restart_resume_saved_cursor(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.json"
            config.write_text(json.dumps({"bot_qq": 123, "trusted_groups": [456]}))
            now = 1800000000
            bridge = Bridge(config, tmp, "x" * 40)
            bridge.api = AsyncMock(return_value={"messages": [self.old_message(10, now, 60)]})
            try:
                with patch("bot.time.time", return_value=now), patch("bot.asyncio.sleep", new_callable=AsyncMock):
                    await bridge.sync_group_history(456, page_limit=1)
                self.assertEqual(bridge.archive(456).get_metadata("backfill_status"), "page_limit")
            finally:
                bridge.db.close()
            bridge = Bridge(config, tmp, "x" * 40)
            bridge.api = AsyncMock(side_effect=[
                {"messages": [self.old_message(12, now, 1)]}, ConnectionError("offline"),
                {"messages": [self.old_message(12, now, 1)]},
                {"messages": [self.old_message(11, now, 366)]},
            ])
            try:
                with patch("bot.time.time", return_value=now + 600):
                    await bridge.sync_group_history(456)
                    self.assertEqual(bridge.archive(456).get_metadata("backfill_status"), "gateway_history_unavailable")
                    await bridge.sync_group_history(456)
                calls = bridge.api.await_args_list
                self.assertEqual(len(calls), 4)
                for call in calls[::2]:
                    self.assertNotIn("message_seq", call.args[1])
                for call in calls[1::2]:
                    self.assertEqual(call.args[1]["message_seq"], "10")
                self.assertEqual(query(bridge.archive(456).path)["total"], 3)
                self.assertEqual(float(bridge.archive(456).get_metadata("last_successful_sync")), now)
            finally:
                bridge.db.close()

    async def test_stalled_backfill_still_repairs_recent_gaps(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.json"
            config.write_text(json.dumps({"bot_qq": 123, "trusted_groups": [456]}))
            bridge = Bridge(config, tmp, "x" * 40)
            now = 1800000000
            old = self.old_message(10, now, 60)
            bridge.api = AsyncMock(side_effect=[{"messages": [old]}, {"messages": [old]}])
            try:
                with patch("bot.time.time", return_value=now), patch("bot.asyncio.sleep", new_callable=AsyncMock):
                    await bridge.sync_group_history(456)
                self.assertEqual(bridge.archive(456).get_metadata("backfill_status"), "pagination_stalled")
                missed = self.old_message(11, now + 500, 0)
                bridge.api = AsyncMock(side_effect=[{"messages": [missed, old]}, {"messages": [old]}])
                with patch("bot.time.time", return_value=now + 600):
                    await bridge.sync_group_history(456)
                self.assertNotIn("message_seq", bridge.api.await_args_list[0].args[1])
                self.assertEqual(bridge.api.await_args_list[1].args[1]["message_seq"], "10")
                self.assertEqual(query(bridge.archive(456).path)["total"], 2)
                self.assertEqual(bridge.archive(456).get_metadata("backfill_status"), "pagination_stalled")
            finally:
                bridge.db.close()

    async def test_account_switch_rollback_and_longer_window_preserve_group_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.json"
            now = 1800000000
            # Original account, temporary account, original again, then a wider window.
            runs = [(123, 30, 1, 31), (789, 365, 2, 366),
                    (123, 30, 3, 1), (123, 365, 4, 366)]
            for account, days, mid, age in runs:
                config.write_text(json.dumps({"bot_qq": account, "trusted_groups": [456],
                                              "history_lookback_days": days}))
                bridge = Bridge(config, tmp, "x" * 40)
                bridge.api = AsyncMock(return_value={"messages": [self.old_message(mid, now, age)]})
                try:
                    with patch("bot.time.time", return_value=now):
                        await bridge.sync_group_history(456)
                    self.assertEqual(bridge.api.await_count, 1)
                    expected = "incremental_caught_up" if mid == 3 else "initial_lookback_limit_reached"
                    self.assertEqual(bridge.archive(456).get_metadata("backfill_status"), expected)
                    self.assertEqual(query(bridge.archive(456).path)["total"], mid)
                finally:
                    bridge.db.close()

    def test_lookback_config_rejects_invalid_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.json"
            for value in (0, -1, 3651, True, "365"):
                config.write_text(json.dumps({"bot_qq": 123, "trusted_groups": [456],
                                              "history_lookback_days": value}))
                with self.assertRaisesRegex(ValueError, "history_lookback_days"):
                    load_config(config)

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

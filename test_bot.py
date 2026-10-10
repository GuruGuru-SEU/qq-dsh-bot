import asyncio
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from aiohttp import ClientSession, WSServerHandshakeError, web
from bot import Bridge, prompt_for
from status_report import last_run, resources, status_report


class BridgeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.config = Path(self.tmp.name) / "config.json"
        self.config.write_text(json.dumps({"trusted_groups": [123456789], "bot_qq": 12345, "history_sync": False}))
        self.jobs = []

        async def runner(job):
            self.jobs.append(job)
            return "VERIFIED_REPLY"

        self.bridge = Bridge(self.config, self.tmp.name, "t" * 40, runner)

    async def asyncTearDown(self):
        if not self.bridge.worker_task:
            self.bridge.db.close()
        self.tmp.cleanup()

    def event(self, mid, group=123456789, sender=8, mention=True):
        segments = [{"type": "text", "data": {"text": "compute 2+2"}}]
        if mention:
            segments.append({"type": "at", "data": {"qq": "12345"}})
        return {"post_type": "message", "message_type": "group", "self_id": 12345,
                "group_id": group, "user_id": sender, "message_id": mid,
                "time": time.time(), "message": segments, "sender": {"nickname": "Alice"}}

    async def test_filter_context_reload_and_dedup(self):
        await self.bridge.event(self.event(1, group=999))
        await self.bridge.event(self.event(2, sender=12345))
        await self.bridge.event(self.event(3, mention=False))
        await self.bridge.event(self.event(4))
        await self.bridge.event(self.event(4))
        self.assertEqual(self.bridge.queue.qsize(), 1)
        job = self.bridge.queue.get_nowait()
        self.assertEqual([x["message_id"] for x in job.context], ["2", "3"])
        self.assertIn("recent_messages", prompt_for(job))
        self.assertNotIn(999, self.bridge.archives)
        self.config.write_text(json.dumps({"trusted_groups": [123456789, 777], "bot_qq": 12345}))
        await self.bridge.event(self.event(5, group=777))
        job = self.bridge.queue.get_nowait()
        self.assertEqual(job.context, [])
        self.config.write_text("invalid")
        await self.bridge.event(self.event(6))
        self.assertTrue(self.bridge.queue.empty())

    async def test_only_explicit_self_mention_and_recent_events(self):
        event = self.event(1)
        event["message"][-1]["data"]["qq"] = "all"
        await self.bridge.event(event)
        event = self.event(2)
        event["message"] = [{"type": "text", "data": {"text": "[CQ:at,qq=12345]"}}]
        await self.bridge.event(event)
        event = self.event(3)
        event["time"] -= 1000
        await self.bridge.event(event)
        self.assertTrue(self.bridge.queue.empty())

    async def test_real_websocket_auth_request_reply_and_reconnect(self):
        app_runner = web.AppRunner(self.bridge.app())
        await app_runner.setup()
        site = web.TCPSite(app_runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        try:
            async with ClientSession() as client:
                with self.assertRaises(WSServerHandshakeError):
                    await client.ws_connect(url + "/onebot")
                headers = {"Authorization": "Bearer " + "t" * 40, "X-Self-ID": "12345"}
                async with client.ws_connect(url + "/onebot", headers=headers) as ws:
                    action = await ws.receive_json(timeout=3)
                    self.assertEqual(action["action"], "get_login_info")
                    await ws.send_json({"status": "ok", "retcode": 0, "echo": action["echo"], "data": {"user_id": 12345}})
                    await asyncio.wait_for(self.bridge.connected.wait(), 3)
                    await ws.send_json(self.event(10, mention=False))
                    await ws.send_json(self.event(11))
                    reply = await ws.receive_json(timeout=3)
                    self.assertEqual(reply["action"], "send_group_msg")
                    self.assertEqual(reply["params"]["group_id"], 123456789)
                    self.assertEqual(reply["params"]["message"][0], {"type": "reply", "data": {"id": "11"}})
                    self.assertEqual(reply["params"]["message"][1]["data"]["text"], "VERIFIED_REPLY")
                    await ws.send_json({"status": "ok", "retcode": 0, "echo": reply["echo"], "data": {"message_id": 12}})
                    await asyncio.wait_for(self.bridge.queue.join(), 3)
                    self.assertEqual(len(self.jobs), 1)
                    self.assertEqual(len(self.jobs[0].context), 1)
                    health = await client.get(url + "/health")
                    self.assertEqual(health.status, 200)
            for _ in range(20):
                if not self.bridge.connected.is_set():
                    break
                await asyncio.sleep(.01)
            self.assertFalse(self.bridge.connected.is_set())
        finally:
            await app_runner.cleanup()

    async def test_removed_group_is_not_replied_to(self):
        await self.bridge.event(self.event(1))
        job = self.bridge.queue.get_nowait()
        self.config.write_text(json.dumps({"trusted_groups": [777], "bot_qq": 12345}))
        self.bridge.connected.set()
        calls = []

        async def api(action, params):
            calls.append((action, params))

        self.bridge.api = api
        await self.bridge.reply(job, "Do not deliver")
        self.assertEqual(calls, [])

    async def test_worker_timeout_replies_with_failure(self):
        async def timeout(job):
            raise asyncio.TimeoutError()

        replies = []

        async def reply(job, text):
            replies.append(text)

        self.bridge.runner = timeout
        self.bridge.reply = reply
        worker = asyncio.create_task(self.bridge.worker())
        try:
            await self.bridge.event(self.event(1))
            await asyncio.wait_for(self.bridge.queue.join(), 2)
            self.assertIn("5 分钟", replies[0])
            self.assertIn('/continue ', replies[0])
            self.assertIn('转到后台继续', replies[0])
            self.assertEqual(self.bridge.db.execute('SELECT state FROM background_jobs').fetchone()[0], 'awaiting_confirmation')
            self.assertFalse(self.bridge.background.execution)
            self.assertIsNone(self.bridge.active)
        finally:
            worker.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await worker

    def status_event(self, mid, **kwargs):
        event = self.event(mid, **kwargs)
        event['message'][0]['data']['text'] = '请检查 /status 现在怎么样'
        return event

    async def test_status_substring_filters_and_dedup(self):
        for mid, kwargs in [(1, {'group': 999}), (2, {'mention': False}), (3, {'sender': 12345})]:
            await self.bridge.event(self.status_event(mid, **kwargs))
        stale = self.status_event(4)
        stale['time'] -= 1000
        await self.bridge.event(stale)
        private = self.status_event(5)
        private['message_type'] = 'private'
        await self.bridge.event(private)
        all_mention = self.status_event(6)
        all_mention['message'][-1]['data']['qq'] = 'all'
        await self.bridge.event(all_mention)
        self.assertTrue(self.bridge.status_queue.empty())
        long = self.status_event(7)
        long['message'][0]['data']['text'] = 'x' * 4000 + '/status'
        await self.bridge.event(long)
        await self.bridge.event(long)
        self.assertEqual(self.bridge.status_queue.qsize(), 1)
        self.assertTrue(self.bridge.queue.empty())
        self.assertEqual(self.jobs, [])
        # Attachment metadata alone must not turn a normal request into a command.
        normal = self.event(8)
        normal['message'].append({'type': 'image', 'data': {'file': '/status'}})
        await self.bridge.event(normal)
        self.assertEqual(self.bridge.queue.qsize(), 1)

    async def test_exact_continuation_confirmation_bypasses_model_and_checks_owner(self):
        from bot import Job
        original = Job(123456789, 'original', 8, 'long task', [])
        jid = self.bridge.background.propose_timeout(original)
        worker = asyncio.create_task(self.bridge.status_worker())
        replies = []
        async def reply(job, text): replies.append(text)
        self.bridge.reply = reply
        try:
            for mid, sender in [(50, 9), (51, 8)]:
                event = self.event(mid, sender=sender)
                event['message'][0]['data']['text'] = '/continue ' + jid
                await self.bridge.event(event)
                await asyncio.wait_for(self.bridge.status_queue.join(), 2)
                state = self.bridge.db.execute('SELECT state FROM background_jobs WHERE id=?', (jid,)).fetchone()[0]
                self.assertEqual(state, 'awaiting_confirmation' if sender == 9 else 'active')
            self.assertIn('只有原请求者', replies[0])
            self.assertIn('已确认', replies[1])
            self.assertTrue(self.bridge.queue.empty())
            self.assertEqual(self.jobs, [])
        finally:
            worker.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await worker

    async def test_status_real_websocket_bypasses_busy_full_model_queue(self):
        started = asyncio.Event()
        async def busy(job):
            self.jobs.append(job)
            started.set()
            await asyncio.Event().wait()
        self.bridge.runner = busy
        app_runner = web.AppRunner(self.bridge.app())
        await app_runner.setup()
        site = web.TCPSite(app_runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            async with ClientSession() as client:
                headers = {'Authorization': 'Bearer ' + 't' * 40, 'X-Self-ID': '12345'}
                async with client.ws_connect(f'http://127.0.0.1:{port}/onebot', headers=headers) as ws:
                    login = await ws.receive_json(timeout=3)
                    await ws.send_json({'status': 'ok', 'retcode': 0, 'echo': login['echo'], 'data': {'user_id': 12345}})
                    await asyncio.wait_for(self.bridge.connected.wait(), 3)
                    await ws.send_json(self.event(20))
                    await asyncio.wait_for(started.wait(), 3)
                    for mid in range(21, 26):
                        await ws.send_json(self.event(mid))
                    await ws.send_json(self.status_event(26))
                    reply = await ws.receive_json(timeout=3)
                    self.assertEqual(reply['action'], 'send_group_msg')
                    self.assertEqual(reply['params']['message'][0]['data']['id'], '26')
                    text = reply['params']['message'][1]['data']['text']
                    self.assertIn('系统状态', text)
                    self.assertIn('排队 5/5', text)
                    self.assertIn('处理中', text)
                    self.assertIn('本次未调用 LLM 或 DSH', text)
                    await ws.send_json({'status': 'ok', 'retcode': 0, 'echo': reply['echo'], 'data': {'message_id': 27}})
                    await asyncio.wait_for(self.bridge.status_queue.join(), 3)
                    self.assertEqual(len(self.jobs), 1)
                    self.assertEqual(self.bridge.queue.qsize(), 5)
                    self.assertIn('27', [row['message_id'] for row in self.bridge.archive(123456789).recent()])
        finally:
            await app_runner.cleanup()

    async def test_status_failure_never_falls_back_to_model(self):
        replies = []
        async def reply(job, text):
            replies.append(text)
            raise ConnectionError('Simulated delivery failure')
        self.bridge.reply = reply
        worker = asyncio.create_task(self.bridge.status_worker())
        try:
            with patch('bot.status_report', side_effect=OSError('private-error')):
                await self.bridge.event(self.status_event(1))
                await asyncio.wait_for(self.bridge.status_queue.join(), 2)
            await self.bridge.event(self.status_event(2))
            await asyncio.wait_for(self.bridge.status_queue.join(), 2)
            self.assertEqual(len(replies), 2)
            self.assertIn('状态读取失败', replies[0])
            self.assertIn('系统状态', replies[1])
            self.assertNotIn('private-error', ''.join(replies))
            self.assertTrue(self.bridge.queue.empty())
            self.assertEqual(self.jobs, [])
        finally:
            worker.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await worker

    async def test_status_queue_bound_and_trust_revocation(self):
        for mid in range(30):
            await self.bridge.event(self.status_event(mid))
        self.assertEqual(self.bridge.status_queue.qsize(), 20)
        self.assertTrue(self.bridge.queue.empty())
        self.config.write_text(json.dumps({'trusted_groups': [777], 'bot_qq': 12345}))
        worker = asyncio.create_task(self.bridge.status_worker())
        try:
            await asyncio.wait_for(self.bridge.status_queue.join(), 2)
            self.assertEqual(self.jobs, [])
        finally:
            worker.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await worker

    async def test_status_group_scope_and_missing_metrics(self):
        db = self.bridge.db
        db.execute("INSERT INTO background_jobs (id,group_id,state) VALUES ('ours',123456789,'awaiting_confirmation')")
        db.execute("INSERT INTO background_jobs (id,group_id,state,spec) VALUES ('theirs',777,'running','PRIVATE PROMPT')")
        db.execute("INSERT INTO schedules (id,group_id,state,text) VALUES ('theirs',777,'active','PRIVATE MESSAGE')")
        db.commit()
        report = status_report(self.bridge, 123456789)
        self.assertIn('待确认 1，待运行 0，运行中 0', report)
        self.assertIn('本群有效定时消息：0', report)
        self.assertNotIn('PRIVATE', report)
        self.assertNotIn('777', report)
        self.assertLess(len(report), 1800)
        directory = Path(self.tmp.name) / 'groups' / '123456789' / 'diagnostics'
        directory.mkdir(parents=True)
        record = {'outcome': 'max-tokens', 'stop_reason': 'max-tokens', 'exit_code': 1,
                  'duration_seconds': 45, 'finished_at': '2026-10-10T14:22:10+08:00', 'secret': 'PRIVATE'}
        (directory / 'last.json').write_text(json.dumps(record))
        report = last_run(self.tmp.name, 123456789)
        self.assertIn('失败', report)
        self.assertIn('max-tokens', report)
        self.assertNotIn('PRIVATE', report)
        self.assertIn('暂无可读诊断', last_run(self.tmp.name, 777))
        (directory / 'last.json').write_text('broken')
        self.assertIn('暂无可读诊断', last_run(self.tmp.name, 123456789))
        with patch('status_report.Path.read_text', side_effect=OSError), patch('status_report.shutil.disk_usage', side_effect=OSError):
            self.assertTrue(all('不可用' in line for line in resources(self.tmp.name)))


if __name__ == "__main__":
    unittest.main()

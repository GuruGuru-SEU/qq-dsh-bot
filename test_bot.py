import asyncio
import json
from pathlib import Path
import tempfile
import time
import unittest

from aiohttp import ClientSession, WSServerHandshakeError, web
from bot import Bridge, prompt_for


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
            self.assertIsNone(self.bridge.active)
        finally:
            worker.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await worker


if __name__ == "__main__":
    unittest.main()

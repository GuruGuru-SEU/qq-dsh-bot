import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest

from aiohttp import ClientSession, web
from bot import Bridge, Job, prompt_for
from history import ZONE, query
from messaging import MessageService, next_cron, once_time
from upload import OneBotError


class ScheduleParsingTests(unittest.TestCase):
    def test_dates_cron_boundaries_and_validation(self):
        now = datetime(2026, 10, 10, 13, 44, tzinfo=ZONE).timestamp()
        due = next_cron("45 13 * * *", now)
        self.assertEqual(datetime.fromtimestamp(due, ZONE).isoformat(), "2026-10-10T13:45:00+08:00")
        self.assertEqual(next_cron("45 13 * * *", due), due + 86400)
        self.assertEqual(once_time("2026-10-10T13:45", now), due)
        self.assertEqual(once_time("2026-10-10T05:45:00+00:00", now), due)
        self.assertEqual(next_cron("0 0 29 2 *", now), datetime(2028, 2, 29, tzinfo=ZONE).timestamp())
        for value in ["13:45", "2026-10-09T13:45", "2026-02-30T13:45"]:
            with self.assertRaises(ValueError):
                once_time(value, now)
        for value in ["* * * * * *", "@daily", "99 * * * *", "0 0 30 2 *", "R * * * *"]:
            with self.assertRaises(ValueError):
                next_cron(value, now)
        prompt = prompt_for(Job(123456789, "1", 8, "hi", []))
        self.assertIn("不要滥用", prompt)
        self.assertNotIn("Do not send messages yourself", prompt)


class MessagingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.config = self.root / "config.json"
        self.config.write_text(json.dumps({"trusted_groups": [123456789, 987654321], "bot_qq": 12345, "history_sync": False}))
        self.bridge = Bridge(self.config, self.root, "t" * 40)
        self.job = Job(123456789, "message-test", 8, "remind us", [])
        self.token = self.bridge.uploads.begin(self.job)
        self.service = self.bridge.messages
        self.grant = self.bridge.uploads.grants[self.token]
        self.calls = []
        async def api(action, params, **kwargs):
            self.calls.append((action, params))
            return {"message_id": len(self.calls)}
        self.bridge.api = api
        self.bridge.connected.set()
        app = web.Application()
        app.router.add_post("/agent/messages", self.service.handle)
        app.router.add_get("/onebot", self.bridge.socket)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        self.client = ClientSession()

    async def asyncTearDown(self):
        if self.bridge.ws:
            await self.bridge.ws.close()
        await self.client.close()
        await self.runner.cleanup()
        self.bridge.db.close()
        self.tmp.cleanup()

    async def post(self, body, token=None):
        async with self.client.post(self.url + "/agent/messages", json=body,
                headers={"Authorization": "Bearer " + (self.token if token is None else token)}) as response:
            return response.status, await response.json()

    async def schedule(self, key="one", seconds=60, **extra):
        body = {"op": "schedule", "key": key, "text": "提醒", "at": datetime.fromtimestamp(time.time() + seconds, ZONE).isoformat()}
        if "cron" in extra:
            body.pop("at")
        body.update(extra)
        status, result = await self.post(body)
        self.assertEqual(status, 200, result)
        return result, body

    def due(self, sid, seconds_ago=0):
        self.bridge.db.execute("UPDATE schedules SET next_run=? WHERE id=?", (time.time() - seconds_ago, sid))
        self.bridge.db.commit()

    def restart(self):
        self.bridge.db.close()
        self.bridge = Bridge(self.config, self.root, "t" * 40)
        self.bridge.connected.set()
        self.service = self.bridge.messages
        async def send(group, text):
            self.calls.append((group, text))
            return {"message_id": len(self.calls)}
        self.service.send = send

    async def test_scope_revocation_and_malformed_payloads(self):
        self.assertEqual((await self.post({"op": "list"}, "bad"))[0], 403)
        for body in [[], {"op": []}, {"op": "send", "group_id": 987654321}, {"op": "schedule", "key": "k", "text": "x"}, {"op": "cancel", "id": 5}]:
            self.assertEqual((await self.post(body))[0], 400)
        self.bridge.uploads.end(self.token)
        self.assertEqual((await self.post({"op": "list"}))[0], 403)
        self.token = self.bridge.uploads.begin(self.job)
        self.config.write_text(json.dumps({"trusted_groups": [987654321], "bot_qq": 12345}))
        self.assertEqual((await self.post({"op": "list"}))[0], 403)

    async def test_progress_throttle_dedup_plain_text_and_archive(self):
        body = {"op": "send", "reason": "progress", "key": "p1", "text": "**working** [CQ:at,qq=all]"}
        self.assertEqual((await self.post(body))[0], 400)
        self.grant.started -= 61
        _, result = await self.post(body)
        self.assertEqual(result["status"], "sent")
        self.assertEqual(self.calls[0][1]["group_id"], 123456789)
        segments = self.calls[0][1]["message"]
        self.assertEqual(segments, [{"type": "text", "data": {"text": "working [CQ:at,qq=all]"}}])
        archived = query(self.bridge.archive(123456789).path)
        self.assertEqual(archived["total"], 1)
        self.assertTrue((await self.post(body))[1]["reused_result"])
        self.assertEqual(len(self.calls), 1)
        self.assertEqual((await self.post({**body, "key": "p2"}))[0], 400)
        self.assertEqual((await self.post({**body, "text": "changed"}))[0], 400)

    async def test_explicit_sends_limit_and_unknown_not_retried(self):
        async def timeout(group, text):
            self.calls.append(text)
            raise asyncio.TimeoutError()
        self.service.send = timeout
        body = {"op": "send", "reason": "requested", "key": "one", "text": "hi"}
        self.assertEqual((await self.post(body))[1]["status"], "unknown")
        self.assertEqual((await self.post(body))[1]["status"], "unknown")
        self.assertEqual(len(self.calls), 1)
        for key in ["two", "three"]:
            await self.post({**body, "key": key})
        self.assertEqual((await self.post({**body, "key": "four"}))[0], 400)

    async def test_one_shot_survives_session_end_restart_and_runs_once(self):
        result, body = await self.schedule()
        same = (await self.post(body))[1]
        self.assertEqual(result["id"], same["id"])
        self.bridge.uploads.end(self.token)
        self.restart()
        self.due(result["id"])
        await self.service.tick()
        await self.service.tick()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.service.rows("SELECT state FROM schedules")[0]["state"], "sent")
        self.restart()
        await self.service.tick()
        self.assertEqual(len(self.calls), 1)

    async def test_list_cancel_group_isolation_and_creation_limit(self):
        result, _ = await self.schedule()
        other = self.bridge.uploads.begin(Job(987654321, "other", 9, "hi", []))
        self.assertEqual((await self.post({"op": "list"}, other))[1]["schedules"], [])
        self.assertEqual((await self.post({"op": "cancel", "id": result["id"]}, other))[0], 400)
        self.assertEqual((await self.post({"op": "cancel", "id": result["id"]}))[1]["status"], "cancelled")
        self.due(result["id"])
        await self.service.tick()
        self.assertFalse(self.calls)
        await self.schedule("two")
        await self.schedule("three")
        status, _ = await self.post({"op": "schedule", "key": "four", "text": "x", "cron": "0 * * * *"})
        self.assertEqual(status, 400)

    async def test_recurring_uses_shanghai_and_skips_offline_backlog(self):
        result, _ = await self.schedule(cron="45 13 * * *")
        self.assertIn("13:45:00+08:00", result["next_run"])
        self.due(result["id"], seconds_ago=3600)
        self.bridge.connected.clear()
        await self.service.tick()
        self.assertFalse(self.calls)
        row = self.service.rows("SELECT * FROM schedules")[0]
        self.assertEqual(row["state"], "active")
        self.assertGreater(row["next_run"], time.time())
        self.assertEqual(json.loads(row["last_result"])["status"], "skipped_missed")
        self.bridge.connected.set()
        self.due(result["id"])
        await self.service.tick()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.service.rows("SELECT state FROM schedules")[0]["state"], "active")

    async def test_offline_grace_missed_once_and_revocation(self):
        first, _ = await self.schedule()
        missed, _ = await self.schedule("missed")
        self.bridge.connected.clear()
        self.due(first["id"], 10)
        self.due(missed["id"], 301)
        await self.service.tick()
        self.assertFalse(self.calls)
        self.bridge.connected.set()
        await self.service.tick()
        self.assertEqual(len(self.calls), 1)
        third, _ = await self.schedule("revoked")
        self.config.write_text(json.dumps({"trusted_groups": [987654321], "bot_qq": 12345}))
        await self.service.tick()
        row = self.service.rows("SELECT state FROM schedules WHERE id=?", (third["id"],))[0]
        self.assertEqual(row["state"], "cancelled")

    async def test_uncertain_crash_and_failed_recurring_jobs_pause(self):
        result, _ = await self.schedule(cron="*/5 * * * *")
        self.due(result["id"])
        async def rejected(group, text):
            raise OneBotError("send_group_msg", 1401)
        self.service.send = rejected
        await self.service.tick()
        self.assertEqual(self.service.rows("SELECT state FROM schedules")[0]["state"], "failed")
        second, _ = await self.schedule("crash")
        self.bridge.db.execute("UPDATE schedules SET state='sending' WHERE id=?", (second["id"],))
        self.bridge.db.commit()
        self.restart()
        await self.service.tick()
        self.assertFalse(self.calls)
        self.assertEqual(self.service.rows("SELECT state FROM schedules WHERE id=?", (second["id"],))[0]["state"], "unknown")

    async def test_cancel_concurrent_with_dispatch_and_group_rate_limit(self):
        first, _ = await self.schedule()
        second, _ = await self.schedule("second")
        self.due(first["id"], 2)
        self.due(second["id"], 1)
        started, release = asyncio.Event(), asyncio.Event()
        async def delayed(group, text):
            started.set()
            await release.wait()
            self.calls.append(text)
            return {"message_id": 1}
        self.service.send = delayed
        task = asyncio.create_task(self.service.tick())
        await started.wait()
        self.assertEqual((await self.post({"op": "cancel", "id": first["id"]}))[0], 400)
        await self.post({"op": "cancel", "id": second["id"]})
        release.set()
        await task
        self.assertEqual(len(self.calls), 1)
        for key in ["a", "b"]:
            await self.post({"op": "send", "key": key, "text": "hi", "reason": "requested"})
        self.assertEqual((await self.post({"op": "send", "key": "c", "text": "hi", "reason": "requested"}))[0], 400)

    async def test_real_cli_and_onebot_while_worker_busy(self):
        self.bridge.api = Bridge.api.__get__(self.bridge)
        self.bridge.connected.clear()
        headers = {"Authorization": "Bearer " + "t" * 40, "X-Self-ID": "12345"}
        async with self.client.ws_connect(self.url + "/onebot", headers=headers) as ws:
            login = await ws.receive_json(timeout=3)
            await ws.send_json({"status": "ok", "retcode": 0, "echo": login["echo"], "data": {"user_id": 12345}})
            await asyncio.wait_for(self.bridge.connected.wait(), 3)
            cap = self.grant.workspace.parent / ".upload-request.json"
            cap.write_text(json.dumps({"token": self.token, "url": self.url + "/agent/upload"}))
            env = {**os.environ, "QQ_UPLOAD_TOKEN": self.token, "QQ_UPLOAD_URL": self.url + "/agent/upload"}
            process = await asyncio.create_subprocess_exec(sys.executable, str(Path(__file__).with_name("messaging.py")),
                "schedule", "--key", "cli", "--at", datetime.fromtimestamp(time.time()+60, ZONE).isoformat(),
                "--text", "hello from test", cwd=self.grant.workspace, env=env,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            out, err = await asyncio.wait_for(process.communicate(), 5)
            self.assertEqual(process.returncode, 0, err)
            result = json.loads(out)
            started, release = asyncio.Event(), asyncio.Event()
            async def long_job(job):
                started.set()
                await release.wait()
                return "done"
            async def no_images(job):
                pass
            async def no_reply(job, text):
                pass
            self.bridge.runner = long_job
            self.bridge.prepare_images = no_images
            self.bridge.reply = no_reply
            worker = asyncio.create_task(self.bridge.worker())
            await self.bridge.queue.put(self.job)
            await started.wait()
            try:
                self.due(result["id"])
                tick = asyncio.create_task(self.service.tick())
                action = await ws.receive_json(timeout=3)
                self.assertEqual(action["action"], "send_group_msg")
                self.assertEqual(action["params"]["group_id"], 123456789)
                self.assertEqual(self.bridge.active, self.job.message_id)
                await ws.send_json({"status": "ok", "retcode": 0, "echo": action["echo"], "data": {"message_id": 123}})
                await tick
            finally:
                release.set()
                await self.bridge.queue.join()
                worker.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await worker

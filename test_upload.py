import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from aiohttp import ClientSession, web

from bot import Bridge, Job
from upload import OneBotError, UploadService, capability
from smoke_upload import verify


class UploadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.config = self.root / "config.json"
        self.config.write_text(json.dumps({"trusted_groups": [123456789], "bot_qq": 12345, "history_sync": False}))
        self.bridge = Bridge(self.config, self.root, "t" * 40)
        self.job = Job(123456789, "upload-test", 8, "upload hello.txt", [])
        self.token = self.bridge.uploads.begin(self.job)
        self.workspace = self.root / "groups/123456789/workspace"
        self.file = self.workspace / "hello.txt"
        self.file.write_text("Hello from qq-dsh-bot!\n")
        app = web.Application()
        app.router.add_post("/agent/upload", self.bridge.uploads.handle)
        app.router.add_get("/transfers/{token}", self.bridge.uploads.download)
        app.router.add_get("/onebot", self.bridge.socket)
        async def qq_file(request):
            return web.Response(body=b"Hello from qq-dsh-bot!\n")
        app.router.add_get("/qq-file", qq_file)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        self.bridge.uploads.transfer_base = self.url
        self.client = ClientSession()

    async def asyncTearDown(self):
        if self.bridge.ws:
            await self.bridge.ws.close()
        await self.client.close()
        await self.runner.cleanup()
        self.bridge.db.close()
        self.tmp.cleanup()

    async def post(self, data=None, token=None):
        response = await self.client.post(self.url + "/agent/upload", json=data or {"file_path": str(self.file)},
                                          headers={"Authorization": "Bearer " + (self.token if token is None else token)})
        return response.status, await response.json()

    async def test_real_websocket_fetch_and_helper_upload_then_deduplicate(self):
        headers = {"Authorization": "Bearer " + "t" * 40, "X-Self-ID": "12345"}
        async with self.client.ws_connect(self.url + "/onebot", headers=headers) as ws:
            login = await ws.receive_json(timeout=3)
            await ws.send_json({"status": "ok", "retcode": 0, "echo": login["echo"], "data": {"user_id": 12345}})
            await asyncio.wait_for(self.bridge.connected.wait(), 3)
            env = {**os.environ, "QQ_UPLOAD_TOKEN": self.token, "QQ_UPLOAD_URL": self.url + "/agent/upload"}
            process = await asyncio.create_subprocess_exec(sys.executable, str(Path(__file__).with_name("upload.py")), str(self.file),
                                                            env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            action = await ws.receive_json(timeout=3)
            self.assertEqual(action["action"], "upload_group_file")
            self.assertEqual(action["params"]["group_id"], "123456789")
            async with self.client.get(action["params"]["file"]) as download:
                self.assertEqual(await download.read(), self.file.read_bytes())
            await ws.send_json({"status": "ok", "retcode": 0, "echo": action["echo"], "data": {"file_id": "qq-file-id"}})
            stdout, stderr = await asyncio.wait_for(process.communicate(), 3)
            self.assertEqual(process.returncode, 0, stderr)
            self.assertEqual(json.loads(stdout)["status"], "uploaded")
            _, duplicate = await self.post()
            self.assertTrue(duplicate["reused_result"])
            self.assertEqual(duplicate["file_id"], "qq-file-id")
            self.assertEqual((await self.client.get(action["params"]["file"])).status, 404)

    async def test_scope_revocation_wrong_group_and_symlink_escape(self):
        self.assertEqual((await self.post(token="wrong"))[0], 403)
        self.assertEqual((await self.post({"file_path": str(self.file), "group_id": 999}))[0], 400)
        outside = self.root / "secret.txt"
        outside.write_text("not for upload")
        (self.workspace / "escape.txt").symlink_to(outside)
        for path in [outside, self.workspace / "escape.txt", self.workspace]:
            self.assertEqual((await self.post({"file_path": str(path)}))[0], 400)
        self.bridge.uploads.end(self.token)
        self.assertEqual((await self.post())[0], 403)
        self.token = self.bridge.uploads.begin(self.job)
        self.config.write_text(json.dumps({"trusted_groups": [999], "bot_qq": 12345}))
        self.assertEqual((await self.post())[0], 403)

    async def test_size_filename_and_empty_file(self):
        with patch("upload.MAX_BYTES", 5):
            self.assertEqual((await self.post())[0], 400)
        for name in ["../bad.txt", "bad\nname.txt", "x" * 201]:
            self.assertEqual((await self.post({"file_path": str(self.file), "name": name}))[0], 400)
        self.file.write_bytes(b"")
        self.assertEqual((await self.post())[0], 400)

    async def test_failure_and_timeout_do_not_claim_success_or_retry(self):
        calls = []
        async def api(action, params, timeout):
            calls.append(action)
            raise asyncio.TimeoutError()
        self.bridge.api = api
        _, result = await self.post()
        self.assertEqual(result["status"], "unknown")
        _, duplicate = await self.post()
        self.assertTrue(duplicate["reused_result"])
        self.assertEqual(len(calls), 1)
        self.file.write_text("A different file")
        async def denied(action, params, timeout):
            raise OneBotError(action, 1401)
        self.bridge.api = denied
        _, result = await self.post()
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["retcode"], 1401)

    async def test_missing_file_id_is_unknown_and_quota_is_enforced(self):
        async def api(action, params, timeout):
            return {"file_id": None}
        self.bridge.api = api
        for i in range(3):
            self.file.write_text(str(i))
            self.assertEqual((await self.post())[1]["status"], "unknown")
        self.file.write_text("fourth")
        self.assertEqual((await self.post())[0], 429)
        self.bridge.uploads.transfers["expired"] = (0, b"gone")
        self.assertEqual((await self.client.get(self.url + "/transfers/expired")).status, 404)

    async def test_restart_marks_inflight_upload_unknown(self):
        self.bridge.uploads.result((123456789, "x", "digest", "hello.txt"), {"status": "uploading"})
        UploadService(self.bridge.db, self.root, self.bridge.trusted, self.bridge.api)
        result = self.bridge.db.execute("SELECT result FROM uploads WHERE message_id='x'").fetchone()[0]
        self.assertEqual(json.loads(result)["status"], "unknown")

    async def test_worker_grants_only_during_dsh_run(self):
        tokens = []
        async def runner(job):
            tokens.append(job.upload_token)
            self.assertIn(job.upload_token, self.bridge.uploads.grants)
            return "done"
        async def reply(job, text):
            self.assertEqual(job.upload_token, "")
            self.assertNotIn(tokens[-1], self.bridge.uploads.grants)
        self.bridge.runner, self.bridge.reply = runner, reply
        worker = asyncio.create_task(self.bridge.worker())
        try:
            self.bridge.queue.put_nowait(self.job)
            await asyncio.wait_for(self.bridge.queue.join(), 3)
            self.assertEqual(len(tokens), 1)
        finally:
            worker.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await worker

    async def test_helper_finds_group_capability_when_shell_scrubs_environment(self):
        path = self.workspace.parent / ".upload-request.json"
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        with patch.dict(os.environ, {}, clear=True), patch("upload.current_archive", return_value=self.workspace.parent / "history.sqlite3"):
            self.assertEqual(capability()["token"], self.token)
            self.bridge.uploads.end(self.token)
            self.assertFalse(path.exists())
            self.assertEqual(capability(), {})

    async def test_live_verification_handles_opaque_listing_ids_and_clock_skew(self):
        calls = []
        timestamp = 1180
        async def api(action, params, timeout):
            calls.append(action)
            if action == "get_group_root_files":
                return {"files": [{"file_id": "opaque-listing-id", "file_name": "hello.txt", "file_size": 23, "uploader": 12345, "upload_time": timestamp}]}
            self.assertEqual(params["file_id"], "opaque-listing-id")
            return {"url": self.url + "/qq-file"}
        self.bridge.api = api
        await verify(self.bridge, 123456789, {"file_id": "/raw-qq-id"}, 1000)
        self.assertEqual(calls, ["get_group_root_files", "get_group_file_url"])
        timestamp = 0
        await verify(self.bridge, 123456789, {"file_id": "/raw-qq-id"}, 1000)


if __name__ == "__main__":
    unittest.main()

import io
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

from aiohttp import web
from PIL import Image
from yarl import URL

from bot import Bridge, prompt_for
from history import History, query
from media import ImageCache, ImageDNSRejected, PublicResolver, checked_url, download_image, image_extension


def picture():
    buffer = io.BytesIO()
    Image.new("RGB", (32, 24), "orange").save(buffer, "PNG")
    return buffer.getvalue()


def message(mid="11", group=123456789, url="https://gchat.qpic.cn/example"):
    return {"message_id": mid, "group_id": group, "user_id": 8, "self_id": 12345,
            "post_type": "message", "message_type": "group", "time": time.time(),
            "message": [{"type": "image", "data": {"file": "example.png", "url": url}},
                        {"type": "at", "data": {"qq": "12345"}}]}


class MediaTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.archive = History(self.root / "groups" / "123456789" / "history.sqlite3")

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def test_image_bytes_persist_and_appear_in_history_and_recent_context(self):
        self.archive.save(message())
        cache = ImageCache(AsyncMock(), lambda g: True)
        with patch("media.download_image", AsyncMock(return_value=picture())) as download:
            await cache.cache_message(self.archive, 123456789, "11")
            await cache.cache_message(self.archive, 123456789, "11")
            self.assertEqual(download.await_count, 1)
        reopened = History(self.archive.path)
        image = query(reopened.path, message_id="11")["messages"][0]["images"][0]
        self.assertEqual(image["status"], "ready")
        self.assertEqual(Path(image["file_path"]).read_bytes(), picture())
        self.assertEqual(reopened.recent()[0]["images"][0], image)
        self.assertNotIn("qpic", json.dumps(image))

    async def test_expired_url_refreshed_and_cross_group_response_rejected(self):
        self.archive.save(message())
        api = AsyncMock(return_value=message(url="https://gchat.qpic.cn/fresh"))
        cache = ImageCache(api, lambda g: True)
        with patch("media.download_image", AsyncMock(side_effect=[ValueError(), picture()])):
            await cache.cache_message(self.archive, 123456789, "11")
        api.assert_awaited_once_with("get_msg", {"message_id": "11"})
        self.assertEqual(self.archive.images("11")[0]["status"], "ready")
        self.archive.save(message("12", url=""))
        api.return_value = message("12", group=999)
        with patch("media.download_image", AsyncMock()) as download:
            await cache.cache_message(self.archive, 123456789, "12")
            download.assert_not_awaited()
        self.assertEqual(self.archive.images("12")[0]["status"], "unavailable")

    async def test_dns_failure_is_reported_even_when_message_refresh_fails(self):
        self.archive.save(message())
        cache = ImageCache(AsyncMock(side_effect=RuntimeError("secret signed URL")), lambda g: True)
        with patch("media.download_image", AsyncMock(side_effect=ImageDNSRejected("private IP"))):
            with self.assertLogs("qq-dsh", level="WARNING") as logs:
                await cache.cache_message(self.archive, 123456789, "11")
        self.assertIn("initial_download_kind=ImageDNSRejected", logs.output[0])
        image = self.archive.images("11")[0]
        self.assertIn("DNS", image["detail"])
        self.assertNotIn("secret", image["detail"] + logs.output[0])

    async def test_public_resolver_rejects_fake_ips_and_private_addresses(self):
        resolver = PublicResolver()
        try:
            for addresses in [[], [{"host": "198.18.0.15"}], [{"host": "127.0.0.1"}],
                              [{"host": "10.0.0.1"}], [{"host": "::1"}],
                              [{"host": "1.1.1.1"}, {"host": "192.168.1.1"}]]:
                with patch("media.ThreadedResolver.resolve", AsyncMock(return_value=addresses)):
                    with self.assertRaises(ImageDNSRejected):
                        await resolver.resolve("multimedia.nt.qq.com.cn", 443)
            addresses = [{"host": "1.1.1.1"}, {"host": "2606:4700:4700::1111"}]
            with patch("media.ThreadedResolver.resolve", AsyncMock(return_value=addresses)):
                self.assertEqual(await resolver.resolve("multimedia.nt.qq.com.cn", 443), addresses)
        finally:
            await resolver.close()

    async def test_trigger_and_quoted_old_image_reach_dsh_prompt(self):
        config = self.root / "config.json"
        config.write_text(json.dumps({"trusted_groups": [123456789], "bot_qq": 12345}))
        bridge = Bridge(config, self.root, "t" * 40)
        try:
            old = message("10")
            old["time"] -= 86400
            bridge.archive(123456789).save(old)
            for i in range(31):
                m = message(str(100 + i))
                m["message"] = [{"type": "text", "data": {"text": "later"}}]
                bridge.archive(123456789).save(m)
            current = message()
            current["message"].append({"type": "reply", "data": {"id": "10"}})
            await bridge.event(current)
            job = bridge.queue.get_nowait()
            self.assertNotIn("10", [m["message_id"] for m in job.context])
            with patch("media.download_image", AsyncMock(return_value=picture())):
                await bridge.prepare_images(job)
            bridge.refresh_images(job)
            self.assertEqual(job.images[0]["status"], "ready")
            quoted = next(m for m in job.context if m["message_id"] == "10")
            self.assertTrue(quoted["quoted"])
            self.assertEqual(quoted["images"][0]["status"], "ready")
            self.assertIn(job.images[0]["file_path"], prompt_for(job))
            self.assertIn("MUST call read_image", prompt_for(job))
        finally:
            bridge.db.close()

    async def test_separate_image_survives_waiting_text_only_request(self):
        config = self.root / "config.json"
        config.write_text(json.dumps({"trusted_groups": [123456789], "bot_qq": 12345, "history_sync": False}))
        bridge = Bridge(config, self.root, "t" * 40)
        try:
            first = message("1")
            first["user_id"] = 9
            first["message"][0] = {"type": "text", "data": {"text": "reply something"}}
            await bridge.event(first)
            image = message("2")
            image["message"] = image["message"][:1]
            await bridge.event(image)
            request = message("3")
            request["message"][0] = {"type": "text", "data": {"text": "what is in this image?"}}
            await bridge.event(request)
            bridge.queue.get_nowait()
            job = bridge.queue.get_nowait()
            for mid in range(100, 205):
                later = message(str(mid))
                later["message"] = [{"type": "text", "data": {"text": "later conversation"}}]
                await bridge.event(later)
            await bridge.prepare_transcriptions(job)
            with patch("media.download_image", AsyncMock(return_value=picture())):
                await bridge.prepare_images(job)
            bridge.refresh_images(job)
            self.assertEqual(job.images, [])
            self.assertEqual([row["message_id"] for row in job.context], ["1", "2"])
            attachment = job.context[1]["images"][0]
            self.assertEqual(attachment["status"], "ready")
            prompt = prompt_for(job)
            self.assertIn(attachment["file_path"], prompt)
            self.assertIn('"recent_images": [{"message_id": "2", "sender_id": 8', prompt)
            self.assertIn("A text-only @mention can refer to an image", prompt)
        finally:
            bridge.db.close()

    async def test_download_validation_size_limit_and_url_boundaries(self):
        for url in ["http://127.0.0.1/private", "file:///etc/passwd", "https://qq.com.evil.test/a", "https://user:pass@gchat.qpic.cn/a", "https://qq.com:8080/a"]:
            with self.assertRaises(ValueError):
                checked_url(url)
        self.assertEqual(checked_url("https://gchat.qpic.cn/a?key=x").host, "gchat.qpic.cn")
        self.assertEqual(checked_url("https://multimedia.nt.qq.com.cn/download?appid=1407").host, "multimedia.nt.qq.com.cn")
        with self.assertRaises(Exception):
            image_extension(b"not an image")
        app = web.Application()
        async def response(request):
            return web.Response(body=picture())
        app.router.add_get("/image", response)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            with patch("media.checked_url", lambda x: URL(x)):
                self.assertEqual(await download_image(f"http://127.0.0.1:{port}/image"), picture())
                with patch("media.MAX_BYTES", 10):
                    with self.assertRaises(ValueError):
                        await download_image(f"http://127.0.0.1:{port}/image")
        finally:
            await runner.cleanup()

    async def test_existing_archive_images_indexed_once_and_removed_trust_stops_fetch(self):
        self.archive.save(message())
        with self.archive.connection() as db:
            db.execute("DELETE FROM images")
            db.execute("DELETE FROM metadata WHERE key='images_indexed'")
        reopened = History(self.archive.path)
        self.assertEqual(reopened.pending_images(), ["11"])
        cache = ImageCache(AsyncMock(), lambda g: False)
        with patch("media.download_image", AsyncMock()) as download:
            await cache.cache_message(reopened, 123456789, "11")
            download.assert_not_awaited()
        self.assertEqual(reopened.images("11")[0]["status"], "pending")


if __name__ == "__main__":
    unittest.main()

import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

from aiohttp import ClientSession, web
from PIL import Image

from bot import Bridge, Job
from send_image import ImageSendService
from upload import OneBotError


class ImageSendTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.config = self.root / 'config.json'
        self.config.write_text(json.dumps({'trusted_groups': [123456789], 'bot_qq': 12345,
                                           'gateway': 'snowluma', 'history_sync': False}))
        self.bridge = Bridge(self.config, self.root, 't' * 40)
        self.job = Job(123456789, 'snowluma:123', 8, 'draw a plot', [])
        self.token = self.bridge.uploads.begin(self.job)
        self.file = self.bridge.uploads.grants[self.token].workspace / 'plot.png'
        Image.new('RGB', (10, 10), 'blue').save(self.file)
        app = web.Application()
        app.router.add_post('/agent/send-image', self.bridge.image_sends.handle)
        app.router.add_get('/transfers/{token}', self.bridge.uploads.download)
        app.router.add_get('/onebot', self.bridge.socket)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, '127.0.0.1', 0)
        await site.start()
        self.url = f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
        self.bridge.uploads.transfer_base = self.url
        self.client = ClientSession()

    async def asyncTearDown(self):
        if self.bridge.ws:
            await self.bridge.ws.close()
        await self.client.close()
        await self.runner.cleanup()
        self.bridge.db.close()
        self.tmp.cleanup()

    async def post(self, body=None, token=None):
        async with self.client.post(self.url + '/agent/send-image',
                json=body if body is not None else {'file_path': str(self.file), 'caption': 'A plot'},
                headers={'Authorization': 'Bearer ' + (self.token if token is None else token)}) as response:
            return response.status, await response.json()

    async def test_cli_websocket_transfer_confirmation_archive_and_dedup(self):
        async with self.client.ws_connect(self.url + '/onebot',
                headers={'Authorization': 'Bearer ' + 't' * 40, 'X-Self-ID': '12345'}) as ws:
            login = await ws.receive_json(timeout=3)
            await ws.send_json({'status': 'ok', 'retcode': 0, 'echo': login['echo'], 'data': {'user_id': 12345}})
            await asyncio.wait_for(self.bridge.connected.wait(), 3)
            process = await asyncio.create_subprocess_exec(sys.executable,
                str(Path(__file__).with_name('send_image.py')), str(self.file), '--caption', 'A plot',
                env={**os.environ, 'QQ_UPLOAD_TOKEN': self.token, 'QQ_UPLOAD_URL': self.url + '/agent/upload'},
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            action = await ws.receive_json(timeout=3)
            self.assertEqual(action['action'], 'send_group_msg')
            self.assertEqual(action['params']['group_id'], 123456789)
            self.assertEqual(action['params']['message'][0], {'type': 'text', 'data': {'text': 'A plot'}})
            image = action['params']['message'][1]
            self.assertEqual(image['type'], 'image')
            async with self.client.get(image['data']['file']) as download:
                self.assertEqual(await download.read(), self.file.read_bytes())
            await ws.send_json({'status': 'ok', 'retcode': 0, 'echo': action['echo'], 'data': {'message_id': -1234}})
            stdout, stderr = await asyncio.wait_for(process.communicate(), 3)
            self.assertEqual(process.returncode, 0, stderr)
            result = json.loads(stdout)
            self.assertEqual(result['status'], 'sent')
            self.assertEqual(result['message_id'], 'snowluma:-1234')
            archive = self.bridge.archive(123456789)
            self.assertIn('A plot', archive.message_text(result['message_id']))
            saved = archive.images(result['message_id'])[0]
            self.assertEqual(saved['status'], 'ready')
            self.assertEqual(Path(saved['file_path']).read_bytes(), self.file.read_bytes())
            self.assertNotIn('/transfers/', json.dumps(archive.segments(result['message_id'])))
            renamed = self.file.with_name('renamed.png')
            renamed.write_bytes(self.file.read_bytes())
            _, duplicate = await self.post({'file_path': str(renamed), 'caption': 'A plot'})
            self.assertTrue(duplicate['reused_result'])
            self.assertEqual(duplicate['message_id'], result['message_id'])
            self.assertEqual((await self.client.get(image['data']['file'])).status, 404)

    async def test_invalid_image_scope_and_revocation(self):
        self.assertEqual((await self.post(token='wrong'))[0], 403)
        self.assertEqual((await self.post({'file_path': str(self.file), 'group_id': 999}))[0], 400)
        outside = self.root / 'secret.png'
        outside.write_bytes(self.file.read_bytes())
        link = self.file.with_name('link.png')
        link.symlink_to(outside)
        for path in (outside, link, self.file.parent):
            self.assertEqual((await self.post({'file_path': str(path)}))[0], 400)
        self.assertEqual((await self.post({'file_path': str(self.file), 'caption': 'a' * 1001}))[0], 400)
        self.file.write_bytes(b'not actually a png')
        self.assertEqual((await self.post())[0], 400)
        self.bridge.uploads.end(self.token)
        self.assertEqual((await self.post())[0], 403)
        self.token = self.bridge.uploads.begin(self.job)
        self.config.write_text(json.dumps({'trusted_groups': [999], 'bot_qq': 12345}))
        self.assertEqual((await self.post())[0], 403)

    async def test_unknown_failure_quota_and_restart(self):
        calls = []
        self.assertEqual((await self.post())[1]['status'], 'unavailable')
        self.bridge.connected.set()
        async def api(action, params, **kwargs):
            calls.append(action)
            raise asyncio.TimeoutError()
        self.bridge.api = api
        self.assertEqual((await self.post())[1]['status'], 'unknown')
        self.assertTrue((await self.post())[1]['reused_result'])
        ImageSendService(self.bridge)
        self.assertTrue((await self.post())[1]['reused_result'])
        self.assertEqual(calls, ['send_group_msg'])
        async def rejected(action, params, **kwargs):
            raise OneBotError(action, 1401)
        self.bridge.api = rejected
        result = (await self.post({'file_path': str(self.file), 'caption': 'Another plot'}))[1]
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['retcode'], 1401)
        async def unconfirmed(action, params, **kwargs):
            return {}
        self.bridge.api = unconfirmed
        self.assertEqual((await self.post({'file_path': str(self.file)}))[1]['status'], 'unknown')
        self.assertEqual((await self.post({'file_path': str(self.file), 'caption': 'fourth'}))[0], 429)
        key = (123456789, 'interrupted', 'digest')
        self.bridge.image_sends.save(key, {'status': 'sending'})
        ImageSendService(self.bridge)
        row = self.bridge.db.execute('SELECT result FROM image_sends WHERE request_id=?', ('interrupted',)).fetchone()
        self.assertEqual(json.loads(row[0])['status'], 'unknown')


if __name__ == '__main__':
    unittest.main()

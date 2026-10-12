import asyncio
import hashlib
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from aiohttp import ClientSession, web
from attachments import checked_download_url
from bot import Bridge, Job, prompt_for
from history import query


class AttachmentTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve()
        self.config = self.root / 'config.json'
        self.config.write_text(json.dumps({'bot_qq': 12345, 'trusted_groups': [123, 456],
            'memory_managers': {'123': [8]}, 'history_sync': False}))
        self.bridge = Bridge(self.config, self.root, 'x' * 40)
        self.job = Job(123, 'request', 8, 'read the file', [])
        self.token = self.bridge.uploads.begin(self.job)
        self.calls = []
        async def api(action, params, **kwargs):
            self.calls.append((action, params))
            return {'url': 'https://example.com/qq-file'}
        self.bridge.api = api
        app = web.Application()
        app.router.add_post('/agent/attachments', self.bridge.attachments.handle)
        app.router.add_post('/agent/skills', self.bridge.skills.handle)
        self.server = web.AppRunner(app)
        await self.server.setup()
        site = web.TCPSite(self.server, '127.0.0.1', 0)
        await site.start()
        self.url = f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/agent/'
        self.client = ClientSession()

    async def asyncTearDown(self):
        await self.client.close()
        await self.server.cleanup()
        await asyncio.gather(*self.bridge.receipt_tasks, return_exceptions=True)
        self.bridge.db.close()
        self.tmp.cleanup()

    async def post(self, endpoint, body, token=None):
        async with self.client.post(self.url + endpoint, json=body,
            headers={'Authorization': 'Bearer ' + (self.token if token is None else token)}) as response:
            return response.status, await response.json()

    def save_file(self, group=123, mid='file-1', size=5):
        self.bridge.archive(group).save({'message_id': mid, 'user_id': 8, 'time': time.time(),
            'message': [{'type': 'file', 'data': {'file_id': 'qq-file-id', 'file_name': '../hello.txt', 'file_size': size}}]})

    async def test_download_cache_group_scope_and_revoked_capabilities(self):
        self.save_file()
        async def download(url, target):
            target.write_bytes(b'hello')
            return hashlib.sha256(b'hello').hexdigest(), 5
        with patch('attachments.download_file', side_effect=download) as fetch:
            code, result = await self.post('attachments', {'op': 'download', 'message_id': 'file-1'})
            self.assertEqual(code, 200)
            self.assertEqual(Path(result['file_path']).read_bytes(), b'hello')
            self.assertTrue(Path(result['file_path']).is_relative_to(self.root / 'groups/123/attachments'))
            self.assertEqual(self.calls, [('get_group_file_url', {'group_id': '123', 'file_id': 'qq-file-id'})])
            self.assertEqual((await self.post('attachments', {'op': 'download', 'message_id': 'file-1'}))[0], 200)
            self.assertEqual(fetch.call_count, 1)
        self.assertEqual(query(self.bridge.archive(123).path, message_id='file-1')['messages'][0]['files'][0]['status'], 'ready')
        other = self.bridge.uploads.begin(Job(456, 'other', 8, 'read', []))
        self.assertEqual((await self.post('attachments', {'op': 'download', 'message_id': 'file-1'}, other))[0], 400)
        self.assertEqual((await self.post('attachments', {'op': 'list', 'group_id': 456}))[0], 400)
        self.bridge.uploads.end(self.token)
        self.assertEqual((await self.post('attachments', {'op': 'list'}))[0], 403)

    async def test_limits_failed_download_cleanup_and_expired_grant(self):
        self.save_file(size=100000000)
        self.assertEqual((await self.post('attachments', {'op': 'download', 'message_id': 'file-1'}))[0], 400)
        self.assertEqual(self.calls, [])
        self.save_file(mid='small')
        async def revoked(url, target):
            target.write_bytes(b'hello')
            self.bridge.uploads.end(self.token)
            return 'digest', 5
        with patch('attachments.download_file', side_effect=revoked):
            self.assertEqual((await self.post('attachments', {'op': 'download', 'message_id': 'small'}))[0], 403)
        self.assertEqual(list((self.root / 'groups/123/attachments').iterdir()), [])

    async def test_upload_notice_and_preceding_message_context(self):
        data = {'post_type': 'notice', 'notice_type': 'group_upload', 'self_id': 12345,
            'group_id': 123, 'user_id': 8, 'time': time.time(), 'file': {'id': 'qq-notice-file', 'name': 'bundle.zip', 'size': 8}}
        await self.bridge.event(data)
        await self.bridge.event(data)
        self.assertTrue(self.bridge.queue_for(123).empty())
        rows = self.bridge.archive(123).recent()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['files'][0]['name'], 'bundle.zip')
        await self.bridge.event({'post_type': 'message', 'message_type': 'group', 'self_id': 12345,
            'group_id': 123, 'user_id': 8, 'time': time.time(), 'message_id': 'mention',
            'message': [{'type': 'at', 'data': {'qq': '12345'}}, {'type': 'text', 'data': {'text': '看看上面的文件'}}]})
        job = self.bridge.queue_for(123).get_nowait()
        self.assertEqual(job.context[0]['files'][0]['name'], 'bundle.zip')
        self.assertIn('recent_files', prompt_for(job))
        data['group_id'] = 999
        await self.bridge.event(data)
        self.assertNotIn(999, self.bridge.archives)

    async def test_skill_permissions_and_fresh_run_catalogs(self):
        workspace = self.bridge.uploads.grants[self.token].workspace
        source = workspace / 'SKILL.md'
        source.write_text('---\nname: test-skill\ndescription: A test skill.\n---\nUse Python to add numbers.\n')
        body = {'op': 'install', 'path': str(source), 'key': 'install'}
        outsider = self.bridge.uploads.begin(Job(123, 'member', 9, 'install', []))
        self.assertEqual((await self.post('skills', body, outsider))[0], 403)
        code, result = await self.post('skills', body)
        self.assertEqual(code, 200)
        revision = result['skill']['revision']
        run = self.root / 'groups/123/foreground/1234567890abcdef'
        p = self.bridge.skills.store(123).prepare(run)
        self.assertEqual(json.loads(p.read_text())[0]['config']['includeDefaultRoots'], False)
        catalog = run / 'workspace/.group-skills/test-skill/SKILL.md'
        self.assertTrue(catalog.exists())
        other = self.bridge.uploads.begin(Job(456, 'other', 8, 'list', []))
        self.assertEqual((await self.post('skills', {'op': 'list'}, other))[1]['skills'], [])
        background = self.bridge.uploads.begin(Job(123, 'bg', 8, 'install', [], background=True,
            run_root=self.root / 'background'))
        self.assertEqual((await self.post('skills', body, background))[0], 403)
        self.assertEqual((await self.post('skills', {'op': 'remove', 'name': 'test-skill', 'revision': revision, 'key': 'remove'}))[0], 200)
        # Running tasks keep their copy; new runs no longer discover the removed skill.
        self.assertTrue(catalog.exists())
        self.bridge.skills.store(123).prepare(run)
        self.assertFalse(catalog.exists())

    def test_download_url_rejects_private_ip_credentials_and_non_http(self):
        for url in ['http://127.0.0.1/a', 'http://169.254.169.254/a', 'file:///etc/passwd',
                    'https://user:pass@example.com/', 'http://[::1]/a', 'https://example.com:8080/a']:
            with self.assertRaises(ValueError):
                checked_download_url(url)


if __name__ == '__main__':
    unittest.main()

import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from aiohttp import ClientSession, web
from bot import Bridge, Job, load_config, prompt_for
from memory import MemoryStore, sensitive
from upload import Grant


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = MemoryStore(self.root / 'memory')
        self.grant = Grant(123, 'message-1', self.root, sender=8)

    def tearDown(self):
        self.tmp.cleanup()

    def add(self, text='本群不要说“xxx”', **kwargs):
        return self.store.mutate(self.grant, dict(op='remember', scope='group', text=text, key='rule', **kwargs), True)

    def test_persistence_markdown_and_audited_update_forget(self):
        entry = self.add()['entry']
        self.assertEqual(entry['source'], 'message-1')
        self.assertEqual(entry['author'], 8)
        self.assertIn('本群不要说', (self.root / 'memory/MEMORY.md').read_text())
        restarted = MemoryStore(self.root / 'memory')
        self.assertIn('xxx', restarted.snapshot(99)['core'])
        changed = restarted.mutate(self.grant, dict(op='update', id=entry['id'], revision=1, text='本群避免这个词', key='edit'), True)
        self.assertEqual(changed['entry']['revision'], 2)
        with self.assertRaisesRegex(ValueError, 'Revision conflict'):
            restarted.mutate(self.grant, dict(op='update', id=entry['id'], revision=1, text='stale', key='stale'), True)
        restarted.mutate(self.grant, dict(op='forget', id=entry['id'], revision=2, key='delete'), True)
        self.assertEqual(restarted.entries(), [])
        journal = [json.loads(line) for line in (self.root / 'memory/journal.jsonl').read_text().splitlines()]
        self.assertEqual([row['op'] for row in journal], ['remember', 'update', 'forget'])
        self.assertEqual(journal[-1]['before']['text'], '本群避免这个词')
        self.assertNotIn('本群避免', restarted.snapshot(8)['core'])

    def test_idempotency_dedup_and_key_reuse(self):
        first = self.add()
        self.assertTrue(self.add()['reused_result'])
        self.grant.message_id = 'message-2'
        self.assertEqual(self.add()['status'], 'unchanged')
        with self.assertRaisesRegex(ValueError, 'different arguments'):
            self.add('different')
        self.assertEqual(len(self.store.entries()), 1)
        self.assertEqual(self.store.entries()[0]['id'], first['entry']['id'])

    def test_member_preferences_are_attributed_and_do_not_enter_group_core(self):
        result = self.store.mutate(self.grant, dict(op='remember', text='我喜欢 C++', key='personal'), False)
        self.assertEqual(result['entry']['subject'], 8)
        self.assertEqual(result['entry']['topic'], 'members')
        self.assertNotIn('我喜欢', self.store.snapshot(8)['core'])
        self.assertEqual(len(self.store.snapshot(8)['personal']), 1)
        self.assertEqual(self.store.snapshot(9)['personal'], [])
        stranger = Grant(123, 'message-2', self.root, sender=9)
        with self.assertRaises(PermissionError):
            self.store.mutate(stranger, dict(op='forget', id=result['entry']['id'], revision=1, key='bad'), False)
        with self.assertRaises(PermissionError):
            self.store.mutate(self.grant, dict(op='remember', scope='group', text='group rule', key='bad2'), False)

    def test_topics_search_pagination_and_empty_topic_removal(self):
        entry = self.add('我们的项目是机器人', topic='projects')['entry']
        self.assertIn('[[projects]]', self.store.snapshot(8)['core'])
        self.assertNotIn('我们的项目', self.store.snapshot(8)['core'])
        self.assertEqual(self.store.recall({'query': '项目'})['total'], 1)
        self.assertEqual(self.store.recall({'topic': 'members'})['total'], 0)
        self.store.mutate(self.grant, dict(op='forget', id=entry['id'], revision=1, key='delete'), True)
        self.assertFalse((self.root / 'memory/topics/projects.md').exists())
        for i in range(31):
            self.grant.message_id = f'new-{i}'
            self.add('项目事项 ' + str(i), topic='projects')
        page = self.store.recall({'topic': 'projects'})
        self.assertEqual(page['next_offset'], 30)
        self.assertEqual(len(self.store.recall({'offset': 30})['entries']), 1)

    def test_oversize_and_secret_writes_leave_no_entries_or_journal(self):
        for text in ['x' * 301, 'sk-' + 'a' * 32, '密码是 secret123', 'token: abc123']:
            with self.assertRaises(ValueError):
                self.add(text)
        self.assertEqual(self.store.entries(), [])
        self.assertEqual((self.root / 'memory/journal.jsonl').read_text(), '')
        self.assertFalse(sensitive('使用密码管理器，项目网址 https://example.com/abc'))
        with patch.dict('os.environ', {'TEST_API_KEY': 'a-specific-secret-value'}):
            self.assertTrue(sensitive('保存 a-specific-secret-value'))

    def test_capacity_rolls_back_and_keeps_existing_memory(self):
        self.add()
        self.grant.message_id = 'message-2'
        with patch('memory.MAIN_LIMIT', 50):
            with self.assertRaisesRegex(ValueError, 'full'):
                self.add('new rule')
        self.assertEqual(len(self.store.entries()), 1)

    def test_committed_memory_survives_failed_markdown_export(self):
        with patch.object(self.store, 'export', side_effect=OSError('disk')):
            with self.assertRaises(OSError):
                self.add()
        restarted = MemoryStore(self.root / 'memory')
        self.assertIn('xxx', (self.root / 'memory/MEMORY.md').read_text())
        self.assertEqual(len(restarted.entries()), 1)
        self.assertTrue(self.add()['reused_result'])


class MemoryBridgeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.config = self.root / 'config.json'
        self.config.write_text(json.dumps(dict(trusted_groups=[123, 456], bot_qq=12345, history_sync=False)))
        self.bridge = Bridge(self.config, self.root, 't' * 40)
        self.job = Job(123, 'message-1', 8, '记住：本群不要说 xxx', [])
        self.token = self.bridge.uploads.begin(self.job)
        self.calls = []
        async def api(action, params, **kwargs):
            self.calls.append((action, params))
            return dict(group_id=params['group_id'], user_id=params['user_id'], role='admin')
        self.bridge.api = api
        app = web.Application()
        app.router.add_post('/agent/memory', self.bridge.memory.handle)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, '127.0.0.1', 0)
        await site.start()
        self.url = f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/agent/memory'
        self.client = ClientSession()

    async def asyncTearDown(self):
        await self.client.close()
        await self.runner.cleanup()
        self.bridge.db.close()
        self.tmp.cleanup()

    async def post(self, body, token=None):
        async with self.client.post(self.url, json=body, headers={'Authorization': 'Bearer ' + (self.token if token is None else token)}) as response:
            return response.status, await response.json()

    async def remember(self, **extra):
        return await self.post(dict(op='remember', scope='group', text='本群不要说 xxx', key='one', **extra))

    async def test_group_scope_auth_and_provenance(self):
        code, result = await self.remember()
        self.assertEqual(code, 200)
        self.assertEqual(result['entry']['author'], 8)
        self.assertEqual(result['entry']['source'], 'message-1')
        other_token = self.bridge.uploads.begin(Job(456, 'message-2', 8, 'read', []))
        _, other = await self.post({'op': 'recall'}, other_token)
        self.assertEqual(other['entries'], [])
        self.assertEqual((await self.remember(group=456))[0], 400)
        self.assertEqual((await self.post({'op': 'recall'}, 'invalid'))[0], 403)
        self.bridge.uploads.end(self.token)
        self.assertEqual((await self.post({'op': 'recall'}))[0], 403)

    async def test_cli_helper_saves_and_recalls_through_scoped_http(self):
        env = dict(os.environ, QQ_UPLOAD_TOKEN=self.token,
                   QQ_UPLOAD_URL=self.url.replace('/memory', '/upload'))
        async def command(*args):
            process = await asyncio.create_subprocess_exec(
                sys.executable, str(Path(__file__).with_name('memory.py')), *args,
                env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await process.communicate()
            self.assertEqual(process.returncode, 0, stderr.decode())
            return json.loads(stdout)
        saved = await command('remember', '--text', '我喜欢 C++17', '--key', 'language')
        self.assertEqual(saved['status'], 'remembered')
        recalled = await command('recall', '--query', 'C++17')
        self.assertEqual(recalled['entries'][0]['id'], saved['entry']['id'])

    async def test_roles_fail_closed_and_configured_manager(self):
        async def unavailable(*args, **kwargs):
            raise ConnectionError()
        self.bridge.api = unavailable
        self.assertEqual((await self.remember())[0], 403)
        code, result = await self.post(dict(op='remember', text='我喜欢 C++', key='personal'))
        self.assertEqual(code, 200)
        self.assertEqual(result['entry']['scope'], 'personal')
        data = json.loads(self.config.read_text())
        data['memory_managers'] = {'123': [8]}
        self.config.write_text(json.dumps(data))
        self.assertEqual((await self.remember())[0], 200)

    async def test_wrong_identity_and_role_revocation(self):
        async def wrong(*args, **kwargs):
            return dict(group_id=456, user_id=8, role='owner')
        self.bridge.api = wrong
        self.assertEqual((await self.remember())[0], 403)
        async def revoked(*args, **kwargs):
            self.bridge.uploads.end(self.token)
            return dict(group_id=123, user_id=8, role='admin')
        self.bridge.api = revoked
        self.assertEqual((await self.remember())[0], 403)
        self.assertEqual(self.bridge.memory.store(123).entries(), [])

    async def test_concurrent_updates_detect_stale_revision(self):
        _, result = await self.remember()
        entry = result['entry']
        responses = await asyncio.gather(*(self.post(dict(op='update', id=entry['id'], revision=1,
                          text=f'new rule {i}', key=f'edit-{i}')) for i in range(2)))
        self.assertEqual(sorted(code for code, _ in responses), [200, 400])
        self.assertEqual(self.bridge.memory.store(123).entries()[0]['revision'], 2)

    async def test_latest_memory_loaded_at_execution_and_on_background_resume(self):
        seen = []
        async def runner(job):
            seen.append(job.memory)
            self.assertIn('group_memory', prompt_for(job))
            return 'ok'
        self.bridge.runner = runner
        next_job = Job(123, 'next', 8, 'reply', [])
        await self.remember()
        await self.bridge.run_agent(next_job)
        self.assertIn('xxx', seen[-1]['core'])
        # Workspace rotation and saved-session adoption never select another memory directory.
        next_job.run_root = self.root / 'groups/123/foreground/rotated'
        next_job.background = True
        next_job.resume_session_id = 'session-example'
        await self.bridge.run_agent(next_job)
        self.assertIn('xxx', seen[-1]['core'])
        await self.bridge.run_agent(Job(456, 'other-group', 8, 'reply', []))
        self.assertNotIn('xxx', seen[-1]['core'])

    async def test_background_can_read_but_not_write(self):
        await self.remember()
        job = Job(123, 'background:1', 8, 'work', [], background=True,
                  run_root=self.root / 'groups/123/background/1')
        token = self.bridge.uploads.begin(job)
        code, result = await self.post({'op': 'recall'}, token)
        self.assertEqual(code, 200)
        self.assertEqual(result['total'], 1)
        self.assertEqual((await self.post(dict(op='remember', text='rewrite', key='bad'), token))[0], 403)

    async def test_trust_revocation_and_invalid_config(self):
        self.config.write_text(json.dumps(dict(trusted_groups=[456], bot_qq=12345)))
        self.assertEqual((await self.post({'op': 'recall'}))[0], 403)
        self.config.write_text(json.dumps(dict(trusted_groups=[123], bot_qq=12345, memory_managers={'123': '8'})))
        with self.assertRaises(ValueError):
            load_config(self.config)


if __name__ == '__main__':
    unittest.main()

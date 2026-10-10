import asyncio
import contextlib
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from aiohttp import web, ClientSession
from diagnostics import DSHFailure
from background import BackgroundService
from bot import Bridge, Job, prompt_for
from history import current_archive, current_workspace, query
from upload import capability


class BackgroundTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.config = self.root / 'config.json'
        self.config.write_text(json.dumps({'trusted_groups': [123456789, 987654321], 'bot_qq': 12345, 'history_sync': False}))
        self.bridge = Bridge(self.config, self.root, 'x' * 40)
        self.service = self.bridge.background
        self.job = Job(123456789, 'parent', 8, 'background work', [])
        self.token = self.bridge.uploads.begin(self.job)
        self.grant = self.bridge.uploads.grants[self.token]
        self.bridge.connected.set()
        self.calls, self.runs = [], []
        async def api(action, params, **kwargs):
            self.calls.append((action, params))
            return {'message_id': len(self.calls)}
        async def runner(job):
            self.runs.append(job)
            return f'第{job.background_info["occurrence"]}条'
        self.bridge.api, self.bridge.runner = api, runner
        app = web.Application()
        app.router.add_post('/agent/background', self.service.handle)
        app.router.add_post('/agent/messages', self.bridge.messages.handle)
        self.web_runner = web.AppRunner(app)
        await self.web_runner.setup()
        site = web.TCPSite(self.web_runner, '127.0.0.1', 0)
        await site.start()
        self.url = f'http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}'
        self.client = ClientSession()

    async def asyncTearDown(self):
        if self.service.execution:
            self.service.execution.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.service.execution
        await self.client.close()
        await self.web_runner.cleanup()
        self.bridge.db.close()
        self.tmp.cleanup()

    async def post(self, body, token=None, endpoint='background'):
        async with self.client.post(self.url + '/agent/' + endpoint, json=body,
                headers={'Authorization': 'Bearer ' + (self.token if token is None else token)}) as response:
            return response.status, await response.json()

    async def create(self, key='one', **extra):
        body = {'op': 'propose', 'key': key, 'prompt': 'Send one short update', **extra}
        status, result = await self.post(body)
        self.assertEqual(status, 200, result)
        self.assertEqual(result['status'], 'proposed')
        token = self.bridge.uploads.begin(Job(123456789, 'confirmation-' + result['id'], 8, 'yes, start', []))
        _, confirmed = await self.post({'op': 'confirm', 'id': result['id']}, token)
        self.bridge.uploads.end(token)
        self.assertEqual(confirmed['status'], 'confirmed', confirmed)
        return confirmed, body

    def row(self, jid):
        return self.service.rows('SELECT * FROM background_jobs WHERE id=?', (jid,))[0]

    async def fire(self, jid, offset=0):
        row = self.row(jid)
        with patch('background.time.time', return_value=row['next_run'] + offset):
            await self.service.tick()
            if self.service.execution:
                await asyncio.wait_for(self.service.execution, 3)
                await self.service.tick()

    async def test_proposal_waits_for_same_user_later_confirmation_and_survives_restart(self):
        _, proposal = await self.post({'op': 'propose', 'key': 'consent', 'prompt': 'lengthy work'})
        self.assertEqual(proposal['state'], 'awaiting_confirmation')
        self.assertIsNone(proposal['next_run'])
        await self.service.tick()
        self.assertIsNone(self.service.execution)
        same_turn = await self.post({'op': 'confirm', 'id': proposal['id']})
        self.assertEqual(same_turn[0], 400)
        other_user = self.bridge.uploads.begin(Job(123456789, 'other-yes', 9, 'yes', []))
        self.assertEqual((await self.post({'op': 'confirm', 'id': proposal['id']}, other_user))[0], 400)
        self.service = BackgroundService(self.bridge, Job)
        self.assertEqual(self.row(proposal['id'])['state'], 'awaiting_confirmation')
        token = self.bridge.uploads.begin(Job(123456789, 'owner-yes', 8, 'yes', []))
        result = await self.service.command(self.bridge.uploads.grants[token], {'op': 'confirm', 'id': proposal['id']})
        self.assertEqual(result['status'], 'confirmed')
        await self.fire(proposal['id'])
        self.assertEqual(len(self.calls), 1)

    async def test_twelve_hour_timeout_is_accepted_and_used_by_worker(self):
        result, _ = await self.create(timeout=43200)
        actual_wait_for = asyncio.wait_for
        timeouts = []
        async def observed(awaitable, timeout):
            timeouts.append(timeout)
            return await actual_wait_for(awaitable, timeout)
        with patch('background.asyncio.wait_for', side_effect=observed):
            await self.fire(result['id'])
        self.assertIn(43200, timeouts)
        self.assertEqual(self.runs[0].timeout, 43200)
        self.assertEqual(self.row(result['id'])['state'], 'completed')

    async def test_background_retains_specific_token_exhaustion_reason(self):
        result, _ = await self.create()
        async def exhausted(job):
            raise DSHFailure('max-tokens', 0)
        self.bridge.runner = exhausted
        await self.fire(result['id'])
        row = self.row(result['id'])
        self.assertEqual(row['state'], 'failed')
        outcome = json.loads(row['last_result'])
        self.assertEqual(outcome['reason'], 'max-tokens')
        self.assertEqual(outcome['exit_code'], 0)
        self.assertFalse(self.calls)

    async def test_three_runs_one_minute_apart_terminate_and_archive(self):
        result, body = await self.create(every=60, count=3)
        first = self.row(result['id'])['first_run']
        self.assertAlmostEqual(first - self.row(result['id'])['created'], 60, delta=1)
        for index in range(1, 4):
            self.assertEqual(self.row(result['id'])['next_run'], first + (index - 1) * 60)
            await self.fire(result['id'])
            self.assertEqual(len(self.calls), index)
        row = self.row(result['id'])
        self.assertEqual((row['state'], row['processed'], row['skipped']), ('completed', 3, 0))
        await self.service.tick()
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(len({r.run_root for r in self.runs}), 3)
        self.assertEqual(query(self.bridge.archive(self.job.group).path)['messages'][0]['text'], '第1条')
        again = (await self.post(body))[1]
        self.assertEqual((again['status'], again['state']), ('existing', 'completed'))

    async def test_scope_validation_limits_and_idempotency(self):
        result, body = await self.create()
        self.assertEqual((await self.post(body))[1]['id'], result['id'])
        self.assertEqual((await self.post({**body, 'prompt': 'different'}))[0], 400)
        other = self.bridge.uploads.begin(Job(987654321, 'other', 9, 'test', []))
        self.assertEqual((await self.post({'op': 'list'}, other))[1]['jobs'], [])
        self.assertEqual((await self.post({'op': 'cancel', 'id': result['id']}, other))[0], 400)
        self.assertEqual((await self.post({'op': 'list'}, 'bad'))[0], 403)
        for fields in [{'every': 1, 'count': 3}, {'every': 60}, {'count': 0}, {'timeout': 43201}, {'delay': -1}, {'count': True}, {'group_id': 987654321}, {'every': 86400, 'count': 60}, {'prompt': ''}]:
            self.assertEqual((await self.post({**body, 'key': 'bad', **fields}))[0], 400, fields)
        await self.create('two')
        await self.create('three')
        self.assertEqual((await self.post({**body, 'key': 'four'}))[0], 400)
        self.bridge.uploads.end(self.token)
        self.assertEqual((await self.post({'op': 'list'}))[0], 403)

    async def test_foreground_replies_while_background_runs_and_capabilities_do_not_collide(self):
        result, _ = await self.create()
        started, release = asyncio.Event(), asyncio.Event()
        foreground_done = asyncio.Event()
        background_grants = []
        async def runner(job):
            grant = self.bridge.uploads.grants[job.upload_token]
            cap = grant.workspace.parent / '.upload-request.json'
            self.assertEqual(json.loads(cap.read_text())['token'], job.upload_token)
            if job.background:
                background_grants.append((job.upload_token, cap))
                started.set()
                await release.wait()
                self.assertEqual(json.loads(cap.read_text())['token'], job.upload_token)
                return 'background complete'
            self.assertNotEqual(cap, background_grants[0][1])
            return 'foreground complete'
        async def reply(job, text):
            self.assertEqual(text, 'foreground complete')
            foreground_done.set()
        self.bridge.runner, self.bridge.reply = runner, reply
        await self.service.tick()
        await asyncio.wait_for(started.wait(), 3)
        worker = asyncio.create_task(self.bridge.worker())
        try:
            await self.bridge.queue.put(Job(123456789, 'next-mention', 8, '2+2?', []))
            await asyncio.wait_for(foreground_done.wait(), 3)
            self.assertFalse(self.service.execution.done())
            release.set()
            await self.service.execution
            self.assertEqual(self.row(result['id'])['state'], 'completed')
            self.assertFalse(background_grants[0][1].exists())
        finally:
            release.set()
            worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker

    async def test_running_cancel_stops_work_and_no_final_send(self):
        result, _ = await self.create()
        started = asyncio.Event()
        async def runner(job):
            started.set()
            await asyncio.Event().wait()
        self.bridge.runner = runner
        await self.service.tick()
        await started.wait()
        self.assertEqual((await self.post({'op': 'cancel', 'id': result['id']}))[1]['status'], 'cancelled')
        with self.assertRaises(asyncio.CancelledError):
            await self.service.execution
        self.assertEqual(self.row(result['id'])['state'], 'cancelled')
        self.assertFalse(self.calls)
        self.assertEqual(list(self.bridge.uploads.grants), [self.token])

    async def test_untrusted_group_revokes_running_background(self):
        result, _ = await self.create()
        started = asyncio.Event()
        async def runner(job):
            started.set()
            await asyncio.Event().wait()
        self.bridge.runner = runner
        await self.service.tick()
        await started.wait()
        self.config.write_text(json.dumps({'trusted_groups': [987654321], 'bot_qq': 12345}))
        await self.service.tick()
        with self.assertRaises(asyncio.CancelledError):
            await self.service.execution
        self.assertEqual(self.row(result['id'])['state'], 'cancelled')
        self.assertFalse(self.calls)

    async def test_pending_persists_and_crashed_running_job_is_not_replayed(self):
        pending, _ = await self.create(delay=60)
        crashed, _ = await self.create('crashed')
        self.bridge.db.execute("UPDATE background_jobs SET state='running' WHERE id=?", (crashed['id'],))
        self.bridge.db.commit()
        self.service = BackgroundService(self.bridge, Job)
        self.assertEqual(self.row(crashed['id'])['state'], 'interrupted')
        await self.fire(pending['id'])
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.row(pending['id'])['state'], 'completed')

    async def test_disconnection_missed_slots_and_no_catchup_flood(self):
        result, _ = await self.create(every=60, count=3)
        self.bridge.connected.clear()
        first = self.row(result['id'])['first_run']
        with patch('background.time.time', return_value=first + 125):
            await self.service.tick()
            self.assertIsNone(self.service.execution)
            self.bridge.connected.set()
            await self.service.tick()
            await self.service.execution
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.runs[0].background_info['occurrence'], 3)
        self.assertEqual(self.row(result['id'])['skipped'], 2)
        expired, _ = await self.create('expired', delay=60)
        await self.fire(expired['id'], offset=301)
        self.assertEqual(self.row(expired['id'])['state'], 'completed')
        self.assertEqual(self.row(expired['id'])['skipped'], 1)
        self.assertEqual(len(self.calls), 1)

    async def test_failures_pause_and_uncertain_send_is_not_retried(self):
        result, _ = await self.create(every=60, count=3)
        async def timeout(action, params, **kwargs):
            self.calls.append(params)
            raise asyncio.TimeoutError()
        self.bridge.api = timeout
        await self.fire(result['id'])
        self.assertEqual(self.row(result['id'])['state'], 'unknown')
        await self.service.tick()
        self.assertEqual(len(self.calls), 1)
        failed, _ = await self.create('failed')
        async def fail(job):
            raise asyncio.TimeoutError()
        self.bridge.runner = fail
        await self.fire(failed['id'])
        self.assertEqual(self.row(failed['id'])['state'], 'failed')
        self.assertEqual(list(self.bridge.uploads.grants), [self.token])

    async def test_child_cannot_spawn_or_schedule_or_send_duplicate_final(self):
        child = Job(123456789, 'child', 8, 'task', [], background=True,
                    run_root=self.root/'groups/123456789/background/0123456789abcdef/runs/1')
        token = self.bridge.uploads.begin(child)
        self.assertEqual((await self.post({'op': 'propose', 'key': 'child', 'prompt': 'more'}, token))[0], 400)
        self.assertEqual((await self.post({'op': 'list'}, token))[0], 200)
        for body in [{'op': 'schedule', 'key': 'bad', 'cron': '* * * * *', 'text': 'x'},
                     {'op': 'send', 'reason': 'requested', 'key': 'bad', 'text': 'x'},
                     {'op': 'cancel', 'id': 'bad'}]:
            self.assertEqual((await self.post(body, token, 'messages'))[0], 400)
        self.assertIn('ONE occurrence', prompt_for(child))

    async def test_scheduler_global_concurrency_limit(self):
        one, _ = await self.create()
        two, _ = await self.create('two')
        started, release = asyncio.Event(), asyncio.Event()
        async def runner(job):
            self.runs.append(job)
            started.set()
            await release.wait()
            return 'done'
        self.bridge.runner = runner
        await self.service.tick()
        await started.wait()
        await self.service.tick()
        self.assertEqual(len(self.runs), 1)
        self.assertEqual(self.row(two['id'])['state'], 'active')
        release.set()
        await self.service.execution
        await self.service.tick()
        await self.service.execution
        self.assertEqual(len(self.runs), 2)

    async def test_timeout_proposal_preserves_session_and_rotates_foreground_after_restart(self):
        root = self.service.foreground_root(self.job.group)
        self.job.run_root, self.job.session_id = root, 'session-progress'
        (root / 'workspace' / 'progress.cpp').write_text('saved progress')
        self.job.images = [{'file_path': '/saved/image.png', 'status': 'ready'}]
        jid = self.service.propose_timeout(self.job)
        self.assertEqual(self.service.propose_timeout(self.job), jid)
        self.assertEqual(self.row(jid)['state'], 'awaiting_confirmation')
        await self.service.tick()
        self.assertFalse(self.runs)
        replacement = self.service.foreground_root(self.job.group)
        self.assertNotEqual(root, replacement)
        restarted = BackgroundService(self.bridge, Job)
        self.assertEqual(restarted.foreground_root(self.job.group), replacement)
        other = self.bridge.uploads.begin(Job(self.job.group, 'wrong-user', 9, 'yes', []))
        with self.assertRaises(ValueError):
            await self.service.command(self.bridge.uploads.grants[other], {'op': 'confirm', 'id': jid})
        confirm = Job(self.job.group, 'later-yes', 8, '', [], background_info={'command': 'continue', 'id': jid})
        self.assertIn('已确认', await self.service.continuation_command(confirm))
        await self.fire(jid)
        resumed = self.runs[0]
        self.assertEqual(resumed.run_root, root)
        self.assertEqual(resumed.resume_session_id, 'session-progress')
        self.assertEqual(resumed.timeout, 43200)
        self.assertEqual(resumed.images, self.job.images)
        self.assertTrue(resumed.background)
        self.assertNotEqual(resumed.diagnostic_root, root)
        self.assertEqual((root / 'workspace' / 'progress.cpp').read_text(), 'saved progress')

    async def test_steer_running_job_resumes_same_session_without_extra_final(self):
        result, _ = await self.create()
        started, cancelled = asyncio.Event(), asyncio.Event()
        sessions = []
        async def runner(job):
            sessions.append((job.run_root, job.resume_session_id, job.text))
            if len(sessions) == 1:
                job.session_id = 'session-original'
                (job.run_root / 'workspace' / 'progress.txt').write_text('preserved')
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()
            self.assertTrue(cancelled.is_set())
            self.assertEqual((job.run_root / 'workspace' / 'progress.txt').read_text(), 'preserved')
            return 'C++ result'
        self.bridge.runner = runner
        await self.service.tick()
        await asyncio.wait_for(started.wait(), 2)
        body = {'op': 'steer', 'id': result['id'], 'key': 'cpp', 'message': 'Use C++ instead of Python'}
        queued = await self.service.command(self.grant, body)
        self.assertEqual(queued['status'], 'queued')
        again = await self.service.command(self.grant, body)
        self.assertEqual(again['seq'], queued['seq'])
        await asyncio.wait_for(self.service.execution, 2)
        self.assertEqual(len(sessions), 2)
        self.assertEqual(sessions[0][0], sessions[1][0])
        self.assertEqual(sessions[1][1], 'session-original')
        self.assertIn('Use C++ instead of Python', sessions[1][2])
        self.assertEqual(self.row(result['id'])['processed'], 1)
        self.assertEqual(self.row(result['id'])['state'], 'completed')
        self.assertEqual(len(self.calls), 1)

    async def test_steering_scope_validation_pending_consent_and_persistence(self):
        _, result = await self.post({'op': 'propose', 'key': 'pending', 'prompt': 'implement in Python'})
        body = {'op': 'steer', 'id': result['id'], 'key': 'cpp', 'message': 'Use C++17'}
        self.assertEqual((await self.post(body))[1]['status'], 'queued')
        self.assertEqual(self.row(result['id'])['state'], 'awaiting_confirmation')
        await self.service.tick()
        self.assertFalse(self.runs)
        restarted = BackgroundService(self.bridge, Job)
        description = restarted.describe(self.row(result['id']))
        self.assertEqual(description['instructions'][0]['text'], 'Use C++17')
        for message, key in [('', 'empty'), ('x' * 2001, 'long'), ('different', 'cpp')]:
            self.assertEqual((await self.post({**body, 'key': key, 'message': message}))[0], 400)
        for group, sender, background in [(123456789, 9, False), (987654321, 8, False), (123456789, 8, True)]:
            token = self.bridge.uploads.begin(Job(group, 'bad', sender, '', [], background=background,
                run_root=self.root / 'other' / f'{group}-{sender}-{background}'))
            self.assertEqual((await self.post(body, token))[0], 400)
        confirm = Job(self.job.group, 'yes', 8, '', [], background_info={'command': 'continue', 'id': result['id']})
        await self.service.continuation_command(confirm)
        await self.fire(result['id'])
        self.assertIn('Use C++17', self.runs[0].text)
        self.assertEqual((await self.post({**body, 'key': 'too-late'}))[0], 400)

    async def test_steering_does_not_reset_outer_deadline(self):
        result, _ = await self.create()
        started = asyncio.Event()
        stopped = []
        async def runner(job):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.append(True)
        self.bridge.runner = runner
        job = Job(self.job.group, 'deadline', 8, 'work', [], background=True)
        task = asyncio.create_task(asyncio.wait_for(self.service.run_with_instructions(job, result['id']), .15))
        self.service.current_id = result['id']
        await started.wait()
        await self.service.command(self.grant, {'op': 'steer', 'id': result['id'], 'key': 'change', 'message': 'Use C++'})
        with self.assertRaises(asyncio.TimeoutError):
            await task
        self.assertEqual(len(stopped), 2)


class WorkspaceTests(unittest.TestCase):
    def test_foreground_background_history_and_capability_paths(self):
        for path, workspace in [
            ('/state/groups/123456789/workspace/sub', '/state/groups/123456789/workspace'),
            ('/state/groups/123456789/foreground/0123456789abcdef/workspace/sub', '/state/groups/123456789/foreground/0123456789abcdef/workspace'),
            ('/state/groups/123456789/background/0123456789abcdef/runs/2/workspace/sub', '/state/groups/123456789/background/0123456789abcdef/runs/2/workspace')]:
            with patch('history.Path.cwd', return_value=Path(path)):
                self.assertEqual(current_workspace(), Path(workspace))
                self.assertEqual(current_archive(), Path('/state/groups/123456789/history.sqlite3'))
        for path in ['/tmp/workspace', '/state/groups/123456789/untrusted/workspace', '/state/groups/123456789/background/invalid/runs/1/workspace']:
            with patch('history.Path.cwd', return_value=Path(path)), self.assertRaises(ValueError):
                current_workspace()

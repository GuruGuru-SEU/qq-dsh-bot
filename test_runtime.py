import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from bot import Bridge, Job, run_dsh
from diagnostics import DSHFailure, record_name, current_diagnostics_root


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.script = self.bin / 'dsh'
        self.job = Job(123456789, 'request/../../id', 8, 'synthetic request', [], run_root=self.root/'run')
        self.env = patch.dict(os.environ, {'PATH': str(self.bin)+os.pathsep+os.environ['PATH']})
        self.env.start()

    async def asyncTearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def executable(self, body):
        self.script.write_text('#!'+sys.executable+'\nimport sys,json,time,os\nsys.stdin.read()\n'+body)
        self.script.chmod(0o700)

    def events(self, events, code=0):
        self.executable('events='+repr(events)+'\nfor event in events: print(json.dumps(event),flush=True)\n'
                        "sys.stderr.write('FAKE_SECRET_PRIVATE_OUTPUT\\n')\n"+f'sys.exit({code})\n')

    def record(self):
        path=self.job.run_root/'diagnostics'/record_name(self.job.message_id)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        result=json.loads(path.read_text())
        self.assertEqual(result,json.loads((path.parent/'last.json').read_text()))
        return result

    async def test_max_tokens_has_specific_failure_and_safe_persistent_diagnostics(self):
        self.events([
            {'type':'session','sessionId':'session-test'},
            {'type':'thinking','text':'FAKE_PRIVATE_REASONING'},
            {'type':'status','phase':'step_end','usage':{'inputTokens':100,'outputTokens':10,'totalTokens':110}},
            {'type':'status','phase':'step_end','usage':{'inputTokens':200,'outputTokens':32768,'totalTokens':32968}},
            {'type':'status','phase':'turn_end','reason':{'kind':'max-tokens','detail':'FAKE_PRIVATE_ERROR'}},
            {'type':'final','text':'FAKE_PARTIAL_ANSWER'}])
        with self.assertLogs('qq-dsh',level='INFO') as logs, self.assertRaises(DSHFailure) as caught:
            await run_dsh(self.job)
        self.assertEqual(caught.exception.reason,'max-tokens')
        self.assertIn('输出额度',caught.exception.user_message)
        record=self.record()
        self.assertEqual(record['stop_reason'],'max-tokens')
        self.assertEqual(record['exit_code'],0)
        self.assertEqual(record['usage']['outputTokens'],32778)
        self.assertEqual(record['last_step_usage']['outputTokens'],32768)
        self.assertEqual(record['usage_steps'],2)
        self.assertGreater(record['stderr_bytes'],0)
        text=json.dumps(record)+'\n'.join(logs.output)
        for private in ['FAKE_SECRET','FAKE_PRIVATE','FAKE_PARTIAL','synthetic request']:
            self.assertNotIn(private,text)

    async def test_completed_reply_and_missing_usage(self):
        self.events([{'type':'status','phase':'step_end'},
                     {'type':'status','phase':'turn_end','reason':{'kind':'completed'}},
                     {'type':'final','text':'done'}])
        self.assertEqual(await run_dsh(self.job),'done')
        record=self.record()
        self.assertEqual(record['outcome'],'completed')
        self.assertEqual((record['step_count'],record['usage_steps'],record['usage']),(1,0,{}))

    async def test_exit_empty_answer_and_missing_terminal_state_are_distinct(self):
        for events,code,reason in [
            ([{'type':'final','text':'partial'}],0,'missing-turn-end'),
            ([{'type':'status','phase':'turn_end','reason':{'kind':'completed'}}],0,'empty-final'),
            ([{'type':'status','phase':'turn_end','reason':{'kind':'completed'}},{'type':'final','text':'done'}],7,'process-exit')]:
            self.events(events,code)
            with self.assertRaises(DSHFailure) as caught:
                await run_dsh(self.job)
            self.assertEqual(caught.exception.reason,reason)
            self.assertEqual(self.record()['exit_code'],code)

    async def test_timeout_and_cancel_kill_process_and_save_metadata(self):
        self.executable("print(json.dumps({'type':'session','sessionId':'slow'}),flush=True)\ntime.sleep(60)\n")
        self.job.timeout=.15
        with self.assertRaises(asyncio.TimeoutError):
            await run_dsh(self.job)
        self.assertEqual(self.record()['outcome'],'timeout')
        self.assertEqual(self.record()['exit_code'],-9)
        self.job.timeout=43200
        task=asyncio.create_task(run_dsh(self.job))
        await asyncio.sleep(.15)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.record()['outcome'],'cancelled')
        self.assertEqual(self.record()['exit_code'],-9)

    async def test_malformed_stream_is_recorded_without_leaking_raw_output(self):
        self.executable("print('FAKE_PRIVATE_NOT_JSON',flush=True)\ntime.sleep(60)\n")
        with self.assertRaises(ValueError):
            await asyncio.wait_for(run_dsh(self.job), 3)
        record = self.record()
        self.assertEqual(record['outcome'], 'error')
        self.assertEqual(record['error_type'], 'JSONDecodeError')
        self.assertNotIn('FAKE_PRIVATE', json.dumps(record))

    async def test_twelve_hour_budget_reaches_runtime_timer(self):
        self.events([{'type':'status','phase':'turn_end','reason':{'kind':'completed'}},{'type':'final','text':'done'}])
        self.job.timeout=43200
        real_wait_for=asyncio.wait_for
        timeouts=[]
        async def observed(awaitable,timeout):
            timeouts.append(timeout)
            return await real_wait_for(awaitable,timeout)
        with patch('bot.asyncio.wait_for',side_effect=observed):
            self.assertEqual(await run_dsh(self.job),'done')
        self.assertIn(43200,timeouts)
        self.assertEqual(self.record()['timeout_seconds'],43200)

    async def test_resume_passes_exact_session_and_retains_workspace(self):
        self.job.resume_session_id = 'session-checkpoint'
        self.job.skills_patch = self.root / 'group-skills.patch.yml'
        workspace = self.job.run_root / 'workspace'
        workspace.mkdir(parents=True)
        (workspace / 'progress.txt').write_text('already computed')
        self.executable("assert sys.argv[sys.argv.index('--session-id')+1] == 'session-checkpoint'\n"
                        "assert len([arg for arg in sys.argv[:sys.argv.index('--json')] if arg == '--patch']) == 2\n"
                        "assert open('progress.txt').read() == 'already computed'\n"
                        "assert os.environ['DSH_HOME'] == os.path.dirname(os.getcwd()) + '/home'\n"
                        "print(json.dumps({'type':'session','sessionId':'session-checkpoint'}))\n"
                        "print(json.dumps({'type':'status','phase':'turn_end','reason':{'kind':'completed'}}))\n"
                        "print(json.dumps({'type':'final','text':'continued'}))\n")
        self.assertEqual(await run_dsh(self.job), 'continued')
        self.assertEqual(self.job.session_id, 'session-checkpoint')
        self.job.resume_session_id = '../../bad'
        with self.assertRaises(ValueError):
            await run_dsh(self.job)

    async def test_continuation_diagnostics_follow_scoped_capability(self):
        group = self.root / 'groups' / '123456789'
        group.mkdir(parents=True)
        target = group / 'background' / '0123456789abcdef' / 'runs' / '1'
        cap = group / '.upload-request.json'
        with patch('diagnostics.current_workspace', return_value=group / 'workspace'), patch('diagnostics.current_archive', return_value=group / 'history.sqlite3'):
            cap.write_text(json.dumps({'diagnostics_root': str(target)}))
            self.assertEqual(current_diagnostics_root(), target)
            cap.write_text(json.dumps({'diagnostics_root': str(self.root / 'other-group')}))
            self.assertEqual(current_diagnostics_root(), group)

    async def test_worker_posts_specific_token_limit_reply(self):
        config=self.root/'config.json'
        config.write_text(json.dumps({'bot_qq':12345,'trusted_groups':[123456789],'history_sync':False}))
        async def runner(job):
            raise DSHFailure('max-tokens',0)
        bridge=Bridge(config,self.root/'state','x'*40,runner)
        replies=[]
        async def reply(job,text): replies.append(text)
        bridge.reply=reply
        task=asyncio.create_task(bridge.worker())
        try:
            await bridge.queue.put(self.job)
            await asyncio.wait_for(bridge.queue.join(),3)
            self.assertIn('输出额度',replies[0])
            self.assertNotIn('处理失败',replies[0])
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
            bridge.db.close()

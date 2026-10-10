"""Content-free DSH run diagnostics, readable from the current workspace."""
import argparse
from datetime import datetime
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import time
import uuid

from history import ZONE, current_workspace, current_archive

LOG = logging.getLogger("qq-dsh")
REASONS = {"completed", "max-tokens", "error", "aborted", "blocked", "interrupted", "forked"}
TOKEN_FIELDS = ("inputTokens", "outputTokens", "totalTokens", "cacheReadTokens", "cacheWriteTokens", "reasoningTokens")


class DSHFailure(RuntimeError):
    def __init__(self, reason, exit_code=None):
        self.reason, self.exit_code = reason, exit_code
        super().__init__(f"DSH stopped: {reason} (exit_code={exit_code})")

    @property
    def user_message(self):
        if self.reason == "max-tokens":
            return "这次推理用完了模型单次输出额度，还没生成完整答案。可以缩小问题范围后再 @ 我。"
        if self.reason == "empty-final":
            return "模型这次没有返回最终答案，请稍后再 @ 我重试。"
        return "这次模型运行异常，未能完成；服务端已记录诊断信息。"


def record_name(message_id):
    return hashlib.sha256(str(message_id).encode()).hexdigest()[:24] + '.json'


class RunDiagnostics:
    def __init__(self, job):
        self.started = time.monotonic()
        self.data = {"message_id": job.message_id, "group_id": job.group,
                     "started_at": datetime.now(ZONE).isoformat(), "timeout_seconds": job.timeout,
                     "background": job.background, "session_id": None, "stop_reason": None,
                     "exit_code": None, "step_count": 0, "usage_steps": 0, "usage": {},
                     "last_step_usage": None, "stderr_bytes": 0, "final_characters": 0}

    def event(self, event):
        if event.get('type') == 'session':
            sid = event.get('sessionId')
            if isinstance(sid, str) and re.fullmatch(r'[a-zA-Z0-9_-]{1,100}', sid):
                self.data['session_id'] = sid
        if event.get('type') != 'status':
            return
        if event.get('phase') == 'turn_end':
            reason = event.get('reason')
            kind = reason.get('kind') if isinstance(reason, dict) else None
            self.data['stop_reason'] = kind if isinstance(kind, str) and kind in REASONS else 'unknown'
        if event.get('phase') == 'step_end':
            self.data['step_count'] += 1
            raw = event.get('usage')
            usage = {k: raw[k] for k in TOKEN_FIELDS if type(raw.get(k)) is int and raw[k] >= 0} if isinstance(raw, dict) else {}
            self.data['last_step_usage'] = usage or None
            if usage:
                self.data['usage_steps'] += 1
                for key, value in usage.items():
                    self.data['usage'][key] = self.data['usage'].get(key, 0) + value

    def save(self, root, process, final, outcome):
        self.data.update(exit_code=process.returncode if process else None,
                         final_characters=len(final), outcome=outcome,
                         duration_seconds=round(time.monotonic() - self.started, 3),
                         finished_at=datetime.now(ZONE).isoformat())
        # Only allowlisted metadata is retained: no prompts, outputs, reasoning,
        # raw errors or stderr, which can contain credentials and conversation text.
        LOG.info('dsh_result %s', json.dumps(self.data, ensure_ascii=False))
        directory = Path(root) / 'diagnostics'
        try:
            directory.mkdir(parents=True, exist_ok=True)
            body = json.dumps(self.data, ensure_ascii=False, indent=2) + '\n'
            for name in [record_name(self.data['message_id']), 'last.json']:
                temp = directory / ('.' + uuid.uuid4().hex + '.tmp')
                try:
                    fd = os.open(temp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                    with os.fdopen(fd, 'w') as file:
                        file.write(body)
                    os.replace(temp, directory / name)
                finally:
                    temp.unlink(missing_ok=True)
        except OSError as exc:
            LOG.error('diagnostic_save_failed kind=%s', type(exc).__name__)


def current_diagnostics_root():
    root = current_workspace().parent
    try:
        data = json.loads((root / '.upload-request.json').read_text())
        target = Path(data['diagnostics_root']).resolve()
        if target.is_relative_to(current_archive().parent.resolve()):
            return target
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return root.parent.parent if root.parent.name == 'foreground' else root


def main():
    parser = argparse.ArgumentParser(description='Read content-free diagnostics for a completed run in this workspace')
    parser.add_argument('--message-id', help='Trigger message ID; defaults to the last completed run')
    args = parser.parse_args()
    try:
        name = record_name(args.message_id) if args.message_id else 'last.json'
        print((current_diagnostics_root() / 'diagnostics' / name).read_text())
    except (ValueError, OSError):
        parser.exit(1, 'No saved diagnostics for that run in this workspace.\n')


if __name__ == '__main__':
    main()

"""Cheap, content-free status snapshots; no subprocesses or model requests."""
from datetime import datetime
import json
import math
from pathlib import Path
import shutil
import time

from diagnostics import REASONS
from history import ZONE


def elapsed(seconds):
    seconds = max(0, int(seconds))
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    return f"{hours}小时{minutes}分{seconds}秒"


def resources(state_path):
    lines = []
    try:
        used = int(Path('/sys/fs/cgroup/memory.current').read_text())
        limit = Path('/sys/fs/cgroup/memory.max').read_text().strip()
        maximum = f"{int(limit) / 2**20:.0f} MiB" if limit != 'max' else '无限制'
        lines.append(f"容器内存：{used / 2**20:.0f} MiB / {maximum}")
    except (OSError, ValueError):
        lines.append("容器内存：不可用")
    try:
        disk = shutil.disk_usage(state_path)
        lines.append(f"存档所在磁盘：剩余 {disk.free / 2**30:.1f} / {disk.total / 2**30:.1f} GiB")
    except OSError:
        lines.append("存档所在磁盘：不可用")
    return lines


def last_run(state_path, group):
    try:
        path = Path(state_path) / 'groups' / str(group) / 'diagnostics' / 'last.json'
        # Diagnostics are small. Bound reads, and never echo arbitrary file fields.
        with path.open() as file:
            data = json.loads(file.read(65536))
        if not isinstance(data, dict):
            raise ValueError('Invalid diagnostics')
        outcomes = dict.fromkeys(REASONS | {'failed', 'empty-final', 'unknown', 'process-exit'}, '失败')
        outcomes.update(completed='完成', timeout='超时', cancelled='中断')
        outcome = outcomes.get(str(data.get('outcome')), '未知')
        reason = data.get('stop_reason')
        reason = reason if isinstance(reason, str) and reason in REASONS else '未报告'
        seconds = data.get('duration_seconds')
        duration = elapsed(seconds) if type(seconds) in (int, float) and math.isfinite(seconds) else '未知'
        code = data.get('exit_code')
        code = str(code) if type(code) is int else '未报告'
        finished = datetime.fromisoformat(data['finished_at']).astimezone(ZONE).strftime('%m-%d %H:%M:%S')
        return f"本群最近前台任务：{finished}，{outcome}，耗时{duration}，停止原因 {reason}，退出码 {code}"
    except (OSError, ValueError, KeyError, TypeError, OverflowError):
        return "本群最近前台任务：暂无可读诊断"


def status_report(bridge, group):
    now = time.monotonic()
    def alive(task):
        return '正常' if task is not None and not task.done() else '未运行'

    background = dict(bridge.db.execute(
        'SELECT state,count(*) FROM background_jobs WHERE group_id=? GROUP BY state', (group,)))
    schedules = bridge.db.execute(
        "SELECT count(*) FROM schedules WHERE group_id=? AND state IN ('active','sending')", (group,)).fetchone()[0]
    active = bridge.active_by_group.get(group)
    active_since = bridge.active_since_by_group.get(group)
    foreground = '空闲' if active is None else '处理中（已用' + elapsed(now - active_since) + '）'
    connected = bridge.connected.is_set() and bridge.identity_ok
    queue = bridge.queue_for(group)
    worker = bridge.worker_tasks.get(group) or (bridge.worker_task if group == bridge.config['trusted_groups'][0] else None)
    return '\n'.join([
        '系统状态（' + datetime.now(ZONE).strftime('%Y-%m-%d %H:%M:%S') + ' 上海时间）',
        '桥接已运行：' + elapsed(now - bridge.started_at),
        ('SnowLuma：' if bridge.config.get('gateway') == 'snowluma' else 'NapCat：') + ('已连接并验证' if connected else '未就绪'),
        f"本群前台：{foreground}；排队 {queue.qsize()}/{queue.maxsize}",
        '后台执行器：' + ('忙碌' if bridge.background.current_id else '空闲') + '；单次上限 12小时',
        f"本群后台任务：待确认 {background.get('awaiting_confirmation', 0)}，待运行 {background.get('active', 0)}，运行中 {background.get('running', 0)}",
        f"本群有效定时消息：{schedules}",
        f"工作循环：本群前台{alive(worker)}，后台{alive(bridge.background_task)}，定时{alive(bridge.scheduler_task)}",
        *resources(bridge.state_path),
        last_run(bridge.state_path, group),
        '本次未调用 LLM 或 DSH。',
    ])

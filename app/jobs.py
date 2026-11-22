"""作业调度：固定数量（默认 2）的并行槽位。

关键约束：槽位 = 一轮实验环境（独立 cgroup）。只有上一轮给出归因结论
（verdict 落定）后，槽位才交还、供下一轮使用。
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .cgroups import BackendStatus, SlotHandle
from .verdict import decide_verdict

SAMPLE_JOB = Path(__file__).resolve().parent / "sample_job.py"
OUTPUT_TAIL_LINES = 50
OOM_SETTLE_SECONDS = 0.1  # 进程回收后稍等再读 oom_kill，确保内核已落账


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


@dataclass
class Job:
    id: str
    slot: int
    memory_mib: int
    hold_seconds: float
    state: str = "running"  # running | finished
    pid: int | None = None
    returncode: int | None = None       # subprocess 约定：负数 = -信号
    exit_signal: int | None = None
    stop_requested: bool = False
    stop_requested_at: float | None = None
    oom_kill_before: int | None = None
    oom_kill_after: int | None = None
    oom_kill_delta: int | None = None
    verdict: str | None = None
    verdict_reason: str | None = None
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    output_tail: deque = field(default_factory=lambda: deque(maxlen=OUTPUT_TAIL_LINES))

    def to_dict(self) -> dict:
        return {
            "job_id": self.id,
            "slot": self.slot,
            "state": self.state,
            "memory_mib": self.memory_mib,
            "hold_seconds": self.hold_seconds,
            "pid": self.pid,
            "exit_status": self.returncode,
            "exit_signal": self.exit_signal,
            "stop_requested": self.stop_requested,
            "oom_kill_before": self.oom_kill_before,
            "oom_kill_after": self.oom_kill_after,
            "oom_kill_delta": self.oom_kill_delta,
            "verdict": self.verdict,
            "verdict_reason": self.verdict_reason,
            "created_at": _iso(self.created_at),
            "finished_at": _iso(self.finished_at),
            "output_tail": list(self.output_tail),
        }


class NotAccepting(Exception):
    """内存上限未生效或 cgroup 不可用，拒绝接作业。"""


class NoSlotAvailable(Exception):
    """两轮槽位都在占用中。"""


class JobManager:
    def __init__(self, backend):
        self.backend = backend
        self.jobs: dict[str, Job] = {}
        self._handles: dict[int, SlotHandle] = {h.index: h for h in backend.handles}
        self._free: asyncio.Queue[int] = asyncio.Queue()
        for h in backend.handles:
            self._free.put_nowait(h.index)
        self._procs: dict[str, asyncio.subprocess.Process] = {}
        self._tasks: set[asyncio.Task] = set()

    # ---------- 查询 ----------

    @property
    def status(self) -> BackendStatus:
        return self.backend.status

    def slots_snapshot(self) -> dict:
        busy = sorted(j.slot for j in self.jobs.values() if j.state == "running")
        return {
            "count": len(self._handles),
            "free": len(self._handles) - len(busy),
            "busy_slots": busy,
            "memory_max_bytes": self.status.slot_memory_max_bytes,
            "memory_max_mib": (
                self.status.slot_memory_max_bytes // (1024 * 1024)
                if self.status.slot_memory_max_bytes
                else None
            ),
        }

    def get(self, job_id: str) -> Job | None:
        return self.jobs.get(job_id)

    def list(self) -> list[Job]:
        return sorted(self.jobs.values(), key=lambda j: j.created_at, reverse=True)

    # ---------- 起作业 ----------

    async def start(self, memory_mib: int, hold_seconds: float) -> Job:
        if not self.backend.status.accepting:
            raise NotAccepting
        try:
            slot = self._free.get_nowait()
        except asyncio.QueueEmpty:
            raise NoSlotAvailable
        job = Job(
            id=uuid.uuid4().hex[:12],
            slot=slot,
            memory_mib=memory_mib,
            hold_seconds=hold_seconds,
        )
        self.jobs[job.id] = job
        task = asyncio.create_task(self._run(job))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return job

    async def _run(self, job: Job) -> None:
        handle = self._handles[job.slot]
        try:
            inner = [
                sys.executable, "-I", "-u", str(SAMPLE_JOB),
                "--mib", str(job.memory_mib),
                "--hold", str(job.hold_seconds),
            ]
            argv = self.backend.child_argv(handle, inner)
            env = {**os.environ, **self.backend.child_env(handle)}
            job.oom_kill_before = self.backend.read_oom_kill(handle)
            try:
                proc = await asyncio.create_subprocess_exec(
                    *argv,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                    env=env,
                )
            except OSError as exc:
                job.output_tail.append(f"spawn failed: {exc}")
                return
            self._procs[job.id] = proc
            job.pid = proc.pid
            # 强停请求可能先于 spawn 到达
            if job.stop_requested and proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass

            async def drain() -> None:
                assert proc.stdout is not None
                async for line in proc.stdout:
                    job.output_tail.append(line.decode(errors="replace").rstrip())

            reader = asyncio.create_task(drain())
            rc = await proc.wait()
            await reader
            job.returncode = rc
            job.exit_signal = -rc if rc is not None and rc < 0 else None
            await asyncio.sleep(OOM_SETTLE_SECONDS)
            job.oom_kill_after = self.backend.read_oom_kill(handle)
        except Exception as exc:  # 兜底：任何异常都不能卡住槽位
            job.output_tail.append(f"runner error: {exc!r}")
        finally:
            self._finalize(job, handle)

    def _finalize(self, job: Job, handle: SlotHandle) -> None:
        if job.oom_kill_before is not None and job.oom_kill_after is not None:
            job.oom_kill_delta = job.oom_kill_after - job.oom_kill_before
        job.verdict, job.verdict_reason = decide_verdict(
            returncode=job.returncode,
            oom_kill_before=job.oom_kill_before,
            oom_kill_after=job.oom_kill_after,
            stop_requested=job.stop_requested,
        )
        job.state = "finished"
        job.finished_at = time.time()
        self._procs.pop(job.id, None)
        # 归因结论已出 —— 本轮实验环境（槽位）此刻才交还，可供下一轮使用
        self._free.put_nowait(job.slot)

    # ---------- 强停 ----------

    async def stop(self, job_id: str) -> Job | None:
        job = self.jobs.get(job_id)
        if job is None or job.state != "running":
            return job
        job.stop_requested = True
        job.stop_requested_at = time.time()
        proc = self._procs.get(job_id)
        if proc is not None and proc.returncode is None:
            try:
                proc.kill()  # SIGKILL
            except ProcessLookupError:
                pass
        return job

    async def shutdown(self) -> None:
        for job in list(self.jobs.values()):
            if job.state == "running":
                await self.stop(job.id)
        if self._tasks:
            await asyncio.wait(self._tasks, timeout=5)

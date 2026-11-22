"""作业调度：固定槽位 + 内存额度双重准入。

准入规则：
  - 总额度 = 容器内存上限 - 服务预留（见 cgroups 后端的 quota_total_bytes）。
  - 每轮作业可在 POST /jobs 另填本轮内存上限（memory_limit_mib，>=48MiB），
    省略则用默认值（总额度按槽位均分）。
  - 接单时同时预留“一个槽位 + 本轮上限对应的额度”：正在设置 cgroup 上限
    （setting）或已启动但尚未出归因结论（running）的作业都按各自上限占额。
  - 额度不足或槽位不足 -> 调用方收到 409。
  - 设置 cgroup 上限是慢操作且在锁外执行：一个槽位设置期间，另一空闲槽位
    只要余额够仍可并行接单。
  - 设置失败 -> 抛出 LimitSetupFailed，并退回本轮槽位与额度。
  - 归因结论落定后，槽位与额度才一起交还，供下一轮复用；复用槽位同样
    先设好本轮上限再启动进程。
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

from .cgroups import MIB, MIN_SLOT_BYTES, LimitSetupError, SlotHandle
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
    memory_limit_mib: int                  # 本轮实际生效的 cgroup 上限（MiB）
    state: str = "running"  # running | finished（接单成功即 running）
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
            "memory_limit_mib": self.memory_limit_mib,
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
    """槽位都在占用中（含正在设置上限的轮次）。"""


class QuotaExceeded(Exception):
    """本轮上限超过剩余内存额度。"""

    def __init__(self, requested_bytes: int, used_bytes: int, total_bytes: int):
        self.requested_bytes = requested_bytes
        self.used_bytes = used_bytes
        self.total_bytes = total_bytes
        super().__init__(
            f"额度不足：本轮申请 {requested_bytes // MIB}MiB，"
            f"已占 {used_bytes // MIB}MiB / 总额度 {total_bytes // MIB}MiB，"
            f"剩余 {(total_bytes - used_bytes) // MIB}MiB"
        )


class LimitSetupFailed(Exception):
    """本轮 cgroup 上限设置失败；槽位与额度已退回。"""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


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
        # 准入账：锁保护；所有访问都在事件循环线程，锁仅用于跨 await 保持一致
        self._lock = asyncio.Lock()
        self._used_quota_bytes = 0
        # slot -> {"job_id": str|None, "limit_bytes": int, "phase": "setting"|"running"}
        self._active: dict[int, dict] = {}

    # ---------- 查询 ----------

    @property
    def status(self):
        return self.backend.status

    @property
    def quota_total_bytes(self) -> int:
        return self.backend.status.quota_total_bytes

    @property
    def default_limit_bytes(self) -> int:
        return self.backend.status.slot_memory_max_bytes

    def slots_snapshot(self) -> dict:
        total = self.quota_total_bytes
        used = self._used_quota_bytes
        return {
            "count": len(self._handles),
            "free": len(self._handles) - len(self._active),
            "busy_slots": sorted(self._active.keys()),
            "default_memory_max_mib": self.default_limit_bytes // MIB,
            # 兼容旧字段名：默认单轮上限
            "memory_max_mib": self.default_limit_bytes // MIB,
            "quota_total_mib": total // MIB,
            "quota_used_mib": used // MIB,
            "quota_free_mib": (total - used) // MIB,
            "active": [
                {
                    "slot": slot,
                    "job_id": info["job_id"],
                    "phase": info["phase"],
                    "limit_mib": info["limit_bytes"] // MIB,
                }
                for slot, info in sorted(self._active.items())
            ],
        }

    def get(self, job_id: str) -> Job | None:
        return self.jobs.get(job_id)

    def list(self) -> list[Job]:
        return sorted(self.jobs.values(), key=lambda j: j.created_at, reverse=True)

    # ---------- 起作业 ----------

    async def start(
        self,
        memory_mib: int,
        hold_seconds: float,
        memory_limit_mib: int | None = None,
    ) -> Job:
        if not self.backend.status.accepting:
            raise NotAccepting

        # 1) 解析本轮上限：省略则用默认（均分）值
        if memory_limit_mib is None:
            limit_bytes = self.default_limit_bytes
            effective_limit_mib = limit_bytes // MIB
        else:
            effective_limit_mib = memory_limit_mib
            limit_bytes = memory_limit_mib * MIB
            if limit_bytes < MIN_SLOT_BYTES:
                # API 层 pydantic(ge=48) 已拦，这里给直接调用方兜底
                raise ValueError(f"本轮内存上限至少 {MIN_SLOT_BYTES // MIB}MiB")

        job_id = uuid.uuid4().hex[:12]

        # 2) 锁内做双重准入判断并预留槽位 + 额度（setting 阶段即占额）
        async with self._lock:
            total = self.quota_total_bytes
            if self._used_quota_bytes + limit_bytes > total:
                raise QuotaExceeded(limit_bytes, self._used_quota_bytes, total)
            try:
                slot = self._free.get_nowait()
            except asyncio.QueueEmpty:
                raise NoSlotAvailable
            self._used_quota_bytes += limit_bytes
            self._active[slot] = {
                "job_id": None,
                "limit_bytes": limit_bytes,
                "phase": "setting",
            }

        # 3) 锁外设置本轮 cgroup 上限（慢操作）：另一空闲槽位在此期间仍能接单
        handle = self._handles[slot]
        try:
            await self.backend.set_slot_limit(handle, limit_bytes)
        except LimitSetupError as exc:
            await self._release(slot, limit_bytes)
            raise LimitSetupFailed(str(exc)) from exc

        # 4) 上限生效后才登记作业并启动进程（复用槽位同样走先设后启）
        job = Job(
            id=job_id,
            slot=slot,
            memory_mib=memory_mib,
            hold_seconds=hold_seconds,
            memory_limit_mib=effective_limit_mib,
        )
        try:
            async with self._lock:
                self._active[slot] = {
                    "job_id": job_id,
                    "limit_bytes": limit_bytes,
                    "phase": "running",
                }
                self.jobs[job_id] = job
            task = asyncio.create_task(self._run(job))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        except Exception:
            # 登记/起任务本身失败（极少见）：保证槽位与额度不泄漏
            await self._release(slot, limit_bytes)
            raise
        return job

    async def _release(self, slot: int, limit_bytes: int) -> None:
        """退回本轮槽位与额度（设置失败或归因落定时调用）。"""
        async with self._lock:
            self._active.pop(slot, None)
            self._used_quota_bytes = max(0, self._used_quota_bytes - limit_bytes)
            self._free.put_nowait(slot)

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
        except Exception as exc:  # 兜底：任何异常都不能卡住槽位与额度
            job.output_tail.append(f"runner error: {exc!r}")
        finally:
            await self._finalize(job, handle)

    async def _finalize(self, job: Job, handle: SlotHandle) -> None:
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
        # 归因结论已出 —— 槽位与额度此刻才一起交还，可供下一轮使用
        await self._release(job.slot, job.memory_limit_mib * MIB)

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

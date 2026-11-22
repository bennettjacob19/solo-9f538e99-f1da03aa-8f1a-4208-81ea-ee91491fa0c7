"""额度/槽位调度单测（纯 stdlib）：python3 -m unittest tests.test_quota -v

用内存假后端验证 JobManager 的准入账，不碰真实 cgroup、不起 HTTP 服务：
  - 额度不足 / 槽位占满分别 409 语义（QuotaExceeded / NoSlotAvailable）
  - 正在设置上限的轮次按本轮上限占额
  - 慢设置期间另一空闲槽可并行接单
  - 设置失败退回槽位与额度，之后可立即再接单
  - 归因落定后槽位与额度一起交还
  - 省略本轮上限时用默认均分值
"""
import asyncio
import sys
import unittest
from pathlib import Path

from app.cgroups import MIB, BackendStatus, LimitSetupError, SlotHandle
from app.jobs import (
    JobManager,
    LimitSetupFailed,
    NoSlotAvailable,
    QuotaExceeded,
)


class FakeBackend:
    mode = "fake"

    def __init__(self, slots=2, quota_mib=160, default_mib=80,
                 delay=0.0, fail_limit_mib=None):
        self.handles = [
            SlotHandle(i, Path(f"/tmp/fake-gsb/slot{i}"),
                       Path(f"/tmp/fake-gsb/slot{i}/events"), default_mib * MIB)
            for i in range(slots)
        ]
        self.status = BackendStatus(
            mode=self.mode,
            accepting=True,
            slot_count=slots,
            slot_memory_max_bytes=default_mib * MIB,
            quota_total_bytes=quota_mib * MIB,
            reserve_bytes=0,
        )
        self.delay = delay
        self.fail_limit_mib = fail_limit_mib
        self.set_calls: list[tuple[int, int]] = []

    async def set_slot_limit(self, handle: SlotHandle, limit_bytes: int) -> None:
        self.set_calls.append((handle.index, limit_bytes // MIB))
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail_limit_mib is not None and limit_bytes // MIB == self.fail_limit_mib:
            raise LimitSetupError(f"注入失败：slot{handle.index} limit={limit_bytes // MIB}MiB")
        handle.memory_max_bytes = limit_bytes

    def read_oom_kill(self, handle: SlotHandle):
        return 0

    def child_argv(self, handle: SlotHandle, inner):
        # 不跑样例作业：起一个立即正常退出的解释器，仅用于驱动槽位交还流程
        return [sys.executable, "-c", "import sys; sys.exit(0)"]

    def child_env(self, handle):
        return {}

    def cleanup(self):
        pass


class QuotaTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.backend = FakeBackend(slots=2, quota_mib=160, default_mib=80)
        self.mgr = JobManager(self.backend)

    async def asyncTearDown(self):
        await self.mgr.shutdown()

    async def test_default_limit_when_omitted(self):
        job = await self.mgr.start(memory_mib=8, hold_seconds=1, memory_limit_mib=None)
        self.assertEqual(job.memory_limit_mib, 80)
        self.assertEqual(self.mgr._used_quota_bytes, 80 * MIB)

    async def test_quota_exceeded_before_slot_check(self):
        # 先占 120，第二轮申请 48：总额 168 > 160 -> 额度不足（此时尚有空闲槽）
        await self.mgr.start(8, 60, memory_limit_mib=120)
        with self.assertRaises(QuotaExceeded):
            await self.mgr.start(8, 60, memory_limit_mib=48)
        # 被拒不占任何资源
        snap = self.mgr.slots_snapshot()
        self.assertEqual(snap["quota_used_mib"], 120)
        self.assertEqual(snap["free"], 1)

    async def test_slot_exhausted_distinct_from_quota(self):
        # 48+48=96 <= 160：第三轮额度够但没有空槽 -> NoSlotAvailable
        await self.mgr.start(8, 60, memory_limit_mib=48)
        await self.mgr.start(8, 60, memory_limit_mib=48)
        with self.assertRaises(NoSlotAvailable):
            await self.mgr.start(8, 60, memory_limit_mib=48)
        snap = self.mgr.slots_snapshot()
        self.assertEqual(snap["free"], 0)
        self.assertEqual(snap["quota_used_mib"], 96)

    async def test_concurrent_starts_during_slow_setup(self):
        # 两个槽位的设置都很慢：并发接单应分别落到不同槽、各占各的额度
        self.backend.delay = 0.2
        j1, j2 = await asyncio.gather(
            self.mgr.start(8, 60, memory_limit_mib=64),
            self.mgr.start(8, 60, memory_limit_mib=48),
        )
        self.assertNotEqual(j1.slot, j2.slot)
        snap = self.mgr.slots_snapshot()
        self.assertEqual(snap["quota_used_mib"], 112)
        self.assertEqual(snap["free"], 0)
        self.assertEqual(sorted(c[0] for c in self.backend.set_calls), [0, 1])

    async def test_setup_failure_rolls_back_slot_and_quota(self):
        self.backend.fail_limit_mib = 50
        with self.assertRaises(LimitSetupFailed):
            await self.mgr.start(8, 60, memory_limit_mib=50)
        snap = self.mgr.slots_snapshot()
        self.assertEqual(snap["quota_used_mib"], 0)
        self.assertEqual(snap["free"], 2)
        self.assertEqual(snap["active"], [])
        # 回退后槽位与额度立即可再用
        job = await self.mgr.start(8, 60, memory_limit_mib=48)
        self.assertEqual(job.memory_limit_mib, 48)
        self.assertEqual(self.mgr.slots_snapshot()["quota_used_mib"], 48)

    async def test_quota_and_slot_released_after_verdict(self):
        job = await self.mgr.start(8, 60, memory_limit_mib=64)
        # 假作业立即退出；等 runner 收尾（归因 + 交还）
        deadline = asyncio.get_running_loop().time() + 5
        while self.mgr.get(job.id).state != "finished" and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)
        self.assertEqual(self.mgr.get(job.id).state, "finished")
        self.assertEqual(self.mgr.get(job.id).verdict, "normal_exit")
        snap = self.mgr.slots_snapshot()
        self.assertEqual(snap["quota_used_mib"], 0)
        self.assertEqual(snap["free"], 2)

    async def test_reused_slot_sets_new_limit_before_start(self):
        # 先占满两槽（各 48MiB），等它们结束；再连续起两轮，其中一轮必然复用
        # 旧槽位，并必须在启动进程前把该槽上限重写为本轮请求值
        jobs = await asyncio.gather(
            self.mgr.start(8, 60, memory_limit_mib=48),
            self.mgr.start(8, 60, memory_limit_mib=48),
        )
        deadline = asyncio.get_running_loop().time() + 5
        while any(self.mgr.get(j.id).state != "finished" for j in jobs) \
                and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)
        self.backend.set_calls.clear()
        j2 = await self.mgr.start(8, 60, memory_limit_mib=64)
        self.assertIn((j2.slot, 64), self.backend.set_calls)
        self.assertEqual(self.mgr._handles[j2.slot].memory_max_bytes, 64 * MIB)
        self.assertEqual(j2.memory_limit_mib, 64)


if __name__ == "__main__":
    unittest.main()

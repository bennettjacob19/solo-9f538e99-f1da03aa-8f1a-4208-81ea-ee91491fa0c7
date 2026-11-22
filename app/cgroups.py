"""cgroup v2 后端。

- CgroupV2Backend：真实后端。检测本容器 cgroup 的内存上限是否生效、
  能否创建子 cgroup；为每一“轮”作业准备一个独立 slot cgroup，并在
  每轮作业启动前按本轮请求重写该 slot 的 memory.max。
- SimulatedBackend：模拟后端（GSB_SIMULATE=1）。不碰 /sys/fs/cgroup，
  用普通目录模拟 memory.events / memory.max，供无特权环境跑通完整流程。

额度模型（两后端一致）：
  总额度 quota = 容器内存上限(memory.max) - 服务预留(GSB_RESERVE_MIB)
  每轮作业按“本轮实际生效的上限”占用额度；正在设置上限或尚未出归因
  结论的作业都持续占额，结论落定后随槽位一起释放。

只有真实后端且全部前置条件满足时，BackendStatus.accepting 才为 True，
此时 API 才接作业（“内存上限生效才接作业”）。
"""
from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

MIB = 1024 * 1024
CGROUP_ROOT = Path("/sys/fs/cgroup")
BASE_DIR_NAME = "gsb"          # 实验 cgroup 根目录名（建在本容器 cgroup 之下）
MIN_SLOT_BYTES = 48 * MIB      # 单轮上限下限（要容得下 Python 解释器 + 样例占用）


class LimitSetupError(Exception):
    """设置本轮 cgroup memory.max 失败（调用方须退回本轮槽位与额度）。"""


@dataclass
class SlotHandle:
    index: int
    path: Path                 # 本轮 cgroup 目录
    events_file: Path          # 本轮 memory.events
    memory_max_bytes: int      # 当前（最近一次设置的）本轮上限


@dataclass
class BackendStatus:
    mode: str                              # "cgroupv2" | "simulation"
    accepting: bool                        # 是否接作业
    reasons: list[str] = field(default_factory=list)   # 不接作业的阻断原因
    notes: list[str] = field(default_factory=list)     # 提示信息（不阻断）
    cgroup_path: str | None = None
    memory_limit_bytes: int | None = None
    memory_current_bytes: int | None = None
    slot_count: int = 0
    slot_memory_max_bytes: int | None = None           # 省略本轮上限时的默认值（均分）
    quota_total_bytes: int | None = None               # 容器上限扣预留后的总额度
    reserve_bytes: int | None = None
    # ---- 模拟后端演示用参数（真实后端无意义）----
    set_delay_seconds: float = 0.0                      # 模拟“设置 cgroup 上限较慢”
    fail_limit_mib: int | None = None                   # 注入设置失败的本轮上限（MiB）


def read_oom_kill(events_file: Path) -> int | None:
    """读取 memory.events 里的 oom_kill 计数；读不到返回 None（证据缺失）。"""
    try:
        for line in events_file.read_text().splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[0] == "oom_kill":
                return int(parts[1])
        return None
    except (OSError, ValueError):
        return None


class CgroupV2Backend:
    mode = "cgroupv2"

    def __init__(self, slots: int = 2, reserve_mib: int = 96):
        self.slots = slots
        self.reserve_bytes = reserve_mib * MIB
        self.base: Path | None = None
        self.handles: list[SlotHandle] = []
        # 总额度与默认单轮上限在 _setup 里按容器实际上限计算
        self.quota_bytes: int | None = None
        self.default_slot_bytes: int | None = None
        self.status = self._setup()

    # ---------- 初始化检测 ----------

    @staticmethod
    def _own_cgroup_path() -> tuple[str | None, str | None]:
        try:
            for line in Path("/proc/self/cgroup").read_text().splitlines():
                if line.startswith("0::"):
                    return line[3:] or "/", None
            return None, "未在 /proc/self/cgroup 找到 cgroup v2 条目（0:: 开头）"
        except OSError as exc:
            return None, f"读取 /proc/self/cgroup 失败: {exc}"

    def _setup(self) -> BackendStatus:
        reasons: list[str] = []
        notes: list[str] = []
        own, err = self._own_cgroup_path()
        if own is None:
            return BackendStatus(self.mode, False, [err or "未知错误"])
        own_dir = CGROUP_ROOT if own == "/" else CGROUP_ROOT / own.lstrip("/")

        limit: int | None = None
        try:
            raw = (own_dir / "memory.max").read_text().strip()
            limit = None if raw == "max" else int(raw)
        except OSError as exc:
            reasons.append(f"读取 {own_dir}/memory.max 失败: {exc}")
        if limit is None:
            reasons.append("内存上限未生效（memory.max 为 max 或不可读），按规则拒绝接作业")

        current: int | None = None
        try:
            current = int((own_dir / "memory.current").read_text().strip())
        except OSError:
            pass

        if limit is not None:
            # 总额度 = 容器上限 - 服务预留
            self.quota_bytes = limit - self.reserve_bytes
            # 省略本轮上限时的默认值：总额度按槽位均分（不低于 48MiB 下限）
            self.default_slot_bytes = max(MIN_SLOT_BYTES, self.quota_bytes // self.slots)
            # 1) 本容器 cgroup 的 subtree_control 启用 memory（容器内是命名空间根，不受
            #    no-internal-process 约束）
            try:
                sc = (own_dir / "cgroup.subtree_control").read_text()
                if "memory" not in sc.split():
                    (own_dir / "cgroup.subtree_control").write_text("+memory")
            except OSError as exc:
                reasons.append(f"无法在 {own_dir} 启用 memory 控制器（cgroup fs 只读？）: {exc}")
            # 2) 建实验根目录并下放 memory 控制器
            base = own_dir / BASE_DIR_NAME
            if not reasons:
                try:
                    base.mkdir(exist_ok=True)
                    sc = (base / "cgroup.subtree_control").read_text()
                    if "memory" not in sc.split():
                        (base / "cgroup.subtree_control").write_text("+memory")
                except OSError as exc:
                    reasons.append(f"无法创建/配置实验 cgroup 目录 {base}: {exc}")
            # 3) 每轮一个 slot cgroup，先写默认（均分）上限；作业启动前会按本轮请求重写
            if not reasons:
                try:
                    for i in range(self.slots):
                        d = base / f"slot{i}"
                        d.mkdir(exist_ok=True)
                        (d / "memory.max").write_text(str(self.default_slot_bytes))
                        self.handles.append(
                            SlotHandle(i, d, d / "memory.events", self.default_slot_bytes)
                        )
                    self.base = base
                    notes.append(
                        f"默认单轮上限 memory.max = {self.default_slot_bytes // MIB}MiB"
                        f"（容器上限 {limit // MIB}MiB 预留 {self.reserve_bytes // MIB}MiB 后"
                        f"总额度 {self.quota_bytes // MIB}MiB，均分 {self.slots} 轮）"
                    )
                except OSError as exc:
                    reasons.append(f"初始化 slot cgroup 失败: {exc}")
                    self.handles.clear()

        return BackendStatus(
            mode=self.mode,
            accepting=not reasons,
            reasons=reasons,
            notes=notes,
            cgroup_path=str(own_dir),
            memory_limit_bytes=limit,
            memory_current_bytes=current,
            slot_count=len(self.handles),
            slot_memory_max_bytes=self.default_slot_bytes,
            quota_total_bytes=self.quota_bytes,
            reserve_bytes=self.reserve_bytes,
        )

    # ---------- 运行期接口 ----------

    async def set_slot_limit(self, handle: SlotHandle, limit_bytes: int) -> None:
        """按本轮请求重写 slot 的 memory.max（复用槽位也必须先设后启）。

        真实 cgroup 写文件可能较慢，放到线程里执行，避免阻塞事件循环，
        保证另一空闲槽位在本槽设置期间仍能接单。
        """
        def _write() -> None:
            try:
                (handle.path / "memory.max").write_text(str(limit_bytes))
            except OSError as exc:
                raise LimitSetupError(
                    f"写入 {handle.path}/memory.max={limit_bytes} 失败: {exc}"
                ) from exc
        await asyncio.to_thread(_write)
        handle.memory_max_bytes = limit_bytes

    def read_oom_kill(self, handle: SlotHandle) -> int | None:
        return read_oom_kill(handle.events_file)

    def child_argv(self, handle: SlotHandle, inner: list[str]) -> list[str]:
        # 先让 shell 把自己迁入本轮 cgroup，再 exec 真正的作业进程；
        # 迁入失败以约定码 69 退出（归因时按证据缺失处理）。
        script = 'printf %s $$ > "$1/cgroup.procs" || exit 69; shift; exec "$@"'
        return ["/bin/sh", "-c", script, "gsb-join", str(handle.path), *inner]

    def child_env(self, handle: SlotHandle) -> dict[str, str]:
        return {}

    def cleanup(self) -> None:
        for h in self.handles:
            try:
                h.path.rmdir()  # cgroupfs 目录只能 rmdir，且需无进程无子项
            except OSError:
                pass
        if self.base is not None:
            try:
                self.base.rmdir()
            except OSError:
                pass


class SimulatedBackend:
    """模拟后端：目录是普通文件夹，memory.events / memory.max 是普通文件。

    超限“OOM”由样例作业自查复现：先自增模拟 memory.events 的 oom_kill，
    再对自己发 SIGKILL —— 与内核 OOM 在 cgroup v2 下的可观测特征一致，
    用于无特权环境（如本机、CI）跑通交错自测。真实 OOM 路径仍以
    docker compose 环境为准。

    另模拟两件真实后端的运行期行为，供演示与自测：
      - set_delay_seconds：每轮设置上限的耗时（真实 cgroup 写 memory.max 较慢）；
      - fail_limit_mib：请求该上限（MiB）时注入一次设置失败，验证失败退额流程。
    """

    mode = "simulation"

    def __init__(
        self,
        slots: int = 2,
        limit_mib: int = 256,
        reserve_mib: int = 96,
        set_delay_seconds: float | None = None,
        fail_limit_mib: int | None = None,
    ):
        self.limit_bytes = limit_mib * MIB
        self.reserve_bytes = reserve_mib * MIB
        self.quota_bytes = self.limit_bytes - self.reserve_bytes
        self.default_slot_bytes = max(MIN_SLOT_BYTES, self.quota_bytes // slots)
        if set_delay_seconds is None:
            set_delay_seconds = float(os.environ.get("GSB_SIM_SET_DELAY_SECONDS", "0.5"))
        if fail_limit_mib is None:
            raw = os.environ.get("GSB_SIM_FAIL_LIMIT_MIB", "")
            fail_limit_mib = int(raw) if raw.strip() else None
        self.set_delay_seconds = set_delay_seconds
        self.fail_limit_mib = fail_limit_mib

        self.root = Path(tempfile.mkdtemp(prefix="gsb-sim-cgroup-"))
        self.handles: list[SlotHandle] = []
        for i in range(slots):
            d = self.root / f"slot{i}"
            d.mkdir()
            (d / "memory.max").write_text(str(self.default_slot_bytes))
            (d / "memory.events").write_text(
                "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n"
            )
            self.handles.append(SlotHandle(i, d, d / "memory.events", self.default_slot_bytes))
        notes = [
            "模拟后端：未使用内核 cgroup 强制，OOM 由样例作业按限制自查复现，仅供流程自测",
            f"默认单轮上限 memory.max = {self.default_slot_bytes // MIB}MiB"
            f"（模拟容器上限 {limit_mib}MiB 预留 {reserve_mib}MiB，"
            f"总额度 {self.quota_bytes // MIB}MiB）",
            f"每轮设置上限模拟耗时 {self.set_delay_seconds}s（GSB_SIM_SET_DELAY_SECONDS 可调）",
        ]
        if fail_limit_mib is not None:
            notes.append(
                f"已注入设置失败：本轮上限请求 {fail_limit_mib}MiB 时 memory.max 写入报错"
                "（GSB_SIM_FAIL_LIMIT_MIB 配置）"
            )
        self.status = BackendStatus(
            mode=self.mode,
            accepting=True,
            reasons=[],
            notes=notes,
            cgroup_path=str(self.root),
            memory_limit_bytes=self.limit_bytes,
            memory_current_bytes=None,
            slot_count=slots,
            slot_memory_max_bytes=self.default_slot_bytes,
            quota_total_bytes=self.quota_bytes,
            reserve_bytes=self.reserve_bytes,
            set_delay_seconds=self.set_delay_seconds,
            fail_limit_mib=self.fail_limit_mib,
        )

    async def set_slot_limit(self, handle: SlotHandle, limit_bytes: int) -> None:
        # 模拟真实 cgroup 设置较慢：期间不持锁，另一空闲槽位仍可并行接单
        if self.set_delay_seconds:
            await asyncio.sleep(self.set_delay_seconds)
        if self.fail_limit_mib is not None and limit_bytes // MIB == self.fail_limit_mib:
            raise LimitSetupError(
                f"模拟注入：slot{handle.index} 设置 memory.max="
                f"{limit_bytes // MIB}MiB 失败（GSB_SIM_FAIL_LIMIT_MIB）"
            )
        try:
            (handle.path / "memory.max").write_text(str(limit_bytes))
        except OSError as exc:
            raise LimitSetupError(f"写入模拟 memory.max 失败: {exc}") from exc
        handle.memory_max_bytes = limit_bytes

    def read_oom_kill(self, handle: SlotHandle) -> int | None:
        return read_oom_kill(handle.events_file)

    def child_argv(self, handle: SlotHandle, inner: list[str]) -> list[str]:
        return list(inner)

    def child_env(self, handle: SlotHandle) -> dict[str, str]:
        return {
            "GSB_SIM_LIMIT_BYTES": str(handle.memory_max_bytes),
            "GSB_SIM_EVENTS_FILE": str(handle.events_file),
        }

    def cleanup(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

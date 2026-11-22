"""cgroup v2 后端。

- CgroupV2Backend：真实后端。检测本容器 cgroup 的内存上限是否生效、
  能否创建子 cgroup；为每一“轮”作业准备一个独立 slot cgroup（各自设
  memory.max），并负责把作业进程迁进去。
- SimulatedBackend：模拟后端（GSB_SIMULATE=1）。不碰 /sys/fs/cgroup，
  用普通目录模拟 memory.events，供无特权环境跑通完整流程的自测。

只有真实后端且全部前置条件满足时，BackendStatus.accepting 才为 True，
此时 API 才接作业（“内存上限生效才接作业”）。
"""
from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

MIB = 1024 * 1024
CGROUP_ROOT = Path("/sys/fs/cgroup")
BASE_DIR_NAME = "gsb"          # 实验 cgroup 根目录名（建在本容器 cgroup 之下）
MIN_SLOT_BYTES = 48 * MIB      # 单轮上限下限（要容得下 Python 解释器 + 样例占用）


@dataclass
class SlotHandle:
    index: int
    path: Path                 # 本轮 cgroup 目录
    events_file: Path          # 本轮 memory.events
    memory_max_bytes: int


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
    slot_memory_max_bytes: int | None = None


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
            # 3) 每轮一个 slot cgroup，各自设 memory.max
            if not reasons:
                slot_max = max(MIN_SLOT_BYTES, (limit - self.reserve_bytes) // self.slots)
                try:
                    for i in range(self.slots):
                        d = base / f"slot{i}"
                        d.mkdir(exist_ok=True)
                        (d / "memory.max").write_text(str(slot_max))
                        self.handles.append(SlotHandle(i, d, d / "memory.events", slot_max))
                    self.base = base
                    notes.append(
                        f"单轮上限 slot memory.max = {slot_max // MIB}MiB"
                        f"（容器上限 {limit // MIB}MiB 预留 {self.reserve_bytes // MIB}MiB 后均分 {self.slots} 轮）"
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
            slot_memory_max_bytes=self.handles[0].memory_max_bytes if self.handles else None,
        )

    # ---------- 运行期接口 ----------

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
    """模拟后端：目录是普通文件夹，memory.events 是普通文件。

    超限“OOM”由样例作业自查复现：先自增模拟 memory.events 的 oom_kill，
    再对自己发 SIGKILL —— 与内核 OOM 在 cgroup v2 下的可观测特征一致，
    用于无特权环境（如本机、CI）跑通交错自测。真实 OOM 路径仍以
    docker compose 环境为准。
    """

    mode = "simulation"

    def __init__(self, slots: int = 2, limit_mib: int = 256, reserve_mib: int = 96):
        self.limit_bytes = limit_mib * MIB
        self.root = Path(tempfile.mkdtemp(prefix="gsb-sim-cgroup-"))
        slot_max = max(MIN_SLOT_BYTES, (self.limit_bytes - reserve_mib * MIB) // slots)
        self.handles: list[SlotHandle] = []
        for i in range(slots):
            d = self.root / f"slot{i}"
            d.mkdir()
            (d / "memory.max").write_text(str(slot_max))
            (d / "memory.events").write_text(
                "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n"
            )
            self.handles.append(SlotHandle(i, d, d / "memory.events", slot_max))
        self.status = BackendStatus(
            mode=self.mode,
            accepting=True,
            reasons=[],
            notes=[
                "模拟后端：未使用内核 cgroup 强制，OOM 由样例作业按限制自查复现，仅供流程自测",
                f"单轮上限 slot memory.max = {slot_max // MIB}MiB（模拟容器上限 {limit_mib}MiB）",
            ],
            cgroup_path=str(self.root),
            memory_limit_bytes=self.limit_bytes,
            memory_current_bytes=None,
            slot_count=slots,
            slot_memory_max_bytes=slot_max,
        )

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

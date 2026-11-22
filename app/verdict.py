"""归因判定：区分 正常退出 / OOM / 主动强停 / 原因不明。

纯函数、无第三方依赖，便于单测。判定依据三类证据：
  1. 进程退出状态（subprocess 约定：负数 = 被 -信号 杀死，如 -9 即 SIGKILL）
  2. 本轮 cgroup memory.events 中 oom_kill 的前后读数（None 表示读数缺失）
  3. 本轮是否收到过主动强停请求

证据缺失或相互冲突时一律报“原因不明”（unknown）。
"""
from __future__ import annotations

import signal

NORMAL_EXIT = "normal_exit"
OOM_KILL = "oom_kill"
FORCE_STOPPED = "force_stopped"
UNKNOWN = "unknown"

ALL_VERDICTS = (NORMAL_EXIT, OOM_KILL, FORCE_STOPPED, UNKNOWN)

# 包装 shell 未能把作业进程迁入本轮 cgroup 时的约定退出码（样例作业自身不会用该码）
JOIN_CGROUP_FAILED_RC = 69


def decide_verdict(
    *,
    returncode: int | None,
    oom_kill_before: int | None,
    oom_kill_after: int | None,
    stop_requested: bool,
) -> tuple[str, str]:
    """返回 (verdict, 中文理由)。"""
    delta: int | None = None
    if oom_kill_before is not None and oom_kill_after is not None:
        delta = oom_kill_after - oom_kill_before

    # ---- 证据缺失 ----
    if returncode is None:
        return UNKNOWN, "缺少进程退出状态（进程未被正常回收），无法归因"

    if returncode == JOIN_CGROUP_FAILED_RC:
        return UNKNOWN, "作业未能进入本轮 cgroup（cgroup.procs 写入失败），限制与计数均不可信"

    # ---- 进程自行退出（退出码 >= 0）----
    if returncode >= 0:
        if delta:
            return UNKNOWN, (
                f"进程自行退出（退出码 {returncode}），但本轮 oom_kill 增加了 {delta}，证据冲突"
            )
        note = "；强停请求在进程退出后才到达，未生效" if stop_requested else ""
        return NORMAL_EXIT, f"进程自行退出，退出码 {returncode}{note}"

    sig = -returncode

    # ---- 被 SIGKILL 杀死：OOM 与主动强停的唯一难分情形，靠计数器与强停记录区分 ----
    if sig == signal.SIGKILL:
        if delta is None:
            return UNKNOWN, "进程被 SIGKILL，但本轮 oom_kill 读数缺失，无法区分 OOM 与主动强停"
        if delta > 0 and stop_requested:
            return UNKNOWN, (
                f"进程被 SIGKILL，本轮 oom_kill +{delta} 且存在强停请求，证据冲突，无法确定致命一击"
            )
        if delta > 0:
            return OOM_KILL, f"进程被 SIGKILL，本轮 cgroup oom_kill +{delta}，且无强停请求记录"
        if stop_requested:
            return FORCE_STOPPED, "进程被 SIGKILL，本轮 oom_kill 计数无变化，且本轮登记了强停请求"
        return UNKNOWN, "进程被 SIGKILL，但 oom_kill 无变化且本轮无强停请求记录（可能被外部杀死）"

    # ---- 被其他信号杀死 ----
    return UNKNOWN, f"进程被信号 {sig} 终止（非 SIGKILL），不在可归因范围"

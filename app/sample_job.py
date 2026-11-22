"""样例批处理作业：真实占用指定 MiB 内存并保持一段时间。

- 逐块分配、逐页写值，确保页真正提交（cgroup 才会计费）。
- 占满后保持 --hold 秒，随后自行退出（退出码 0）。
- 模拟模式（环境变量 GSB_SIM_LIMIT_BYTES 非 0 时）：占用一旦超过该限制，
  先自增模拟 memory.events 的 oom_kill，再对自己发 SIGKILL —— 复现内核
  OOM 在 cgroup v2 下的可观测特征。真实模式（docker compose）下这两个
  环境变量不存在，超限由内核直接 SIGKILL。
"""
from __future__ import annotations

import argparse
import os
import signal
import sys
import time

PAGE = 4096


def _bump_simulated_oom(events_file: str) -> None:
    try:
        with open(events_file, "r", encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        out = []
        for line in lines:
            key, _, val = line.partition(" ")
            if key == "oom_kill":
                try:
                    line = f"oom_kill {int(val) + 1}"
                except ValueError:
                    pass
            out.append(line)
        with open(events_file, "w", encoding="utf-8") as fh:
            fh.write("\n".join(out) + "\n")
    except OSError as exc:
        print(f"[sample-job] WARN: 模拟 oom_kill 计数失败: {exc}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="样例内存占用作业")
    ap.add_argument("--mib", type=int, required=True, help="要实际占用的内存 MiB")
    ap.add_argument("--hold", type=float, default=30.0, help="占满后保持秒数")
    ap.add_argument("--chunk-mib", type=int, default=4, help="每次分配的块大小 MiB")
    ap.add_argument("--step-sleep", type=float, default=0.02, help="每块之间的间隔秒")
    args = ap.parse_args()

    sim_limit = int(os.environ.get("GSB_SIM_LIMIT_BYTES", "0") or "0")
    sim_events = os.environ.get("GSB_SIM_EVENTS_FILE", "")

    target = args.mib * 1024 * 1024
    chunk = max(1, args.chunk_mib) * 1024 * 1024
    chunks: list[bytearray] = []
    alloc = 0
    print(
        f"[sample-job] pid={os.getpid()} target={args.mib}MiB hold={args.hold}s"
        + (f" sim_limit={sim_limit}" if sim_limit else ""),
        flush=True,
    )
    try:
        while alloc < target:
            n = min(chunk, target - alloc)
            block = bytearray(n)
            for i in range(0, n, PAGE):
                block[i] = 1  # 逐页触碰，真正提交内存
            chunks.append(block)
            alloc += n
            print(f"[sample-job] allocated_mib={alloc >> 20}", flush=True)
            if sim_limit and alloc > sim_limit:
                print(
                    f"[sample-job] SIMULATED-OOM: alloc={alloc}B > limit={sim_limit}B，"
                    "自增 oom_kill 后自杀（SIGKILL）",
                    flush=True,
                )
                if sim_events:
                    _bump_simulated_oom(sim_events)
                os.kill(os.getpid(), signal.SIGKILL)
            time.sleep(args.step_sleep)
    except MemoryError:
        print("[sample-job] MemoryError: 分配失败（未到目标）", flush=True)
        return 1

    print("[sample-job] target reached, holding", flush=True)
    deadline = time.monotonic() + args.hold
    while time.monotonic() < deadline:
        time.sleep(0.1)
    print("[sample-job] hold complete, exiting 0", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""端到端自测：一轮 OOM 与一轮强停交错并行，再验证槽位交还与正常退出。

前置：服务已在运行 ——
  * 真实 cgroup 环境：docker compose up --build
  * 本机无特权环境：  GSB_SIMULATE=1 python3 -m uvicorn app.main:app

用法：python3 scripts/selftest.py [--base-url http://127.0.0.1:8000]
退出码：0 全部通过；1 有用例失败；2 环境不满足（服务未接作业等）。
"""
from __future__ import annotations

import argparse
import json
import sys
import time

import httpx


def wait_finished(client: httpx.Client, base: str, job_id: str, timeout: float = 120) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"{base}/jobs/{job_id}").json()
        if body["state"] == "finished":
            return body
        time.sleep(0.5)
    raise TimeoutError(f"job {job_id} 超时未完成")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="http://127.0.0.1:8000")
    args = ap.parse_args()
    base = args.base_url.rstrip("/")
    results: list[tuple[str, bool, str]] = []

    def check(name: str, cond: bool, detail: str = "") -> None:
        results.append((name, bool(cond), detail))
        print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  -- {detail}" if detail else ""))

    with httpx.Client(timeout=10) as client:
        # 0) 等服务就绪、确认接作业
        st = None
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                st = client.get(f"{base}/status").json()
                break
            except Exception:
                time.sleep(1)
        if st is None:
            print("服务不可达")
            return 2
        print(f"== /status: mode={st['mode']} accepting={st['accepting_jobs']} "
              f"slot_max={st['slots']['memory_max_mib']}MiB ==")
        if not st["accepting_jobs"]:
            print(f"服务未接作业: {st['blocking_reasons']}")
            return 2
        slot_mib = st["slots"]["memory_max_mib"]
        if not slot_mib or slot_mib < 64:
            print(f"单轮上限 {slot_mib}MiB 太小，无法安全自测（建议容器内存上限 >= 256MiB）")
            return 2

        oom_mib = slot_mib * 2                 # 明确超限 -> 预期 OOM
        ok_mib = max(8, slot_mib // 3)         # 明确装得下 -> 用于强停轮

        # 1) 交错并行：OOM 轮 + 强停轮同时起跑
        ra = client.post(f"{base}/jobs", json={"memory_mib": oom_mib, "hold_seconds": 120})
        rb = client.post(f"{base}/jobs", json={"memory_mib": ok_mib, "hold_seconds": 120})
        check("两轮作业并行受理(201)", ra.status_code == 201 and rb.status_code == 201,
              f"got {ra.status_code}/{rb.status_code}")
        if ra.status_code != 201 or rb.status_code != 201:
            return 2
        a, b = ra.json(), rb.json()
        print(f"OOM 轮  job={a['job_id']} slot={a['slot']} 目标 {oom_mib}MiB")
        print(f"强停轮  job={b['job_id']} slot={b['slot']} 目标 {ok_mib}MiB")

        # 2) 并行上限 = 2：第三轮应立即被 409 拒绝
        r = client.post(f"{base}/jobs", json={"memory_mib": 8, "hold_seconds": 5})
        check("第三轮被拒(409，槽位未空)", r.status_code == 409, f"got {r.status_code}")

        # 3) 交错操作：OOM 轮爬升期间，强停另一轮
        time.sleep(2)
        r = client.post(f"{base}/jobs/{b['job_id']}/stop")
        check("强停请求被接受(200)", r.status_code == 200, f"got {r.status_code}")

        # 4) 等两轮各自出归因结论
        ja = wait_finished(client, base, a["job_id"])
        jb = wait_finished(client, base, b["job_id"])
        print("\n== OOM 轮结果 ==")
        print(json.dumps({k: ja[k] for k in (
            "job_id", "exit_status", "exit_signal", "stop_requested",
            "oom_kill_before", "oom_kill_after", "oom_kill_delta",
            "verdict", "verdict_reason")}, ensure_ascii=False, indent=2))
        print("== 强停轮结果 ==")
        print(json.dumps({k: jb[k] for k in (
            "job_id", "exit_status", "exit_signal", "stop_requested",
            "oom_kill_before", "oom_kill_after", "oom_kill_delta",
            "verdict", "verdict_reason")}, ensure_ascii=False, indent=2))
        print()

        check("OOM 轮归因 = oom_kill", ja["verdict"] == "oom_kill", ja["verdict_reason"] or "")
        check("OOM 轮退出信号 = SIGKILL(9)", ja["exit_signal"] == 9, f"rc={ja['exit_status']}")
        check("OOM 轮 oom_kill 计数 +1 以上", (ja["oom_kill_delta"] or 0) >= 1,
              f"delta={ja['oom_kill_delta']}")
        check("强停轮归因 = force_stopped", jb["verdict"] == "force_stopped",
              jb["verdict_reason"] or "")
        check("强停轮退出信号 = SIGKILL(9)", jb["exit_signal"] == 9, f"rc={jb['exit_status']}")
        check("强停轮 oom_kill 计数无变化", jb["oom_kill_delta"] == 0,
              f"delta={jb['oom_kill_delta']}")

        # 5) 归因结论已出 -> 槽位交还：再跑一轮短作业验证正常退出
        r = client.post(f"{base}/jobs", json={"memory_mib": 8, "hold_seconds": 2})
        check("归因后槽位交还，新作业被受理(201)", r.status_code == 201, f"got {r.status_code}")
        if r.status_code == 201:
            jc = wait_finished(client, base, r.json()["job_id"], timeout=30)
            check("短作业归因 = normal_exit", jc["verdict"] == "normal_exit",
                  jc["verdict_reason"] or "")

    failed = [n for n, ok, _ in results if not ok]
    print(f"\n== 自测完成：{len(results) - len(failed)}/{len(results)} 项通过 ==")
    if failed:
        print("失败项: " + "; ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

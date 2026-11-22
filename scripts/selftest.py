#!/usr/bin/env python3
"""端到端自测：大小两轮并行接单 + 额度/槽位 409 + 设置失败回退 + 归因与槽位交还。

前置：服务已在运行 ——
  * 真实 cgroup 环境：docker compose up --build
  * 本机无特权环境：  GSB_SIMULATE=1 python3 -m uvicorn app.main:app
    （模拟后端每轮设置上限默认耗时 0.5s，可 GSB_SIM_SET_DELAY_SECONDS 调整；
      设置失败回退用例需另设 GSB_SIM_FAIL_LIMIT_MIB=50）

用法：python3 scripts/selftest.py [--base-url http://127.0.0.1:8000]
退出码：0 全部通过；1 有用例失败；2 环境不满足（服务未接作业等）。
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
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

    with httpx.Client(timeout=30) as client:
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
        sim = st.get("simulation") or {}
        print(
            f"== /status: mode={st['mode']} accepting={st['accepting_jobs']} "
            f"quota={st['slots']['quota_total_mib']}MiB "
            f"default_limit={st['slots']['default_memory_max_mib']}MiB "
            f"slots={st['slots']['count']} =="
        )
        if not st["accepting_jobs"]:
            print(f"服务未接作业: {st['blocking_reasons']}")
            return 2
        quota_total = st["slots"]["quota_total_mib"]
        slot_count = st["slots"]["count"]
        if quota_total < 112 or slot_count < 2:
            print(f"总额度 {quota_total}MiB 或槽位 {slot_count} 不足，无法安全自测"
                  "（建议容器内存上限 >= 256MiB、槽位 >= 2）")
            return 2

        # 本轮规划：大轮 64MiB 上限（目标 96MiB -> OOM），小轮 48MiB 上限（强停）
        big_limit, big_target = 64, 96
        small_limit, small_target = 48, 8

        # 1) 入参校验：本轮上限必须是 >=48 的整数 MiB
        r = client.post(f"{base}/jobs", json={
            "memory_mib": 8, "memory_limit_mib": 47, "hold_seconds": 5})
        check("本轮上限 47MiB 被拒(422，下限 48)", r.status_code == 422, f"got {r.status_code}")
        r = client.post(f"{base}/jobs", json={
            "memory_mib": 8, "memory_limit_mib": 48.5, "hold_seconds": 5})
        check("非整数本轮上限被拒(422)", r.status_code == 422, f"got {r.status_code}")

        # 2) 额度不足 -> 409（此刻无作业在跑，申请超过总额度）
        r = client.post(f"{base}/jobs", json={
            "memory_mib": 8, "memory_limit_mib": quota_total + 64, "hold_seconds": 5})
        check("超额申请被拒(409，额度不足且不占资源)", r.status_code == 409,
              f"got {r.status_code}: {r.text[:160]}")
        st = client.get(f"{base}/status").json()
        check("被拒后额度与槽位原样不变",
              st["slots"]["quota_used_mib"] == 0 and st["slots"]["free"] == slot_count,
              f"used={st['slots']['quota_used_mib']} free={st['slots']['free']}")

        # 3) 大小两轮并行接单：大轮在“设置上限”窗口内时，小轮仍被另一空闲槽受理
        holder: dict = {}

        def post_big() -> None:
            holder["resp"] = client.post(f"{base}/jobs", json={
                "memory_mib": big_target,
                "memory_limit_mib": big_limit,
                "hold_seconds": 120,
            })

        t = threading.Thread(target=post_big)
        t.start()
        time.sleep(0.15)  # 大轮正处于 setting（模拟设置耗时 >=0.5s；真实 cgroup 也可能较慢）
        rb = client.post(f"{base}/jobs", json={
            "memory_mib": small_target,
            "memory_limit_mib": small_limit,
            "hold_seconds": 120,
        })
        t.join()
        ra = holder["resp"]
        check("设置窗口内另一空闲槽并行受理，大小两轮均 201",
              ra.status_code == 201 and rb.status_code == 201,
              f"got {ra.status_code}/{rb.status_code}")
        if ra.status_code != 201 or rb.status_code != 201:
            return 2
        a, b = ra.json(), rb.json()
        print(f"大轮(OOM) job={a['job_id']} slot={a['slot']} "
              f"目标 {big_target}MiB / 本轮上限 {a['memory_limit_mib']}MiB")
        print(f"小轮(强停) job={b['job_id']} slot={b['slot']} "
              f"目标 {small_target}MiB / 本轮上限 {b['memory_limit_mib']}MiB")
        check("两轮落在不同槽位", a["slot"] != b["slot"], f"{a['slot']} vs {b['slot']}")
        check("作业号可查本轮实际生效上限",
              a["memory_limit_mib"] == big_limit and b["memory_limit_mib"] == small_limit
              and client.get(f"{base}/jobs/{a['job_id']}").json()["memory_limit_mib"] == big_limit)

        # 4) /status 显示剩余额度（112/160，剩 48）与两个占用槽
        st = client.get(f"{base}/status").json()
        check("/status 剩余额度 = 总额度-两轮上限",
              st["slots"]["quota_used_mib"] == big_limit + small_limit
              and st["slots"]["quota_free_mib"] == quota_total - big_limit - small_limit,
              f"used={st['slots']['quota_used_mib']} free={st['slots']['quota_free_mib']}")
        check("/status 两槽均忙", st["slots"]["free"] == 0 and len(st["slots"]["busy_slots"]) == 2)

        # 5) 槽位占满 -> 409（申请 48MiB 额度刚好够，但没有空槽）
        r = client.post(f"{base}/jobs", json={
            "memory_mib": 8, "memory_limit_mib": small_limit, "hold_seconds": 5})
        check("槽位占满被拒(409)", r.status_code == 409, f"got {r.status_code}")

        # 6) 交错操作：大轮爬升期间，强停小轮
        time.sleep(1)
        r = client.post(f"{base}/jobs/{b['job_id']}/stop")
        check("强停请求被接受(200)", r.status_code == 200, f"got {r.status_code}")

        # 7) 等两轮各自出归因结论
        ja = wait_finished(client, base, a["job_id"])
        jb = wait_finished(client, base, b["job_id"])
        print("\n== 大轮(OOM)结果 ==")
        print(json.dumps({k: ja[k] for k in (
            "job_id", "memory_limit_mib", "exit_status", "exit_signal", "stop_requested",
            "oom_kill_before", "oom_kill_after", "oom_kill_delta",
            "verdict", "verdict_reason")}, ensure_ascii=False, indent=2))
        print("== 小轮(强停)结果 ==")
        print(json.dumps({k: jb[k] for k in (
            "job_id", "memory_limit_mib", "exit_status", "exit_signal", "stop_requested",
            "oom_kill_before", "oom_kill_after", "oom_kill_delta",
            "verdict", "verdict_reason")}, ensure_ascii=False, indent=2))
        print()

        check("大轮归因 = oom_kill", ja["verdict"] == "oom_kill", ja["verdict_reason"] or "")
        check("大轮退出信号 = SIGKILL(9)", ja["exit_signal"] == 9, f"rc={ja['exit_status']}")
        check("大轮 oom_kill 计数 +1 以上", (ja["oom_kill_delta"] or 0) >= 1,
              f"delta={ja['oom_kill_delta']}")
        check("小轮归因 = force_stopped", jb["verdict"] == "force_stopped",
              jb["verdict_reason"] or "")
        check("小轮退出信号 = SIGKILL(9)", jb["exit_signal"] == 9, f"rc={jb['exit_status']}")
        check("小轮 oom_kill 计数无变化", jb["oom_kill_delta"] == 0,
              f"delta={jb['oom_kill_delta']}")

        # 8) 归因后槽位与额度一起交还
        st = client.get(f"{base}/status").json()
        check("结论落定后额度全额交还",
              st["slots"]["quota_used_mib"] == 0
              and st["slots"]["quota_free_mib"] == quota_total,
              f"used={st['slots']['quota_used_mib']}")
        check("结论落定后槽位全额交还", st["slots"]["free"] == slot_count)

        # 9) 复用槽位：省略本轮上限 -> 默认均分上限；正常退出
        r = client.post(f"{base}/jobs", json={"memory_mib": 8, "hold_seconds": 2})
        check("省略本轮上限用默认值并被受理(201)", r.status_code == 201, f"got {r.status_code}")
        if r.status_code == 201:
            jc = r.json()
            default_mib = st["slots"]["default_memory_max_mib"]
            check("默认上限 = 总额度按槽位均分", jc["memory_limit_mib"] == default_mib,
                  f"{jc['memory_limit_mib']} vs {default_mib}")
            jd = wait_finished(client, base, jc["job_id"], timeout=30)
            check("短作业归因 = normal_exit", jd["verdict"] == "normal_exit",
                  jd["verdict_reason"] or "")

        # 10) 设置失败回退（仅模拟后端且配置了 GSB_SIM_FAIL_LIMIT_MIB 时可演示）
        fail_mib = sim.get("fail_limit_mib")
        if st["mode"] == "simulation" and fail_mib:
            r = client.post(f"{base}/jobs", json={
                "memory_mib": 8, "memory_limit_mib": fail_mib, "hold_seconds": 5})
            check("设置失败返回错误(500)", r.status_code == 500,
                  f"got {r.status_code}: {r.text[:160]}")
            st2 = client.get(f"{base}/status").json()
            check("设置失败后槽位与额度已退回",
                  st2["slots"]["quota_used_mib"] == 0
                  and st2["slots"]["free"] == slot_count,
                  f"used={st2['slots']['quota_used_mib']} free={st2['slots']['free']}")
            r = client.post(f"{base}/jobs", json={
                "memory_mib": 8, "memory_limit_mib": 48, "hold_seconds": 2})
            check("回退后槽位可立即再接单(201)", r.status_code == 201, f"got {r.status_code}")
            if r.status_code == 201:
                wait_finished(client, base, r.json()["job_id"], timeout=30)
        else:
            print("[SKIP] 设置失败回退：当前后端未注入失败"
                  "（模拟后端可用 GSB_SIM_FAIL_LIMIT_MIB=50 启动后演示）")

    failed = [n for n, ok, _ in results if not ok]
    print(f"\n== 自测完成：{len(results) - len(failed)}/{len(results)} 项通过 ==")
    if failed:
        print("失败项: " + "; ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

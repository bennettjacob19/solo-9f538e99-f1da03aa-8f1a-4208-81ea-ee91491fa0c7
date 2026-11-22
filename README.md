# cgroup v2 SIGKILL 归因诊断 API

批处理子进程收到 SIGKILL 时，值班员用它分清是 **OOM** 还是 **主动强停**。

## 原理

- 每轮作业跑在**独立 slot cgroup**（`/sys/fs/cgroup/gsb/slotN`），各自设 `memory.max`。
- 起作业前、作业回收后各读一次本轮 `memory.events` 的 `oom_kill` 计数。
- 结合三样证据归因：**进程退出状态**、**oom_kill 前后差值**、**本轮是否登记了强停请求**。

| 退出状态 | oom_kill 差值 | 有强停请求 | 结论 |
|---|---|---|---|
| 0 / 非零自行退出 | 0 或缺失 | 任意 | `normal_exit` |
| 自行退出 | >0 | 任意 | `unknown`（证据冲突） |
| -9 (SIGKILL) | >0 | 无 | `oom_kill` |
| -9 (SIGKILL) | 0 | 有 | `force_stopped` |
| -9 (SIGKILL) | >0 | 有 | `unknown`（证据冲突） |
| -9 (SIGKILL) | 0 | 无 | `unknown`（可能被外部杀死） |
| -9 (SIGKILL) | 缺失 | 任意 | `unknown`（证据缺失） |
| 其他信号 / 退出状态缺失 | 任意 | 任意 | `unknown` |

## 规则实现

- **上限生效才接作业**：启动时检测本容器 cgroup v2 的 `memory.max` 为具体数值、
  且能创建子 cgroup 并启用 memory 控制器，否则 `POST /jobs` 一律 503，`GET /status` 给出原因。
- **两轮并行**：槽位数默认 2（`GSB_SLOTS`）。第三轮在槽位占满时收到 409。
- **结论后才交还**：槽位只在归因结论（含 `unknown`）落定后交还，供下一轮使用。

## 启动（docker compose）

要求：cgroup v2 宿主机 + Docker ≥ 20.10（默认 private cgroup 命名空间，容器内
`/sys/fs/cgroup` 可写；不依赖 privileged）。

```bash
docker compose up --build
# 打开 http://127.0.0.1:8000/docs
```

compose 里 `mem_limit: 384m`（等值 `memswap_limit` 禁 swap）即“内存上限生效”。
单轮上限 = (384 − 96 预留) / 2 = **144MiB**，见 `GET /status`。

## 用法（/docs 里点，或 curl）

```bash
# 1. 看上限是否生效、单轮上限多少
curl -s localhost:8000/status

# 2. 起一轮：目标 200MiB > 单轮上限 144MiB -> 预期 OOM
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
     -d '{"memory_mib": 200, "hold_seconds": 120}'

# 3. 再起一轮小的，然后凭作业号强停 -> 预期 force_stopped
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
     -d '{"memory_mib": 32, "hold_seconds": 120}'
curl -s -X POST localhost:8000/jobs/<job_id>/stop

# 4. 查归因
curl -s localhost:8000/jobs/<job_id>
```

返回字段：`exit_status`（subprocess 约定，-9 = SIGKILL）、`exit_signal`、
`oom_kill_before/after/delta`、`stop_requested`、`verdict`、`verdict_reason`、
`output_tail`。

> 注意：Python 解释器自身开销（约 20–40MiB）也计入本轮 cgroup，选目标 MiB 时留余量。

## 自测

```bash
# 真实 cgroup 环境（推荐）：一轮 OOM 与一轮强停交错并行 + 槽位交还 + 正常退出
docker compose run --rm selftest

# 本机无特权环境：模拟后端跑通同一套流程（OOM 由样例作业按限制自查复现）
GSB_SIMULATE=1 python3 -m uvicorn app.main:app &
python3 scripts/selftest.py

# 归因真值表单测（纯 stdlib，无依赖）
python3 -m unittest tests.test_verdict -v
```

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `GSB_SLOTS` | 2 | 并行槽位（轮）数 |
| `GSB_RESERVE_MIB` | 96 | 给 API 进程预留的内存 MiB |
| `GSB_SIMULATE` | 0 | 1 = 模拟后端（不碰 /sys/fs/cgroup） |
| `GSB_SIM_LIMIT_MIB` | 256 | 模拟后端的“容器上限” |

## 已知边界

- 作业记录保存在进程内存，重启即失；单 worker 运行。
- 两轮同时都打满内存时，容器级 `memory.max` 可能先于 slot 级触发，
  此时 slot 计数不增、归因落 `unknown` —— 属“证据不足按不明处理”的预期行为。

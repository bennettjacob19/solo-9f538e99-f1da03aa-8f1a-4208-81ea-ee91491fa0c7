# cgroup v2 SIGKILL 归因诊断 API（带每轮内存上限与额度准入）

批处理子进程收到 SIGKILL 时，值班员用它分清是 **OOM** 还是 **主动强停**。
复测常要同时跑一大一小两轮，因此每轮可**单独指定本轮内存上限**，
并由服务端按**总额度 + 槽位**双重准入，避免均分上限让大作业提前 OOM。

## 原理与额度模型

- 每轮作业跑在**独立 slot cgroup**（`/sys/fs/cgroup/gsb/slotN`），启动前按
  本轮请求重写该 slot 的 `memory.max`；复用槽位也是**先设好本轮上限再启进程**。
- **总额度** = 容器内存上限（容器 cgroup `memory.max`）− 服务预留
  （`GSB_RESERVE_MIB`，默认 96MiB）。
- 每轮可在 `POST /jobs` 另填 `memory_limit_mib`（整数 MiB，**至少 48**）；
  省略则用默认值——总额度按槽位均分。
- 接单时同时预留「一个槽位 + 本轮上限对应的额度」。**正在设置上限
  （setting）或已启动但尚未出归因（running）的作业都按各自上限占额**；
  归因结论落定后槽位与额度一起交还。
- 额度不足或槽位不足 → **409**，且不占用任何资源。
- 设置 cgroup 上限是慢操作，在准入锁外执行：一个槽位设置期间，另一空闲
  槽位只要余额够仍能接单。
- 设置失败 → **500**，自动退回本轮槽位与额度，可立即重试。
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
- **结论后才交还**：槽位与额度只在归因结论（含 `unknown`）落定后交还。
- **无可写 cgroup 也能演示**：`GSB_SIMULATE=1` 启用模拟后端，用普通目录模拟
  `memory.max`/`memory.events`，完整跑通同一套接单、额度、回退流程（OOM 由
  样例作业按本轮限制自查复现）。

## 启动（docker compose）

要求：cgroup v2 宿主机 + Docker ≥ 20.10（默认 private cgroup 命名空间，容器内
`/sys/fs/cgroup` 可写；不依赖 privileged）。

```bash
docker compose up --build
# 打开 http://127.0.0.1:8000/docs
```

compose 里 `mem_limit: 384m`（等值 `memswap_limit` 禁 swap）即“内存上限生效”。
总额度 = 384 − 96 = **288MiB**；省略本轮上限时默认均分 = 288 / 2 = **144MiB**，
见 `GET /status`。

宿主没有可写 cgroup（只读 cgroup、本机、CI）时用模拟后端：

```bash
docker compose --profile simulate up oom-doctor-sim
# 同样打开 http://127.0.0.1:8000/docs
```

## 用法（/docs 里点，或 curl）

```bash
# 1. 看总额度/剩余额度、默认单轮上限、槽位占用
curl -s localhost:8000/status

# 2. 大轮：本轮上限 200MiB，目标占用 220MiB -> 预期 OOM
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
     -d '{"memory_mib": 220, "memory_limit_mib": 200, "hold_seconds": 120}'

# 3. 小轮并行：本轮上限 48MiB（下限），目标 32MiB，随后凭作业号强停 -> 预期 force_stopped
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
     -d '{"memory_mib": 32, "memory_limit_mib": 48, "hold_seconds": 120}'
curl -s -X POST localhost:8000/jobs/<job_id>/stop

# 4. 省略 memory_limit_mib -> 用默认均分上限
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' \
     -d '{"memory_mib": 32, "hold_seconds": 60}'

# 5. 凭作业号查本轮实际生效上限与归因
curl -s localhost:8000/jobs/<job_id>
```

返回字段：`memory_limit_mib`（**本轮实际生效上限**）、`exit_status`
（subprocess 约定，-9 = SIGKILL）、`exit_signal`、
`oom_kill_before/after/delta`、`stop_requested`、`verdict`、`verdict_reason`、
`output_tail`。

`/status` 中 `slots.quota_total_mib / quota_used_mib / quota_free_mib`
即总额度、已占额度（含 setting 阶段）与**剩余额度**；`slots.active` 列出每个
占用槽的阶段（setting/running）与本轮上限。

### 拒绝语义

| 场景 | 状态码 | 说明 |
|---|---|---|
| cgroup 不可用 / 上限未生效 | 503 | `blocking_reasons` 给原因 |
| 本轮上限 < 48MiB 或非整数 | 422 | pydantic 入参校验 |
| 本轮上限 > 剩余额度 | 409 | 响应体含申请/已占/剩余 MiB |
| 槽位占满（含设置中的轮次） | 409 | 等上一轮出归因后再试 |
| 本轮 cgroup 上限设置失败 | 500 | 槽位与额度已自动退回，可重试 |

> 注意：Python 解释器自身开销（约 20–40MiB）也计入本轮 cgroup，选目标 MiB 时留余量。

## 自测

```bash
# 真实 cgroup 环境：大小两轮并行 + 额度/槽位 409 + 归因 + 槽位交还
docker compose run --rm selftest

# 模拟后端（含设置失败回退用例，需注入失败上限）
docker compose --profile simulate run --rm selftest-sim

# 本机无特权环境：模拟后端跑通同一套流程
GSB_SIMULATE=1 GSB_SIM_SET_DELAY_SECONDS=0.6 GSB_SIM_FAIL_LIMIT_MIB=50 \
  python3 -m uvicorn app.main:app &
python3 scripts/selftest.py

# 纯 stdlib 单测：归因真值表 + 额度/槽位调度（假后端，不起服务）
python3 -m unittest discover -s tests -v
```

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `GSB_SLOTS` | 2 | 并行槽位（轮）数 |
| `GSB_RESERVE_MIB` | 96 | 给 API 进程预留的内存 MiB（容器上限减它得总额度） |
| `GSB_SIMULATE` | 0 | 1 = 模拟后端（不碰 /sys/fs/cgroup） |
| `GSB_SIM_LIMIT_MIB` | 256 | 模拟后端的“容器上限” |
| `GSB_SIM_SET_DELAY_SECONDS` | 0.5 | 模拟后端每轮设置上限的耗时（演示慢设置不阻塞另一槽） |
| `GSB_SIM_FAIL_LIMIT_MIB` | 空 | 设为整数（如 50）后，请求该上限即注入设置失败，演示回退 |

## 已知边界 / 未完成事项

- 作业记录保存在进程内存，重启即失；单 worker 运行，多 worker 会各持一份账。
- 额度按“申请的上限”而非实际占用计费——这是准入语义（保证最坏情况下不超
  容器上限）；作业提前结束后额度即随结论交还。
- 真实后端的设置失败注入只能靠制造真实写失败（如 cgroup 被重新挂为只读），
  失败回退路径的自动化覆盖目前以模拟后端 + 调度层假后端单测为准。
- 两轮同时都打满内存时，容器级 `memory.max` 可能先于 slot 级触发，
  此时 slot 计数不增、归因落 `unknown` —— 属“证据不足按不明处理”的预期行为。
- 本环境无 Docker 且 `/sys/fs/cgroup` 只读，真实 cgroup 路径未在本机实测，
  需在满足前置条件的宿主上 `docker compose up` 复验。

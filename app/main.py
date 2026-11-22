"""FastAPI 入口：cgroup v2 批处理作业 SIGKILL 归因诊断。

用法（/docs 交互式）：
  1. GET  /status            看内存上限是否生效、单轮上限多少 MiB
  2. POST /jobs              填 memory_mib 启动样例作业（最多两轮并行）
  3. POST /jobs/{id}/stop    凭作业号强停（SIGKILL）
  4. GET  /jobs/{id}         查退出状态、oom_kill 前后读数与归因结论
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from .cgroups import CgroupV2Backend, SimulatedBackend
from .jobs import JobManager, NoSlotAvailable, NotAccepting


def _build_manager() -> JobManager:
    slots = int(os.environ.get("GSB_SLOTS", "2"))
    reserve = int(os.environ.get("GSB_RESERVE_MIB", "96"))
    if os.environ.get("GSB_SIMULATE", "0") == "1":
        limit_mib = int(os.environ.get("GSB_SIM_LIMIT_MIB", "256"))
        backend = SimulatedBackend(slots=slots, limit_mib=limit_mib, reserve_mib=reserve)
    else:
        backend = CgroupV2Backend(slots=slots, reserve_mib=reserve)
    return JobManager(backend)


@asynccontextmanager
async def lifespan(app: FastAPI):
    manager = _build_manager()
    app.state.manager = manager
    yield
    await manager.shutdown()
    manager.backend.cleanup()


app = FastAPI(
    title="cgroup v2 SIGKILL 归因诊断",
    version="1.0.0",
    description=(
        "批处理子进程被 SIGKILL 时，区分 **OOM（oom_kill）** 与 **主动强停（force_stopped）**，"
        "另可判 **正常退出（normal_exit）**；证据缺失或冲突时报 **原因不明（unknown）**。"
        "每轮作业跑在独立 cgroup，凭本轮 memory.events 的 oom_kill 前后读数归因；"
        "上一轮给出结论后其槽位才交给下一轮。"
    ),
    lifespan=lifespan,
)


def _mgr(request: Request) -> JobManager:
    return request.app.state.manager


class StartJobIn(BaseModel):
    memory_mib: int = Field(
        ..., ge=1, le=1024 * 1024,
        description="样例作业要实际占用的目标内存（MiB）。超过 /status 里的单轮上限即触发 OOM",
        examples=[160],
    )
    hold_seconds: float = Field(
        30, ge=1, le=3600,
        description="占满目标内存后保持的秒数，期间可被强停",
    )


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.get("/status", tags=["诊断"])
def status(request: Request):
    """内存上限是否生效、当前是否接作业、槽位占用情况。"""
    mgr = _mgr(request)
    st = mgr.status
    return {
        "mode": st.mode,
        "accepting_jobs": st.accepting,
        "blocking_reasons": st.reasons,
        "notes": st.notes,
        "cgroup_path": st.cgroup_path,
        "memory_limit_bytes": st.memory_limit_bytes,
        "memory_limit_mib": st.memory_limit_bytes // (1024 * 1024) if st.memory_limit_bytes else None,
        "memory_current_bytes": st.memory_current_bytes,
        "slots": mgr.slots_snapshot(),
        "how_to": "POST /jobs 起作业（memory_mib 超单轮上限即 OOM）；"
                  "POST /jobs/{job_id}/stop 强停；GET /jobs/{job_id} 查归因",
    }


@app.post("/jobs", status_code=201, tags=["诊断"])
async def start_job(body: StartJobIn, request: Request):
    """启动一轮样例作业：实际占用 memory_mib MiB 内存并保持 hold_seconds 秒。

    - 503：内存上限未生效 / cgroup 不可用（按规则不接作业）
    - 409：两轮槽位都在跑，等上一轮出归因结论后再试
    """
    mgr = _mgr(request)
    if not mgr.status.accepting:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "内存上限未生效或 cgroup 不可用，按规则拒绝接作业",
                "reasons": mgr.status.reasons,
            },
        )
    try:
        job = await mgr.start(body.memory_mib, body.hold_seconds)
    except NotAccepting:
        raise HTTPException(status_code=503, detail="当前不接作业")
    except NoSlotAvailable:
        raise HTTPException(
            status_code=409,
            detail="两轮作业槽位均在占用中；上一轮给出归因结论后槽位才会交还",
        )
    return job.to_dict()


@app.get("/jobs", tags=["诊断"])
def list_jobs(request: Request):
    """全部作业记录（新的在前）。"""
    return [j.to_dict() for j in _mgr(request).list()]


@app.get("/jobs/{job_id}", tags=["诊断"])
def get_job(job_id: str, request: Request):
    """凭作业号查结果：退出状态、oom_kill 前后读数、归因结论。"""
    job = _mgr(request).get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"未知作业号 {job_id}")
    return job.to_dict()


@app.post("/jobs/{job_id}/stop", tags=["诊断"])
async def stop_job(job_id: str, request: Request):
    """凭作业号强停（SIGKILL）。归因时会与 oom_kill 计数对照。"""
    mgr = _mgr(request)
    job = mgr.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"未知作业号 {job_id}")
    if job.state != "running":
        raise HTTPException(status_code=409, detail=f"作业已结束，结论={job.verdict}")
    await mgr.stop(job_id)
    return job.to_dict()

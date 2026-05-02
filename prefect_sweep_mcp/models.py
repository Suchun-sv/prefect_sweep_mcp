from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Project(BaseModel):
    id: str
    name: str
    description: str = ""


class ExecutionTemplate(BaseModel):
    id: str
    name: str
    project_id: str | None = None
    deployment_name: str
    repo_url: str
    branch: str | None = None
    default_env: dict[str, str] = Field(default_factory=dict)
    work_pool: str
    work_queue: str
    command_template: str
    description: str = ""
    allowed_queues: list[str] = Field(default_factory=list)


class BatchLaunch(BaseModel):
    id: str
    template_id: str
    submitted_at: str = Field(default_factory=utc_now)
    submitted_by: str = "mcp"
    status: str = "submitted"


class ShardRun(BaseModel):
    id: str
    batch_id: str
    shard_id: str
    worker_id: int | None = None
    prefect_flow_run_id: str
    command: str
    status: str = "submitted"


class WorkerQueueStatus(BaseModel):
    pool: str
    queue: str
    online_workers: int = 0
    recent_runs: int = 0
    capacity: int | None = None


class RunSummary(BaseModel):
    shard_id: str
    worker_id: int | None = None
    prefect_flow_run_id: str
    status: str
    command: str


class SubmitBatchRequest(BaseModel):
    template_name: str
    parameter_overrides: dict[str, Any] = Field(default_factory=dict)
    sweep_kind: str = "single"
    expected_shards: int = 1
    work_pool: str | None = None
    work_queue: str | None = None
    submitted_by: str = "mcp"


class SubmitBatchResponse(BaseModel):
    batch_id: str
    submitted_count: int
    run_ids: list[str]
    work_pool: str
    work_queue: str


class BatchStatusResponse(BaseModel):
    batch_id: str
    overall_status: str
    submitted: int
    running: int
    completed: int
    failed: int
    cancelled: int
    missing: int
    all_shards_present: bool
    run_summaries: list[RunSummary]


class RetryFailedShardsResponse(BaseModel):
    batch_id: str
    retried_count: int
    new_run_ids: list[str]


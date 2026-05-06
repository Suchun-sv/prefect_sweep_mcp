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
    repo_local_path: str
    default_branch: str | None = None
    job_variables: dict[str, Any] = Field(default_factory=dict)
    work_pool: str
    work_queue: str
    default_cmd: str
    command_template: str | None = None
    description: str = ""
    allowed_queues: list[str] = Field(default_factory=list)
    allowed_launch_overrides: list[str] = Field(default_factory=list)
    allowed_tasks: list[str] = Field(default_factory=list)


class BatchLaunch(BaseModel):
    id: str
    template_id: str
    submitted_at: str = Field(default_factory=utc_now)
    submitted_by: str = "mcp"
    status: str = "submitted"
    launch_overrides: dict[str, Any] = Field(default_factory=dict)


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


class GeneratedDeploymentConfigResponse(BaseModel):
    path: str
    deployments: list[str]
    contents: str | None = None


class DeployTemplateResponse(BaseModel):
    deployment_name: str
    prefect_id: str | None = None
    url: str | None = None


class TemplateDeployStatusResponse(BaseModel):
    exists: bool
    template_name: str
    deployment_name: str
    pool: str
    queue: str
    prefect_id: str | None = None
    url: str | None = None
    detail: str | None = None


class SubmitRunResponse(BaseModel):
    template_name: str
    deployment_name: str
    flow_run_id: str
    url: str | None = None


class RunStatusResponse(BaseModel):
    flow_run_id: str
    state: str
    url: str | None = None
    deployment_name: str | None = None
    detail: str | None = None


class RunLogsResponse(BaseModel):
    flow_run_id: str
    logs: list[str]


class CancelRunResponse(BaseModel):
    flow_run_id: str
    state: str
    detail: str | None = None


class CancelBatchResponse(BaseModel):
    batch_id: str
    cancelled_count: int
    flow_run_ids: list[str]


class GeneratedArtifactGitignoreResponse(BaseModel):
    generated_dir: str
    ignored: bool
    suggested_entry: str


class RegisterTemplateResponse(BaseModel):
    template_name: str
    deployment_name: str
    persisted_to_catalog: bool
    overwritten: bool = False


class UnregisterTemplateResponse(BaseModel):
    template_name: str
    removed_from_store: bool
    removed_from_catalog: bool


class DeploymentMutationResponse(BaseModel):
    template_name: str
    deployment_name: str
    deployment_id: str
    action: str


class DeploymentRunSummary(BaseModel):
    flow_run_id: str
    name: str | None = None
    state: str
    expected_start_time: str | None = None
    start_time: str | None = None
    end_time: str | None = None


class ListRunsInDeploymentResponse(BaseModel):
    template_name: str
    deployment_name: str
    deployment_id: str
    runs: list[DeploymentRunSummary]


class FlowRunSummary(BaseModel):
    flow_run_id: str
    name: str | None = None
    state: str
    deployment_id: str | None = None
    expected_start_time: str | None = None
    start_time: str | None = None
    end_time: str | None = None


class ListFlowRunsResponse(BaseModel):
    runs: list[FlowRunSummary]


class RetryRunResponse(BaseModel):
    flow_run_id: str
    state: str


class TemplateRuntimeRequirementsResponse(BaseModel):
    template_name: str
    deployment_name: str
    repo_url: str
    repo_local_path: str
    default_branch: str | None = None
    work_pool: str
    work_queue: str
    default_cmd: str
    allowed_launch_overrides: list[str]

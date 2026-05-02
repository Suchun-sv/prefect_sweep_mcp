from __future__ import annotations

import uuid
from collections import Counter
from typing import Any

from .models import (
    BatchStatusResponse,
    ExecutionTemplate,
    RetryFailedShardsResponse,
    RunSummary,
    ShardRun,
    SubmitBatchRequest,
    SubmitBatchResponse,
    WorkerQueueStatus,
)
from .platform_store import PlatformStore
from .prefect_adapter import PrefectAdapter


class BatchService:
    def __init__(self, store: PlatformStore, prefect: PrefectAdapter):
        self.store = store
        self.prefect = prefect

    def list_templates(self) -> list[ExecutionTemplate]:
        return self.store.list_templates()

    def list_workers(self) -> list[WorkerQueueStatus]:
        workers = self.prefect.list_workers()
        counts: dict[tuple[str, str], int] = {}
        for worker in workers:
            pool = worker.get("work_pool_name", "")
            queue = worker.get("work_queue_name", "")
            status = str(worker.get("status", "")).upper()
            key = (pool, queue)
            if status == "ONLINE":
                counts[key] = counts.get(key, 0) + 1
            else:
                counts.setdefault(key, 0)
        return [
            WorkerQueueStatus(pool=pool, queue=queue, online_workers=count)
            for (pool, queue), count in sorted(counts.items())
        ]

    def submit_batch(self, request: SubmitBatchRequest) -> SubmitBatchResponse:
        template = self.store.get_template_by_name(request.template_name)
        if template is None:
            raise ValueError(f"Unknown template: {request.template_name}")

        work_pool = request.work_pool or template.work_pool
        work_queue = request.work_queue or template.work_queue
        if template.allowed_queues and work_queue not in template.allowed_queues:
            raise ValueError(f"Queue {work_queue!r} is not allowed for template {template.name!r}")

        batch = self.store.create_batch(template.id, request.submitted_by)
        commands = self._expand_commands(template, request)
        run_ids: list[str] = []
        for worker_id, command in commands:
            parameters = {
                "repo_url": template.repo_url,
                "repo_local_path": request.parameter_overrides.get("repo_local_path", "~/github/run-target"),
                "branch": request.parameter_overrides.get("branch", template.branch),
                "cmd": command,
            }
            run_id = self.prefect.create_flow_run_from_deployment(template.deployment_name, parameters)
            shard = ShardRun(
                id=str(uuid.uuid4()),
                batch_id=batch.id,
                shard_id=f"worker-{worker_id}" if worker_id is not None else "single",
                worker_id=worker_id,
                prefect_flow_run_id=run_id,
                command=command,
                status="submitted",
            )
            self.store.add_shard_run(shard)
            run_ids.append(run_id)
        return SubmitBatchResponse(
            batch_id=batch.id,
            submitted_count=len(run_ids),
            run_ids=run_ids,
            work_pool=work_pool,
            work_queue=work_queue,
        )

    def get_batch_status(self, batch_id: str) -> BatchStatusResponse:
        shard_runs = self.store.list_shard_runs(batch_id)
        if not shard_runs:
            raise ValueError(f"Unknown batch id: {batch_id}")

        run_summaries: list[RunSummary] = []
        counts = Counter()
        for shard in shard_runs:
            flow_run = self.prefect.get_flow_run(shard.prefect_flow_run_id)
            state = flow_run.get("state_name") or flow_run.get("state", {}).get("name") or shard.status
            normalized = self._normalize_state(state)
            self.store.update_shard_status(shard.prefect_flow_run_id, normalized)
            counts[normalized] += 1
            run_summaries.append(
                RunSummary(
                    shard_id=shard.shard_id,
                    worker_id=shard.worker_id,
                    prefect_flow_run_id=shard.prefect_flow_run_id,
                    status=normalized,
                    command=shard.command,
                )
            )

        total = len(shard_runs)
        missing = 0
        all_shards_present = total > 0
        overall = self._aggregate_status(counts)
        self.store.update_batch_status(batch_id, overall)
        return BatchStatusResponse(
            batch_id=batch_id,
            overall_status=overall,
            submitted=counts["submitted"],
            running=counts["running"],
            completed=counts["completed"],
            failed=counts["failed"],
            cancelled=counts["cancelled"],
            missing=missing,
            all_shards_present=all_shards_present,
            run_summaries=run_summaries,
        )

    def retry_failed_shards(self, batch_id: str) -> RetryFailedShardsResponse:
        shard_runs = self.store.list_shard_runs(batch_id)
        failed_shards = [shard for shard in shard_runs if shard.status == "failed"]
        if not failed_shards:
            return RetryFailedShardsResponse(batch_id=batch_id, retried_count=0, new_run_ids=[])

        batch = self.store.get_batch(batch_id)
        if batch is None:
            raise ValueError(f"Unknown batch id: {batch_id}")
        templates = {template.id: template for template in self.store.list_templates()}
        template = templates.get(batch.template_id)
        if template is None:
            raise RuntimeError(f"Unable to resolve template {batch.template_id!r} for retry")

        new_run_ids: list[str] = []
        for shard in failed_shards:
            parameters = {
                "repo_url": template.repo_url,
                "repo_local_path": "~/github/run-target",
                "branch": template.branch,
                "cmd": shard.command,
            }
            run_id = self.prefect.create_flow_run_from_deployment(template.deployment_name, parameters)
            retried = ShardRun(
                id=str(uuid.uuid4()),
                batch_id=batch_id,
                shard_id=shard.shard_id,
                worker_id=shard.worker_id,
                prefect_flow_run_id=run_id,
                command=shard.command,
                status="submitted",
            )
            self.store.add_shard_run(retried)
            new_run_ids.append(run_id)
        return RetryFailedShardsResponse(batch_id=batch_id, retried_count=len(new_run_ids), new_run_ids=new_run_ids)

    def cancel_batch(self, batch_id: str) -> None:
        for shard in self.store.list_shard_runs(batch_id):
            self.prefect.cancel_flow_run(shard.prefect_flow_run_id)
            self.store.update_shard_status(shard.prefect_flow_run_id, "cancelled")
        self.store.update_batch_status(batch_id, "cancelled")

    def get_run_logs(self, flow_run_id: str, limit: int = 200) -> list[str]:
        return self.prefect.get_run_logs(flow_run_id, limit=limit)

    def _expand_commands(self, template: ExecutionTemplate, request: SubmitBatchRequest) -> list[tuple[int | None, str]]:
        overrides = dict(request.parameter_overrides)
        if request.expected_shards <= 1:
            return [(None, template.command_template.format(**overrides))]

        commands: list[tuple[int | None, str]] = []
        total_workers = request.expected_shards
        for worker_id in range(total_workers):
            params: dict[str, Any] = {
                **overrides,
                "worker_id": worker_id,
                "total_workers": total_workers,
            }
            commands.append((worker_id, template.command_template.format(**params)))
        return commands

    def _normalize_state(self, state: str) -> str:
        lowered = state.lower()
        if lowered in {"scheduled", "pending"}:
            return "submitted"
        if lowered in {"running"}:
            return "running"
        if lowered in {"completed"}:
            return "completed"
        if lowered in {"failed", "crashed"}:
            return "failed"
        if lowered in {"cancelled", "cancelling"}:
            return "cancelled"
        return lowered

    def _aggregate_status(self, counts: Counter[str]) -> str:
        if counts["failed"] > 0:
            return "failed"
        if counts["running"] > 0:
            return "running"
        if counts["submitted"] > 0:
            return "submitted"
        if counts["cancelled"] > 0 and counts["completed"] == 0:
            return "cancelled"
        return "completed"

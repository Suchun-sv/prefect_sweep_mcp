from __future__ import annotations

import tempfile
import unittest

from prefect_sweep_mcp.batch_service import BatchService
from prefect_sweep_mcp.models import ExecutionTemplate, SubmitBatchRequest
from prefect_sweep_mcp.platform_store import PlatformStore


class FakePrefectAdapter:
    def __init__(self):
        self.counter = 0
        self.states: dict[str, str] = {}
        self.submitted_commands: list[str] = []

    def list_work_pools(self):
        return []

    def list_work_queues(self):
        return []

    def list_workers(self):
        return [
            {"work_pool_name": "GPU_pool", "work_queue_name": "vectorbench"},
            {"work_pool_name": "GPU_pool", "work_queue_name": "vectorbench"},
        ]

    def create_flow_run_from_deployment(self, deployment_name: str, parameters: dict) -> str:
        self.counter += 1
        run_id = f"run-{self.counter}"
        self.states[run_id] = "Scheduled"
        self.submitted_commands.append(parameters["cmd"])
        return run_id

    def get_flow_run(self, flow_run_id: str):
        return {"id": flow_run_id, "state_name": self.states[flow_run_id]}

    def cancel_flow_run(self, flow_run_id: str):
        self.states[flow_run_id] = "Cancelled"

    def get_run_logs(self, flow_run_id: str, limit: int = 200):
        return [f"log for {flow_run_id}"]


class BatchServiceTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = PlatformStore(f"{self.tempdir.name}/platform.db")
        self.prefect = FakePrefectAdapter()
        self.service = BatchService(self.store, self.prefect)
        self.store.seed_template(
            ExecutionTemplate(
                id="template-1",
                name="vectorbench_embedding_shards",
                deployment_name="setup-update-run-cmd-flow/vectorbench_embedding_shards",
                repo_url="https://example.com/repo.git",
                branch="main",
                default_env={},
                work_pool="GPU_pool",
                work_queue="vectorbench",
                allowed_queues=["vectorbench"],
                command_template="run --worker-id {worker_id} --total-workers {total_workers}",
            )
        )

    def tearDown(self):
        self.tempdir.cleanup()

    def test_submit_batch_expands_shards(self):
        response = self.service.submit_batch(
            SubmitBatchRequest(
                template_name="vectorbench_embedding_shards",
                expected_shards=4,
                work_queue="vectorbench",
            )
        )
        self.assertEqual(response.submitted_count, 4)
        self.assertEqual(len(response.run_ids), 4)
        self.assertEqual(
            self.prefect.submitted_commands,
            [
                "run --worker-id 0 --total-workers 4",
                "run --worker-id 1 --total-workers 4",
                "run --worker-id 2 --total-workers 4",
                "run --worker-id 3 --total-workers 4",
            ],
        )

    def test_get_batch_status_aggregates_states(self):
        response = self.service.submit_batch(
            SubmitBatchRequest(
                template_name="vectorbench_embedding_shards",
                expected_shards=2,
                work_queue="vectorbench",
            )
        )
        self.prefect.states[response.run_ids[0]] = "Completed"
        self.prefect.states[response.run_ids[1]] = "Running"

        status = self.service.get_batch_status(response.batch_id)

        self.assertEqual(status.completed, 1)
        self.assertEqual(status.running, 1)
        self.assertEqual(status.overall_status, "running")

    def test_retry_failed_shards_only_retries_failed_ones(self):
        response = self.service.submit_batch(
            SubmitBatchRequest(
                template_name="vectorbench_embedding_shards",
                expected_shards=3,
                work_queue="vectorbench",
            )
        )
        self.prefect.states[response.run_ids[0]] = "Completed"
        self.prefect.states[response.run_ids[1]] = "Failed"
        self.prefect.states[response.run_ids[2]] = "Failed"
        self.service.get_batch_status(response.batch_id)

        retried = self.service.retry_failed_shards(response.batch_id)

        self.assertEqual(retried.retried_count, 2)
        self.assertEqual(retried.new_run_ids, ["run-4", "run-5"])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import tempfile
import unittest

from prefect_sweep_mcp.batch_service import BatchService
from prefect_sweep_mcp.models import ExecutionTemplate, SubmitBatchRequest
from prefect_sweep_mcp.platform_store import PlatformStore


class FakePrefectAdapter:
    def __init__(self):
        self.runs: dict[str, dict] = {}
        self.counter = 0

    def list_work_pools(self):
        return [{"name": "GPU_pool"}]

    def list_work_queues(self):
        return [{"name": "vectorbench", "work_pool_name": "GPU_pool"}]

    def list_workers(self):
        return [{"work_pool_name": "GPU_pool", "work_queue_name": "vectorbench", "status": "ONLINE"}]

    def create_flow_run_from_deployment(self, deployment_name: str, parameters: dict):
        self.counter += 1
        run_id = f"run-{self.counter}"
        self.runs[run_id] = {
            "id": run_id,
            "deployment_name": deployment_name,
            "parameters": parameters,
            "state_name": "Scheduled",
        }
        return run_id

    def get_flow_run(self, flow_run_id: str):
        return self.runs[flow_run_id]

    def cancel_flow_run(self, flow_run_id: str):
        self.runs[flow_run_id]["state_name"] = "Cancelled"

    def get_run_logs(self, flow_run_id: str, limit: int = 200):
        return [f"log for {flow_run_id}"]


class BatchServiceTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = PlatformStore(f"{self.tempdir.name}/mcp.db")
        self.prefect = FakePrefectAdapter()
        self.service = BatchService(self.store, self.prefect)
        self.store.seed_template(
            ExecutionTemplate(
                id="template-1",
                name="vectorbench_embedding_shards",
                deployment_name="vectortranslation",
                repo_url="https://github.com/DBgroup-Edinburgh/VectorBenchmark",
                repo_local_path="~/github/VectorBenchmark",
                default_branch="encode-all-beir",
                default_env={},
                work_pool="GPU_pool",
                work_queue="vectorbench",
                allowed_queues=["vectorbench"],
                default_cmd="run model=gte dataset=quora worker=0/1",
                command_template="run model={model} dataset={dataset} worker={worker_id}/{total_workers}",
                allowed_launch_overrides=["model", "dataset"],
            )
        )

    def tearDown(self):
        self.tempdir.cleanup()

    def test_submit_batch_expands_worker_shards(self):
        response = self.service.submit_batch(
            SubmitBatchRequest(
                template_name="vectorbench_embedding_shards",
                parameter_overrides={"model": "gte", "dataset": "quora"},
                expected_shards=4,
            )
        )
        self.assertEqual(response.submitted_count, 4)
        shard_runs = self.store.list_shard_runs(response.batch_id)
        self.assertEqual(len(shard_runs), 4)
        self.assertEqual([shard.worker_id for shard in shard_runs], [0, 1, 2, 3])

    def test_get_batch_status_aggregates_prefect_states(self):
        response = self.service.submit_batch(
            SubmitBatchRequest(
                template_name="vectorbench_embedding_shards",
                parameter_overrides={"model": "gte", "dataset": "quora"},
                expected_shards=4,
            )
        )
        runs = list(self.prefect.runs.values())
        runs[0]["state_name"] = "Completed"
        runs[1]["state_name"] = "Running"
        runs[2]["state_name"] = "Failed"
        runs[3]["state_name"] = "Cancelled"
        status = self.service.get_batch_status(response.batch_id)
        self.assertEqual(status.completed, 1)
        self.assertEqual(status.running, 1)
        self.assertEqual(status.failed, 1)
        self.assertEqual(status.cancelled, 1)
        self.assertEqual(status.overall_status, "failed")

    def test_retry_failed_shards_resubmits_only_failed_runs(self):
        response = self.service.submit_batch(
            SubmitBatchRequest(
                template_name="vectorbench_embedding_shards",
                parameter_overrides={"model": "gte", "dataset": "quora"},
                expected_shards=4,
            )
        )
        runs = list(self.prefect.runs.values())
        runs[0]["state_name"] = "Completed"
        runs[1]["state_name"] = "Failed"
        runs[2]["state_name"] = "Failed"
        runs[3]["state_name"] = "Completed"
        self.service.get_batch_status(response.batch_id)
        retried = self.service.retry_failed_shards(response.batch_id)
        self.assertEqual(retried.retried_count, 2)
        self.assertEqual(len(retried.new_run_ids), 2)

    def test_submit_rejects_unapproved_runtime_overrides(self):
        with self.assertRaisesRegex(ValueError, "does not allow runtime overrides"):
            self.service.submit_batch(
                SubmitBatchRequest(
                    template_name="vectorbench_embedding_shards",
                    parameter_overrides={"repo_url": "https://example.com/override.git"},
                )
            )


if __name__ == "__main__":
    unittest.main()

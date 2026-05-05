from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from prefect_sweep_mcp.config import MCPConfig
from prefect_sweep_mcp.models import ExecutionTemplate
from prefect_sweep_mcp.operator_service import OperatorService
from prefect_sweep_mcp.platform_store import PlatformStore


class FakePrefectAdapter:
    def __init__(self):
        self.runs: dict[str, dict] = {}
        self.deployments: dict[str, dict] = {
            "practice_101": {"id": "dep-1"},
        }

    def list_work_pools(self):
        return [{"name": "CPU_pool"}]

    def list_work_queues(self):
        return [{"name": "practice", "work_pool_name": "CPU_pool"}]

    def list_workers(self):
        return [{"work_pool_name": "CPU_pool", "work_queue_name": "practice", "status": "ONLINE"}]

    def get_deployment_by_name(self, deployment_name: str):
        if deployment_name not in self.deployments:
            raise RuntimeError("not found")
        return self.deployments[deployment_name]

    def create_flow_run_from_deployment(self, deployment_name: str, parameters: dict):
        run_id = f"run-{len(self.runs) + 1}"
        self.runs[run_id] = {
            "id": run_id,
            "deployment_id": self.deployments.get(deployment_name, {}).get("id"),
            "state_name": "Scheduled",
            "parameters": parameters,
        }
        return run_id

    def get_flow_run(self, flow_run_id: str):
        return self.runs[flow_run_id]

    def cancel_flow_run(self, flow_run_id: str):
        self.runs[flow_run_id]["state_name"] = "Cancelled"

    def get_run_logs(self, flow_run_id: str, limit: int = 200):
        return [f"log for {flow_run_id}"]


class OperatorServiceTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        db_path = f"{self.tempdir.name}/operator.db"
        self.store = PlatformStore(db_path)
        self.prefect = FakePrefectAdapter()
        self.generated_dir = f"{self.tempdir.name}/generated"
        self.service = OperatorService(
            self.store,
            self.prefect,
            MCPConfig(
                prefect_api_url="http://example.com/api",
                sqlite_path=db_path,
                generated_dir=".prefect_mcp",
            ),
        )
        self.repo_root = Path(self.tempdir.name) / "repo"
        self.repo_root.mkdir(parents=True, exist_ok=True)
        self.service.repo_root = self.repo_root
        self.service.generated_dir = Path(self.generated_dir)
        self.service.generated_prefect_file = self.service.generated_dir / "prefect.yaml"
        self.service.generated_metadata_file = self.service.generated_dir / "last_generated.json"
        (self.repo_root / "ui").mkdir(parents=True, exist_ok=True)
        (self.repo_root / "ui" / "config.json").write_text(
            '{"prefect_demo_repo_url":"https://github.com/Suchun-sv/prefect_sweep_mcp.git","worker_branch":"main","github_token":""}'
        )
        self.store.seed_template(
            ExecutionTemplate(
                id="practice_101",
                name="practice_101",
                deployment_name="practice_101",
                repo_url="https://github.com/Suchun-sv/prefect_101",
                repo_local_path="~/github/prefect_101",
                default_branch="main",
                job_variables={},
                work_pool="CPU_pool",
                work_queue="practice",
                default_cmd="uv run practice env",
                command_template=None,
                description="Practice workload",
                allowed_queues=["practice"],
                allowed_launch_overrides=[],
                allowed_tasks=["Practice"],
            )
        )

    def tearDown(self):
        self.tempdir.cleanup()

    def test_generate_deployment_config_writes_generated_prefect_yaml(self):
        response = self.service.generate_deployment_config(template_name="practice_101")
        self.assertTrue(self.service.generated_prefect_file.exists())
        self.assertEqual(response.deployments, ["practice_101"])
        contents = self.service.generated_prefect_file.read_text()
        self.assertIn("practice_101", contents)
        self.assertIn("https://github.com/Suchun-sv/prefect_101", contents)

    def test_check_generated_artifact_gitignore_reports_suggestion(self):
        response = self.service.check_generated_artifact_gitignore()
        self.assertFalse(response.ignored)
        self.assertEqual(response.suggested_entry, ".prefect_mcp/")

    @patch("prefect_sweep_mcp.operator_service.subprocess.run")
    def test_deploy_template_returns_prefect_status(self, mock_run):
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = ""
        mock_run.return_value.stderr = ""
        response = self.service.deploy_template("practice_101")
        self.assertEqual(response.deployment_name, "practice_101")
        self.assertEqual(response.prefect_id, "dep-1")

    def test_submit_run_returns_flow_run_id(self):
        response = self.service.submit_run("practice_101")
        self.assertEqual(response.deployment_name, "practice_101")
        self.assertTrue(response.flow_run_id.startswith("run-"))

    def test_register_template_persists_and_seeds_store(self):
        catalog_path = Path(self.tempdir.name) / "catalog.yaml"
        catalog_path.write_text("templates: []\n")
        self.service.catalog_path_override = str(catalog_path)

        response = self.service.register_template(
            name="my_repo",
            deployment_name="my_repo_dep",
            repo_url="https://github.com/me/my_repo",
            repo_local_path="~/github/my_repo",
            work_pool="CPU_pool",
            work_queue="practice",
            default_cmd="python train.py",
            command_template="python train.py --epochs {epochs}",
            allowed_launch_overrides=["epochs"],
        )

        self.assertEqual(response.template_name, "my_repo")
        self.assertTrue(response.persisted_to_catalog)
        self.assertFalse(response.overwritten)
        self.assertIsNotNone(self.store.get_template_by_name("my_repo"))
        self.assertIn("my_repo", catalog_path.read_text())

    def test_register_template_rejects_duplicate_without_overwrite(self):
        catalog_path = Path(self.tempdir.name) / "catalog.yaml"
        catalog_path.write_text("templates: []\n")
        self.service.catalog_path_override = str(catalog_path)
        with self.assertRaises(ValueError):
            self.service.register_template(
                name="practice_101",
                deployment_name="practice_101_v2",
                repo_url="https://github.com/x/y",
                repo_local_path="~/github/y",
                work_pool="CPU_pool",
                work_queue="practice",
                default_cmd="echo hi",
            )

    def test_register_template_rejects_duplicate_deployment_name(self):
        catalog_path = Path(self.tempdir.name) / "catalog.yaml"
        catalog_path.write_text("templates: []\n")
        self.service.catalog_path_override = str(catalog_path)
        with self.assertRaises(ValueError):
            self.service.register_template(
                name="another",
                deployment_name="practice_101",
                repo_url="https://github.com/x/y",
                repo_local_path="~/github/y",
                work_pool="CPU_pool",
                work_queue="practice",
                default_cmd="echo hi",
            )

    def test_unregister_template_removes_from_store_and_catalog(self):
        catalog_path = Path(self.tempdir.name) / "catalog.yaml"
        catalog_path.write_text("templates: []\n")
        self.service.catalog_path_override = str(catalog_path)
        self.service.register_template(
            name="ephemeral",
            deployment_name="ephemeral_dep",
            repo_url="https://github.com/x/ephemeral",
            repo_local_path="~/github/ephemeral",
            work_pool="CPU_pool",
            work_queue="practice",
            default_cmd="echo gone",
        )

        response = self.service.unregister_template("ephemeral")

        self.assertTrue(response.removed_from_store)
        self.assertTrue(response.removed_from_catalog)
        self.assertIsNone(self.store.get_template_by_name("ephemeral"))
        self.assertNotIn("ephemeral", catalog_path.read_text())


if __name__ == "__main__":
    unittest.main()

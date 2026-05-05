from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any, Iterable

import yaml

from template_catalog import (
    CATALOG_PATH,
    RepoTemplate,
    TemplateCatalogError,
    load_template_catalog,
    render_prefect_yaml,
    render_single_template_prefect_yaml,
)

from .config import MCPConfig
from .models import (
    DeployTemplateResponse,
    ExecutionTemplate,
    CancelRunResponse,
    GeneratedArtifactGitignoreResponse,
    GeneratedDeploymentConfigResponse,
    RegisterTemplateResponse,
    RunLogsResponse,
    RunStatusResponse,
    SubmitRunResponse,
    TemplateDeployStatusResponse,
    TemplateRuntimeRequirementsResponse,
    UnregisterTemplateResponse,
    WorkerQueueStatus,
)
from .platform_store import PlatformStore
from .prefect_adapter import PrefectAdapter


class OperatorService:
    def __init__(self, store: PlatformStore, prefect: PrefectAdapter, config: MCPConfig):
        self.store = store
        self.prefect = prefect
        self.config = config
        self.repo_root = Path(__file__).resolve().parent.parent
        self.generated_dir = (self.repo_root / config.generated_dir).resolve()
        self.generated_prefect_file = self.generated_dir / "prefect.yaml"
        self.generated_metadata_file = self.generated_dir / "last_generated.json"

    def list_work_pools(self) -> list[dict]:
        return self.prefect.list_work_pools()

    def list_work_queues(self) -> list[dict]:
        return self.prefect.list_work_queues()

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

    def generate_deployment_config(self, template_name: str | None = None, include_all: bool = False) -> GeneratedDeploymentConfigResponse:
        data = self._render_generated_config(template_name=template_name, include_all=include_all)
        self.generated_dir.mkdir(parents=True, exist_ok=True)
        contents = yaml.dump(data, default_flow_style=False, allow_unicode=True, sort_keys=False)
        self.generated_prefect_file.write_text(contents)
        self.generated_metadata_file.write_text(
            json.dumps(
                {
                    "generated_path": str(self.generated_prefect_file),
                    "deployments": [d["name"] for d in data["deployments"]],
                },
                indent=2,
            )
        )
        return GeneratedDeploymentConfigResponse(
            path=str(self.generated_prefect_file),
            deployments=[d["name"] for d in data["deployments"]],
        )

    def get_generated_deployment_config(self, template_name: str | None = None, include_all: bool = False) -> GeneratedDeploymentConfigResponse:
        self.generate_deployment_config(template_name=template_name, include_all=include_all)
        contents = self.generated_prefect_file.read_text()
        deployments = [d["name"] for d in yaml.safe_load(contents).get("deployments", [])]
        return GeneratedDeploymentConfigResponse(
            path=str(self.generated_prefect_file),
            deployments=deployments,
            contents=contents,
        )

    def deploy_template(self, template_name: str) -> DeployTemplateResponse:
        template = self._get_template(template_name)
        self.generate_deployment_config(template_name=template_name, include_all=False)
        self._run_prefect_deploy([template.deployment_name])
        status = self.get_template_deploy_status(template_name)
        return DeployTemplateResponse(
            deployment_name=template.deployment_name,
            prefect_id=status.prefect_id,
            url=status.url,
        )

    def deploy_all_templates(self) -> list[DeployTemplateResponse]:
        templates = self.store.list_templates()
        self.generate_deployment_config(include_all=True)
        self._run_prefect_deploy([template.deployment_name for template in templates])
        responses: list[DeployTemplateResponse] = []
        for template in templates:
            status = self.get_template_deploy_status(template.name)
            responses.append(
                DeployTemplateResponse(
                    deployment_name=template.deployment_name,
                    prefect_id=status.prefect_id,
                    url=status.url,
                )
            )
        return responses

    def get_template_deploy_status(self, template_name: str) -> TemplateDeployStatusResponse:
        template = self._get_template(template_name)
        try:
            deployment = self.prefect.get_deployment_by_name(template.deployment_name)
        except Exception as exc:
            return TemplateDeployStatusResponse(
                exists=False,
                template_name=template.name,
                deployment_name=template.deployment_name,
                pool=template.work_pool,
                queue=template.work_queue,
                detail=str(exc),
            )
        deployment_id = deployment.get("id")
        return TemplateDeployStatusResponse(
            exists=True,
            template_name=template.name,
            deployment_name=template.deployment_name,
            pool=template.work_pool,
            queue=template.work_queue,
            prefect_id=deployment_id,
            url=self._flow_ui_url("deployments/deployment", deployment_id) if deployment_id else None,
        )

    def submit_run(self, template_name: str, parameter_overrides: dict | None = None) -> SubmitRunResponse:
        template = self._get_template(template_name)
        overrides = parameter_overrides or {}
        self._validate_overrides(template, overrides)
        command = self._render_command(template, overrides, worker_id=None, total_workers=None)
        flow_run_id = self.prefect.create_flow_run_from_deployment(
            template.deployment_name,
            {
                "repo_url": template.repo_url,
                "repo_local_path": template.repo_local_path,
                "branch": overrides.get("branch", template.default_branch),
                "commit": overrides.get("commit"),
                "cmd": command,
            },
        )
        return SubmitRunResponse(
            template_name=template.name,
            deployment_name=template.deployment_name,
            flow_run_id=flow_run_id,
            url=self._flow_ui_url("runs/flow-run", flow_run_id),
        )

    def get_run_status(self, flow_run_id: str) -> RunStatusResponse:
        flow_run = self.prefect.get_flow_run(flow_run_id)
        state = flow_run.get("state_name") or flow_run.get("state", {}).get("name") or "unknown"
        detail = None
        lowered = str(state).lower()
        if lowered in {"late", "scheduled", "pending"}:
            online_workers = sum(item.online_workers for item in self.list_workers())
            if online_workers == 0:
                detail = "No online workers detected for the configured work pools/queues."
        return RunStatusResponse(
            flow_run_id=flow_run_id,
            state=state,
            url=self._flow_ui_url("runs/flow-run", flow_run_id),
            deployment_name=None,
            detail=detail,
        )

    def get_run_logs(self, flow_run_id: str, limit: int = 50, tail: bool = True) -> RunLogsResponse:
        return RunLogsResponse(flow_run_id=flow_run_id, logs=self.prefect.get_run_logs(flow_run_id, limit=limit, tail=tail))

    def cancel_run(self, flow_run_id: str) -> CancelRunResponse:
        self.prefect.cancel_flow_run(flow_run_id)
        self.store.update_shard_status(flow_run_id, "cancelled")
        flow_run = self.prefect.get_flow_run(flow_run_id)
        state = flow_run.get("state_name") or flow_run.get("state", {}).get("name") or "Cancelling"
        return CancelRunResponse(flow_run_id=flow_run_id, state=state)

    def get_template_runtime_requirements(self, template_name: str) -> TemplateRuntimeRequirementsResponse:
        template = self._get_template(template_name)
        return TemplateRuntimeRequirementsResponse(
            template_name=template.name,
            deployment_name=template.deployment_name,
            repo_url=template.repo_url,
            repo_local_path=template.repo_local_path,
            default_branch=template.default_branch,
            work_pool=template.work_pool,
            work_queue=template.work_queue,
            default_cmd=template.default_cmd,
            allowed_launch_overrides=template.allowed_launch_overrides,
        )

    def register_template(
        self,
        name: str,
        deployment_name: str,
        repo_url: str,
        repo_local_path: str,
        work_pool: str,
        work_queue: str,
        default_cmd: str,
        description: str = "",
        default_branch: str | None = None,
        job_variables: dict[str, Any] | None = None,
        command_template: str | None = None,
        allowed_launch_overrides: list[str] | None = None,
        allowed_tasks: list[str] | None = None,
        overwrite: bool = False,
        persist: bool = True,
    ) -> RegisterTemplateResponse:
        repo_template = RepoTemplate(
            name=name,
            description=description,
            deployment_name=deployment_name,
            repo_url=repo_url,
            repo_local_path=repo_local_path,
            default_branch=default_branch,
            work_pool=work_pool,
            work_queue=work_queue,
            job_variables=job_variables or {},
            default_cmd=default_cmd,
            command_template=command_template,
            allowed_launch_overrides=allowed_launch_overrides or [],
            allowed_tasks=allowed_tasks or [],
        )

        existing = self.store.get_template_by_name(name)
        if existing is not None and not overwrite:
            raise ValueError(
                f"Template {name!r} already exists. Pass overwrite=True to replace it."
            )

        for stored in self.store.list_templates():
            if stored.name == name:
                continue
            if stored.deployment_name == repo_template.deployment_name:
                raise ValueError(
                    f"Deployment name {repo_template.deployment_name!r} is already used by template {stored.name!r}."
                )

        self.store.seed_template(
            ExecutionTemplate(
                id=repo_template.name,
                name=repo_template.name,
                deployment_name=repo_template.deployment_name,
                repo_url=repo_template.repo_url,
                repo_local_path=repo_template.repo_local_path,
                default_branch=repo_template.default_branch,
                job_variables=repo_template.job_variables,
                work_pool=repo_template.work_pool,
                work_queue=repo_template.work_queue,
                default_cmd=repo_template.default_cmd,
                command_template=repo_template.command_template,
                description=repo_template.description,
                allowed_queues=[repo_template.work_queue],
                allowed_launch_overrides=repo_template.allowed_launch_overrides,
                allowed_tasks=repo_template.allowed_tasks,
            )
        )

        persisted = False
        if persist:
            self._upsert_template_in_catalog(repo_template)
            persisted = True

        return RegisterTemplateResponse(
            template_name=repo_template.name,
            deployment_name=repo_template.deployment_name,
            persisted_to_catalog=persisted,
            overwritten=existing is not None,
        )

    def unregister_template(self, template_name: str, persist: bool = True) -> UnregisterTemplateResponse:
        removed_store = self.store.delete_template_by_name(template_name)
        removed_catalog = False
        if persist:
            removed_catalog = self._remove_template_from_catalog(template_name)
        if not removed_store and not removed_catalog:
            raise ValueError(f"Unknown template: {template_name}")
        return UnregisterTemplateResponse(
            template_name=template_name,
            removed_from_store=removed_store,
            removed_from_catalog=removed_catalog,
        )

    def _upsert_template_in_catalog(self, template: RepoTemplate) -> None:
        catalog_path = self._catalog_path()
        data = self._read_catalog(catalog_path)
        entry = template.model_dump(exclude_none=False)
        replaced = False
        for index, existing in enumerate(data["templates"]):
            if existing.get("name") == template.name:
                data["templates"][index] = entry
                replaced = True
                break
        if not replaced:
            data["templates"].append(entry)
        self._write_catalog(catalog_path, data)
        try:
            load_template_catalog(catalog_path)
        except TemplateCatalogError:
            raise

    def _remove_template_from_catalog(self, template_name: str) -> bool:
        catalog_path = self._catalog_path()
        if not catalog_path.exists():
            return False
        data = self._read_catalog(catalog_path)
        before = len(data["templates"])
        data["templates"] = [t for t in data["templates"] if t.get("name") != template_name]
        if len(data["templates"]) == before:
            return False
        self._write_catalog(catalog_path, data)
        return True

    def _catalog_path(self) -> Path:
        override = getattr(self, "catalog_path_override", None)
        return Path(override) if override else CATALOG_PATH

    def _read_catalog(self, path: Path) -> dict:
        if not path.exists():
            return {"templates": []}
        raw = yaml.safe_load(path.read_text()) or {}
        if "templates" not in raw or not isinstance(raw["templates"], list):
            raw["templates"] = []
        return raw

    def _write_catalog(self, path: Path, data: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True))

    def check_generated_artifact_gitignore(self) -> GeneratedArtifactGitignoreResponse:
        gitignore = self.repo_root / ".gitignore"
        suggested_entry = f"{Path(self.config.generated_dir).as_posix().rstrip('/')}/"
        ignored = False
        if gitignore.exists():
            entries = {line.strip() for line in gitignore.read_text().splitlines() if line.strip() and not line.strip().startswith("#")}
            ignored = suggested_entry in entries or self.config.generated_dir in entries
        return GeneratedArtifactGitignoreResponse(
            generated_dir=str(self.generated_dir),
            ignored=ignored,
            suggested_entry=suggested_entry,
        )

    def _render_generated_config(self, template_name: str | None, include_all: bool) -> dict:
        templates = load_template_catalog()
        control_repo_url, control_repo_branch, github_token = self._control_repo_settings()
        if include_all or not template_name:
            return render_prefect_yaml(templates, control_repo_url, control_repo_branch, github_token)
        return render_single_template_prefect_yaml(
            template_name,
            templates,
            control_repo_url,
            control_repo_branch,
            github_token,
        )

    def _run_prefect_deploy(self, deployment_names: Iterable[str]) -> None:
        env = {**os.environ, "PREFECT_API_URL": self.config.prefect_api_url}
        cmd = ["uv", "run", "prefect", "deploy", "--prefect-file", str(self.generated_prefect_file)]
        for deployment_name in deployment_names:
            cmd.extend(["--name", deployment_name])
        result = subprocess.run(
            cmd,
            cwd=self.repo_root,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Prefect deploy failed:\n{result.stdout}\n{result.stderr}")

    def _get_template(self, template_name: str) -> ExecutionTemplate:
        template = self.store.get_template_by_name(template_name)
        if template is None:
            raise ValueError(f"Unknown template: {template_name}")
        return template

    def _render_command(
        self,
        template: ExecutionTemplate,
        overrides: dict,
        worker_id: int | None,
        total_workers: int | None,
    ) -> str:
        values = dict(overrides)
        values.pop("branch", None)
        values.pop("commit", None)
        if worker_id is not None:
            values["worker_id"] = worker_id
            values["total_workers"] = total_workers
        if not template.command_template:
            return template.default_cmd
        try:
            return template.command_template.format(**values)
        except KeyError as exc:
            import string
            missing = exc.args[0] if exc.args else "<unknown>"
            required = sorted({f for _, f, _, _ in string.Formatter().parse(template.command_template) if f})
            raise ValueError(
                f"Template {template.name!r} command_template references {{{missing}}} but parameter_overrides did not supply it. "
                f"Required keys: {required}"
            ) from exc

    def _validate_overrides(self, template: ExecutionTemplate, overrides: dict) -> None:
        allowed = {"branch", "commit", *template.allowed_launch_overrides}
        invalid = sorted(set(overrides) - set(allowed))
        if invalid:
            raise ValueError(
                f"Template {template.name!r} does not allow runtime overrides for: {', '.join(invalid)}"
            )

    def _control_repo_settings(self) -> tuple[str, str, str]:
        config_path = self.repo_root / "ui" / "config.json"
        if not config_path.exists():
            return "https://github.com/Suchun-sv/prefect_sweep_mcp.git", "main", ""
        raw = json.loads(config_path.read_text())
        return (
            raw.get("prefect_demo_repo_url", "https://github.com/Suchun-sv/prefect_sweep_mcp.git"),
            raw.get("worker_branch", "main"),
            raw.get("github_token", ""),
        )

    def _flow_ui_url(self, prefix: str, object_id: str | None) -> str | None:
        if not object_id:
            return None
        base = self.config.prefect_api_url
        if base.endswith("/api"):
            base = base[:-4]
        return f"{base}/{prefix}/{object_id}"

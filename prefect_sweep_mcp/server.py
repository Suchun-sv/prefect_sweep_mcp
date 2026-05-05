from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from .batch_service import BatchService
from .config import MCPConfig
from .models import ExecutionTemplate, SubmitBatchRequest
from .operator_service import OperatorService
from .platform_store import PlatformStore
from .prefect_adapter import HTTPPrefectAdapter
from template_catalog import load_template_catalog


config = MCPConfig()
store = PlatformStore(config.sqlite_path)
prefect = HTTPPrefectAdapter(config.prefect_api_url)
batch_service = BatchService(store, prefect)
operator_service = OperatorService(store, prefect, config)

for template in load_template_catalog():
    store.seed_template(
        ExecutionTemplate(
            id=template.name,
            name=template.name,
            deployment_name=template.deployment_name,
            repo_url=template.repo_url,
            repo_local_path=template.repo_local_path,
            default_branch=template.default_branch,
            job_variables=template.job_variables,
            work_pool=template.work_pool,
            work_queue=template.work_queue,
            default_cmd=template.default_cmd,
            command_template=template.command_template,
            description=template.description,
            allowed_queues=[template.work_queue],
            allowed_launch_overrides=template.allowed_launch_overrides,
            allowed_tasks=template.allowed_tasks,
        )
    )

mcp = FastMCP("prefect-sweep")


@mcp.tool()
def list_templates() -> list[dict]:
    return [template.model_dump() for template in batch_service.list_templates()]


@mcp.tool()
def get_template(template_name: str) -> dict:
    for template in batch_service.list_templates():
        if template.name == template_name:
            return template.model_dump()
    raise ValueError(f"Unknown template: {template_name}")


@mcp.tool()
def list_workers() -> list[dict]:
    return [worker.model_dump() for worker in operator_service.list_workers()]


@mcp.tool()
def list_work_pools() -> list[dict]:
    return operator_service.list_work_pools()


@mcp.tool()
def list_work_queues() -> list[dict]:
    return operator_service.list_work_queues()


@mcp.tool()
def generate_deployment_config(template_name: str | None = None, include_all: bool = False) -> dict:
    return operator_service.generate_deployment_config(template_name=template_name, include_all=include_all).model_dump()


@mcp.tool()
def get_generated_deployment_config(template_name: str | None = None, include_all: bool = False) -> dict:
    return operator_service.get_generated_deployment_config(template_name=template_name, include_all=include_all).model_dump()


@mcp.tool()
def deploy_template(template_name: str) -> dict:
    return operator_service.deploy_template(template_name).model_dump()


@mcp.tool()
def deploy_all_templates() -> list[dict]:
    return [item.model_dump() for item in operator_service.deploy_all_templates()]


@mcp.tool()
def get_template_deploy_status(template_name: str) -> dict:
    return operator_service.get_template_deploy_status(template_name).model_dump()


@mcp.tool()
def submit_run(template_name: str, parameter_overrides: dict | None = None) -> dict:
    return operator_service.submit_run(template_name, parameter_overrides or {}).model_dump()


@mcp.tool()
def get_run_status(flow_run_id: str) -> dict:
    return operator_service.get_run_status(flow_run_id).model_dump()


@mcp.tool()
def get_run_logs(flow_run_id: str, limit: int = 50, tail: bool = True) -> dict:
    return operator_service.get_run_logs(flow_run_id, limit=limit, tail=tail).model_dump()


@mcp.tool()
def register_template(
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
) -> dict:
    return operator_service.register_template(
        name=name,
        deployment_name=deployment_name,
        repo_url=repo_url,
        repo_local_path=repo_local_path,
        work_pool=work_pool,
        work_queue=work_queue,
        default_cmd=default_cmd,
        description=description,
        default_branch=default_branch,
        job_variables=job_variables,
        command_template=command_template,
        allowed_launch_overrides=allowed_launch_overrides,
        allowed_tasks=allowed_tasks,
        overwrite=overwrite,
        persist=persist,
    ).model_dump()


@mcp.tool()
def unregister_template(template_name: str, persist: bool = True) -> dict:
    return operator_service.unregister_template(template_name, persist=persist).model_dump()


@mcp.tool()
def get_template_runtime_requirements(template_name: str) -> dict:
    return operator_service.get_template_runtime_requirements(template_name).model_dump()


@mcp.tool()
def check_generated_artifact_gitignore() -> dict:
    return operator_service.check_generated_artifact_gitignore().model_dump()


@mcp.tool()
def submit_batch(
    template_name: str,
    parameter_overrides: dict | None = None,
    sweep_kind: str = "single",
    expected_shards: int = 1,
    work_pool: str | None = None,
    work_queue: str | None = None,
) -> dict:
    request = SubmitBatchRequest(
        template_name=template_name,
        parameter_overrides=parameter_overrides or {},
        sweep_kind=sweep_kind,
        expected_shards=expected_shards,
        work_pool=work_pool,
        work_queue=work_queue,
    )
    return batch_service.submit_batch(request).model_dump()


@mcp.tool()
def get_batch_status(batch_id: str) -> dict:
    return batch_service.get_batch_status(batch_id).model_dump()


@mcp.tool()
def retry_failed_shards(batch_id: str) -> dict:
    return batch_service.retry_failed_shards(batch_id).model_dump()


if __name__ == "__main__":
    mcp.run()

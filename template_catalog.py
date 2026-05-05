from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import yaml
from pydantic import BaseModel, Field, ValidationError


FLOW_NAME = "setup-update-run-cmd-flow"
ENTRYPOINT = "worker_flow.py:setup_update_run_cmd_flow"
CATALOG_PATH = Path(__file__).parent / "templates" / "catalog.yaml"


class TemplateCatalogError(ValueError):
    pass


class RepoTemplate(BaseModel):
    name: str
    description: str = ""
    deployment_name: str
    repo_url: str
    repo_local_path: str
    default_branch: str | None = None
    work_pool: str
    work_queue: str
    job_variables: dict[str, Any] = Field(default_factory=dict)
    default_cmd: str
    command_template: str | None = None
    allowed_launch_overrides: list[str] = Field(default_factory=list)
    allowed_tasks: list[str] = Field(default_factory=list)

    @property
    def effective_command_template(self) -> str:
        return self.command_template or self.default_cmd


class TemplateCatalog(BaseModel):
    templates: list[RepoTemplate]


def load_template_catalog(path: Path | None = None) -> list[RepoTemplate]:
    catalog_path = path or CATALOG_PATH
    if not catalog_path.exists():
        raise TemplateCatalogError(f"Template catalog not found: {catalog_path}")

    try:
        raw = yaml.safe_load(catalog_path.read_text()) or {}
        catalog = TemplateCatalog.model_validate(raw)
    except ValidationError as exc:
        raise TemplateCatalogError(f"Invalid template catalog: {exc}") from exc
    except yaml.YAMLError as exc:
        raise TemplateCatalogError(f"Invalid YAML in template catalog: {exc}") from exc

    _validate_templates(catalog.templates)
    return catalog.templates


def get_template_by_name(name: str, templates: Iterable[RepoTemplate]) -> RepoTemplate:
    for template in templates:
        if template.name == name:
            return template
    raise TemplateCatalogError(f"Unknown template: {name}")


def validate_task_bindings(templates: Iterable[RepoTemplate], task_names: Iterable[str]) -> None:
    allowed_tasks = set(task_names)
    invalid: list[str] = []
    for template in templates:
        for task_name in template.allowed_tasks:
            if task_name not in allowed_tasks:
                invalid.append(f"{template.name}:{task_name}")
    if invalid:
        raise TemplateCatalogError(
            "Template catalog references unknown task bindings: " + ", ".join(sorted(invalid))
        )


def render_prefect_yaml(templates: Iterable[RepoTemplate], control_repo_url: str, control_repo_branch: str, github_token: str) -> dict:
    deployments = []
    for template in templates:
        deployments.append(
            {
                "name": template.deployment_name,
                "version": "0.0.1",
                "tags": [template.name],
                "description": template.description,
                "schedule": {},
                "flow_name": FLOW_NAME,
                "entrypoint": ENTRYPOINT,
                "parameters": {
                    "repo_url": template.repo_url,
                    "repo_local_path": template.repo_local_path,
                    "branch": template.default_branch,
                    "cmd": template.default_cmd,
                },
                "work_pool": {
                    "name": template.work_pool,
                    "work_queue_name": template.work_queue,
                    "job_variables": dict(template.job_variables),
                },
            }
        )

    return {
        "name": "prefect-sweep",
        "prefect-version": "3.6.2",
        "build": None,
        "push": None,
        "pull": [
            {
                "prefect.deployments.steps.git_clone": {
                    "repository": control_repo_url,
                    "branch": control_repo_branch,
                    "access_token": github_token or None,
                }
            }
        ],
        "deployments": deployments,
    }


def render_single_template_prefect_yaml(
    template_name: str,
    templates: Iterable[RepoTemplate],
    control_repo_url: str,
    control_repo_branch: str,
    github_token: str,
) -> dict:
    template = get_template_by_name(template_name, templates)
    return render_prefect_yaml([template], control_repo_url, control_repo_branch, github_token)


def _validate_templates(templates: list[RepoTemplate]) -> None:
    seen_names: set[str] = set()
    seen_deployments: set[str] = set()
    for template in templates:
        if template.name in seen_names:
            raise TemplateCatalogError(f"Duplicate template name: {template.name}")
        seen_names.add(template.name)

        if template.deployment_name in seen_deployments:
            raise TemplateCatalogError(f"Duplicate deployment name: {template.deployment_name}")
        seen_deployments.add(template.deployment_name)

        override_names = set(template.allowed_launch_overrides)
        if len(override_names) != len(template.allowed_launch_overrides):
            raise TemplateCatalogError(f"Duplicate launch override in template {template.name}")

from __future__ import annotations

from typing import Any, Protocol
import requests
from template_catalog import FLOW_NAME


class PrefectAdapter(Protocol):
    def list_work_pools(self) -> list[dict[str, Any]]: ...
    def list_work_queues(self) -> list[dict[str, Any]]: ...
    def list_workers(self) -> list[dict[str, Any]]: ...
    def get_deployment_by_name(self, deployment_name: str) -> dict[str, Any]: ...
    def create_flow_run_from_deployment(self, deployment_name: str, parameters: dict[str, Any]) -> str: ...
    def get_flow_run(self, flow_run_id: str) -> dict[str, Any]: ...
    def cancel_flow_run(self, flow_run_id: str) -> None: ...
    def get_run_logs(self, flow_run_id: str, limit: int = 200) -> list[str]: ...


class HTTPPrefectAdapter:
    def __init__(self, api_url: str):
        self.api_url = api_url.rstrip("/")

    def _post(self, path: str, payload: dict[str, Any]) -> Any:
        response = requests.post(f"{self.api_url}{path}", json=payload, timeout=30)
        response.raise_for_status()
        return response.json()

    def _get(self, path: str) -> Any:
        response = requests.get(f"{self.api_url}{path}", timeout=30)
        response.raise_for_status()
        return response.json()

    def list_work_pools(self) -> list[dict[str, Any]]:
        data = self._post("/work_pools/filter", {})
        return data if isinstance(data, list) else []

    def list_work_queues(self) -> list[dict[str, Any]]:
        data = self._post("/work_queues/filter", {})
        return data if isinstance(data, list) else []

    def list_workers(self) -> list[dict[str, Any]]:
        try:
            data = self._post("/workers/filter", {})
            return data if isinstance(data, list) else []
        except requests.HTTPError as exc:
            response = exc.response
            if response is None or response.status_code != 404:
                raise

        pools = self.list_work_pools()
        workers: list[dict[str, Any]] = []
        for pool in pools:
            pool_name = pool.get("name")
            if not pool_name:
                continue
            data = self._post(f"/work_pools/{pool_name}/workers/filter", {})
            if not isinstance(data, list):
                continue
            for worker in data:
                if not isinstance(worker, dict):
                    continue
                worker.setdefault("work_pool_name", pool_name)
                workers.append(worker)
        return workers

    def get_deployment_by_name(self, deployment_name: str) -> dict[str, Any]:
        return self._get(f"/deployments/name/{FLOW_NAME}/{deployment_name}")

    def create_flow_run_from_deployment(self, deployment_name: str, parameters: dict[str, Any]) -> str:
        payload = {
            "deployment_name": deployment_name,
            "parameters": parameters,
        }
        try:
            result = self._post("/deployments/create_flow_run", payload)
        except requests.HTTPError as exc:
            response = exc.response
            if response is None or response.status_code != 404:
                raise
            deployment = self._get(f"/deployments/name/{FLOW_NAME}/{deployment_name}")
            deployment_id = deployment.get("id")
            if not deployment_id:
                raise RuntimeError(f"Prefect did not return an id for deployment {FLOW_NAME}/{deployment_name}")
            result = self._post(f"/deployments/{deployment_id}/create_flow_run", {"parameters": parameters})
        flow_run_id = result.get("id")
        if not flow_run_id:
            raise RuntimeError(f"Prefect did not return a flow-run id for deployment {deployment_name}")
        return flow_run_id

    def get_flow_run(self, flow_run_id: str) -> dict[str, Any]:
        return self._get(f"/flow_runs/{flow_run_id}")

    def cancel_flow_run(self, flow_run_id: str) -> None:
        self._post(f"/flow_runs/{flow_run_id}/set_state", {"name": "Cancelling"})

    def get_run_logs(self, flow_run_id: str, limit: int = 200) -> list[str]:
        records = self._post("/logs/filter", {"logs": {"flow_run_id": {"any_": [flow_run_id]}}, "limit": limit})
        if not isinstance(records, list):
            return []
        return [record.get("message", "") for record in records]

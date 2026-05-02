from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from .batch_service import BatchService
from .config import MCPConfig
from .models import ExecutionTemplate, SubmitBatchRequest
from .platform_store import PlatformStore
from .prefect_adapter import HTTPPrefectAdapter


config = MCPConfig()
store = PlatformStore(config.sqlite_path)
prefect = HTTPPrefectAdapter(config.prefect_api_url)
service = BatchService(store, prefect)

# Seed one concrete template for VectorBenchmark shard jobs.
store.seed_template(
    ExecutionTemplate(
        id="vectorbench-embedding-template",
        name="vectorbench_embedding_shards",
        deployment_name="setup-update-run-cmd-flow/vectorbench_embedding_shards",
        repo_url="https://github.com/DBgroup-Edinburgh/VectorBenchmark",
        branch="encode-all-beir",
        default_env={},
        work_pool="GPU_pool",
        work_queue="vectorbench",
        allowed_queues=["vectorbench"],
        command_template=(
            "uv run vectorbench embedding generate --model {model} --dataset {dataset} "
            "--source {source} --data-path {data_path} --embedding-path {embedding_path} "
            "--embedding-model-path {embedding_model_path} --embedding-cache-path {embedding_cache_path} "
            "--batch-size {batch_size} --total-workers {total_workers} --worker-id {worker_id} --no-upload"
        ),
        description="Shard-based VectorBenchmark embedding generation template.",
    )
)

mcp = FastMCP("prefect-sweep")


@mcp.tool()
def list_templates() -> list[dict]:
    return [template.model_dump() for template in service.list_templates()]


@mcp.tool()
def list_workers() -> list[dict]:
    return [worker.model_dump() for worker in service.list_workers()]


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
    return service.submit_batch(request).model_dump()


@mcp.tool()
def get_batch_status(batch_id: str) -> dict:
    return service.get_batch_status(batch_id).model_dump()


@mcp.tool()
def retry_failed_shards(batch_id: str) -> dict:
    return service.retry_failed_shards(batch_id).model_dump()


if __name__ == "__main__":
    mcp.run()


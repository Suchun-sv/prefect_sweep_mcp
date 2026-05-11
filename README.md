# Prefect_Sweep_MCP

Let your AI agent handles you large-scale multi-machine experiments.

> ***One-sentence intuition:*** Treat each machine as a command-waiting agent: we submit `scripts` into a shared `queue`, and each worker acts as a consumer that executes one script, reports the result, and then fetches the next task.

- Further explanation for `script` and `queue`
    1. The scripts: I've prepared a git and uv based template for users to use, nearly fit for any ML experiments.
    2. The queue: I'll use [Prefect](https://www.prefect.io/) as the foundational service.

# Installation

## 0. Install Prefect

We prefer you install via Docker:

```bash
docker run -d -p 4200:4200 prefecthq/prefect:3-latest -- prefect server start --host 0.0.0.0
```

You can check more options at https://docs.prefect.io/v3/get-started/install

## 1. Expose to public network (optional)

This is not an essential step — just lets your remote workers reach the Prefect API.

**[Recommended]** use cloudflared to publish your service:

```bash
cloudflared tunnel --url http://localhost:4200
```

Any other proxy you're comfortable with also works.

## 2. Install on a worker machine

First go to the Prefect UI and create the `work_pool` you want this worker to listen on. Then edit the env values below and run on a fresh worker host:

```bash
PREFECT_API_URL=http://your-prefect-host:4200/api \
WORK_POOL=CPU_pool \
WORK_QUEUE=default \
WORKER_LIMIT=1 \
  bash <(curl -fsSL https://raw.githubusercontent.com/Suchun-sv/prefect_sweep_mcp/5971427/scripts/install_worker.sh)
```

The full installer reference — every env var, the cron, private-repo auth, tmux session management — is documented under [Bootstrap a Worker on a New Machine](#bootstrap-a-worker-on-a-new-machine) below.

---

## About the MCP server

`prefect_sweep_mcp` is a small MCP server that sits in front of a Prefect deployment and a local SQLite metadata store.

It is meant to give an agent a safer control surface than raw Prefect objects. Instead of exposing arbitrary commands, it exposes a curated set of tools spanning the full lifecycle:

- **Onboard a new repo at runtime**: `register_template`, `unregister_template`
- **Inspect**: `list_templates`, `get_template`, `list_workers`, `list_work_pools`, `list_work_queues`, `list_deployments`, `get_template_runtime_requirements`
- **Deploy lifecycle**: `generate_deployment_config`, `deploy_template`, `deploy_all_templates`, `get_template_deploy_status`, `pause_deployment`, `resume_deployment`, `delete_deployment`
- **Run**: `submit_run`, `submit_batch`, `get_run_status`, `get_run_logs`, `cancel_run`, `retry_run`, `list_runs_in_deployment`, `list_flow_runs`
- **Batch ops**: `get_batch_status`, `retry_failed_shards`, `cancel_batch`

Templates are seeded from `templates/catalog.yaml` at startup, and additional templates can be added at runtime via `register_template` (which also persists back to the catalog by default).

## What This Repo Contains

This repo currently contains only the Python package:

- `prefect_sweep_mcp/config.py`: environment-based configuration
- `prefect_sweep_mcp/models.py`: Pydantic models for templates, batches, shards, and tool I/O
- `prefect_sweep_mcp/platform_store.py`: SQLite metadata store
- `prefect_sweep_mcp/prefect_adapter.py`: thin HTTP client for Prefect
- `prefect_sweep_mcp/batch_service.py`: batch orchestration logic
- `prefect_sweep_mcp/server.py`: MCP entrypoint and tool registration

## Prerequisites

You need:

1. Python 3.11+
2. A reachable Prefect API server
3. A Prefect deployment that can execute a `repo_url + branch + cmd` style flow
4. Network access from the MCP process to the Prefect API

The current server bootstrap assumes a Prefect deployment named:

```text
setup-update-run-cmd-flow/vectorbench_embedding_shards
```

If your deployment has a different name, update the seeded template in `prefect_sweep_mcp/server.py`.

## Install

This repo does not yet include its own `pyproject.toml`, so install dependencies manually in a virtual environment.

```bash
python -m venv .venv
source .venv/bin/activate
pip install mcp pydantic requests
```

If you prefer `uv`:

```bash
uv venv
source .venv/bin/activate
uv pip install mcp pydantic requests
```

## Configuration

The server reads configuration from environment variables in `prefect_sweep_mcp/config.py`.

### Required / important variables

#### `PREFECT_API_URL`

Prefect API base URL.

Default:

```text
http://localhost:4200/api
```

Example:

```bash
export PREFECT_API_URL="http://your-prefect-host:4200/api"
```

#### `PREFECT_SWEEP_MCP_DB`

Path to the SQLite file used to persist templates, batches, and shard runs.

Default:

```text
prefect_sweep_mcp.db
```

Example:

```bash
export PREFECT_SWEEP_MCP_DB="$PWD/data/prefect_sweep_mcp.db"
```

#### `PREFECT_SWEEP_ALLOW_UNREGISTERED`

Currently parsed by config but not yet used in the server logic. Leave it unset for now.

## How the Server Works

When `prefect_sweep_mcp.server` starts, it does four things:

1. Loads config from environment variables
2. Creates or opens the SQLite database
3. Instantiates the Prefect HTTP adapter
4. Seeds one `ExecutionTemplate` named `vectorbench_embedding_shards`

That seeded template is configured for:

- repo: `https://github.com/DBgroup-Edinburgh/VectorBenchmark`
- branch: `encode-all-beir`
- pool: `GPU_pool`
- queue: `vectorbench`
- deployment: `setup-update-run-cmd-flow/vectorbench_embedding_shards`

Its command template expands shard parameters into:

```bash
uv run vectorbench embedding generate \
  --model {model} \
  --dataset {dataset} \
  --source {source} \
  --data-path {data_path} \
  --embedding-path {embedding_path} \
  --embedding-model-path {embedding_model_path} \
  --embedding-cache-path {embedding_cache_path} \
  --batch-size {batch_size} \
  --total-workers {total_workers} \
  --worker-id {worker_id} \
  --no-upload
```

## Run the MCP Server

From the repo root:

```bash
source .venv/bin/activate
python -m prefect_sweep_mcp.server
```

That starts the FastMCP server defined in `prefect_sweep_mcp/server.py`.

## Bootstrap a Worker on a New Machine

`scripts/install_worker.sh` clones this repo into `~/.prefect_sweep_mcp`, installs `uv`, runs `uv sync`, and launches a Prefect worker inside a timestamped tmux session (e.g. `prefect-worker-20260505-142301`).

One-shot install (interactive — prompts for missing values):

```bash
curl -fsSL https://raw.githubusercontent.com/Suchun-sv/prefect_sweep_mcp/5971427/scripts/install_worker.sh | bash
```

Non-interactive — preset the values via env:

```bash
PREFECT_API_URL=http://your-prefect-host:4200/api \
WORK_POOL=CPU_pool \
WORK_QUEUE=practice \
WORKER_LIMIT=1 \
  bash <(curl -fsSL https://raw.githubusercontent.com/Suchun-sv/prefect_sweep_mcp/5971427/scripts/install_worker.sh)
```

> The URL pins commit `5971427` so `raw.githubusercontent.com`'s CDN serves the exact file immediately. The `main`-pinned URL works too but can be cached for several minutes after a push.

Variables read by the script:

| Variable | Required | Default | Notes |
| --- | --- | --- | --- |
| `PREFECT_API_URL` | yes | — | e.g. `http://host:4200/api` |
| `WORK_POOL` | yes | — | e.g. `CPU_pool`, `GPU_pool` |
| `WORK_QUEUE` | no | all queues | leave blank to listen on every queue in the pool |
| `WORKER_LIMIT` | no | `1` | max concurrent flow runs the worker accepts; positive integer |
| `GITHUB_TOKEN` | no | — | PAT with `repo` scope. When set, the script installs `git config --global url.https://x-access-token:$TOKEN@github.com/.insteadOf git@github.com:` (and the `ssh:` / `git+ssh:` variants) so private repos and private transitive deps clone over HTTPS. Persisted to `~/.prefect_sweep_mcp/.env` (chmod 600). |
| `PREFECT_SWEEP_MCP_HOME` | no | `~/.prefect_sweep_mcp` | install location |
| `PREFECT_SWEEP_MCP_REPO` | no | `git@github.com:Suchun-sv/prefect_sweep_mcp.git` | override for fork/private mirror |
| `PREFECT_SWEEP_MCP_BRANCH` | no | `main` | branch to check out |

Manage the worker session:

```bash
tmux ls                                      # find the session name
tmux attach -t prefect-worker-<timestamp>    # attach
tmux kill-session -t prefect-worker-<timestamp>  # stop
```

Requires `tmux` and `git` already installed on the host. `uv` is auto-installed if missing.

### Per-run isolation and shared venv

Every flow run clones the user repo fresh into `<repo_local_path>/.runs/<flow_run_id>/`. Concurrent runs therefore never share a working tree — no lock contention, no half-applied `git reset --hard`. On success the run dir is removed automatically.

Python dependencies are installed once per repo into a **shared** venv at `<repo_local_path>/.venv-shared` and reused across runs. `uv sync` is filelocked so concurrent setups don't corrupt the env; the actual command then runs against the shared venv via `UV_PROJECT_ENVIRONMENT` / `VIRTUAL_ENV`.

Implication for user code: the run dir is wiped on success, so write heavy artifacts (datasets, model weights, embedding caches) under stable shared locations like `~/.cache/dataset/`, `~/.cache/model/`, `$HF_HOME`, etc., rather than `./data` or `./models`.

`install_worker.sh` also installs a daily cron that GCs failed/abandoned run dirs older than 1 day:

```cron
0 4 * * * RUNS_TTL_DAYS=1 RUNS_CLEANUP_ROOT="$HOME/github" ~/.prefect_sweep_mcp/scripts/cleanup_run_dirs.sh
```

Override `RUNS_CLEANUP_ROOT` or `RUNS_TTL_DAYS` by editing the crontab line if your repos live elsewhere or you want a different retention window.

## MCP Client Configuration

An MCP client needs to launch the server as a local process.

Example client config:

```json
{
  "mcpServers": {
    "prefect-sweep": {
      "command": "/absolute/path/to/.venv/bin/python",
      "args": ["-m", "prefect_sweep_mcp.server"],
      "cwd": "/absolute/path/to/prefect_sweep_mcp",
      "env": {
        "PREFECT_API_URL": "http://your-prefect-host:4200/api",
        "PREFECT_SWEEP_MCP_DB": "/absolute/path/to/prefect_sweep_mcp.db"
      }
    }
  }
}
```

Replace the paths with real absolute paths on the machine where the MCP client runs.

## Onboard a Brand-New Repo Through the MCP

Once the MCP server is connected to your client (e.g. Claude Code via `claude mcp add prefect-sweep ...`), an agent can register a new repo, deploy it, and submit runs **without any YAML edits or server restarts**:

1. `register_template(name, deployment_name, repo_url, repo_local_path, work_pool, work_queue, default_cmd, ...)` — seeds SQLite immediately and (by default) appends/updates the entry in `templates/catalog.yaml` so it survives a restart.
2. `deploy_template(name)` — generates `.prefect_mcp/prefect.yaml` and runs `prefect deploy` for that one template.
3. `submit_run(name, parameter_overrides=...)` — fires one flow run.
4. `get_run_status(flow_run_id)` / `get_run_logs(flow_run_id)` — poll until done.

Use `unregister_template(name)` to remove a template from both SQLite and `catalog.yaml`.

## Tools Exposed

### `register_template`

Adds a new execution template at runtime.

Required arguments: `name`, `deployment_name`, `repo_url`, `repo_local_path`, `work_pool`, `work_queue`, `default_cmd`.

Optional: `description`, `default_branch`, `job_variables` (free-form dict written into the deployment's `job_variables` block, e.g. `{"env": {"FOO": "bar"}, "working_dir": "/srv"}`), `command_template`, `allowed_launch_overrides`, `allowed_tasks`, `overwrite` (default `False`), `persist` (default `True`, writes back to `templates/catalog.yaml`).

Rejects duplicates: refuses if a template with the same `name` already exists (unless `overwrite=True`) or if `deployment_name` is already used by a different template.

### `unregister_template`

Removes a template by `name` from SQLite and (unless `persist=False`) from `templates/catalog.yaml`.

### `list_templates`

Returns the templates currently stored in SQLite.

Right now, server bootstrap seeds one template:

- `vectorbench_embedding_shards`

### `list_workers`

Fetches workers from Prefect and groups them by:

- `work_pool_name`
- `work_queue_name`

The response reports `online_workers` per `(pool, queue)` pair.

### `submit_batch`

Submits one logical batch against a known template.

Arguments:

- `template_name`
- `parameter_overrides`
- `sweep_kind`
- `expected_shards`
- `work_pool`
- `work_queue`

If `expected_shards > 1`, the service expands the template into one command per `worker_id`.

#### Example

```json
{
  "template_name": "vectorbench_embedding_shards",
  "parameter_overrides": {
    "model": "gte",
    "dataset": "QuoraRetrieval",
    "source": "mteb",
    "data_path": "./data/raw/mteb/",
    "embedding_path": "./data/processed/embeddings/",
    "embedding_model_path": "./models",
    "embedding_cache_path": "./cache",
    "batch_size": 32
  },
  "expected_shards": 4,
  "work_queue": "vectorbench"
}
```

That will generate four Prefect flow runs with:

- `worker_id = 0`
- `worker_id = 1`
- `worker_id = 2`
- `worker_id = 3`

and `total_workers = 4`.

### `get_batch_status`

Looks up all stored shard runs for a batch, fetches live Prefect state for each one, normalizes the state, and returns an aggregated batch status.

Returned counters include:

- `submitted`
- `running`
- `completed`
- `failed`
- `cancelled`

### `retry_failed_shards`

Resubmits only shards whose stored status is currently `failed`.

This is useful for shard-based batch jobs where you want to rerun only the broken workers instead of relaunching the full batch.

### `cancel_run`

Cancels a single Prefect flow run by id (sets the flow run state to `Cancelling`). Also marks the matching shard run as `cancelled` in SQLite if one exists.

### `cancel_batch`

Cancels every shard flow run in a batch and marks the batch as `cancelled`.

### `list_deployments`

Returns the raw list of Prefect deployments visible to the configured API. Useful for sanity-checking what is actually published vs. what is registered as a template locally.

### `delete_deployment`

Deletes the Prefect deployment behind a registered template. The template itself stays registered — pair with `unregister_template` for a full teardown.

### `pause_deployment` / `resume_deployment`

Pauses or resumes the Prefect deployment behind a template. While paused, new flow runs will not be picked up by workers; the template, deployment metadata, and any in-flight runs are untouched.

### `list_runs_in_deployment`

Lists recent flow runs for the deployment behind a template (default `limit=50`, sorted by expected start time descending). Each entry returns `flow_run_id`, `state`, and timing fields. Useful for inspecting runs that were *not* submitted through this MCP (e.g. retries from the Prefect UI).

### `retry_run`

Re-schedules a single flow run by setting its state to `Scheduled` with `force=True`. Works for runs in any terminal state (`Failed`, `Crashed`, `Cancelled`). The flow run id is preserved — Prefect tracks the retry as a new attempt on the same id.

### `list_flow_runs`

Cross-deployment flow run query. All arguments optional:

- `template_name` — limit to one template's deployment.
- `states` — list of state names to match (e.g. `["Failed", "Crashed"]`).
- `since` — ISO 8601 timestamp; only runs whose `expected_start_time` is after this are returned.
- `limit` — default 50.

Returns a flat list of `FlowRunSummary` (`flow_run_id`, `state`, `deployment_id`, timing fields). Pair with `retry_run` to fan-out a recovery sweep across recent failures.

## Database Layout

The SQLite database currently contains three tables:

- `execution_templates`
- `batch_launches`
- `shard_runs`

This is enough for the v1 server to:

- remember templates
- group runs into batches
- map each shard to a Prefect flow run id

## Expected Prefect Side

This MCP package does not create Prefect deployments for you.

You need an existing Prefect deployment that accepts parameters shaped like:

```json
{
  "repo_url": "https://github.com/DBgroup-Edinburgh/VectorBenchmark",
  "repo_local_path": "~/github/run-target",
  "branch": "encode-all-beir",
  "cmd": "uv run vectorbench embedding generate ..."
}
```

The current adapter calls these Prefect API operations:

- `POST /work_pools/filter`
- `POST /work_queues/filter`
- `POST /workers/filter`
- `POST /deployments/create_flow_run`
- `GET /flow_runs/{id}`
- `POST /flow_runs/{id}/set_state`
- `POST /logs/filter`

If your Prefect version uses different endpoints or payloads, adjust `prefect_sweep_mcp/prefect_adapter.py`.

## Local Smoke Test

The fastest way to test the server without integrating a full MCP client is:

1. Start a reachable Prefect API
2. Ensure the target deployment exists
3. Export the required environment variables
4. Run:

```bash
python -m prefect_sweep_mcp.server
```

Then connect from your MCP client and try:

1. `list_templates`
2. `list_workers`
3. `submit_batch` with `expected_shards=1`
4. `get_batch_status`

After that, test a shard launch with `expected_shards > 1`.

## Known Gaps

This is a v1 skeleton, not a finished platform.

Current limitations:

- no authentication layer in front of the MCP server
- template seeding is hardcoded in `server.py`
- no separate admin tool for managing templates
- no `README`-level packaging metadata yet
- `PREFECT_SWEEP_ALLOW_UNREGISTERED` is not wired into behavior
- Prefect endpoint compatibility depends on your deployed Prefect version

## Suggested Next Steps

If you want to take this beyond the bootstrap stage, the next practical steps are:

1. add a `pyproject.toml` for this standalone repo
2. move template registration out of `server.py`
3. add tools such as `get_template`, `list_work_pools`, `list_work_queues`, `cancel_batch`, and `get_run_logs`
4. add integration tests against a real Prefect instance
5. add audit logging and queue-level safety checks

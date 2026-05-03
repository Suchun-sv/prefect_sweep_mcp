from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

from .models import BatchLaunch, ExecutionTemplate, ShardRun


class PlatformStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(execution_templates)").fetchall()}
            if columns and "repo_local_path" not in columns:
                conn.executescript(
                    """
                    DROP TABLE IF EXISTS execution_templates;
                    DROP TABLE IF EXISTS batch_launches;
                    DROP TABLE IF EXISTS shard_runs;
                    """
                )
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS execution_templates (
                    id TEXT PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    project_id TEXT,
                    deployment_name TEXT NOT NULL,
                    repo_url TEXT NOT NULL,
                    repo_local_path TEXT NOT NULL,
                    default_branch TEXT,
                    default_env_json TEXT NOT NULL,
                    work_pool TEXT NOT NULL,
                    work_queue TEXT NOT NULL,
                    default_cmd TEXT NOT NULL,
                    command_template TEXT,
                    description TEXT NOT NULL,
                    allowed_queues_json TEXT NOT NULL,
                    allowed_launch_overrides_json TEXT NOT NULL,
                    allowed_tasks_json TEXT NOT NULL
                );
                
                CREATE TABLE IF NOT EXISTS batch_launches (
                    id TEXT PRIMARY KEY,
                    template_id TEXT NOT NULL,
                    submitted_at TEXT NOT NULL,
                    submitted_by TEXT NOT NULL,
                    status TEXT NOT NULL,
                    launch_overrides_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS shard_runs (
                    id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL,
                    shard_id TEXT NOT NULL,
                    worker_id INTEGER,
                    prefect_flow_run_id TEXT NOT NULL,
                    command TEXT NOT NULL,
                    status TEXT NOT NULL
                );
                """
            )

    def seed_template(self, template: ExecutionTemplate) -> ExecutionTemplate:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO execution_templates
                (id, name, project_id, deployment_name, repo_url, repo_local_path, default_branch, default_env_json,
                 work_pool, work_queue, default_cmd, command_template, description, allowed_queues_json,
                 allowed_launch_overrides_json, allowed_tasks_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    template.id,
                    template.name,
                    template.project_id,
                    template.deployment_name,
                    template.repo_url,
                    template.repo_local_path,
                    template.default_branch,
                    json.dumps(template.default_env),
                    template.work_pool,
                    template.work_queue,
                    template.default_cmd,
                    template.command_template,
                    template.description,
                    json.dumps(template.allowed_queues),
                    json.dumps(template.allowed_launch_overrides),
                    json.dumps(template.allowed_tasks),
                ),
            )
        return template

    def list_templates(self) -> list[ExecutionTemplate]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM execution_templates ORDER BY name").fetchall()
        return [self._template_from_row(row) for row in rows]

    def get_template_by_name(self, name: str) -> ExecutionTemplate | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM execution_templates WHERE name = ?",
                (name,),
            ).fetchone()
        return self._template_from_row(row) if row else None

    def create_batch(self, template_id: str, submitted_by: str, launch_overrides: dict | None = None) -> BatchLaunch:
        batch = BatchLaunch(
            id=str(uuid.uuid4()),
            template_id=template_id,
            submitted_by=submitted_by,
            launch_overrides=launch_overrides or {},
        )
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO batch_launches (id, template_id, submitted_at, submitted_by, status, launch_overrides_json) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    batch.id,
                    batch.template_id,
                    batch.submitted_at,
                    batch.submitted_by,
                    batch.status,
                    json.dumps(batch.launch_overrides),
                ),
            )
        return batch

    def update_batch_status(self, batch_id: str, status: str) -> None:
        with self._connect() as conn:
            conn.execute("UPDATE batch_launches SET status = ? WHERE id = ?", (status, batch_id))

    def get_batch(self, batch_id: str) -> BatchLaunch | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM batch_launches WHERE id = ?",
                (batch_id,),
            ).fetchone()
        if row is None:
            return None
        return BatchLaunch(
            id=row["id"],
            template_id=row["template_id"],
            submitted_at=row["submitted_at"],
            submitted_by=row["submitted_by"],
            status=row["status"],
            launch_overrides=json.loads(row["launch_overrides_json"]),
        )

    def add_shard_run(self, shard: ShardRun) -> ShardRun:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO shard_runs
                (id, batch_id, shard_id, worker_id, prefect_flow_run_id, command, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    shard.id,
                    shard.batch_id,
                    shard.shard_id,
                    shard.worker_id,
                    shard.prefect_flow_run_id,
                    shard.command,
                    shard.status,
                ),
            )
        return shard

    def list_shard_runs(self, batch_id: str) -> list[ShardRun]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM shard_runs WHERE batch_id = ? ORDER BY worker_id, shard_id",
                (batch_id,),
            ).fetchall()
        return [self._shard_from_row(row) for row in rows]

    def update_shard_status(self, prefect_flow_run_id: str, status: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE shard_runs SET status = ? WHERE prefect_flow_run_id = ?",
                (status, prefect_flow_run_id),
            )

    def _template_from_row(self, row: sqlite3.Row) -> ExecutionTemplate:
        return ExecutionTemplate(
            id=row["id"],
            name=row["name"],
            project_id=row["project_id"],
            deployment_name=row["deployment_name"],
            repo_url=row["repo_url"],
            repo_local_path=row["repo_local_path"],
            default_branch=row["default_branch"],
            default_env=json.loads(row["default_env_json"]),
            work_pool=row["work_pool"],
            work_queue=row["work_queue"],
            default_cmd=row["default_cmd"],
            command_template=row["command_template"],
            description=row["description"],
            allowed_queues=json.loads(row["allowed_queues_json"]),
            allowed_launch_overrides=json.loads(row["allowed_launch_overrides_json"]),
            allowed_tasks=json.loads(row["allowed_tasks_json"]),
        )

    def _shard_from_row(self, row: sqlite3.Row) -> ShardRun:
        return ShardRun(
            id=row["id"],
            batch_id=row["batch_id"],
            shard_id=row["shard_id"],
            worker_id=row["worker_id"],
            prefect_flow_run_id=row["prefect_flow_run_id"],
            command=row["command"],
            status=row["status"],
        )

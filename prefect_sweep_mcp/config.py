from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class MCPConfig:
    prefect_api_url: str = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")
    sqlite_path: str = os.getenv("PREFECT_SWEEP_MCP_DB", "prefect_sweep_mcp.db")
    allow_unregistered_templates: bool = os.getenv("PREFECT_SWEEP_ALLOW_UNREGISTERED", "false").lower() == "true"
    generated_dir: str = os.getenv("PREFECT_SWEEP_MCP_GENERATED_DIR", str(Path(".prefect_mcp")))

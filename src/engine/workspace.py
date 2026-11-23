"""Workspace (tenant) boundary and the single source of truth for lakehouse paths.

The default workspace keeps the historical layout (``<root>/<layer>/<table>``), so existing
data, commands and tests are unaffected. Any other workspace lives under
``<root>/workspaces/<workspace_id>/``. This module does not import ``src.config``, which lets
the configuration use it without an import cycle.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

DEFAULT_WORKSPACE = "default"
RUN_EVENTS_LAYER = "_ops"
RUN_EVENTS_TABLE = "run_events"
_WORKSPACE_ID = re.compile(r"^[a-z0-9][a-z0-9_-]{0,47}$")


@dataclass(frozen=True)
class Workspace:
    """A tenant's view of the lakehouse: where its Delta tables and reports live."""

    workspace_id: str
    storage: Literal["local", "s3"]
    local_root: Path
    s3_bucket: str
    s3_prefix: str

    def __post_init__(self) -> None:
        if not _WORKSPACE_ID.match(self.workspace_id):
            raise ValueError(
                "workspace_id must be 1-48 chars of lowercase letters, digits, '-' or '_', "
                f"got '{self.workspace_id}'"
            )

    @property
    def is_default(self) -> bool:
        return self.workspace_id == DEFAULT_WORKSPACE

    def table_uri(self, layer: str, name: str) -> str:
        """Delta table URI for a layer/table pair (local path or ``s3://`` URI)."""
        if self.storage == "s3":
            segment = "" if self.is_default else f"workspaces/{self.workspace_id}/"
            return f"s3://{self.s3_bucket}/{self.s3_prefix}/{segment}{layer}/{name}"
        base = self.local_root
        if not self.is_default:
            base = base / "workspaces" / self.workspace_id
        return str(base / layer / name)

    def run_events_uri(self) -> str:
        return self.table_uri(RUN_EVENTS_LAYER, RUN_EVENTS_TABLE)

    def reports_dir(self, output_dir: Path) -> Path:
        """Report folder for this workspace (the default workspace writes to ``output_dir``)."""
        return output_dir if self.is_default else output_dir / "workspaces" / self.workspace_id

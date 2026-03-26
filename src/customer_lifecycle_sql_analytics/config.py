from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProjectPaths:
    root: Path
    raw_dir: Path
    warehouse_dir: Path
    processed_dir: Path
    artifacts_dir: Path

    @classmethod
    def from_root(cls, root: Path) -> "ProjectPaths":
        return cls(
            root=root,
            raw_dir=root / "data" / "raw",
            warehouse_dir=root / "data" / "warehouse",
            processed_dir=root / "data" / "processed",
            artifacts_dir=root / "artifacts",
        )

    def ensure(self) -> None:
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.warehouse_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

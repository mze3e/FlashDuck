"""Local disk implementation of the SyncBase interface."""

from __future__ import annotations

import os
import shutil
from typing import Any, Dict, List, Optional

from .sync_base import SyncBase


class LocalDriveSyncManager(SyncBase):
    """Synchronize files using a local directory as the backend.

    This manager mirrors the behaviour of cloud-based sync providers but
    operates entirely on the local filesystem. It allows the package to have a
    drop-in default implementation without requiring external services.
    """

    def __init__(self, root_path: str) -> None:
        self.root_path = root_path
        self._running = False
        self._upload_info: Dict[str, Any] = {}
        self._download_info: Dict[str, Any] = {}

    def start(self) -> None:  # pragma: no cover - thin wrapper
        """Mark the manager as running."""
        self._running = True

    def stop(self) -> None:  # pragma: no cover - thin wrapper
        """Mark the manager as stopped."""
        self._running = False

    def upload_status(self) -> Dict[str, Any]:
        """Return information about the last upload."""
        return {"running": self._running, **self._upload_info}

    def download_status(self) -> Dict[str, Any]:
        """Return information about the last download."""
        return {"running": self._running, **self._download_info}

    def list_files(self) -> List[str]:
        """List relative file paths under ``root_path``."""
        files: List[str] = []
        for dirpath, _, filenames in os.walk(self.root_path):
            for name in filenames:
                full = os.path.join(dirpath, name)
                rel = os.path.relpath(full, self.root_path)
                files.append(rel.replace(os.sep, "/"))
        return files

    def upload_file(self, local_path: str, remote_path: Optional[str] = None) -> None:
        """Copy ``local_path`` into the managed directory."""
        remote_path = remote_path or os.path.basename(local_path)
        dest = os.path.join(self.root_path, remote_path)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.copy2(local_path, dest)
        self._upload_info = {"source": local_path, "destination": dest}

    def download_file(self, remote_path: str, local_path: str) -> None:
        """Copy ``remote_path`` from the managed directory to ``local_path``."""
        src = os.path.join(self.root_path, remote_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        shutil.copy2(src, local_path)
        self._download_info = {"source": src, "destination": local_path}

    def delete_file(self, remote_path: str) -> None:
        """Delete ``remote_path`` from the managed directory if it exists."""
        try:
            os.remove(os.path.join(self.root_path, remote_path))
        except FileNotFoundError:
            pass


__all__ = ["LocalDriveSyncManager"]

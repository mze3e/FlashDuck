"""Google Drive implementation of the :class:`SyncBase` interface."""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from .sync_base import SyncBase

try:  # pragma: no cover - optional dependency
    from pydrive2.auth import GoogleAuth
    from pydrive2.drive import GoogleDrive
except Exception:  # pragma: no cover - make library optional at import time
    GoogleAuth = None  # type: ignore
    GoogleDrive = None  # type: ignore


class GDriveSyncManager(SyncBase):
    """Synchronization manager for Google Drive.

    This manager handles OAuth2 authentication via ``pydrive2`` and stores
    refresh tokens on disk.  It exposes basic helpers for listing, uploading,
    downloading and deleting files using their Drive file IDs.
    """

    def __init__(
        self,
        client_config_file: str,
        credentials_file: str = "gdrive_token.json",
    ) -> None:
        if GoogleAuth is None or GoogleDrive is None:  # pragma: no cover
            raise ImportError("pydrive2 is required to use GDriveSyncManager")

        self.gauth = GoogleAuth()
        self.gauth.LoadClientConfigFile(client_config_file)
        self.credentials_file = credentials_file
        self.gauth.LoadCredentialsFile(credentials_file)
        if not self.gauth.credentials:
            self.gauth.LocalWebserverAuth()
        elif self.gauth.access_token_expired:
            self.gauth.Refresh()
        else:
            self.gauth.Authorize()
        self.gauth.SaveCredentialsFile(credentials_file)
        self.drive = GoogleDrive(self.gauth)
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

    def list_files(self, query: str = "trashed=false") -> List[Dict[str, Any]]:
        """List files in Drive matching ``query``.

        Parameters
        ----------
        query:
            Google Drive search query. Defaults to listing all non-trashed
            files.
        """
        return [
            {"id": f["id"], "title": f["title"]}
            for f in self.drive.ListFile({"q": query}).GetList()
        ]

    def upload_file(
        self,
        local_path: str,
        *,
        remote_name: Optional[str] = None,
        parent_id: Optional[str] = None,
    ) -> str:
        """Upload ``local_path`` to Drive and return the file ID."""
        metadata: Dict[str, Any] = {
            "title": remote_name or os.path.basename(local_path)
        }
        if parent_id:
            metadata["parents"] = [{"id": parent_id}]
        gfile = self.drive.CreateFile(metadata)
        gfile.SetContentFile(local_path)
        gfile.Upload()
        file_id = gfile["id"]
        self._upload_info = {"source": local_path, "file_id": file_id}
        return file_id

    def download_file(self, file_id: str, local_path: str) -> None:
        """Download the file with ``file_id`` to ``local_path``."""
        gfile = self.drive.CreateFile({"id": file_id})
        gfile.GetContentFile(local_path)
        self._download_info = {"file_id": file_id, "destination": local_path}

    def delete_file(self, file_id: str) -> None:
        """Delete the file with ``file_id`` from Drive."""
        gfile = self.drive.CreateFile({"id": file_id})
        gfile.Delete()


__all__ = ["GDriveSyncManager"]

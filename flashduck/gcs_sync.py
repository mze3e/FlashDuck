"""Google Cloud Storage implementation of the :class:`SyncBase` interface."""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from .sync_base import SyncBase

try:  # pragma: no cover - optional dependency
    from google.cloud import storage
except Exception:  # pragma: no cover - make library optional at import time
    storage = None  # type: ignore


class GCSSyncManager(SyncBase):
    """Synchronization manager for Google Cloud Storage.

    This manager provides minimal start/stop and status methods mirroring
    :class:`SMBSyncManager` while implementing basic file upload, download
    and deletion using the ``google-cloud-storage`` client.
    """

    def __init__(
        self,
        bucket_name: str,
        credentials_path: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        if storage is None:  # pragma: no cover - requires optional dependency
            raise ImportError(
                "google-cloud-storage is required to use GCSSyncManager"
            )

        if credentials_path:
            self.client = storage.Client.from_service_account_json(credentials_path)
        else:
            self.client = storage.Client()

        self.bucket = self.client.bucket(bucket_name)
        self._running = False
        self._upload_info: Dict[str, Any] = {}
        self._download_info: Dict[str, Any] = {}
        self.config = kwargs

    def start(self) -> None:
        """Mark the manager as running."""
        self._running = True

    def stop(self) -> None:
        """Mark the manager as stopped."""
        self._running = False

    def upload_file(self, local_path: str, remote_path: Optional[str] = None) -> None:
        """Upload a local file to the GCS bucket."""
        remote_path = remote_path or os.path.basename(local_path)
        blob = self.bucket.blob(remote_path)
        blob.upload_from_filename(local_path)
        self._upload_info = {"source": local_path, "destination": remote_path}

    def download_file(self, remote_path: str, local_path: str) -> None:
        """Download a file from the GCS bucket."""
        blob = self.bucket.blob(remote_path)
        blob.download_to_filename(local_path)
        self._download_info = {"source": remote_path, "destination": local_path}

    def delete_file(self, remote_path: str) -> None:
        """Delete a file from the GCS bucket."""
        blob = self.bucket.blob(remote_path)
        blob.delete()

    def upload_status(self) -> Dict[str, Any]:
        """Return status information about uploads."""
        return {"running": self._running, **self._upload_info}

    def download_status(self) -> Dict[str, Any]:
        """Return status information about downloads."""
        return {"running": self._running, **self._download_info}

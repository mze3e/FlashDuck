"""Dropbox implementation of the SyncBase interface."""

from __future__ import annotations

import os
import posixpath
from typing import Any, Dict, List, Set, Optional

import dropbox
from dropbox.files import FileMetadata, WriteMode

from .sync_base import SyncBase
from .config import Config


class DropboxSyncManager(SyncBase):
    """Synchronize local files with a Dropbox folder.

    This implementation performs a one-shot synchronization when ``start``
    is called. It uploads files from the ``pending_writes_dir`` to the
    configured Dropbox path, downloads any files present remotely but missing
    locally, and removes files that no longer exist on the other side so that
    both locations mirror each other.
    """

    def __init__(self, config: Config) -> None:
        token = config.dropbox_token
        if not token:
            raise ValueError("dropbox_token must be configured")
        self.config = config
        self.dbx = dropbox.Dropbox(token)
        self.base_path = config.dropbox_root_path or ""
        self._running = False
        self._upload_info: Dict[str, Any] = {}
        self._download_info: Dict[str, Any] = {}

    # SyncBase interface -------------------------------------------------
    def start(self) -> None:  # pragma: no cover - thin wrapper
        """Run a single synchronization cycle and mark as running."""
        self._running = True
        self.sync_once()

    def stop(self) -> None:  # pragma: no cover - thin wrapper
        """Mark the manager as stopped."""
        self._running = False

    def upload_status(self) -> Dict[str, Any]:
        return {"running": self._running, **self._upload_info}

    def download_status(self) -> Dict[str, Any]:
        return {"running": self._running, **self._download_info}

    # Internal helpers ---------------------------------------------------
    def sync_once(self) -> None:
        """Perform a single sync cycle."""
        remote_files = set(self._list_remote_files())
        uploaded = self._upload_pending()
        remote_files.update(uploaded)
        local_files = self._list_local_files()
        remote_deleted = self._delete_remote_stale(remote_files, local_files)
        for rel in remote_deleted:
            remote_files.discard(rel)
        downloaded = self._download_missing(remote_files, local_files)
        local_deleted = self._delete_local_stale(local_files, remote_files)
        self._upload_info = {"uploaded": uploaded, "remote_deleted": remote_deleted}
        self._download_info = {
            "downloaded": downloaded,
            "deleted": local_deleted,
        }

    def _full_path(self, rel: str) -> str:
        """Convert a relative path to a Dropbox API path."""
        rel = rel.replace(os.sep, "/")
        base = self.base_path.strip("/")
        if base:
            return "/" + posixpath.join(base, rel)
        return "/" + rel

    def _rel_path(self, path: str) -> str:
        """Convert a Dropbox API path to a relative path."""
        path = path.lstrip("/")
        base = self.base_path.strip("/").lower()
        lower = path.lower()
        if base and lower.startswith(base + "/"):
            path = path[len(base) + 1 :]
        return path.replace("/", os.sep)

    def _list_remote_files(self) -> List[str]:
        files: List[str] = []
        path = "/" + self.base_path.strip("/") if self.base_path else ""
        res = self.dbx.files_list_folder(path, recursive=True)
        while True:
            for entry in res.entries:
                if isinstance(entry, FileMetadata):
                    files.append(self._rel_path(entry.path_lower))
            if not res.has_more:
                break
            res = self.dbx.files_list_folder_continue(res.cursor)
        return files

    def _upload_pending(self) -> List[str]:
        uploaded: List[str] = []
        pending_dir = self.config.pending_writes_dir
        if not pending_dir or not os.path.isdir(pending_dir):
            return uploaded
        for root, _, files in os.walk(pending_dir):
            for name in files:
                local_path = os.path.join(root, name)
                rel = os.path.relpath(local_path, pending_dir)
                dest = self._full_path(rel)
                with open(local_path, "rb") as f:
                    self.dbx.files_upload(f.read(), dest, mode=WriteMode("overwrite"))
                uploaded.append(rel.replace(os.sep, "/"))
        return uploaded

    def _list_local_files(self) -> Set[str]:
        """List relative file paths in db_root excluding pending writes."""
        local: Set[str] = set()
        base = self.config.db_root
        pending_dir = self.config.pending_writes_dir
        pending_rel: Optional[str] = None
        if pending_dir:
            try:
                pending_rel = os.path.relpath(pending_dir, base)
            except ValueError:
                pending_rel = None
        for root, _, files in os.walk(base):
            for name in files:
                full = os.path.join(root, name)
                rel = os.path.relpath(full, base)
                if pending_rel and rel.startswith(pending_rel):
                    continue
                local.add(rel.replace(os.sep, "/"))
        return local

    def _download_missing(
        self, remote_files: Set[str], local_files: Set[str]
    ) -> List[str]:
        downloaded: List[str] = []
        base = self.config.db_root
        for rel in remote_files:
            if rel in local_files:
                continue
            dest = os.path.join(base, rel)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            self.dbx.files_download_to_file(dest, self._full_path(rel))
            downloaded.append(rel)
        return downloaded

    def _delete_local_stale(
        self, local_files: Set[str], remote_files: Set[str]
    ) -> List[str]:
        deleted: List[str] = []
        base = self.config.db_root
        for rel in local_files - remote_files:
            try:
                os.remove(os.path.join(base, rel))
                deleted.append(rel)
            except FileNotFoundError:
                pass
        return deleted

    def _delete_remote_stale(
        self, remote_files: Set[str], local_files: Set[str]
    ) -> List[str]:
        deleted: List[str] = []
        for rel in remote_files - local_files:
            try:
                self.dbx.files_delete_v2(self._full_path(rel))
                deleted.append(rel)
            except dropbox.exceptions.ApiError:
                pass
        return deleted

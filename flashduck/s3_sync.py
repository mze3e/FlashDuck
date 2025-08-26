"""S3 implementation of the SyncBase interface."""

from __future__ import annotations

import os
from typing import Any, Dict, List, Set, Optional

import boto3

from .sync_base import SyncBase
from .config import Config


class S3SyncManager(SyncBase):
    """Synchronize local files with an S3 bucket.

    This implementation performs a one-shot synchronization when ``start``
    is called. It uploads files from the ``pending_writes_dir`` to the
    configured bucket, downloads any objects present in S3 but missing
    locally, and removes local files that no longer exist in the bucket.
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self._running = False
        self._upload_info: Dict[str, Any] = {}
        self._download_info: Dict[str, Any] = {}
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            aws_session_token=config.aws_session_token,
            region_name=config.s3_region,
        )

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
        bucket = self.config.s3_bucket
        if not bucket:
            raise ValueError("s3_bucket must be configured")

        s3_objects = set(self._list_bucket_objects(bucket))
        uploaded = self._upload_pending(bucket)
        local_files = self._list_local_files()
        downloaded = self._download_missing(bucket, s3_objects, local_files)
        deleted = self._delete_stale(local_files, s3_objects)
        self._upload_info = {"uploaded": uploaded}
        self._download_info = {"downloaded": downloaded, "deleted": deleted}

    def _list_bucket_objects(self, bucket: str) -> List[str]:
        """Return all object keys in the bucket."""
        keys: List[str] = []
        token = None
        while True:
            params = {"Bucket": bucket}
            if token:
                params["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**params)
            for obj in resp.get("Contents", []):
                keys.append(obj["Key"])
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break
        return keys

    def _upload_pending(self, bucket: str) -> List[str]:
        uploaded: List[str] = []
        pending_dir = self.config.pending_writes_dir
        if not pending_dir or not os.path.isdir(pending_dir):
            return uploaded
        for root, _, files in os.walk(pending_dir):
            for name in files:
                local_path = os.path.join(root, name)
                key = os.path.relpath(local_path, pending_dir)
                with open(local_path, "rb") as f:
                    self.s3.put_object(Bucket=bucket, Key=key, Body=f)
                uploaded.append(key)
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
                local.add(rel)
        return local

    def _download_missing(
        self, bucket: str, s3_objects: Set[str], local_files: Set[str]
    ) -> List[str]:
        downloaded: List[str] = []
        base = self.config.db_root
        for key in s3_objects:
            if key in local_files:
                continue
            dest = os.path.join(base, key)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            obj = self.s3.get_object(Bucket=bucket, Key=key)
            body = obj["Body"].read()
            with open(dest, "wb") as f:
                f.write(body)
            downloaded.append(key)
        return downloaded

    def _delete_stale(
        self, local_files: Set[str], s3_objects: Set[str]
    ) -> List[str]:
        deleted: List[str] = []
        base = self.config.db_root
        for rel in local_files - s3_objects:
            try:
                os.remove(os.path.join(base, rel))
                deleted.append(rel)
            except FileNotFoundError:
                pass
        return deleted

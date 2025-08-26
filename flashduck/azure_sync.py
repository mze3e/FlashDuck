"""Azure Blob Storage synchronization helpers.

This module provides simple upload, download and delete loops for
interacting with an Azure Blob Storage container.  These utilities are
designed to be lightweight and rely only on the ``azure-storage-blob``
package.  Each loop runs until the supplied ``threading.Event`` is set,
allowing callers to manage the lifecycle from a different thread.

The functions operate relative to a local directory.  Files within the
directory are uploaded to the container using their relative paths as
blob names.  Downloads and deletions mirror this behaviour to keep the
local directory and the remote container in sync.
"""

from __future__ import annotations

import os
from typing import Iterable
from threading import Event

from azure.storage.blob import ContainerClient


def _iter_local_files(root: str) -> Iterable[tuple[str, str]]:
    """Yield tuples of (absolute_path, relative_blob_name)."""

    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            full_path = os.path.join(dirpath, name)
            # Always use forward slashes for blob names
            rel = os.path.relpath(full_path, root).replace("\\", "/")
            yield full_path, rel


def upload_once(local_dir: str, container: ContainerClient) -> None:
    """Upload all files from ``local_dir`` to the Azure container."""

    for full_path, blob_name in _iter_local_files(local_dir):
        with open(full_path, "rb") as data:
            container.upload_blob(name=blob_name, data=data, overwrite=True)


def download_once(local_dir: str, container: ContainerClient) -> None:
    """Download any blobs that do not exist locally."""

    for blob in container.list_blobs():
        local_path = os.path.join(local_dir, blob.name)
        if os.path.exists(local_path):
            continue
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as fh:
            data = container.download_blob(blob.name).readall()
            fh.write(data)


def delete_once(local_dir: str, container: ContainerClient) -> None:
    """Delete blobs that no longer exist locally."""

    local_files = {rel for _, rel in _iter_local_files(local_dir)}
    remote_files = {blob.name for blob in container.list_blobs()}
    for blob_name in remote_files - local_files:
        container.delete_blob(blob_name)


def upload_loop(local_dir: str, container: ContainerClient, stop_event: Event, *, interval: int = 60) -> None:
    """Continuously upload files until ``stop_event`` is set."""

    while not stop_event.is_set():
        upload_once(local_dir, container)
        stop_event.wait(interval)


def download_loop(local_dir: str, container: ContainerClient, stop_event: Event, *, interval: int = 60) -> None:
    """Continuously download missing files until ``stop_event`` is set."""

    while not stop_event.is_set():
        download_once(local_dir, container)
        stop_event.wait(interval)


def delete_loop(local_dir: str, container: ContainerClient, stop_event: Event, *, interval: int = 60) -> None:
    """Continuously delete orphaned blobs until ``stop_event`` is set."""

    while not stop_event.is_set():
        delete_once(local_dir, container)
        stop_event.wait(interval)


__all__ = [
    "upload_once",
    "download_once",
    "delete_once",
    "upload_loop",
    "download_loop",
    "delete_loop",
]


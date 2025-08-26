"""Tests for Azure blob synchronization helpers."""

import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

# Ensure the project root is on the path for import
sys.path.append(str(Path(__file__).resolve().parent.parent))

from flashduck.azure_sync import upload_once, download_once, delete_once


class DummyContainer:
    """Minimal stand-in for :class:`ContainerClient` used in tests."""

    def __init__(self):
        self.blobs: dict[str, bytes] = {}

    # Methods that mirror the real container client
    def upload_blob(self, name: str, data, overwrite: bool = False) -> None:  # noqa: D401
        self.blobs[name] = data.read()

    def list_blobs(self):  # noqa: D401
        for name in self.blobs:
            yield SimpleNamespace(name=name)

    def download_blob(self, name: str):  # noqa: D401
        data = self.blobs[name]

        class Downloader:
            def readall(self) -> bytes:  # noqa: D401
                return data

        return Downloader()

    def delete_blob(self, name: str) -> None:  # noqa: D401
        self.blobs.pop(name, None)


def test_upload_download_delete_cycle():
    container = DummyContainer()
    with tempfile.TemporaryDirectory() as tmp:
        local_file = os.path.join(tmp, "sample.txt")

        # Create and upload a file
        with open(local_file, "w", encoding="utf-8") as fh:
            fh.write("hi")
        upload_once(tmp, container)
        assert "sample.txt" in container.blobs

        # Remove it locally and download from container
        os.remove(local_file)
        download_once(tmp, container)
        assert os.path.exists(local_file)

        # Remove locally again and ensure remote deletion
        os.remove(local_file)
        delete_once(tmp, container)
        assert "sample.txt" not in container.blobs


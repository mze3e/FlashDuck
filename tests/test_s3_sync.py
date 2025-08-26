import io
import sys
from pathlib import Path
from typing import Dict

import boto3
import pytest

# Ensure package root on path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from flashduck.config import Config
from flashduck.s3_sync import S3SyncManager


class FakeS3Client:
    """Very small in-memory mock of S3."""

    def __init__(self) -> None:
        self.objects: Dict[str, bytes] = {}

    def list_objects_v2(self, Bucket, ContinuationToken=None):  # noqa: N802
        contents = [{"Key": k} for k in sorted(self.objects.keys())]
        return {"Contents": contents, "IsTruncated": False}

    def put_object(self, Bucket, Key, Body):  # noqa: N802
        data = Body.read() if hasattr(Body, "read") else Body
        self.objects[Key] = data
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def get_object(self, Bucket, Key):  # noqa: N802
        return {"Body": io.BytesIO(self.objects[Key])}


@pytest.fixture
def fake_s3(monkeypatch):
    client = FakeS3Client()

    def factory(service_name, **kwargs):
        assert service_name == "s3"
        return client

    monkeypatch.setattr(boto3, "client", factory)
    return client


def test_s3_sync_cycle(tmp_path, fake_s3):
    # remote object to be downloaded
    fake_s3.objects["remote.txt"] = b"remote data"

    db_root = tmp_path / "db"
    pending = db_root / "pending"
    db_root.mkdir()
    pending.mkdir()

    # create pending upload
    (pending / "upload.txt").write_text("upload data")
    # create stale file that should be deleted
    (db_root / "stale.txt").write_text("stale")

    config = Config(
        db_root=str(db_root),
        pending_writes_dir=str(pending),
        s3_bucket="bucket",
    )

    mgr = S3SyncManager(config)
    mgr.start()

    # Upload occurred
    assert fake_s3.objects["upload.txt"] == b"upload data"
    assert "upload.txt" in mgr.upload_status()["uploaded"]

    # Download occurred
    assert (db_root / "remote.txt").read_text() == "remote data"
    assert "remote.txt" in mgr.download_status()["downloaded"]

    # Stale file deleted
    assert not (db_root / "stale.txt").exists()
    assert "stale.txt" in mgr.download_status()["deleted"]

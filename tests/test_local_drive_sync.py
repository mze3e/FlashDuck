import os
from flashduck.local_drive_sync import LocalDriveSyncManager


def test_local_drive_sync_cycle(tmp_path):
    remote = tmp_path / "remote"
    remote.mkdir()

    mgr = LocalDriveSyncManager(str(remote))
    mgr.start()

    # Upload a file into remote directory
    src = tmp_path / "src.txt"
    src.write_text("hello")
    mgr.upload_file(str(src), "uploaded.txt")
    assert (remote / "uploaded.txt").read_text() == "hello"
    assert mgr.upload_status()["destination"].endswith("uploaded.txt")

    # List files
    files = mgr.list_files()
    assert "uploaded.txt" in files

    # Download the file to a new location
    dest = tmp_path / "downloaded.txt"
    mgr.download_file("uploaded.txt", str(dest))
    assert dest.read_text() == "hello"
    assert mgr.download_status()["source"].endswith("uploaded.txt")

    # Delete the file from the remote directory
    mgr.delete_file("uploaded.txt")
    assert not (remote / "uploaded.txt").exists()

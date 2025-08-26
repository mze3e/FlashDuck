"""SMB implementation of the :class:`SyncBase` interface."""

from typing import Any, Dict

from .sync_base import SyncBase


class SMBSyncManager(SyncBase):
    """Basic synchronization manager for SMB shares.

    This class currently provides minimal structure required by the
    :class:`SyncBase` interface. Real SMB interactions such as actual file
    transfers should be implemented by expanding these methods.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Create a new :class:`SMBSyncManager`.

        Parameters in ``kwargs`` are stored for future use by concrete
        implementations. They may include connection details like ``server``
        or authentication information. This keeps the class flexible while
        satisfying the interface for testing purposes.
        """
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

    def upload_status(self) -> Dict[str, Any]:
        """Return status information about uploads."""
        return {"running": self._running, **self._upload_info}

    def download_status(self) -> Dict[str, Any]:
        """Return status information about downloads."""
        return {"running": self._running, **self._download_info}

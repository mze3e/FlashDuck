"""Base interface for synchronization managers."""

from abc import ABC, abstractmethod
from typing import Any, Dict


class SyncBase(ABC):
    """Abstract base class for file synchronization managers.

    Concrete implementations should provide logic for starting and
    stopping synchronization as well as reporting upload and download
    status information.
    """

    @abstractmethod
    def start(self) -> None:
        """Begin synchronization operations."""

    @abstractmethod
    def stop(self) -> None:
        """Stop any active synchronization operations."""

    @abstractmethod
    def upload_status(self) -> Dict[str, Any]:
        """Return information about current or last upload operation."""

    @abstractmethod
    def download_status(self) -> Dict[str, Any]:
        """Return information about current or last download operation."""

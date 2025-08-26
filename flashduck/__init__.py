"""FlashDuck: High-performance data management using DuckDB."""

from .core import FlashDuckEngine
from .cache import CacheManager
from .query import QueryEngine
from .parquet_writer import ParquetWriter
from .file_monitor import FileMonitor
from .config import Config
from .sync_base import SyncBase
from .smb_sync_manager import SMBSyncManager
from .azure_sync import (
    upload_loop,
    download_loop,
    delete_loop,
)
from .gcs_sync import GCSSyncManager
from .s3_sync import S3SyncManager
from .dropbox_sync import DropboxSyncManager

__version__ = "0.1.0"
__author__ = "FlashDuck Contributors"
__email__ = "ahmedmzl@gmail.com"

__all__ = [
    "FlashDuckEngine",
    "CacheManager",
    "QueryEngine",
    "ParquetWriter",
    "FileMonitor",
    "Config",
    "SyncBase",
    "SMBSyncManager",
    "upload_loop",
    "download_loop",
    "delete_loop",
    "GCSSyncManager",
    "S3SyncManager",
    "DropboxSyncManager",
]

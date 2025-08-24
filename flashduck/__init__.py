"""FlashDuck: High-performance data management using DuckDB."""

from .core import FlashDuckEngine
from .cache import CacheManager
from .query import QueryEngine
from .parquet_writer import ParquetWriter
from .file_monitor import FileMonitor
from .config import Config

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
]

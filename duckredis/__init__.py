"""
DuckRedis: High-performance data management combining DuckDB and Redis
"""

from .core import DuckRedisEngine
from .cache import CacheManager
from .query import QueryEngine
from .parquet_writer import ParquetWriter
from .file_monitor import FileMonitor
from .config import Config

__version__ = "0.1.0"
__author__ = "DuckRedis Contributors"
__email__ = "contact@duckredis.org"

__all__ = [
    "DuckRedisEngine",
    "CacheManager", 
    "QueryEngine",
    "ParquetWriter",
    "FileMonitor",
    "Config"
]

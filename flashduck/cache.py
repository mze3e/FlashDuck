"""Backward compatibility wrapper for DuckDB-based cache."""
from .duckdb_cache import DuckDBCache as CacheManager, DuckDBCache

__all__ = ["CacheManager", "DuckDBCache"]

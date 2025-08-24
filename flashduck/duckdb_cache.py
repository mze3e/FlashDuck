import os
import json
import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd

from .config import Config


class DuckDBCache:
    """Persistent DuckDB-backed cache for table snapshots and change tracking."""

    def __init__(self, config: Config, cache_db_path: Optional[str] = None):
        self.config = config
        self.cache_db_path = cache_db_path or config.cache_db_path
        os.makedirs(os.path.dirname(self.cache_db_path), exist_ok=True)

        self.conn = duckdb.connect(self.cache_db_path)
        self.logger = logging.getLogger(__name__)
        self._init_metadata_table()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _init_metadata_table(self) -> None:
        """Ensure metadata table exists."""
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                table_name TEXT PRIMARY KEY,
                row_count  BIGINT,
                columns    TEXT,
                last_refresh TIMESTAMP
            )
            """
        )

    def _update_metadata(self, table_name: str) -> None:
        """Update metadata for a specific table."""
        row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        columns = [c[1] for c in self.conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()]
        self.conn.execute(
            "INSERT OR REPLACE INTO metadata VALUES (?, ?, ?, ?)",
            [
                table_name,
                row_count,
                json.dumps(columns),
                datetime.utcnow(),
            ],
        )

    def _write_pending(self, table_name: str, df: pd.DataFrame, op: str) -> str:
        """Write changed rows to the configured pending_writes_dir as Parquet."""
        pending_dir = self.config.pending_writes_dir
        os.makedirs(pending_dir, exist_ok=True)
        filename = f"{table_name}_{int(time.time()*1000)}_{op}.parquet"
        path = os.path.join(pending_dir, filename)
        df.to_parquet(path, index=False)
        return path

    # ------------------------------------------------------------------
    # Snapshot operations
    # ------------------------------------------------------------------
    def store_table_snapshot(self, table_name: str, df: pd.DataFrame) -> None:
        """Persist entire table snapshot into DuckDB."""
        self.conn.register("_df", df)
        self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM _df")
        self.conn.unregister("_df")
        self._update_metadata(table_name)
        self.logger.info(
            "Stored table '%s': %d rows, %d columns",
            table_name,
            len(df),
            len(df.columns),
        )

    def store_snapshot(self, df: pd.DataFrame) -> None:
        self.store_table_snapshot(self.config.table_name, df)

    def load_table_snapshot(self, table_name: str) -> Optional[pd.DataFrame]:
        try:
            return self.conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        except Exception:
            return None

    def load_snapshot(self) -> Optional[pd.DataFrame]:
        return self.load_table_snapshot(self.config.table_name)

    def get_table_names(self) -> List[str]:
        rows = self.conn.execute("SELECT table_name FROM metadata").fetchall()
        return [r[0] for r in rows]

    def get_all_tables(self) -> Dict[str, pd.DataFrame]:
        tables: Dict[str, pd.DataFrame] = {}
        for name in self.get_table_names():
            df = self.load_table_snapshot(name)
            if df is not None:
                tables[name] = df
        return tables

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        row = self.conn.execute(
            "SELECT row_count, columns, last_refresh FROM metadata WHERE table_name = ?",
            [table_name],
        ).fetchone()
        if not row:
            return {}
        return {
            "rows": row[0],
            "column_names": json.loads(row[1]),
            "last_refresh": row[2].isoformat() if row[2] else None,
        }

    def get_snapshot_info(self) -> Dict[str, Any]:
        return self.get_table_info(self.config.table_name)

    def clear_cache(self) -> None:
        for name in self.get_table_names():
            self.conn.execute(f"DROP TABLE IF EXISTS {name}")
        self.conn.execute("DELETE FROM metadata")
        self.logger.info("Cache cleared")

    # ------------------------------------------------------------------
    # Change queue operations
    # ------------------------------------------------------------------
    def enqueue_upsert(self, record_id: Any, data: Dict[str, Any]) -> str:
        table_name = self.config.table_name
        pk = self.config.table_primary_keys.get(table_name, "id")
        df = pd.DataFrame([data])
        if pk not in df.columns:
            df[pk] = record_id
        record_id = df[pk].iloc[0]

        # Delete existing and insert new
        self.conn.execute(f"DELETE FROM {table_name} WHERE {pk} = ?", [record_id])
        self.conn.register("_temp", df)
        self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM _temp")
        self.conn.unregister("_temp")

        self._update_metadata(table_name)
        path = self._write_pending(table_name, df, "upsert")
        self.logger.info("Upserted id %s into %s", record_id, table_name)
        return path

    def enqueue_delete(self, record_id: Any) -> str:
        table_name = self.config.table_name
        pk = self.config.table_primary_keys.get(table_name, "id")
        self.conn.execute(f"DELETE FROM {table_name} WHERE {pk} = ?", [record_id])
        self._update_metadata(table_name)
        df = pd.DataFrame([{pk: record_id}])
        path = self._write_pending(table_name, df, "delete")
        self.logger.info("Deleted id %s from %s", record_id, table_name)
        return path

    # ------------------------------------------------------------------
    def is_connected(self) -> bool:
        try:
            self.conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

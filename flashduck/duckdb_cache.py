import os
import json
import time
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import duckdb
import pandas as pd

from .config import Config


class DuckDBCache:
    """In-memory DuckDB-backed cache for table snapshots and change tracking."""

    def __init__(self, config: Config):
        self.config = config
        # Use an in-memory DuckDB instance instead of a file-based database
        self.conn = duckdb.connect()
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

    def _resolve_pk(
        self, table_name: str, df: Optional[pd.DataFrame] = None
    ) -> Optional[str]:
        """Resolve primary key using config or schema inspection."""
        pk = self.config.table_primary_keys.get(table_name)
        if isinstance(pk, (list, tuple)):
            pk = pk[0] if pk else None
        if pk:
            return pk
        columns: List[str] = []
        if df is not None:
            columns = df.columns.tolist()
        else:
            try:
                columns = [
                    c[1]
                    for c in self.conn.execute(
                        f"PRAGMA table_info('{table_name}')"
                    ).fetchall()
                ]
            except Exception:
                pass
        if 'id' in columns:
            return 'id'
        if 'key' in columns:
            return 'key'
        return None

    # ------------------------------------------------------------------
    # Pending writes helpers
    # ------------------------------------------------------------------
    def list_pending_writes(self) -> List[str]:
        """Return list of pending write Parquet files"""
        pending_dir = self.config.pending_writes_dir
        if not os.path.exists(pending_dir):
            return []
        return sorted(
            [
                os.path.join(pending_dir, f)
                for f in os.listdir(pending_dir)
                if f.endswith(".parquet")
            ]
        )

    def purge_pending_writes(self) -> int:
        """Delete all pending write files"""
        files = self.list_pending_writes()
        for path in files:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        return len(files)

    def process_pending_writes(
        self,
        upload_fn: Optional[Callable[[str], None]] = None,
        purge: bool = True,
    ) -> List[str]:
        """Optionally upload and purge pending write files"""
        files = self.list_pending_writes()
        for path in files:
            if upload_fn:
                try:
                    upload_fn(path)
                except Exception as e:
                    self.logger.error(f"Upload failed for {path}: {e}")
        if purge:
            self.purge_pending_writes()
        return files

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

    def store_ranked_table(
        self, table_name: str, df: pd.DataFrame, primary_key: Optional[Any] = None
    ) -> None:
        """Store table and create ranked view ordered by _modified_time.

        Supports both single and composite primary keys.
        """
        raw_table = f"{table_name}__raw"
        self.conn.register("_df", df)
        self.conn.execute(f"CREATE OR REPLACE TABLE {raw_table} AS SELECT * FROM _df")
        self.conn.unregister("_df")

        pk_cols: List[str] = []
        if isinstance(primary_key, (list, tuple)):
            pk_cols = [col for col in primary_key if col in df.columns]
        elif isinstance(primary_key, str) and primary_key in df.columns:
            pk_cols = [primary_key]

        if pk_cols and "_modified_time" in df.columns:
            partition_by = ", ".join(pk_cols)
            self.conn.execute(
                f"""
                CREATE OR REPLACE VIEW {table_name} AS
                SELECT * FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY _modified_time DESC) AS rn
                    FROM {raw_table}
                ) WHERE rn = 1
                """
            )
        else:
            # If primary key or _modified_time missing, expose raw table directly
            self.conn.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM {raw_table}"
            )

        self._update_metadata(table_name)
        self.logger.info(
            "Stored ranked table '%s': %d rows, %d columns",
            table_name,
            len(df),
            len(df.columns),
        )

    def remove_table(self, table_name: str) -> None:
        """Drop view/table and remove metadata for a table."""
        raw_table = f"{table_name}__raw"
        self.conn.execute(f"DROP VIEW IF EXISTS {table_name}")
        self.conn.execute(f"DROP TABLE IF EXISTS {raw_table}")
        self.conn.execute("DELETE FROM metadata WHERE table_name = ?", [table_name])

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

    def get_all_table_info(self) -> Dict[str, Dict[str, Any]]:
        info: Dict[str, Dict[str, Any]] = {}
        for name in self.get_table_names():
            info[name] = self.get_table_info(name)
        return info

    def clear_cache(self) -> None:
        for name in self.get_table_names():
            raw_table = f"{name}__raw"
            self.conn.execute(f"DROP VIEW IF EXISTS {name}")
            self.conn.execute(f"DROP TABLE IF EXISTS {raw_table}")
        self.conn.execute("DELETE FROM metadata")
        self.logger.info("Cache cleared")

    # ------------------------------------------------------------------
    # Change queue operations
    # ------------------------------------------------------------------
    def enqueue_upsert(self, record_id: Any, data: Dict[str, Any]) -> str:
        table_name = self.config.table_name
        df = pd.DataFrame([data])
        pk = self._resolve_pk(table_name, df)
        if not pk:
            raise ValueError(f"Cannot determine primary key for table '{table_name}'")
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
        pk = self._resolve_pk(table_name)
        if not pk:
            raise ValueError(f"Cannot determine primary key for table '{table_name}'")
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

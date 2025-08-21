"""
Utility functions for FlashDuck
"""

import os
import json
import logging
import tempfile
from typing import Any, Dict, List, Optional
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger('flashduck')


def ensure_directory(path: str) -> None:
    """Ensure directory exists"""
    Path(path).mkdir(parents=True, exist_ok=True)


def atomic_write_file(filepath: str, content: bytes) -> None:
    """Atomically write content to file"""
    directory = os.path.dirname(filepath)
    ensure_directory(directory)
    
    # Write to temporary file first
    with tempfile.NamedTemporaryFile(
        dir=directory, 
        delete=False, 
        prefix='.tmp_',
        suffix=os.path.basename(filepath)
    ) as tmp_file:
        tmp_file.write(content)
        tmp_filepath = tmp_file.name
    
    # Atomic replace
    os.replace(tmp_filepath, filepath)


def load_json_files(directory: str, pattern: str = "*.json") -> List[Dict[str, Any]]:
    """Load all JSON files from directory matching pattern"""
    data = []
    directory_path = Path(directory)
    
    if not directory_path.exists():
        return data
    
    for file_path in directory_path.glob(pattern):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                file_data = json.load(f)
                # Add metadata
                if isinstance(file_data, dict):
                    file_data['_source_file'] = str(file_path.name)
                    file_data['_modified_time'] = file_path.stat().st_mtime
                data.append(file_data)
        except (json.JSONDecodeError, IOError) as e:
            logging.warning(f"Failed to load {file_path}: {e}")
    
    return data


def dataframe_to_arrow_ipc(df: pd.DataFrame) -> bytes:
    """Convert DataFrame to Arrow IPC bytes"""
    table = pa.Table.from_pandas(df)
    buffer = pa.BufferOutputStream()
    with pa.ipc.new_stream(buffer, table.schema) as writer:
        writer.write_table(table)
    return buffer.getvalue().to_pybytes()


def arrow_ipc_to_dataframe(data: bytes) -> pd.DataFrame:
    """Convert Arrow IPC bytes to DataFrame"""
    reader = pa.ipc.open_stream(pa.py_buffer(data))
    table = reader.read_all()
    return table.to_pandas()


def dataframe_to_parquet_bytes(df: pd.DataFrame, compression: str = "zstd") -> bytes:
    """Convert DataFrame to Parquet bytes"""
    table = pa.Table.from_pandas(df)
    buffer = pa.BufferOutputStream()
    pq.write_table(table, buffer, compression=compression)
    return buffer.getvalue().to_pybytes()


def merge_schemas(dataframes: List[pd.DataFrame], evolution: str = "union") -> pd.DataFrame:
    """Merge multiple DataFrames with schema evolution"""
    if not dataframes:
        return pd.DataFrame()
    
    if len(dataframes) == 1:
        return dataframes[0]
    
    if evolution == "strict":
        # All dataframes must have same columns
        first_columns = set(dataframes[0].columns)
        for i, df in enumerate(dataframes[1:], 1):
            if set(df.columns) != first_columns:
                raise ValueError(
                    f"Schema mismatch in dataframe {i}: "
                    f"expected {first_columns}, got {set(df.columns)}"
                )
        return pd.concat(dataframes, ignore_index=True)
    
    elif evolution == "union":
        # Union all columns, fill missing with nulls
        all_columns = set()
        for df in dataframes:
            all_columns.update(df.columns)
        
        # Pad missing columns with nulls
        padded_dfs = []
        for df in dataframes:
            padded_df = df.copy()
            for col in all_columns:
                if col not in padded_df.columns:
                    padded_df[col] = None
            # Reorder columns consistently
            padded_df = padded_df[sorted(all_columns)]
            padded_dfs.append(padded_df)
        
        return pd.concat(padded_dfs, ignore_index=True)
    
    else:
        raise ValueError(f"Unknown schema evolution strategy: {evolution}")


def format_bytes(size: int) -> str:
    """Format bytes as human readable string"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"


def validate_sql_readonly(sql: str) -> None:
    """Validate that SQL is read-only (no DDL/DML)"""
    sql_upper = sql.upper().strip()
    
    # List of forbidden keywords that indicate write operations
    forbidden_keywords = [
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 
        'TRUNCATE', 'REPLACE', 'MERGE', 'COPY', 'CALL', 'EXEC'
    ]
    
    # Check if any forbidden keyword appears at the start of a statement
    statements = [stmt.strip() for stmt in sql_upper.split(';') if stmt.strip()]
    
    for statement in statements:
        first_word = statement.split()[0] if statement.split() else ''
        if first_word in forbidden_keywords:
            raise ValueError(f"Write operation not allowed: {first_word}")

"""
DuckDB query engine for FlashDuck
"""

import json
import logging
from typing import Any, Dict, List, Optional, Union
import duckdb
import pandas as pd
from .cache import CacheManager
from .config import Config
from .utils import validate_sql_readonly, dataframe_to_arrow_ipc


class QueryEngine:
    """Executes SQL queries using DuckDB on cached data"""
    
    def __init__(self, config: Config, cache_manager: CacheManager):
        self.config = config
        self.cache_manager = cache_manager
        self.logger = logging.getLogger(__name__)
    
    def execute_sql(self, sql: str) -> Dict[str, Any]:
        """Execute SQL query and return results"""
        try:
            # Validate SQL is read-only
            validate_sql_readonly(sql)
            
            # Load all tables from cache
            tables = self.cache_manager.get_all_tables()
            if not tables:
                return {
                    "success": False,
                    "error": "No tables available in cache",
                    "rows": 0,
                    "columns": [],
                    "data": []
                }
            
            # Execute query with DuckDB
            conn = duckdb.connect()
            try:
                # Register all tables in DuckDB
                for table_name, df in tables.items():
                    conn.register(table_name, df)
                
                # Execute query
                result = conn.execute(sql).fetchdf()
                
                # Format output based on configuration
                if self.config.sql_output_format == "json":
                    data = result.to_dict('records')
                elif self.config.sql_output_format == "csv":
                    data = result.to_csv(index=False)
                elif self.config.sql_output_format == "arrow":
                    data = dataframe_to_arrow_ipc(result).hex()  # Hex encode for JSON serialization
                else:
                    data = result.to_dict('records')  # Default to JSON
                
                return {
                    "success": True,
                    "rows": len(result),
                    "columns": list(result.columns),
                    "data": data,
                    "sql": sql,
                    "format": self.config.sql_output_format,
                    "available_tables": list(tables.keys())
                }
                
            finally:
                conn.close()
                
        except Exception as e:
            self.logger.error(f"SQL execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "rows": 0,
                "columns": [],
                "data": [],
                "sql": sql
            }
    
    def execute_sql_direct_parquet(self, sql: str, db_root: str) -> Dict[str, Any]:
        """Execute SQL query directly against parquet files with ranked deduplication"""
        try:
            import time
            from pathlib import Path
            
            start_time = time.time()
            
            # Validate SQL is read-only
            validate_sql_readonly(sql)
            
            db_path = Path(db_root)
            if not db_path.exists():
                return {
                    "success": False,
                    "error": f"Database directory does not exist: {db_root}",
                    "rows": 0,
                    "columns": [],
                    "data": [],
                    "sql": sql
                }
            
            conn = duckdb.connect()
            
            try:
                # Group parquet files by table name and create ranked views
                parquet_files = list(db_path.glob("*.parquet"))
                table_files = {}
                
                for file_path in parquet_files:
                    filename = file_path.stem
                    
                    # Extract table name from partition filename or use as is
                    if "_partition_" in filename:
                        table_name = filename.split("_partition_")[0]
                    else:
                        table_name = filename
                    
                    if table_name not in table_files:
                        table_files[table_name] = []
                    table_files[table_name].append(str(file_path))
                
                # For each table, create a view with ranked deduplication
                for table_name, files in table_files.items():
                    # Create file pattern for DuckDB
                    file_pattern = f"[{','.join(repr(f) for f in files)}]"
                    
                    # Get primary key for this table
                    primary_key = self.config.table_primary_keys.get(table_name, 'id')
                    
                    # Create ranked view with deduplication
                    ranked_sql = f"""
                    CREATE OR REPLACE VIEW {table_name} AS
                    SELECT * FROM (
                        SELECT *,
                               ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY _modified_time DESC) as rn
                        FROM read_parquet({file_pattern})
                    ) ranked
                    WHERE rn = 1
                    """
                    
                    try:
                        conn.execute(ranked_sql)
                        self.logger.debug(f"Created ranked view for table '{table_name}' from {len(files)} files")
                    except Exception as e:
                        # If ranking fails (e.g., no primary key column), use simple union
                        union_sql = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet({file_pattern})"
                        conn.execute(union_sql)
                        self.logger.debug(f"Created simple view for table '{table_name}' from {len(files)} files")
                
                # Execute the user's query
                result = conn.execute(sql).fetchdf()
                
                query_time = time.time() - start_time
                
                # Format output based on configuration
                if self.config.sql_output_format == "json":
                    data = result.to_dict('records')
                elif self.config.sql_output_format == "csv":
                    data = result.to_csv(index=False)
                elif self.config.sql_output_format == "arrow":
                    data = dataframe_to_arrow_ipc(result).hex()  # Hex encode for JSON serialization
                else:
                    data = result.to_dict('records')  # Default to JSON
                
                return {
                    "success": True,
                    "rows": len(result),
                    "columns": list(result.columns),
                    "data": data,
                    "sql": sql,
                    "format": self.config.sql_output_format,
                    "query_time": query_time,
                    "source": "direct_parquet",
                    "available_tables": list(table_files.keys())
                }
                
            finally:
                conn.close()
                
        except Exception as e:
            self.logger.error(f"Direct parquet SQL execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "rows": 0,
                "columns": [],
                "data": [],
                "sql": sql,
                "source": "direct_parquet"
            }
    
    def get_table_info(self) -> Dict[str, Any]:
        """Get information about all cached tables"""
        try:
            # Get info for all tables
            all_table_info = self.cache_manager.get_all_table_info()
            table_names = self.cache_manager.get_table_names()
            
            if not table_names:
                return {
                    "exists": False,
                    "tables": {},
                    "total_tables": 0,
                    "total_rows": 0,
                    "total_size_bytes": 0
                }
            
            total_rows = sum(info.get("rows", 0) for info in all_table_info.values())
            total_size = sum(info.get("size_bytes", 0) for info in all_table_info.values())
            
            return {
                "exists": True,
                "tables": all_table_info,
                "total_tables": len(table_names),
                "total_rows": total_rows,
                "total_size_bytes": total_size,
                "table_names": table_names
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get table info: {e}")
            return {
                "exists": False,
                "error": str(e)
            }
    
    def get_sample_data(self, limit: int = 10, table_name: str = None) -> Dict[str, Any]:
        """Get sample data from tables"""
        if table_name:
            sql = f"SELECT * FROM {table_name} LIMIT {limit}"
        else:
            # Get sample from all tables
            table_names = self.cache_manager.get_table_names()
            if not table_names:
                return {
                    "success": False,
                    "error": "No tables available",
                    "rows": 0,
                    "columns": [],
                    "data": []
                }
            
            # For "All Tables" view, just show first table with table info
            # UNION ALL would fail due to different schemas across tables
            first_table = table_names[0]
            sql = f"SELECT '{first_table}' as _table_name, * FROM {first_table} LIMIT {limit}"
        
        return self.execute_sql(sql)
    
    def get_column_stats(self, column: str) -> Dict[str, Any]:
        """Get statistics for a specific column"""
        sql = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT({column}) as non_null_count,
            COUNT(DISTINCT {column}) as unique_count
        FROM {self.config.table_name}
        """
        
        result = self.execute_sql(sql)
        if not result["success"]:
            return result
        
        # Add type information
        try:
            df = self.cache_manager.load_snapshot()
            if df is not None and column in df.columns:
                result["data"][0]["data_type"] = str(df[column].dtype)
        except Exception as e:
            self.logger.warning(f"Failed to get column type: {e}")
        
        return result
    
    def validate_query(self, sql: str) -> Dict[str, Any]:
        """Validate SQL query without executing it"""
        try:
            validate_sql_readonly(sql)
            
            # Try to parse the query with DuckDB
            df = self.cache_manager.load_snapshot()
            if df is None:
                return {
                    "valid": False,
                    "error": "No data available for validation"
                }
            
            conn = duckdb.connect()
            try:
                conn.register(self.config.table_name, df)
                # Try to explain the query (doesn't execute it)
                conn.execute(f"EXPLAIN {sql}")
                return {"valid": True}
            finally:
                conn.close()
                
        except Exception as e:
            return {
                "valid": False,
                "error": str(e)
            }

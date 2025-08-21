"""
DuckDB query engine for DuckRedis
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
            
            # Load snapshot from cache
            df = self.cache_manager.load_snapshot()
            if df is None:
                return {
                    "success": False,
                    "error": "No data available in cache",
                    "rows": 0,
                    "columns": [],
                    "data": []
                }
            
            # Execute query with DuckDB
            conn = duckdb.connect()
            try:
                # Register dataframe as table
                conn.register(self.config.table_name, df)
                
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
                    "format": self.config.sql_output_format
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
    
    def get_table_info(self) -> Dict[str, Any]:
        """Get information about the cached table"""
        try:
            # Get snapshot info from cache
            info = self.cache_manager.get_snapshot_info()
            
            if not info:
                return {
                    "exists": False,
                    "rows": 0,
                    "columns": 0,
                    "column_names": [],
                    "size_bytes": 0
                }
            
            return {
                "exists": True,
                "rows": info.get("rows", 0),
                "columns": info.get("columns", 0),
                "column_names": info.get("column_names", []),
                "size_bytes": info.get("size_bytes", 0),
                "format": info.get("format", "unknown")
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get table info: {e}")
            return {
                "exists": False,
                "error": str(e)
            }
    
    def get_sample_data(self, limit: int = 10) -> Dict[str, Any]:
        """Get sample data from the table"""
        sql = f"SELECT * FROM {self.config.table_name} LIMIT {limit}"
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

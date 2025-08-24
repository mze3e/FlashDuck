"""
File system monitoring for FlashDuck
"""

import os
import time
import logging
import threading
import json
from typing import Set, List, Dict, Any, Callable, Optional
from pathlib import Path
import pandas as pd
from .config import Config
from .cache import CacheManager
from .utils import load_json_files, merge_schemas


class FileMonitor:
    """Monitors file system changes and updates cache"""
    
    def __init__(self, config: Config, cache_manager: CacheManager):
        self.config = config
        self.cache_manager = cache_manager
        self.logger = logging.getLogger(__name__)
        
        self._stop_event = threading.Event()
        self._last_file_list: Set[str] = set()
        self._cache_update_callbacks: List[Callable] = []
        
        # Ensure DB root directory exists
        Path(config.db_root).mkdir(parents=True, exist_ok=True)
    
    def add_cache_update_callback(self, callback: Callable) -> None:
        """Add callback to be called when cache is updated"""
        self._cache_update_callbacks.append(callback)
    
    def _notify_cache_update(self) -> None:
        """Notify all callbacks that cache was updated"""
        for callback in self._cache_update_callbacks:
            try:
                callback()
            except Exception as e:
                self.logger.error(f"Cache update callback failed: {e}")
    
    def scan_files(self) -> Set[str]:
        """Scan directory for files and return set of filenames"""
        try:
            db_path = Path(self.config.db_root)
            if not db_path.exists():
                return set()
            
            # Get data files based on configured format
            files = set()
            if self.config.file_format == "parquet":
                pattern = "*.parquet"
            else:
                pattern = "*.json"
                
            for file_path in db_path.glob(pattern):
                if file_path.is_file():
                    files.add(file_path.name)
            
            return files
            
        except Exception as e:
            self.logger.error(f"Failed to scan files: {e}")
            return set()
    
    def load_and_cache_data(self) -> bool:
        """Load data from partitioned files and update cache with deduplication"""
        try:
            db_path = Path(self.config.db_root)
            if not db_path.exists():
                self.logger.warning("Database directory not found")
                return True
            
            # Get data files based on format
            if self.config.file_format == "parquet":
                data_files = list(db_path.glob("*.parquet"))
                file_type = "Parquet"
            else:
                data_files = list(db_path.glob("*.json"))
                file_type = "JSON"
            
            if not data_files:
                self.logger.warning(f"No {file_type} files found")
                return True
            
            # Group files by table name (handle both partitioned and legacy files)
            table_files = {}
            for file_path in data_files:
                filename = file_path.stem  # Get filename without extension
                
                # Extract table name from partition filename or use as is
                if "_partition_" in filename:
                    table_name = filename.split("_partition_")[0]
                else:
                    # Legacy single file format
                    table_name = filename
                
                if table_name not in table_files:
                    table_files[table_name] = []
                table_files[table_name].append(file_path)
            
            total_rows = 0
            tables_loaded = 0
            
            # Process each table by combining partitions and deduplicating
            for table_name, file_paths in table_files.items():
                try:
                    dataframes = []

                    # Load all partition files for this table
                    for file_path in file_paths:
                        df = self._load_data_file(file_path)

                        if df is not None and not df.empty:
                            # Add metadata columns
                            df['_source_file'] = file_path.name
                            if '_modified_time' not in df.columns:
                                df['_modified_time'] = file_path.stat().st_mtime

                            dataframes.append(df)

                    if dataframes:
                        # Combine all partitions for this table
                        combined_df = pd.concat(dataframes, ignore_index=True)

                        # Determine primary key for this table
                        primary_key = self._resolve_primary_key(table_name, combined_df)

                        # Apply primary key based deduplication using _modified_time ranking
                        combined_df = self._extract_latest_records_from_partitions(
                            combined_df, table_name, primary_key
                        )

                        # Store ranked view/table in DuckDB cache
                        self.cache_manager.store_ranked_table(table_name, combined_df, primary_key)

                        total_rows += len(combined_df)
                        tables_loaded += 1

                        self.logger.info(f"Loaded table '{table_name}': {len(combined_df)} rows")
                    
                except Exception as e:
                    self.logger.error(f"Failed to load table '{table_name}': {e}")
                    continue
            
            # Remove tables that no longer have files
            current_tables = set(table_files.keys())
            existing_tables = set(self.cache_manager.get_table_names())
            for missing in existing_tables - current_tables:
                self.logger.info(f"Removing table '{missing}' as source files are missing")
                self.cache_manager.remove_table(missing)

            self.logger.info(f"Loaded {total_rows} total rows from {tables_loaded} tables")

            # Notify callbacks
            self._notify_cache_update()

            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load and cache data: {e}")
            return False

    def _resolve_primary_key(self, table_name: str, df: pd.DataFrame) -> Optional[Any]:
        """Determine primary key for a table using config or common conventions."""
        pk = self.config.table_primary_keys.get(table_name)
        if pk:
            return pk
        if 'id' in df.columns:
            return 'id'
        if 'key' in df.columns:
            return 'key'
        return None
    
    def _load_data_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Load data from a single file (JSON or Parquet)"""
        try:
            if file_path.suffix.lower() == '.parquet':
                return pd.read_parquet(file_path)
            elif file_path.suffix.lower() == '.json':
                with open(file_path, 'r', encoding='utf-8') as f:
                    file_data = json.load(f)
                
                # Convert to DataFrame
                if isinstance(file_data, list):
                    return pd.DataFrame(file_data)
                elif isinstance(file_data, dict):
                    return pd.DataFrame([file_data])
                else:
                    self.logger.warning(f"Unsupported data format in {file_path.name}")
                    return None
            else:
                self.logger.warning(f"Unsupported file format: {file_path.suffix}")
                return None
                
        except Exception as e:
            self.logger.error(f"Failed to load file {file_path}: {e}")
            return None
    
    def _extract_latest_records(
        self, df: pd.DataFrame, table_name: str, primary_key: Optional[Any]
    ) -> pd.DataFrame:
        """Extract latest records based on primary key"""
        try:
            pk_cols = (
                list(primary_key)
                if isinstance(primary_key, (list, tuple))
                else ([primary_key] if primary_key else [])
            )
            if not pk_cols or any(col not in df.columns for col in pk_cols):
                # No primary key defined or columns don't exist, return as-is
                return df

            # Sort by modification time and keep latest record for each primary key combination
            df_sorted = df.sort_values('_modified_time', ascending=False)
            latest_df = df_sorted.drop_duplicates(subset=pk_cols, keep='first')

            self.logger.debug(
                f"Table '{table_name}': {len(df)} total records, {len(latest_df)} latest records by key '{primary_key}'"
            )
            return latest_df

        except Exception as e:
            self.logger.error(f"Failed to extract latest records for table '{table_name}': {e}")
            return df

    def _extract_latest_records_from_partitions(
        self, df: pd.DataFrame, table_name: str, primary_key: Optional[Any]
    ) -> pd.DataFrame:
        """Extract latest records from partitioned files using primary key and _modified_time ranking"""
        try:
            pk_cols = (
                list(primary_key)
                if isinstance(primary_key, (list, tuple))
                else ([primary_key] if primary_key else [])
            )
            if not pk_cols or any(col not in df.columns for col in pk_cols):
                # No primary key defined or columns don't exist, return as-is
                return df

            # Use ranking approach: sort by _modified_time descending, then drop duplicates to keep latest
            df_sorted = df.sort_values('_modified_time', ascending=False)
            latest_df = df_sorted.drop_duplicates(subset=pk_cols, keep='first')

            dropped_count = len(df) - len(latest_df)
            if dropped_count > 0:
                self.logger.info(
                    f"Deduplicated {dropped_count} records for table '{table_name}' using primary key '{primary_key}' ranked by _modified_time"
                )

            return latest_df

        except Exception as e:
            self.logger.error(
                f"Failed to extract latest records from partitions for table '{table_name}': {e}"
            )
            return df
    
    def check_for_changes(self) -> bool:
        """Check if files have changed since last scan"""
        current_files = self.scan_files()
        
        if current_files != self._last_file_list:
            self.logger.info(
                f"File changes detected: "
                f"added={current_files - self._last_file_list}, "
                f"removed={self._last_file_list - current_files}"
            )
            self._last_file_list = current_files
            return True
        
        return False
    
    def start_monitoring(self) -> threading.Thread:
        """Start background file monitoring"""
        def monitor_loop():
            self.logger.info("Started file monitoring")
            
            # Initial load
            self.load_and_cache_data()
            self._last_file_list = self.scan_files()
            
            while not self._stop_event.is_set():
                try:
                    if self.check_for_changes():
                        self.load_and_cache_data()
                    
                    # Wait before next scan
                    self._stop_event.wait(timeout=self.config.scan_interval_sec)
                    
                except Exception as e:
                    self.logger.error(f"Monitor loop error: {e}")
                    self._stop_event.wait(timeout=5)  # Brief pause on error
            
            self.logger.info("File monitoring stopped")
        
        thread = threading.Thread(target=monitor_loop, daemon=True)
        thread.start()
        return thread
    
    def stop_monitoring(self) -> None:
        """Stop file monitoring"""
        self._stop_event.set()
    
    def force_refresh(self) -> bool:
        """Force refresh of cache from files"""
        self.logger.info("Forcing cache refresh")
        return self.load_and_cache_data()
    
    def get_file_info(self) -> Dict[str, Any]:
        """Get information about monitored files"""
        try:
            db_path = Path(self.config.db_root)
            files_info = []
            
            if db_path.exists():
                # Get files based on configured format
                if self.config.file_format == "parquet":
                    pattern = "*.parquet"
                else:
                    pattern = "*.json"
                    
                for file_path in db_path.glob(pattern):
                    if file_path.is_file():
                        stat = file_path.stat()
                        files_info.append({
                            "name": file_path.name,
                            "size_bytes": stat.st_size,
                            "modified_time": stat.st_mtime,
                            "path": str(file_path)
                        })
            
            return {
                "directory": str(db_path),
                "file_count": len(files_info),
                "files": files_info,
                "monitoring": not self._stop_event.is_set(),
                "file_format": self.config.file_format
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get file info: {e}")
            return {
                "directory": self.config.db_root,
                "error": str(e)
            }
    
    def create_table_update(self, table_name: str, records: List[Dict[str, Any]]) -> bool:
        """Create/update table with new records using partitioned files"""
        try:
            db_path = Path(self.config.db_root)
            db_path.mkdir(parents=True, exist_ok=True)
            
            # Convert records to DataFrame
            df = pd.DataFrame(records)
            
            if df.empty:
                self.logger.warning(f"No records to update for table '{table_name}'")
                return False
            
            # Add timestamp for tracking latest records
            import time
            from datetime import datetime
            current_time = time.time()
            df['_modified_time'] = current_time
            
            # Create partitioned filename with timestamp
            timestamp_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")[:-3]  # Remove last 3 microsecond digits
            partition_filename = f"{table_name}_partition_{timestamp_str}.parquet"
            
            # Write as Parquet or JSON based on configuration
            if self.config.file_format == "parquet":
                file_path = db_path / partition_filename
                df.to_parquet(file_path, compression=self.config.parquet_compression, index=False)
            else:
                # For JSON, still use partitioned approach
                partition_filename = f"{table_name}_partition_{timestamp_str}.json"
                file_path = db_path / partition_filename
                records_dict = df.drop(columns=['_modified_time']).to_dict('records')
                import json
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(records_dict, f, indent=2)
            
            self.logger.info(f"Created partition file '{partition_filename}' with {len(records)} records")
            
            # Trigger reload to refresh cache with deduplication
            self.load_and_cache_data()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create partition for table '{table_name}': {e}")
            return False

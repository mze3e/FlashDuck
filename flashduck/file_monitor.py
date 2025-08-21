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


class CacheComparator:
    """Compares cache snapshots to identify precise changes for delta partition files"""
    
    def __init__(self, config: Config, cache_manager: CacheManager):
        self.config = config
        self.cache_manager = cache_manager
        self.logger = logging.getLogger(__name__ + ".CacheComparator")
    
    def compare_table_snapshots(self, table_name: str, primary_key: str = None) -> Dict[str, Any]:
        """Compare current table snapshot with previous state and return changes"""
        try:
            if primary_key is None:
                primary_key = self.config.table_primary_keys.get(table_name, 'id')
            
            # Load current and previous snapshots
            current_df = self.cache_manager.load_table_snapshot(table_name)
            previous_df = self.cache_manager.load_table_snapshot_state(table_name, "previous")
            
            if current_df is None or current_df.empty:
                return {
                    "has_changes": False,
                    "added": pd.DataFrame(),
                    "modified": pd.DataFrame(),
                    "deleted": pd.DataFrame(),
                    "unchanged": pd.DataFrame(),
                    "summary": "No current data"
                }
            
            # Clean dataframes for comparison (remove metadata columns)
            current_clean = current_df.drop(columns=['_source_file', '_modified_time'], errors='ignore')
            
            if previous_df is None or previous_df.empty:
                # No previous state, everything is new
                return {
                    "has_changes": True,
                    "added": current_clean.copy(),
                    "modified": pd.DataFrame(),
                    "deleted": pd.DataFrame(),
                    "unchanged": pd.DataFrame(),
                    "summary": f"All {len(current_clean)} records are new"
                }
            
            previous_clean = previous_df.drop(columns=['_source_file', '_modified_time'], errors='ignore')
            
            # Ensure both dataframes have the same columns for comparison
            all_columns = list(set(current_clean.columns) | set(previous_clean.columns))
            for col in all_columns:
                if col not in current_clean.columns:
                    current_clean[col] = None
                if col not in previous_clean.columns:
                    previous_clean[col] = None
            
            # Reorder columns to match
            current_clean = current_clean[all_columns]
            previous_clean = previous_clean[all_columns]
            
            # Identify changes based on primary key
            if primary_key not in current_clean.columns:
                # No primary key available, treat as all new
                return {
                    "has_changes": True,
                    "added": current_clean.copy(),
                    "modified": pd.DataFrame(),
                    "deleted": pd.DataFrame(),
                    "unchanged": pd.DataFrame(),
                    "summary": f"No primary key '{primary_key}' found, treating all {len(current_clean)} records as new"
                }
            
            # Get sets of primary keys
            current_keys = set(current_clean[primary_key].values)
            previous_keys = set(previous_clean[primary_key].values) if primary_key in previous_clean.columns else set()
            
            # Identify added, deleted, and potentially modified records
            added_keys = current_keys - previous_keys
            deleted_keys = previous_keys - current_keys
            common_keys = current_keys & previous_keys
            
            # Extract added records
            added_records = current_clean[current_clean[primary_key].isin(added_keys)] if added_keys else pd.DataFrame()
            
            # Extract deleted records  
            deleted_records = previous_clean[previous_clean[primary_key].isin(deleted_keys)] if deleted_keys else pd.DataFrame()
            
            # Check for modifications in common records
            modified_records = []
            unchanged_records = []
            
            for key in common_keys:
                current_record = current_clean[current_clean[primary_key] == key].iloc[0]
                previous_record = previous_clean[previous_clean[primary_key] == key].iloc[0]
                
                # Compare all values (convert to string for comparison to handle different data types)
                record_changed = False
                for col in all_columns:
                    if col != primary_key:  # Skip primary key in comparison
                        current_val = str(current_record[col]) if pd.notna(current_record[col]) else ""
                        previous_val = str(previous_record[col]) if pd.notna(previous_record[col]) else ""
                        if current_val != previous_val:
                            record_changed = True
                            break
                
                if record_changed:
                    modified_records.append(current_record)
                else:
                    unchanged_records.append(current_record)
            
            modified_df = pd.DataFrame(modified_records) if modified_records else pd.DataFrame()
            unchanged_df = pd.DataFrame(unchanged_records) if unchanged_records else pd.DataFrame()
            
            # Calculate summary
            has_changes = len(added_records) > 0 or len(modified_df) > 0 or len(deleted_records) > 0
            summary_parts = []
            if len(added_records) > 0:
                summary_parts.append(f"{len(added_records)} added")
            if len(modified_df) > 0:
                summary_parts.append(f"{len(modified_df)} modified")
            if len(deleted_records) > 0:
                summary_parts.append(f"{len(deleted_records)} deleted")
            if not summary_parts:
                summary_parts.append("no changes")
            
            summary = f"Changes detected: {', '.join(summary_parts)}"
            
            return {
                "has_changes": has_changes,
                "added": added_records,
                "modified": modified_df,
                "deleted": deleted_records,
                "unchanged": unchanged_df,
                "summary": summary,
                "stats": {
                    "added_count": len(added_records),
                    "modified_count": len(modified_df),
                    "deleted_count": len(deleted_records),
                    "unchanged_count": len(unchanged_df)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to compare snapshots for table '{table_name}': {e}")
            return {
                "has_changes": False,
                "added": pd.DataFrame(),
                "modified": pd.DataFrame(),
                "deleted": pd.DataFrame(),
                "unchanged": pd.DataFrame(),
                "summary": f"Comparison failed: {str(e)}"
            }
    
    def update_previous_snapshot(self, table_name: str) -> bool:
        """Update the previous snapshot to current state"""
        try:
            current_df = self.cache_manager.load_table_snapshot(table_name)
            if current_df is not None and not current_df.empty:
                self.cache_manager.store_table_snapshot_state(table_name, current_df, "previous")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Failed to update previous snapshot for table '{table_name}': {e}")
            return False


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
                all_files = list(db_path.glob("*.parquet"))
                # Filter out batch partition files to avoid feedback loop
                data_files = [f for f in all_files if "_batch_" not in f.name and "_partition_" not in f.name]
                file_type = "Parquet"
            else:
                all_files = list(db_path.glob("*.json"))
                # Filter out batch partition files to avoid feedback loop
                data_files = [f for f in all_files if "_batch_" not in f.name and "_partition_" not in f.name]
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
                        
                        # Apply primary key based deduplication using _modified_time ranking
                        combined_df = self._extract_latest_records_from_partitions(combined_df, table_name)
                        
                        # Store table in cache
                        self.cache_manager.store_table_snapshot(table_name, combined_df)
                        
                        total_rows += len(combined_df)
                        tables_loaded += 1
                        
                        self.logger.info(f"Loaded table '{table_name}': {len(combined_df)} rows")
                    
                except Exception as e:
                    self.logger.error(f"Failed to load table '{table_name}': {e}")
                    continue
            
            self.logger.info(f"Loaded {total_rows} total rows from {tables_loaded} tables")
            
            # Notify callbacks
            self._notify_cache_update()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load and cache data: {e}")
            return False
    
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
    
    def _extract_latest_records(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Extract latest records based on primary key"""
        try:
            primary_key = self.config.table_primary_keys.get(table_name)
            if not primary_key or primary_key not in df.columns:
                # No primary key defined or column doesn't exist, return as-is
                return df
            
            # Sort by modification time and keep latest record for each primary key
            df_sorted = df.sort_values('_modified_time', ascending=False)
            latest_df = df_sorted.drop_duplicates(subset=[primary_key], keep='first')
            
            self.logger.debug(f"Table '{table_name}': {len(df)} total records, {len(latest_df)} latest records by key '{primary_key}'")
            return latest_df
            
        except Exception as e:
            self.logger.error(f"Failed to extract latest records for table '{table_name}': {e}")
            return df
    
    def _extract_latest_records_from_partitions(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Extract latest records from partitioned files using primary key and _modified_time ranking"""
        try:
            primary_key = self.config.table_primary_keys.get(table_name)
            if not primary_key or primary_key not in df.columns:
                # No primary key defined or column doesn't exist, return as-is
                return df
            
            # Use ranking approach: sort by _modified_time descending, then drop duplicates to keep latest
            df_sorted = df.sort_values('_modified_time', ascending=False)
            latest_df = df_sorted.drop_duplicates(subset=[primary_key], keep='first')
            
            dropped_count = len(df) - len(latest_df)
            if dropped_count > 0:
                self.logger.info(f"Deduplicated {dropped_count} records for table '{table_name}' using primary key '{primary_key}' ranked by _modified_time")
            
            return latest_df
            
        except Exception as e:
            self.logger.error(f"Failed to extract latest records from partitions for table '{table_name}': {e}")
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
    
    def create_sample_files(self) -> None:
        """Create sample data files for demo purposes"""
        try:
            db_path = Path(self.config.db_root)
            db_path.mkdir(parents=True, exist_ok=True)
            
            # Sample data with proper types for Parquet
            sample_data = [
                {
                    "table_name": "users",
                    "data": [
                        {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "age": 28, "city": "New York", "active": True},
                        {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 34, "city": "San Francisco", "active": True},
                        {"id": 3, "name": "Carol Davis", "email": "carol@example.com", "age": 29, "city": "Chicago", "active": False}
                    ]
                },
                {
                    "table_name": "products", 
                    "data": [
                        {"id": 101, "name": "Laptop", "price": 999.99, "category": "Electronics", "in_stock": True, "rating": 4.5},
                        {"id": 102, "name": "Mouse", "price": 29.99, "category": "Electronics", "in_stock": True, "rating": 4.2},
                        {"id": 103, "name": "Keyboard", "price": 79.99, "category": "Electronics", "in_stock": False, "rating": 4.8}
                    ]
                },
                {
                    "table_name": "orders",
                    "data": [
                        {"order_id": 1001, "user_id": 1, "product_id": 101, "quantity": 1, "total": 999.99, "date": "2025-01-15", "status": "completed"},
                        {"order_id": 1002, "user_id": 2, "product_id": 102, "quantity": 2, "total": 59.98, "date": "2025-01-16", "status": "pending"},
                        {"order_id": 1003, "user_id": 3, "product_id": 103, "quantity": 1, "total": 79.99, "date": "2025-01-17", "status": "shipped"}
                    ]
                }
            ]
            
            for sample in sample_data:
                df = pd.DataFrame(sample["data"])
                
                # Ensure proper data types
                if sample["table_name"] == "users":
                    df["id"] = df["id"].astype("int32")
                    df["age"] = df["age"].astype("int32")
                    df["active"] = df["active"].astype("bool")
                elif sample["table_name"] == "products":
                    df["id"] = df["id"].astype("int32")
                    df["price"] = df["price"].astype("float64")
                    df["in_stock"] = df["in_stock"].astype("bool")
                    df["rating"] = df["rating"].astype("float32")
                elif sample["table_name"] == "orders":
                    df["order_id"] = df["order_id"].astype("int32")
                    df["user_id"] = df["user_id"].astype("int32")
                    df["product_id"] = df["product_id"].astype("int32")
                    df["quantity"] = df["quantity"].astype("int32")
                    df["total"] = df["total"].astype("float64")
                    df["date"] = pd.to_datetime(df["date"])
                
                # Write as Parquet or JSON based on configuration
                if self.config.file_format == "parquet":
                    file_path = db_path / f"{sample['table_name']}.parquet"
                    df.to_parquet(file_path, compression=self.config.parquet_compression, index=False)
                else:
                    file_path = db_path / f"{sample['table_name']}.json"
                    import json
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(sample["data"], f, indent=2)
                
                self.logger.info(f"Created sample file: {file_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to create sample files: {e}")
    
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


class BackgroundPartitionWriter:
    """Background writer that monitors cache changes and writes partition files"""
    
    def __init__(self, config: Config, cache_manager: CacheManager):
        self.config = config
        self.cache_manager = cache_manager
        self.logger = logging.getLogger(__name__ + ".PartitionWriter")
        self.comparator = CacheComparator(config, cache_manager)
        
        self._stop_event = threading.Event()
        self._write_callbacks: List[Callable] = []
        self._batch_counter = 0
        
        # Ensure partition directory exists (separate from source files to avoid feedback loop)
        self.partition_dir = Path(config.db_root) / "partitions"
        self.partition_dir.mkdir(parents=True, exist_ok=True)
    
    def add_write_callback(self, callback: Callable) -> None:
        """Add callback to be called after partition writes"""
        self._write_callbacks.append(callback)
    
    def _notify_write_callbacks(self) -> None:
        """Notify all callbacks that partition write occurred"""
        for callback in self._write_callbacks:
            try:
                callback()
            except Exception as e:
                self.logger.error(f"Write callback failed: {e}")
    
    def write_cache_changes_to_partitions(self) -> bool:
        """Write cache changes to partition files for all updated tables using precise comparison"""
        try:
            updated_tables = self.cache_manager.get_cache_updated_tables()
            
            if not updated_tables:
                return True  # No changes to write
            
            write_success = True
            self._batch_counter += 1
            
            for table_name in updated_tables:
                try:
                    # Get the primary key for this table
                    primary_key = self.config.table_primary_keys.get(table_name, 'id')
                    
                    # Compare current cache state with previous snapshot
                    comparison = self.comparator.compare_table_snapshots(table_name, primary_key)
                    
                    if not comparison["has_changes"]:
                        self.logger.info(f"No actual changes detected for table '{table_name}': {comparison['summary']}")
                        self.cache_manager.clear_cache_updated_flag(table_name)
                        continue
                    
                    self.logger.info(f"Table '{table_name}' - {comparison['summary']}")
                    
                    # Combine added and modified records for the partition file
                    records_to_write = []
                    
                    # Add new records
                    if not comparison["added"].empty:
                        added_records = comparison["added"].copy()
                        added_records['_change_type'] = 'ADDED'
                        added_records['_batch_id'] = self._batch_counter
                        added_records['_batch_timestamp'] = pd.Timestamp.now().timestamp()
                        records_to_write.append(added_records)
                    
                    # Add modified records
                    if not comparison["modified"].empty:
                        modified_records = comparison["modified"].copy()
                        modified_records['_change_type'] = 'MODIFIED'
                        modified_records['_batch_id'] = self._batch_counter
                        modified_records['_batch_timestamp'] = pd.Timestamp.now().timestamp()
                        records_to_write.append(modified_records)
                    
                    # Optionally add deleted records (marked as such)
                    if not comparison["deleted"].empty:
                        deleted_records = comparison["deleted"].copy()
                        deleted_records['_change_type'] = 'DELETED'
                        deleted_records['_batch_id'] = self._batch_counter
                        deleted_records['_batch_timestamp'] = pd.Timestamp.now().timestamp()
                        records_to_write.append(deleted_records)
                    
                    if not records_to_write:
                        self.logger.info(f"No records to write for table '{table_name}'")
                        self.cache_manager.clear_cache_updated_flag(table_name)
                        continue
                    
                    # Combine all change types into single DataFrame
                    partition_df = pd.concat(records_to_write, ignore_index=True)
                    
                    # Create batch partition filename with change tracking
                    from datetime import datetime
                    timestamp_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")[:-3]
                    partition_filename = f"{table_name}_batch_{self._batch_counter}_{timestamp_str}.parquet"
                    
                    # Write partition file to separate directory
                    file_path = self.partition_dir / partition_filename
                    
                    if self.config.file_format == "parquet":
                        partition_df.to_parquet(file_path, compression=self.config.parquet_compression, index=False)
                    else:
                        # For JSON, still use partitioned approach
                        partition_filename = f"{table_name}_batch_{self._batch_counter}_{timestamp_str}.json"
                        file_path = self.partition_dir / partition_filename
                        records_dict = partition_df.to_dict('records')
                        import json
                        with open(file_path, 'w', encoding='utf-8') as f:
                            json.dump(records_dict, f, indent=2)
                    
                    self.logger.info(
                        f"Batch {self._batch_counter}: Wrote partition '{partition_filename}' with {len(partition_df)} delta records "
                        f"({comparison.get('stats', {}).get('added_count', 0)} added, {comparison.get('stats', {}).get('modified_count', 0)} modified, "
                        f"{comparison.get('stats', {}).get('deleted_count', 0)} deleted)"
                    )
                    
                    # Update previous snapshot state for next comparison
                    self.comparator.update_previous_snapshot(table_name)
                    
                    # Clear the update flag for this table
                    self.cache_manager.clear_cache_updated_flag(table_name)
                    
                except Exception as e:
                    self.logger.error(f"Failed to write batch partition for table '{table_name}': {e}")
                    write_success = False
            
            # Notify callbacks that partitions were written
            if updated_tables:
                self._notify_write_callbacks()
            
            return write_success
            
        except Exception as e:
            self.logger.error(f"Failed to write cache changes to partitions: {e}")
            return False
    
    def start_background_writer(self, interval_sec: float = 5.0) -> threading.Thread:
        """Start background partition writer"""
        def writer_loop():
            self.logger.info(f"Started background partition writer (interval: {interval_sec}s)")
            
            while not self._stop_event.is_set():
                try:
                    # Check for cache changes and write partitions
                    self.write_cache_changes_to_partitions()
                    
                    # Wait before next check
                    self._stop_event.wait(timeout=interval_sec)
                    
                except Exception as e:
                    self.logger.error(f"Background partition writer error: {e}")
                    self._stop_event.wait(timeout=5)  # Brief pause on error
            
            self.logger.info("Background partition writer stopped")
        
        thread = threading.Thread(target=writer_loop, daemon=True)
        thread.start()
        return thread
    
    def stop_background_writer(self) -> None:
        """Stop background partition writer"""
        self._stop_event.set()
    
    def force_write_all_tables(self) -> bool:
        """Force write all cached tables to partition files"""
        try:
            table_names = self.cache_manager.get_table_names()
            
            if not table_names:
                self.logger.info("No tables found in cache for forced write")
                return True
            
            success = True
            
            for table_name in table_names:
                try:
                    # Mark table as updated to trigger partition write
                    self.cache_manager._mark_cache_updated(table_name)
                except Exception as e:
                    self.logger.error(f"Failed to mark table '{table_name}' as updated: {e}")
                    success = False
            
            # Now write all the marked tables
            return self.write_cache_changes_to_partitions() and success
            
        except Exception as e:
            self.logger.error(f"Failed to force write all tables: {e}")
            return False

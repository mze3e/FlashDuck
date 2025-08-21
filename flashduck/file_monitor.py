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
        """Load data from files and update cache"""
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
            
            total_rows = 0
            tables_loaded = 0
            
            # Load each file as a separate table
            for file_path in data_files:
                try:
                    table_name = file_path.stem  # Get filename without extension
                    df = self._load_data_file(file_path)
                    
                    if df is not None and not df.empty:
                        # Add metadata columns
                        df['_source_file'] = file_path.name
                        df['_modified_time'] = file_path.stat().st_mtime
                        
                        # Apply primary key based deduplication to get latest records
                        df = self._extract_latest_records(df, table_name)
                        
                        # Store table in cache
                        self.cache_manager.store_table_snapshot(table_name, df)
                        
                        total_rows += len(df)
                        tables_loaded += 1
                        
                        self.logger.info(f"Loaded table '{table_name}': {len(df)} rows")
                    
                except Exception as e:
                    self.logger.error(f"Failed to load file {file_path.name}: {e}")
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
        """Create/update table with new records, preserving types"""
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
            df['_updated_at'] = time.time()
            
            # Write as Parquet or JSON based on configuration
            if self.config.file_format == "parquet":
                file_path = db_path / f"{table_name}.parquet"
                
                # If file exists, append records (allowing for primary key deduplication later)
                if file_path.exists():
                    existing_df = pd.read_parquet(file_path)
                    # Combine existing and new records
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df.to_parquet(file_path, compression=self.config.parquet_compression, index=False)
                else:
                    df.to_parquet(file_path, compression=self.config.parquet_compression, index=False)
            else:
                file_path = db_path / f"{table_name}.json"
                # For JSON, we'll overwrite for simplicity
                records_dict = df.drop(columns=['_updated_at']).to_dict('records')
                import json
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(records_dict, f, indent=2)
            
            self.logger.info(f"Updated table '{table_name}' with {len(records)} records")
            
            # Trigger reload
            self.load_and_cache_data()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update table '{table_name}': {e}")
            return False

"""
File system monitoring for FlashDuck
"""

import os
import time
import logging
import threading
from typing import Set, List, Dict, Any, Callable
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
            
            # Get all JSON files (assuming JSON format for demo)
            files = set()
            for file_path in db_path.glob("*.json"):
                if file_path.is_file():
                    files.add(file_path.name)
            
            return files
            
        except Exception as e:
            self.logger.error(f"Failed to scan files: {e}")
            return set()
    
    def load_and_cache_data(self) -> bool:
        """Load data from files and update cache"""
        try:
            # Load all JSON files
            data = load_json_files(self.config.db_root, "*.json")
            
            if not data:
                self.logger.warning("No data files found")
                # Store empty DataFrame
                empty_df = pd.DataFrame()
                self.cache_manager.store_snapshot(empty_df)
                return True
            
            # Convert to DataFrames
            dataframes = []
            for item in data:
                if isinstance(item, dict):
                    df = pd.DataFrame([item])
                    dataframes.append(df)
                elif isinstance(item, list):
                    df = pd.DataFrame(item)
                    dataframes.append(df)
            
            if not dataframes:
                empty_df = pd.DataFrame()
                self.cache_manager.store_snapshot(empty_df)
                return True
            
            # Merge with schema evolution
            merged_df = merge_schemas(dataframes, self.config.schema_evolution)
            
            # Store in cache
            self.cache_manager.store_snapshot(merged_df)
            
            self.logger.info(f"Loaded {len(merged_df)} rows from {len(dataframes)} files")
            
            # Notify callbacks
            self._notify_cache_update()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load and cache data: {e}")
            return False
    
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
                for file_path in db_path.glob("*.json"):
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
                "monitoring": not self._stop_event.is_set()
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
            
            # Sample data files
            sample_data = [
                {
                    "filename": "users.json",
                    "data": [
                        {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "age": 28, "city": "New York"},
                        {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 34, "city": "San Francisco"},
                        {"id": 3, "name": "Carol Davis", "email": "carol@example.com", "age": 29, "city": "Chicago"}
                    ]
                },
                {
                    "filename": "products.json", 
                    "data": [
                        {"id": 101, "name": "Laptop", "price": 999.99, "category": "Electronics", "in_stock": True},
                        {"id": 102, "name": "Mouse", "price": 29.99, "category": "Electronics", "in_stock": True},
                        {"id": 103, "name": "Keyboard", "price": 79.99, "category": "Electronics", "in_stock": False}
                    ]
                },
                {
                    "filename": "orders.json",
                    "data": [
                        {"order_id": 1001, "user_id": 1, "product_id": 101, "quantity": 1, "total": 999.99, "date": "2025-01-15"},
                        {"order_id": 1002, "user_id": 2, "product_id": 102, "quantity": 2, "total": 59.98, "date": "2025-01-16"},
                        {"order_id": 1003, "user_id": 3, "product_id": 103, "quantity": 1, "total": 79.99, "date": "2025-01-17"}
                    ]
                }
            ]
            
            import json
            for sample in sample_data:
                file_path = db_path / sample["filename"]
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(sample["data"], f, indent=2)
                
                self.logger.info(f"Created sample file: {file_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to create sample files: {e}")

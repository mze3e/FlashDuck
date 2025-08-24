"""
Core FlashDuck engine that orchestrates all components
"""

import logging
import threading
from typing import Optional, Dict, Any, List
from .config import Config
from .cache import CacheManager
from .query import QueryEngine
from .parquet_writer import ParquetWriter
from .file_monitor import FileMonitor
from .utils import setup_logging


class FlashDuckEngine:
    """Main engine that coordinates all FlashDuck components"""
    
    def __init__(self, config: Optional[Config] = None):
        # Setup configuration
        self.config = config or Config.from_env()
        self.config.validate()
        
        # Setup logging
        self.logger = setup_logging()
        self.logger.info(
            f"Initializing FlashDuck engine for table: {self.config.table_name}"
        )
        
        # Initialize components
        self.cache_manager = CacheManager(self.config)
        self.query_engine = QueryEngine(self.config, self.cache_manager)
        self.parquet_writer = ParquetWriter(self.config, self.cache_manager)
        self.file_monitor = FileMonitor(self.config, self.cache_manager)
        
        # Connect parquet writer to cache updates
        self.file_monitor.add_cache_update_callback(
            lambda: self.parquet_writer.write_parquet()
        )
        
        # Background threads
        self._monitor_thread: Optional[threading.Thread] = None
        self._parquet_thread: Optional[threading.Thread] = None
        self._running = False
    
    def start(self) -> None:
        """Start all background services"""
        if self._running:
            self.logger.warning("Engine already running")
            return

        try:
            if not self.cache_manager.is_connected():
                raise RuntimeError("Cannot access DuckDB cache")

            self.logger.info("Starting FlashDuck engine...")

            # Start file monitoring
            self._monitor_thread = self.file_monitor.start_monitoring()
            
            # Start background parquet writer if configured
            if self.config.parquet_debounce_sec is not None:
                self._parquet_thread = self.parquet_writer.start_background_writer()
            
            self._running = True
            self.logger.info("FlashDuck engine started successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to start engine: {e}")
            self.stop()
            raise
    
    def stop(self) -> None:
        """Stop all background services"""
        if not self._running:
            return
        
        self.logger.info("Stopping FlashDuck engine...")
        
        # Stop monitoring
        self.file_monitor.stop_monitoring()
        self.parquet_writer.stop_background_writer()
        
        # Wait for threads to finish
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5)
        
        if self._parquet_thread and self._parquet_thread.is_alive():
            self._parquet_thread.join(timeout=5)
        
        self._running = False
        self.logger.info("FlashDuck engine stopped")
    
    def is_running(self) -> bool:
        """Check if engine is running"""
        return self._running
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive engine status"""
        try:
            return {
                "engine": {
                    "running": self._running,
                    "table_name": self.config.table_name,
                    "db_root": self.config.db_root
                },
                "cache": self.cache_manager.get_snapshot_info(),
                "files": self.file_monitor.get_file_info(),
                "parquet": self.parquet_writer.get_parquet_info(),
                "config": {
                    "scan_interval_sec": self.config.scan_interval_sec,
                    "snapshot_format": self.config.snapshot_format,
                    "parquet_compression": self.config.parquet_compression,
                    "schema_evolution": self.config.schema_evolution
                }
            }
        except Exception as e:
            self.logger.error(f"Failed to get status: {e}")
            return {"error": str(e)}
    
    # Convenience methods for common operations
    
    def sql(self, query: str) -> Dict[str, Any]:
        """Execute SQL query"""
        return self.query_engine.execute_sql(query)
    
    def get_table_info(self) -> Dict[str, Any]:
        """Get table information"""
        return self.query_engine.get_table_info()
    
    def get_sample_data(self, limit: int = 10, table_name: str = None) -> Dict[str, Any]:
        """Get sample data"""
        return self.query_engine.get_sample_data(limit, table_name)
    
    def force_refresh(self) -> bool:
        """Force refresh cache from files"""
        return self.file_monitor.force_refresh()
    
    def write_parquet(self) -> bool:
        """Write current cache to Parquet file"""
        return self.parquet_writer.write_parquet(force=True)
    
    def enqueue_upsert(self, record_id: str, data: Dict[str, Any]) -> str:
        """Apply upsert and queue pending write"""
        return self.cache_manager.enqueue_upsert(record_id, data)

    def enqueue_delete(self, record_id: str) -> str:
        """Apply delete and queue pending write"""
        return self.cache_manager.enqueue_delete(record_id)
    
    def validate_query(self, sql: str) -> Dict[str, Any]:
        """Validate SQL query"""
        return self.query_engine.validate_query(sql)
    
    def clear_cache(self) -> None:
        """Clear all cached data"""
        self.cache_manager.clear_cache()
    
    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()

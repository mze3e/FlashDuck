"""
Core FlashDuck engine that orchestrates all components
"""

import logging
import threading
from typing import Optional, Dict, Any, List, Callable
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
        self._pending_thread: Optional[threading.Thread] = None
        self._pending_stop: Optional[threading.Event] = None
        self._running = False

    def start(self, create_sample_data: bool = False) -> None:
        """Start all background services.

        Args:
            create_sample_data: When ``True``, generate small sample files in the
                configured database directory before monitoring begins. Useful
                for demos or initial exploration when no data exists yet.
        """
        if self._running:
            self.logger.warning("Engine already running")
            return

        try:
            if not self.cache_manager.is_connected():
                raise RuntimeError("Cannot access DuckDB cache")

            self.logger.info("Starting FlashDuck engine...")

            if create_sample_data:
                try:
                    sample_records = [
                        {"id": 1, "value": "sample-1"},
                        {"id": 2, "value": "sample-2"},
                    ]
                    self.file_monitor.create_table_update(
                        self.config.table_name, sample_records
                    )
                except Exception as e:
                    self.logger.error(f"Failed to create sample data: {e}")

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
        self.stop_pending_sync()
        
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

    def list_pending_writes(self) -> List[str]:
        """List pending write Parquet files"""
        return self.cache_manager.list_pending_writes()

    def purge_pending_writes(self) -> int:
        """Delete all pending write files"""
        return self.cache_manager.purge_pending_writes()

    def process_pending_writes(
        self,
        upload_fn: Optional[Callable[[str], None]] = None,
        purge: bool = True,
    ) -> List[str]:
        """Upload and optionally purge pending writes"""
        return self.cache_manager.process_pending_writes(upload_fn, purge)

    def start_pending_sync(
        self, upload_fn: Callable[[str], None], interval: int = 60
    ) -> None:
        """Start background job to sync pending writes"""
        if self._pending_thread and self._pending_thread.is_alive():
            self.logger.warning("Pending sync already running")
            return

        self._pending_stop = threading.Event()

        def worker():
            while not self._pending_stop.is_set():
                try:
                    self.cache_manager.process_pending_writes(upload_fn)
                except Exception as e:
                    self.logger.error(f"Pending sync error: {e}")
                self._pending_stop.wait(interval)

        self._pending_thread = threading.Thread(target=worker, daemon=True)
        self._pending_thread.start()

    def stop_pending_sync(self) -> None:
        """Stop background pending sync job"""
        if self._pending_stop:
            self._pending_stop.set()
        if self._pending_thread and self._pending_thread.is_alive():
            self._pending_thread.join(timeout=5)
        self._pending_thread = None
    
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

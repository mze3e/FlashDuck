"""
Parquet file writer for DuckRedis
"""

import os
import time
import logging
import threading
from typing import Optional
import pandas as pd
from .cache import CacheManager
from .config import Config
from .utils import atomic_write_file, dataframe_to_parquet_bytes, format_bytes


class ParquetWriter:
    """Writes consolidated Parquet files from cache snapshots"""
    
    def __init__(self, config: Config, cache_manager: CacheManager):
        self.config = config
        self.cache_manager = cache_manager
        self.logger = logging.getLogger(__name__)
        
        self._stop_event = threading.Event()
        self._last_write_time = 0
        self._write_lock = threading.Lock()
        
        # Parquet file path
        self.parquet_path = os.path.join(
            config.db_root, 
            f"{config.table_name}.parquet"
        )
    
    def write_parquet(self, force: bool = False) -> bool:
        """Write current cache snapshot to Parquet file"""
        try:
            # Check debounce timing
            current_time = time.time()
            if not force and self.config.parquet_debounce_sec:
                time_since_last = current_time - self._last_write_time
                if time_since_last < self.config.parquet_debounce_sec:
                    self.logger.debug(
                        f"Skipping write due to debounce: "
                        f"{time_since_last:.1f}s < {self.config.parquet_debounce_sec}s"
                    )
                    return False
            
            with self._write_lock:
                # Load current snapshot
                df = self.cache_manager.load_snapshot()
                if df is None or df.empty:
                    self.logger.warning("No data to write to Parquet")
                    return False
                
                # Convert to Parquet bytes
                parquet_data = dataframe_to_parquet_bytes(
                    df, 
                    self.config.parquet_compression
                )
                
                # Atomic write
                atomic_write_file(self.parquet_path, parquet_data)
                
                self._last_write_time = current_time
                
                self.logger.info(
                    f"Wrote Parquet file: {self.parquet_path} "
                    f"({len(df)} rows, {format_bytes(len(parquet_data))})"
                )
                
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to write Parquet file: {e}")
            return False
    
    def start_background_writer(self) -> threading.Thread:
        """Start background thread that writes Parquet on cache updates"""
        def background_writer():
            self.logger.info("Started background Parquet writer")
            last_snapshot_info = None
            
            while not self._stop_event.is_set():
                try:
                    # Check if snapshot has changed
                    current_info = self.cache_manager.get_snapshot_info()
                    
                    if current_info and current_info != last_snapshot_info:
                        self.logger.debug("Cache updated, writing Parquet")
                        self.write_parquet()
                        last_snapshot_info = current_info.copy()
                    
                    # Sleep before next check
                    self._stop_event.wait(timeout=self.config.scan_interval_sec)
                    
                except Exception as e:
                    self.logger.error(f"Background writer error: {e}")
                    self._stop_event.wait(timeout=5)  # Brief pause on error
            
            self.logger.info("Background Parquet writer stopped")
        
        thread = threading.Thread(target=background_writer, daemon=True)
        thread.start()
        return thread
    
    def stop_background_writer(self) -> None:
        """Stop background writer thread"""
        self._stop_event.set()
    
    def get_parquet_info(self) -> dict:
        """Get information about the Parquet file"""
        try:
            if not os.path.exists(self.parquet_path):
                return {
                    "exists": False,
                    "path": self.parquet_path
                }
            
            stat = os.stat(self.parquet_path)
            
            # Try to read parquet metadata
            try:
                import pyarrow.parquet as pq
                parquet_file = pq.ParquetFile(self.parquet_path)
                metadata = parquet_file.metadata
                
                return {
                    "exists": True,
                    "path": self.parquet_path,
                    "size_bytes": stat.st_size,
                    "modified_time": stat.st_mtime,
                    "rows": metadata.num_rows,
                    "columns": metadata.num_columns,
                    "compression": str(metadata.row_group(0).column(0).compression) if metadata.num_row_groups > 0 else "unknown"
                }
            except Exception:
                return {
                    "exists": True,
                    "path": self.parquet_path,
                    "size_bytes": stat.st_size,
                    "modified_time": stat.st_mtime,
                    "error": "Failed to read Parquet metadata"
                }
                
        except Exception as e:
            return {
                "exists": False,
                "path": self.parquet_path,
                "error": str(e)
            }
    
    def read_parquet(self) -> Optional[pd.DataFrame]:
        """Read DataFrame from Parquet file"""
        try:
            if not os.path.exists(self.parquet_path):
                return None
            
            df = pd.read_parquet(self.parquet_path)
            self.logger.info(f"Read Parquet file: {len(df)} rows")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read Parquet file: {e}")
            return None

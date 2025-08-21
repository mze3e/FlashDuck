"""
Configuration management for FlashDuck
"""

import os
from typing import Optional
from dataclasses import dataclass


@dataclass
class Config:
    """Configuration settings for FlashDuck"""
    
    # Core settings
    table_name: str = "default_table"
    db_root: str = "./shared_db"
    redis_url: str = "redis://localhost:6379"
    
    # Monitoring settings
    scan_interval_sec: int = 5
    
    # Data format settings
    snapshot_format: str = "arrow"  # arrow, parquet, json
    parquet_compression: str = "zstd"  # zstd, snappy, none
    
    # Performance settings
    parquet_debounce_sec: Optional[int] = None
    sql_output_format: str = "json"  # json, csv, arrow
    
    # Schema evolution
    schema_evolution: str = "union"  # union, strict
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Create configuration from environment variables"""
        return cls(
            table_name=os.getenv("TABLE", "default_table"),
            db_root=os.getenv("DB_ROOT", "./shared_db"),
            redis_url=os.getenv("REDIS_URL", "redis://localhost:6379"),
            scan_interval_sec=int(os.getenv("SCAN_INTERVAL_SEC", "5")),
            snapshot_format=os.getenv("SNAPSHOT_FORMAT", "arrow"),
            parquet_compression=os.getenv("PARQUET_COMPRESSION", "zstd"),
            parquet_debounce_sec=int(os.getenv("PARQUET_DEBOUNCE_SEC", "0")) or None,
            sql_output_format=os.getenv("SQL_OUTPUT_FORMAT", "json"),
            schema_evolution=os.getenv("SCHEMA_EVOLUTION", "union")
        )
    
    def validate(self) -> None:
        """Validate configuration settings"""
        if self.snapshot_format not in ["arrow", "parquet", "json"]:
            raise ValueError(f"Invalid snapshot_format: {self.snapshot_format}")
        
        if self.parquet_compression not in ["zstd", "snappy", "none"]:
            raise ValueError(f"Invalid parquet_compression: {self.parquet_compression}")
        
        if self.sql_output_format not in ["json", "csv", "arrow"]:
            raise ValueError(f"Invalid sql_output_format: {self.sql_output_format}")
        
        if self.schema_evolution not in ["union", "strict"]:
            raise ValueError(f"Invalid schema_evolution: {self.schema_evolution}")

"""
Redis cache management for DuckRedis
"""

import json
import logging
from typing import Any, Dict, List, Optional, Union
import redis
import pandas as pd
from .config import Config
from .utils import (
    dataframe_to_arrow_ipc, 
    arrow_ipc_to_dataframe,
    dataframe_to_parquet_bytes
)


class CacheManager:
    """Manages Redis cache for table snapshots"""
    
    def __init__(self, config: Config):
        self.config = config
        self.redis_client = redis.from_url(config.redis_url, decode_responses=False)
        self.logger = logging.getLogger(__name__)
        
        # Redis keys
        self.snapshot_key = f"snapshot:{config.table_name}"
        self.metadata_key = f"metadata:{config.table_name}"
        self.stream_key = f"writes:{config.table_name}"
        
    def store_snapshot(self, df: pd.DataFrame) -> None:
        """Store DataFrame snapshot in Redis"""
        try:
            if self.config.snapshot_format == "arrow":
                data = dataframe_to_arrow_ipc(df)
                format_type = "arrow"
            elif self.config.snapshot_format == "parquet":
                data = dataframe_to_parquet_bytes(df, self.config.parquet_compression)
                format_type = "parquet"
            elif self.config.snapshot_format == "json":
                data = df.to_json(orient='records').encode('utf-8')
                format_type = "json"
            else:
                raise ValueError(f"Unsupported snapshot format: {self.config.snapshot_format}")
            
            # Store data and metadata atomically using pipeline
            with self.redis_client.pipeline() as pipe:
                pipe.set(self.snapshot_key, data)
                pipe.hset(self.metadata_key, mapping={
                    "format": format_type,
                    "rows": len(df),
                    "columns": len(df.columns),
                    "size_bytes": len(data),
                    "column_names": json.dumps(list(df.columns))
                })
                pipe.execute()
            
            self.logger.info(
                f"Stored snapshot: {len(df)} rows, {len(df.columns)} columns, "
                f"{len(data)} bytes ({format_type} format)"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to store snapshot: {e}")
            raise
    
    def load_snapshot(self) -> Optional[pd.DataFrame]:
        """Load DataFrame snapshot from Redis"""
        try:
            # Get data and metadata
            data = self.redis_client.get(self.snapshot_key)
            metadata = self.redis_client.hgetall(self.metadata_key)
            
            if not data or not metadata:
                self.logger.warning("No snapshot found in cache")
                return None
            
            format_type = metadata.get(b'format', b'').decode('utf-8')
            
            if format_type == "arrow":
                df = arrow_ipc_to_dataframe(data)
            elif format_type == "parquet":
                import pyarrow.parquet as pq
                import io
                df = pq.read_table(io.BytesIO(data)).to_pandas()
            elif format_type == "json":
                df = pd.read_json(data.decode('utf-8'), orient='records')
            else:
                raise ValueError(f"Unknown snapshot format: {format_type}")
            
            rows = int(metadata.get(b'rows', 0))
            self.logger.info(f"Loaded snapshot: {rows} rows ({format_type} format)")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to load snapshot: {e}")
            raise
    
    def get_snapshot_info(self) -> Dict[str, Any]:
        """Get snapshot metadata"""
        try:
            metadata = self.redis_client.hgetall(self.metadata_key)
            if not metadata:
                return {}
            
            # Decode bytes to strings
            info = {}
            for key, value in metadata.items():
                key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                value_str = value.decode('utf-8') if isinstance(value, bytes) else value
                
                if key_str in ['rows', 'columns', 'size_bytes']:
                    info[key_str] = int(value_str)
                elif key_str == 'column_names':
                    info[key_str] = json.loads(value_str)
                else:
                    info[key_str] = value_str
            
            return info
            
        except Exception as e:
            self.logger.error(f"Failed to get snapshot info: {e}")
            return {}
    
    def enqueue_write(self, operation: str, record_id: str, data: Optional[Dict[str, Any]] = None) -> str:
        """Enqueue write operation to Redis Stream"""
        try:
            stream_data = {
                'operation': operation,
                'id': record_id,
                'timestamp': pd.Timestamp.now().isoformat()
            }
            
            if data:
                stream_data['data'] = json.dumps(data)
            
            message_id = self.redis_client.xadd(self.stream_key, stream_data)
            self.logger.info(f"Enqueued {operation} for id {record_id}: {message_id}")
            return message_id.decode('utf-8') if isinstance(message_id, bytes) else message_id
            
        except Exception as e:
            self.logger.error(f"Failed to enqueue write: {e}")
            raise
    
    def consume_writes(self, consumer_group: str = "workers", consumer_name: str = "worker1", count: int = 10) -> List[Dict[str, Any]]:
        """Consume write operations from Redis Stream"""
        try:
            # Create consumer group if it doesn't exist
            try:
                self.redis_client.xgroup_create(self.stream_key, consumer_group, id='0', mkstream=True)
            except redis.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise
            
            # Read messages
            messages = self.redis_client.xreadgroup(
                consumer_group, 
                consumer_name, 
                {self.stream_key: '>'}, 
                count=count,
                block=1000  # 1 second timeout
            )
            
            writes = []
            for stream, msgs in messages:
                for msg_id, fields in msgs:
                    try:
                        # Decode message
                        decoded_fields = {}
                        for key, value in fields.items():
                            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                            value_str = value.decode('utf-8') if isinstance(value, bytes) else value
                            decoded_fields[key_str] = value_str
                        
                        write_op = {
                            'message_id': msg_id.decode('utf-8') if isinstance(msg_id, bytes) else msg_id,
                            'operation': decoded_fields.get('operation'),
                            'id': decoded_fields.get('id'),
                            'timestamp': decoded_fields.get('timestamp'),
                            'data': json.loads(decoded_fields['data']) if 'data' in decoded_fields else None
                        }
                        writes.append(write_op)
                        
                        # Acknowledge message
                        self.redis_client.xack(self.stream_key, consumer_group, msg_id)
                        
                    except Exception as e:
                        self.logger.error(f"Failed to process message {msg_id}: {e}")
            
            return writes
            
        except Exception as e:
            self.logger.error(f"Failed to consume writes: {e}")
            return []
    
    def clear_cache(self) -> None:
        """Clear all cached data"""
        try:
            self.redis_client.delete(self.snapshot_key, self.metadata_key)
            self.logger.info("Cache cleared")
        except Exception as e:
            self.logger.error(f"Failed to clear cache: {e}")
            raise
    
    def is_connected(self) -> bool:
        """Check if Redis is connected"""
        try:
            self.redis_client.ping()
            return True
        except:
            return False

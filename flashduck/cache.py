"""
Redis cache management for FlashDuck
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
        self.redis_client = redis.Redis.from_url(config.redis_url, decode_responses=False)
        self.logger = logging.getLogger(__name__)
        
        # Redis keys for multiple tables
        self.tables_list_key = "flashduck:tables"
        
        # Legacy compatibility attributes
        self.stream_key = f"writes:{config.table_name}"
        self.snapshot_key = f"snapshot:{config.table_name}"
        self.metadata_key = f"metadata:{config.table_name}"
    
    def _get_table_keys(self, table_name: str) -> tuple:
        """Get Redis keys for a specific table"""
        snapshot_key = f"snapshot:{table_name}"
        metadata_key = f"metadata:{table_name}"
        stream_key = f"writes:{table_name}"
        return snapshot_key, metadata_key, stream_key
        
    def store_table_snapshot(self, table_name: str, df: pd.DataFrame) -> None:
        """Store DataFrame snapshot for a specific table in Redis"""
        try:
            snapshot_key, metadata_key, _ = self._get_table_keys(table_name)
            
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
                pipe.set(snapshot_key, data)
                pipe.hset(metadata_key, mapping={
                    "format": format_type,
                    "rows": len(df),
                    "columns": len(df.columns),
                    "size_bytes": len(data),
                    "column_names": json.dumps(list(df.columns))
                })
                pipe.sadd(self.tables_list_key, table_name)  # Track table names
                pipe.execute()
            
            self.logger.info(
                f"Stored table '{table_name}': {len(df)} rows, {len(df.columns)} columns, "
                f"{len(data)} bytes ({format_type} format)"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to store snapshot for table '{table_name}': {e}")
            raise
    
    def store_snapshot(self, df: pd.DataFrame) -> None:
        """Store DataFrame snapshot in Redis (legacy method for backwards compatibility)"""
        self.store_table_snapshot(self.config.table_name, df)
    
    def load_table_snapshot(self, table_name: str) -> Optional[pd.DataFrame]:
        """Load DataFrame snapshot for a specific table from Redis"""
        try:
            snapshot_key, metadata_key, _ = self._get_table_keys(table_name)
            
            # Get data and metadata
            data = self.redis_client.get(snapshot_key)
            metadata = self.redis_client.hgetall(metadata_key)
            
            if not data or not metadata:
                self.logger.warning(f"No snapshot found in cache for table '{table_name}'")
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
            self.logger.info(f"Loaded table '{table_name}': {rows} rows ({format_type} format)")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to load snapshot for table '{table_name}': {e}")
            raise
    
    def load_snapshot(self) -> Optional[pd.DataFrame]:
        """Load DataFrame snapshot from Redis (legacy method for backwards compatibility)"""
        return self.load_table_snapshot(self.config.table_name)
    
    def get_table_names(self) -> List[str]:
        """Get list of all table names in cache"""
        try:
            table_names = self.redis_client.smembers(self.tables_list_key)
            return [name.decode('utf-8') if isinstance(name, bytes) else name for name in table_names]
        except Exception as e:
            self.logger.error(f"Failed to get table names: {e}")
            return []
    
    def get_all_tables(self) -> Dict[str, pd.DataFrame]:
        """Load all tables from cache"""
        tables = {}
        for table_name in self.get_table_names():
            df = self.load_table_snapshot(table_name)
            if df is not None:
                tables[table_name] = df
        return tables
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get metadata for a specific table"""
        try:
            _, metadata_key, _ = self._get_table_keys(table_name)
            metadata = self.redis_client.hgetall(metadata_key)
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
            self.logger.error(f"Failed to get table info for '{table_name}': {e}")
            return {}
    
    def get_snapshot_info(self) -> Dict[str, Any]:
        """Get snapshot metadata (legacy method for backwards compatibility)"""
        return self.get_table_info(self.config.table_name)
    
    def get_all_table_info(self) -> Dict[str, Dict[str, Any]]:
        """Get metadata for all tables"""
        all_info = {}
        for table_name in self.get_table_names():
            all_info[table_name] = self.get_table_info(table_name)
        return all_info
    
    def get_all_redis_keys(self) -> List[str]:
        """Get all Redis keys related to FlashDuck"""
        try:
            # Get all keys with FlashDuck patterns
            patterns = ["snapshot:*", "metadata:*", "writes:*", "flashduck:*"]
            all_keys = set()
            
            for pattern in patterns:
                keys = self.redis_client.keys(pattern)
                for key in keys:
                    if isinstance(key, bytes):
                        all_keys.add(key.decode('utf-8'))
                    else:
                        all_keys.add(key)
            
            return sorted(list(all_keys))
        except Exception as e:
            self.logger.error(f"Failed to get Redis keys: {e}")
            return []
    
    def get_redis_key_info(self, key: str) -> Dict[str, Any]:
        """Get information about a specific Redis key"""
        try:
            # Check if key exists
            if not self.redis_client.exists(key):
                return {"exists": False}
            
            # Get key type
            key_type = self.redis_client.type(key).decode('utf-8')
            
            info = {
                "exists": True,
                "type": key_type,
                "ttl": self.redis_client.ttl(key)
            }
            
            # Get size and sample data based on type
            if key_type == "string":
                value = self.redis_client.get(key)
                info["size_bytes"] = len(value) if value else 0
                
                # Try to decode as text if small enough
                if value and len(value) < 1000:
                    try:
                        info["sample_value"] = value.decode('utf-8')[:200]
                    except UnicodeDecodeError:
                        info["sample_value"] = f"<Binary data, {len(value)} bytes>"
                else:
                    info["sample_value"] = f"<Large binary data, {len(value)} bytes>"
                    
            elif key_type == "hash":
                hash_data = self.redis_client.hgetall(key)
                info["hash_fields"] = len(hash_data)
                info["sample_fields"] = {}
                
                # Show first few fields
                for i, (field, value) in enumerate(hash_data.items()):
                    if i >= 5:  # Limit to first 5 fields
                        break
                    field_str = field.decode('utf-8') if isinstance(field, bytes) else field
                    value_str = value.decode('utf-8') if isinstance(value, bytes) else str(value)
                    info["sample_fields"][field_str] = value_str[:100]  # Truncate long values
                    
            elif key_type == "set":
                set_size = self.redis_client.scard(key)
                info["set_size"] = set_size
                
                # Get sample members
                sample_members = self.redis_client.srandmember(key, 5)
                info["sample_members"] = []
                for member in sample_members:
                    member_str = member.decode('utf-8') if isinstance(member, bytes) else str(member)
                    info["sample_members"].append(member_str)
                    
            elif key_type == "stream":
                stream_info = self.redis_client.xinfo_stream(key)
                info["stream_length"] = stream_info.get("length", 0)
                info["stream_groups"] = stream_info.get("groups", 0)
                
                # Get latest entries
                try:
                    latest_entries = self.redis_client.xrevrange(key, count=3)
                    info["latest_entries"] = []
                    for entry_id, fields in latest_entries:
                        entry_id_str = entry_id.decode('utf-8') if isinstance(entry_id, bytes) else entry_id
                        fields_dict = {}
                        for field, value in fields.items():
                            field_str = field.decode('utf-8') if isinstance(field, bytes) else field
                            value_str = value.decode('utf-8') if isinstance(value, bytes) else str(value)
                            fields_dict[field_str] = value_str[:50]  # Truncate
                        info["latest_entries"].append({"id": entry_id_str, "fields": fields_dict})
                except Exception:
                    info["latest_entries"] = []
            
            return info
            
        except Exception as e:
            self.logger.error(f"Failed to get info for key '{key}': {e}")
            return {"exists": False, "error": str(e)}
    
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
            for table_name in self.get_table_names():
                snapshot_key, metadata_key, stream_key = self._get_table_keys(table_name)
                self.redis_client.delete(snapshot_key, metadata_key, stream_key)
            self.redis_client.delete(self.tables_list_key)
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

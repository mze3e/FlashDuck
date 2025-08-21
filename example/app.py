"""
Streamlit demo interface for FlashDuck
"""

import streamlit as st
import json
import time
import pandas as pd
from datetime import datetime
import os
import sys

# Add parent directory to path to import flashduck
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from flashduck import FlashDuckEngine, Config
from flashduck.utils import format_bytes


# Configure Streamlit page
st.set_page_config(
    page_title="FlashDuck Demo",
    page_icon="🦆",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'engine' not in st.session_state:
    st.session_state.engine = None
if 'auto_refresh' not in st.session_state:
    st.session_state.auto_refresh = False
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = 0


def get_engine():
    """Get or create FlashDuck engine"""
    if st.session_state.engine is None:
        config = Config.from_env()
        st.session_state.engine = FlashDuckEngine(config)
        
        # Start engine with sample data if not running
        if not st.session_state.engine.is_running():
            try:
                st.session_state.engine.start(create_sample_data=True)
            except Exception as e:
                st.error(f"Failed to start FlashDuck engine: {e}")
                return None
    
    return st.session_state.engine


def render_header():
    """Render application header"""
    st.title("🦆 FlashDuck Demo")
    st.markdown("""
    **High-performance data management combining DuckDB and Redis**
    
    This demo showcases real-time file monitoring, Redis caching, SQL querying, and Parquet export capabilities.
    """)


def render_sidebar():
    """Render sidebar with controls"""
    st.sidebar.title("🎛️ Controls")
    
    # Auto-refresh toggle
    auto_refresh = st.sidebar.checkbox(
        "Auto-refresh (5s)", 
        value=st.session_state.auto_refresh,
        help="Automatically refresh data every 5 seconds"
    )
    st.session_state.auto_refresh = auto_refresh
    
    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Now", use_container_width=True):
        st.rerun()
    
    # Force cache refresh
    if st.sidebar.button("📊 Force Cache Refresh", use_container_width=True):
        engine = get_engine()
        if engine:
            with st.spinner("Refreshing cache..."):
                success = engine.force_refresh()
                if success:
                    st.sidebar.success("Cache refreshed!")
                else:
                    st.sidebar.error("Failed to refresh cache")
            st.rerun()
    
    # Export Parquet
    if st.sidebar.button("💾 Export Parquet", use_container_width=True):
        engine = get_engine()
        if engine:
            with st.spinner("Exporting Parquet..."):
                success = engine.write_parquet()
                if success:
                    st.sidebar.success("Parquet exported!")
                else:
                    st.sidebar.error("Failed to export Parquet")
            st.rerun()
    
    st.sidebar.divider()
    
    # Configuration display
    st.sidebar.subheader("⚙️ Configuration")
    engine = get_engine()
    if engine:
        config = engine.config
        st.sidebar.text(f"Table: {config.table_name}")
        st.sidebar.text(f"DB Root: {config.db_root}")
        st.sidebar.text(f"Redis: {config.redis_url}")
        st.sidebar.text(f"Scan Interval: {config.scan_interval_sec}s")
        st.sidebar.text(f"Format: {config.snapshot_format}")


def render_status_overview():
    """Render system status overview"""
    st.subheader("📈 System Status")
    
    engine = get_engine()
    if not engine:
        st.error("FlashDuck engine not available")
        return
    
    status = engine.get_status()
    
    # Create metrics columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        engine_status = status.get('engine', {})
        running = engine_status.get('running', False)
        st.metric(
            "Engine Status", 
            "🟢 Running" if running else "🔴 Stopped",
            delta=None
        )
    
    with col2:
        redis_status = status.get('redis', {})
        connected = redis_status.get('connected', False)
        st.metric(
            "Redis Status",
            "🟢 Connected" if connected else "🔴 Disconnected",
            delta=None
        )
    
    with col3:
        cache_status = status.get('cache', {})
        rows = cache_status.get('rows', 0)
        st.metric("Cache Rows", f"{rows:,}", delta=None)
    
    with col4:
        file_status = status.get('files', {})
        file_count = file_status.get('file_count', 0)
        st.metric("Files Monitored", file_count, delta=None)
    
    # Detailed status in expandable section
    with st.expander("📋 Detailed Status", expanded=False):
        
        # Cache details
        st.subheader("💾 Cache Status")
        cache_cols = st.columns(3)
        
        with cache_cols[0]:
            st.write(f"**Rows:** {cache_status.get('rows', 0):,}")
            st.write(f"**Columns:** {cache_status.get('columns', 0)}")
        
        with cache_cols[1]:
            size_bytes = cache_status.get('size_bytes', 0)
            st.write(f"**Size:** {format_bytes(size_bytes)}")
            st.write(f"**Format:** {cache_status.get('format', 'N/A')}")
        
        with cache_cols[2]:
            columns = cache_status.get('column_names', [])
            if columns:
                st.write(f"**Columns:** {', '.join(columns[:5])}")
                if len(columns) > 5:
                    st.write(f"... and {len(columns) - 5} more")
        
        # File details
        st.subheader("📁 File Status")
        files = file_status.get('files', [])
        if files:
            files_df = pd.DataFrame(files)
            files_df['size_mb'] = files_df['size_bytes'] / (1024 * 1024)
            files_df['modified'] = pd.to_datetime(files_df['modified_time'], unit='s')
            
            st.dataframe(
                files_df[['name', 'size_mb', 'modified']].round(2),
                use_container_width=True
            )
        else:
            st.info("No files found")
        
        # Parquet details
        st.subheader("📄 Parquet Status")
        parquet_status = status.get('parquet', {})
        if parquet_status.get('exists'):
            parquet_cols = st.columns(3)
            with parquet_cols[0]:
                st.write(f"**Rows:** {parquet_status.get('rows', 0):,}")
                st.write(f"**Columns:** {parquet_status.get('columns', 0)}")
            with parquet_cols[1]:
                size_bytes = parquet_status.get('size_bytes', 0)
                st.write(f"**Size:** {format_bytes(size_bytes)}")
                st.write(f"**Compression:** {parquet_status.get('compression', 'N/A')}")
            with parquet_cols[2]:
                modified_time = parquet_status.get('modified_time', 0)
                if modified_time:
                    modified_dt = datetime.fromtimestamp(modified_time)
                    st.write(f"**Modified:** {modified_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            st.info("No Parquet file found")


def render_data_explorer():
    """Render data exploration interface"""
    st.subheader("🔍 Data Explorer")
    
    engine = get_engine()
    if not engine:
        st.error("FlashDuck engine not available")
        return
    
    # Get table info
    table_info = engine.get_table_info()
    
    if not table_info.get('exists'):
        st.warning("No data available. Try refreshing the cache or adding some files.")
        return
    
    # Display available tables
    tables_info = table_info.get('tables', {})
    table_names = table_info.get('table_names', [])
    
    if table_names:
        st.write("**Available Tables:**")
        # Table selector
        selected_table = st.selectbox(
            "Choose a table to explore:",
            ["All Tables"] + table_names,
            key="table_selector"
        )
        
        # Sample data view
        if selected_table == "All Tables":
            st.write("**Sample Data from All Tables (first 20 rows):**")
            sample_result = engine.get_sample_data(20)
        else:
            st.write(f"**Sample Data from '{selected_table}' (first 20 rows):**")
            sample_result = engine.get_sample_data(20, selected_table)
        
        if sample_result['success'] and sample_result['data']:
            sample_df = pd.DataFrame(sample_result['data'])
            st.dataframe(sample_df, use_container_width=True)
            
            # Basic statistics
            st.write("**Database Statistics:**")
            stats_cols = st.columns(4)
            
            with stats_cols[0]:
                st.metric("Total Tables", table_info.get('total_tables', 0))
            
            with stats_cols[1]:
                st.metric("Total Rows", f"{table_info.get('total_rows', 0):,}")
            
            with stats_cols[2]:
                total_size = table_info.get('total_size_bytes', 0)
                st.metric("Total Cache Size", format_bytes(total_size))
            
            with stats_cols[3]:
                if selected_table != "All Tables" and selected_table in tables_info:
                    table_rows = tables_info[selected_table].get('rows', 0)
                    st.metric(f"'{selected_table}' Rows", f"{table_rows:,}")
        
        # Table details
        if len(tables_info) > 0:
            st.write("**Table Details:**")
            table_details = []
            for tname, tinfo in tables_info.items():
                table_details.append({
                    'Table': tname,
                    'Rows': tinfo.get('rows', 0),
                    'Columns': tinfo.get('columns', 0),
                    'Size': format_bytes(tinfo.get('size_bytes', 0)),
                    'Format': tinfo.get('format', 'unknown')
                })
            
            if table_details:
                details_df = pd.DataFrame(table_details)
                st.dataframe(details_df, use_container_width=True)
    else:
        st.info("No tables found")
        return
    
    # Column analysis for selected data
    if sample_result['success'] and sample_result['data']:
        sample_df = pd.DataFrame(sample_result['data'])
        
        if len(sample_df) > 0:
            # Column analysis - using simple charts instead of Plotly to avoid import issues
            with st.expander("📊 Column Analysis", expanded=False):
                for column in sample_df.columns:
                    st.write(f"**{column}**")
                    
                    if sample_df[column].dtype in ['int64', 'float64']:
                        # Numeric column - show basic statistics
                        col_stats = sample_df[column].describe()
                        stats_col1, stats_col2 = st.columns(2)
                        
                        with stats_col1:
                            st.metric("Mean", f"{col_stats['mean']:.2f}")
                            st.metric("Min", f"{col_stats['min']:.2f}")
                        
                        with stats_col2:
                            st.metric("Max", f"{col_stats['max']:.2f}")
                            st.metric("Std", f"{col_stats['std']:.2f}")
                        
                        # Simple display instead of charts to avoid JS issues
                        st.write(f"Data type: {sample_df[column].dtype}")
                        st.write(f"Sample values: {list(sample_df[column].dropna().head(3))}")
                        
                    elif len(sample_df[column].unique()) < 20:
                        # Categorical column with few values
                        value_counts = sample_df[column].value_counts()
                        st.write(f"Unique values: {len(sample_df[column].unique())}")
                        
                        # Simple text display
                        for val, count in value_counts.head(10).items():
                            st.write(f"• {val}: {count}")
                    
                    else:
                        st.write(f"Data type: {sample_df[column].dtype}")
                        st.write(f"Unique values: {len(sample_df[column].unique())}")
                    
                    st.divider()
    else:
        st.error(f"Failed to load sample data: {sample_result.get('error', 'Unknown error')}")


def render_sql_interface():
    """Render SQL query interface"""
    st.subheader("🗃️ SQL Query Interface")
    
    engine = get_engine()
    if not engine:
        st.error("FlashDuck engine not available")
        return
    
    # Query input
    default_query = "SELECT * FROM users LIMIT 10"
    
    sql_query = st.text_area(
        "Enter SQL Query:",
        value=st.session_state.get('sql_input', default_query),
        height=100,
        help="Enter a read-only SQL query. Write operations are not allowed.",
        key="sql_input"
    )
    
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        execute_button = st.button("▶️ Execute Query", use_container_width=True)
    
    with col2:
        validate_button = st.button("✅ Validate Query", use_container_width=True)
    
    # Validate query
    if validate_button:
        validation = engine.validate_query(sql_query)
        if validation['valid']:
            st.success("✅ Query is valid!")
        else:
            st.error(f"❌ Query validation failed: {validation['error']}")
    
    # Execute query
    if execute_button:
        if not sql_query.strip():
            st.error("Please enter a SQL query")
            return
        
        with st.spinner("Executing query..."):
            result = engine.sql(sql_query)
        
        if result['success']:
            st.success(f"✅ Query executed successfully! Returned {result['rows']} rows.")
            
            if result['data']:
                # Display results
                result_df = pd.DataFrame(result['data'])
                st.dataframe(result_df, use_container_width=True)
                
                # Download options
                if len(result_df) > 0:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        csv_data = result_df.to_csv(index=False)
                        st.download_button(
                            "📥 Download CSV",
                            csv_data,
                            "query_result.csv",
                            "text/csv",
                            use_container_width=True
                        )
                    
                    with col2:
                        json_data = result_df.to_json(orient='records', indent=2)
                        if json_data:
                            st.download_button(
                                "📥 Download JSON",
                                json_data,
                                "query_result.json",
                                "application/json",
                                use_container_width=True
                            )
            else:
                st.info("Query returned no results")
        else:
            st.error(f"❌ Query failed: {result['error']}")
    
    # Query examples
    with st.expander("📚 Example Queries", expanded=False):
        examples = [
            "SELECT * FROM users LIMIT 5",
            "SELECT * FROM products WHERE price > 100",
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.user_id = o.user_id",
            "SELECT COUNT(*) as total_products FROM products",
            "SELECT 'users' as table_name, COUNT(*) as rows FROM users UNION ALL SELECT 'products', COUNT(*) FROM products",
        ]
        
        for i, example in enumerate(examples, 1):
            if st.button(f"Example {i}", key=f"example_{i}"):
                st.session_state.sql_input = example
                st.rerun()
            st.code(example, language="sql")


def render_write_operations():
    """Render write operations interface"""
    st.subheader("✏️ Write Operations")
    
    engine = get_engine()
    if not engine:
        st.error("FlashDuck engine not available")
        return
    
    st.info("💡 Write operations are queued in Redis Streams for processing by background workers.")
    
    # Upsert operation
    st.write("**Upsert Record:**")
    
    col1, col2 = st.columns(2)
    
    with col1:
        upsert_id = st.text_input("Record ID:", placeholder="e.g., user_123")
    
    with col2:
        upsert_data = st.text_area(
            "JSON Data:",
            placeholder='{"name": "John Doe", "age": 30}',
            height=100
        )
    
    if st.button("📤 Queue Upsert", use_container_width=True):
        if not upsert_id:
            st.error("Please enter a record ID")
        elif not upsert_data:
            st.error("Please enter JSON data")
        else:
            try:
                data_dict = json.loads(upsert_data)
                message_id = engine.enqueue_upsert(upsert_id, data_dict)
                st.success(f"✅ Upsert queued! Message ID: {message_id}")
            except json.JSONDecodeError as e:
                st.error(f"❌ Invalid JSON: {e}")
            except Exception as e:
                st.error(f"❌ Error: {e}")
    
    st.divider()
    
    # Delete operation
    st.write("**Delete Record:**")
    
    delete_id = st.text_input("Record ID to Delete:", placeholder="e.g., user_123")
    
    if st.button("🗑️ Queue Delete", use_container_width=True):
        if not delete_id:
            st.error("Please enter a record ID")
        else:
            try:
                message_id = engine.enqueue_delete(delete_id)
                st.success(f"✅ Delete queued! Message ID: {message_id}")
            except Exception as e:
                st.error(f"❌ Error: {e}")
    
    st.divider()
    
    # Pending operations
    st.write("**Pending Operations:**")
    
    if st.button("🔍 Check Pending Writes", use_container_width=True):
        try:
            writes = engine.consume_writes(count=50)
            if writes:
                st.write(f"Found {len(writes)} pending operations:")
                writes_df = pd.DataFrame(writes)
                st.dataframe(writes_df, use_container_width=True)
            else:
                st.info("No pending write operations")
        except Exception as e:
            st.error(f"❌ Error checking writes: {e}")


def render_redis_explorer():
    """Render Redis cache explorer interface"""
    st.subheader("🗄️ Redis Cache Explorer")
    
    engine = get_engine()
    if not engine:
        st.error("FlashDuck engine not available")
        return
    
    st.info("💡 Explore all Redis keys and cached data used by FlashDuck")
    
    # Get all Redis keys
    redis_keys = engine.cache_manager.get_all_redis_keys()
    
    if not redis_keys:
        st.warning("No Redis keys found")
        return
    
    st.write(f"**Found {len(redis_keys)} Redis keys:**")
    
    # Key selector
    selected_key = st.selectbox(
        "Select a Redis key to inspect:",
        redis_keys,
        key="redis_key_selector"
    )
    
    if selected_key:
        st.divider()
        
        # Get key information
        key_info = engine.cache_manager.get_redis_key_info(selected_key)
        
        if not key_info.get("exists"):
            st.error(f"Key '{selected_key}' does not exist or has an error")
            if "error" in key_info:
                st.error(f"Error: {key_info['error']}")
            return
        
        # Display key metadata
        st.write(f"**Key:** `{selected_key}`")
        
        info_cols = st.columns(4)
        with info_cols[0]:
            st.metric("Type", key_info.get("type", "unknown"))
        with info_cols[1]:
            ttl = key_info.get("ttl", -1)
            ttl_text = "Never expires" if ttl == -1 else f"{ttl}s" if ttl > 0 else "Expired"
            st.metric("TTL", ttl_text)
        with info_cols[2]:
            if "size_bytes" in key_info:
                st.metric("Size", format_bytes(key_info["size_bytes"]))
        with info_cols[3]:
            if "hash_fields" in key_info:
                st.metric("Hash Fields", key_info["hash_fields"])
            elif "set_size" in key_info:
                st.metric("Set Size", key_info["set_size"])
            elif "stream_length" in key_info:
                st.metric("Stream Length", key_info["stream_length"])
        
        # Display key content based on type
        key_type = key_info.get("type")
        
        if key_type == "string":
            st.write("**String Content:**")
            if "sample_value" in key_info:
                if key_info["sample_value"].startswith("<"):
                    st.info(key_info["sample_value"])
                else:
                    st.code(key_info["sample_value"], language="json")
        
        elif key_type == "hash":
            st.write("**Hash Fields (first 5):**")
            if "sample_fields" in key_info and key_info["sample_fields"]:
                hash_df = pd.DataFrame([
                    {"Field": k, "Value": v} 
                    for k, v in key_info["sample_fields"].items()
                ])
                st.dataframe(hash_df, use_container_width=True)
            else:
                st.info("No hash fields to display")
        
        elif key_type == "set":
            st.write("**Set Members (sample):**")
            if "sample_members" in key_info and key_info["sample_members"]:
                for i, member in enumerate(key_info["sample_members"], 1):
                    st.write(f"{i}. {member}")
            else:
                st.info("No set members to display")
        
        elif key_type == "stream":
            st.write("**Stream Information:**")
            stream_info_cols = st.columns(2)
            with stream_info_cols[0]:
                st.metric("Total Entries", key_info.get("stream_length", 0))
            with stream_info_cols[1]:
                st.metric("Consumer Groups", key_info.get("stream_groups", 0))
                
            if "latest_entries" in key_info and key_info["latest_entries"]:
                st.write("**Latest Entries:**")
                for entry in key_info["latest_entries"]:
                    with st.expander(f"Entry ID: {entry['id']}", expanded=False):
                        entry_df = pd.DataFrame([
                            {"Field": k, "Value": v} 
                            for k, v in entry["fields"].items()
                        ])
                        st.dataframe(entry_df, use_container_width=True)
            else:
                st.info("No stream entries to display")
        
        # If this is a table snapshot key, show table data
        if selected_key.startswith("snapshot:"):
            table_name = selected_key.replace("snapshot:", "")
            st.divider()
            st.write(f"**Table Data for '{table_name}':**")
            
            try:
                table_df = engine.cache_manager.load_table_snapshot(table_name)
                if table_df is not None and not table_df.empty:
                    st.write(f"**Rows:** {len(table_df)}, **Columns:** {len(table_df.columns)}")
                    
                    # Show sample data
                    sample_size = min(20, len(table_df))
                    st.write(f"**Sample Data (first {sample_size} rows):**")
                    st.dataframe(table_df.head(sample_size), use_container_width=True)
                    
                    # Column info
                    with st.expander("📊 Column Information", expanded=False):
                        col_info = []
                        for col in table_df.columns:
                            col_info.append({
                                "Column": col,
                                "Type": str(table_df[col].dtype),
                                "Non-null": table_df[col].count(),
                                "Unique": table_df[col].nunique()
                            })
                        
                        col_df = pd.DataFrame(col_info)
                        st.dataframe(col_df, use_container_width=True)
                else:
                    st.info("Table is empty or could not be loaded")
            except Exception as e:
                st.error(f"Failed to load table data: {e}")
    
    # Summary section
    st.divider()
    st.write("**Redis Cache Summary:**")
    
    # Categorize keys
    snapshot_keys = [k for k in redis_keys if k.startswith("snapshot:")]
    metadata_keys = [k for k in redis_keys if k.startswith("metadata:")]
    stream_keys = [k for k in redis_keys if k.startswith("writes:")]
    other_keys = [k for k in redis_keys if not any(k.startswith(p) for p in ["snapshot:", "metadata:", "writes:"])]
    
    summary_cols = st.columns(4)
    with summary_cols[0]:
        st.metric("Table Snapshots", len(snapshot_keys))
    with summary_cols[1]:
        st.metric("Metadata Keys", len(metadata_keys))
    with summary_cols[2]:
        st.metric("Write Streams", len(stream_keys))
    with summary_cols[3]:
        st.metric("Other Keys", len(other_keys))
    
    # Key categories
    with st.expander("📋 Key Categories", expanded=False):
        if snapshot_keys:
            st.write("**Table Snapshots:**")
            for key in snapshot_keys:
                table_name = key.replace("snapshot:", "")
                st.write(f"• `{key}` → Table: {table_name}")
        
        if metadata_keys:
            st.write("**Metadata Keys:**")
            for key in metadata_keys:
                st.write(f"• `{key}`")
        
        if stream_keys:
            st.write("**Write Streams:**")
            for key in stream_keys:
                st.write(f"• `{key}`")
        
        if other_keys:
            st.write("**Other Keys:**")
            for key in other_keys:
                st.write(f"• `{key}`")


def handle_auto_refresh():
    """Handle auto-refresh functionality"""
    if st.session_state.auto_refresh:
        current_time = time.time()
        if current_time - st.session_state.last_refresh > 5:  # 5 seconds
            st.session_state.last_refresh = current_time
            st.rerun()


def main():
    """Main application"""
    # Handle auto-refresh
    handle_auto_refresh()
    
    # Render main interface
    render_header()
    render_sidebar()
    
    # Main content tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Status Overview", 
        "🔍 Data Explorer", 
        "🗃️ SQL Interface", 
        "✏️ Write Operations",
        "🗄️ Redis Cache"
    ])
    
    with tab1:
        render_status_overview()
    
    with tab2:
        render_data_explorer()
    
    with tab3:
        render_sql_interface()
    
    with tab4:
        render_write_operations()
    
    with tab5:
        render_redis_explorer()
    
    # Footer
    st.divider()
    st.markdown("""
    <div style='text-align: center; color: #666; font-size: 0.8em;'>
        🦆 FlashDuck v0.1.0 - High-performance data management with DuckDB and Redis
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()

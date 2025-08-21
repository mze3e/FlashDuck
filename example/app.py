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
import importlib

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

    if st.sidebar.button("🛠️ Reset Engine", use_container_width=True):
        for module in sys.modules.values():
            try:
                importlib.reload(module)
            except Exception as e: 
                pass
        st.session_state.engine = None
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
    """Render SQL query interface with performance comparison"""
    st.subheader("🗃️ SQL Interface with Performance Comparison")
    
    engine = get_engine()
    if not engine:
        st.error("FlashDuck engine not available")
        return
    
    st.info("💡 Compare performance between Redis cache queries and direct Parquet file queries with partitioned deduplication.")
    
    # Query mode selection
    query_mode = st.radio(
        "Select query execution mode:",
        ["🏁 Performance Comparison", "🗄️ Redis Cache Only", "📁 Direct Parquet Only"],
        key="sql_query_mode",
        horizontal=True
    )
    
    # Query input
    default_query = "SELECT * FROM users LIMIT 10"
    
    sql_query = st.text_area(
        "Enter SQL Query:",
        value=st.session_state.get('sql_input', default_query),
        height=150,
        help="Enter a read-only SQL query. Only SELECT statements are allowed.",
        key="sql_input"
    )
    
    # Pre-filled examples
    example_queries = [
        "SELECT * FROM users LIMIT 10",
        "SELECT name, age FROM users WHERE age > 25",
        "SELECT category, COUNT(*) as count FROM products GROUP BY category",
        "SELECT u.name, p.name as product_name, o.total FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id",
        "SELECT status, COUNT(*) as order_count, AVG(total) as avg_total FROM orders GROUP BY status",
        "SELECT * FROM products WHERE in_stock = true ORDER BY rating DESC"
    ]
    
    selected_example = st.selectbox(
        "Or choose an example query:",
        ["Custom Query"] + example_queries,
        key="sql_example_selector"
    )
    
    if selected_example != "Custom Query":
        st.session_state.sql_input = selected_example
        st.rerun()
    
    # Query execution buttons
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        execute_button = st.button("🚀 Execute Query", use_container_width=True)
    
    with col2:
        validate_button = st.button("✅ Validate", use_container_width=True)
    
    with col3:
        clear_button = st.button("🧹 Clear", use_container_width=True)
    
    if clear_button:
        st.session_state.sql_input = ""
        st.rerun()
    
    # Validate query
    if validate_button:
        try:
            validation = engine.validate_query(sql_query)
            if validation['valid']:
                st.success("✅ Query is valid!")
            else:
                st.error(f"❌ Query validation failed: {validation['error']}")
        except Exception as e:
            st.error(f"❌ Validation error: {e}")
    
    # Execute query
    if execute_button:
        if not sql_query.strip():
            st.error("Please enter a SQL query")
            return
        
        try:
            if query_mode == "🏁 Performance Comparison":
                # Run both queries and compare performance
                st.write("**Performance Comparison Results:**")
                
                with st.spinner("Running performance comparison..."):
                    # Execute Redis cache query
                    import time
                    start_time = time.time()
                    cache_result = engine.query_engine.execute_sql(sql_query)
                    cache_time = time.time() - start_time
                    
                    # Execute direct parquet query
                    start_time = time.time()
                    parquet_result = engine.query_engine.execute_sql_direct_parquet(sql_query, engine.config.db_root)
                    parquet_time = time.time() - start_time
                
                # Performance comparison metrics
                perf_cols = st.columns(4)
                with perf_cols[0]:
                    st.metric("🗄️ Redis Cache", f"{cache_time:.4f}s", delta=f"{cache_result.get('rows', 0)} rows")
                with perf_cols[1]:
                    st.metric("📁 Direct Parquet", f"{parquet_time:.4f}s", delta=f"{parquet_result.get('rows', 0)} rows")
                with perf_cols[2]:
                    if parquet_time > 0:
                        speedup = cache_time / parquet_time if cache_time > parquet_time else parquet_time / cache_time
                        winner = "Redis Cache" if cache_time < parquet_time else "Direct Parquet"
                        st.metric("🏆 Winner", winner, delta=f"{speedup:.1f}x faster")
                    else:
                        st.metric("🏆 Winner", "Redis Cache", delta="N/A")
                with perf_cols[3]:
                    cache_success = cache_result.get("success", False)
                    parquet_success = parquet_result.get("success", False)
                    both_success = cache_success and parquet_success
                    st.metric("✅ Results Match", "Yes" if both_success else "No", delta="Both succeeded" if both_success else "Check errors")
                
                # Show results from cache (they should be the same)
                if cache_result.get("success", False):
                    st.success(f"✅ Query executed successfully! ({cache_result['rows']} rows returned)")
                    
                    if cache_result["rows"] > 0 and cache_result.get("data"):
                        df = pd.DataFrame(cache_result["data"])
                        st.dataframe(df, use_container_width=True)
                        
                        # Download options
                        col1, col2 = st.columns(2)
                        with col1:
                            csv_data = df.to_csv(index=False)
                            st.download_button("📥 Download CSV", csv_data, "query_results.csv", "text/csv", use_container_width=True)
                        with col2:
                            json_data = df.to_json(orient='records', indent=2)
                            st.download_button("📥 Download JSON", json_data, "query_results.json", "application/json", use_container_width=True)
                    else:
                        st.info("Query executed successfully but returned no rows")
                else:
                    st.error(f"❌ Cache query failed: {cache_result.get('error', 'Unknown error')}")
                    
                if not parquet_result.get("success", False):
                    st.error(f"❌ Parquet query failed: {parquet_result.get('error', 'Unknown error')}")
                    
            elif query_mode == "🗄️ Redis Cache Only":
                # Execute only Redis cache query
                with st.spinner("Executing Redis cache query..."):
                    result = engine.query_engine.execute_sql(sql_query)
                
                if result.get("success", False):
                    st.success(f"✅ Redis Cache query executed! ({result['rows']} rows returned)")
                    
                    if result["rows"] > 0 and result.get("data"):
                        df = pd.DataFrame(result["data"])
                        st.dataframe(df, use_container_width=True)
                        
                        # Download options
                        col1, col2 = st.columns(2)
                        with col1:
                            csv_data = df.to_csv(index=False)
                            st.download_button("📥 Download CSV", csv_data, "query_results.csv", "text/csv", use_container_width=True)
                        with col2:
                            json_data = df.to_json(orient='records', indent=2)
                            st.download_button("📥 Download JSON", json_data, "query_results.json", "application/json", use_container_width=True)
                    else:
                        st.info("Query executed successfully but returned no rows")
                else:
                    st.error(f"❌ Query failed: {result.get('error', 'Unknown error')}")
                    
            elif query_mode == "📁 Direct Parquet Only":
                # Execute only direct parquet query
                with st.spinner("Executing direct Parquet query..."):
                    result = engine.query_engine.execute_sql_direct_parquet(sql_query, engine.config.db_root)
                
                if result.get("success", False):
                    query_time = result.get("query_time", 0)
                    st.success(f"✅ Direct Parquet query executed in {query_time:.4f}s! ({result['rows']} rows returned)")
                    
                    if result["rows"] > 0 and result.get("data"):
                        df = pd.DataFrame(result["data"])
                        st.dataframe(df, use_container_width=True)
                        
                        # Download options
                        col1, col2 = st.columns(2)
                        with col1:
                            csv_data = df.to_csv(index=False)
                            st.download_button("📥 Download CSV", csv_data, "query_results.csv", "text/csv", use_container_width=True)
                        with col2:
                            json_data = df.to_json(orient='records', indent=2)
                            st.download_button("📥 Download JSON", json_data, "query_results.json", "application/json", use_container_width=True)
                    else:
                        st.info("Query executed successfully but returned no rows")
                else:
                    st.error(f"❌ Query failed: {result.get('error', 'Unknown error')}")
                    
        except Exception as e:
            st.error(f"❌ Error executing query: {e}")
    
    # Query examples section
    with st.expander("💡 Query Examples & Tips", expanded=False):
        st.write("**Example Queries:**")
        
        examples = [
            ("Basic Selection", "SELECT * FROM users WHERE active = true"),
            ("Aggregation", "SELECT category, COUNT(*) as count, AVG(price) as avg_price FROM products GROUP BY category"),
            ("Join Query", "SELECT u.name, o.total, p.name as product FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id"),
            ("Window Function", "SELECT name, age, RANK() OVER (ORDER BY age DESC) as age_rank FROM users"),
            ("Date Filtering", "SELECT * FROM orders WHERE date >= '2025-01-01'"),
            ("Complex Analytics", "WITH user_stats AS (SELECT user_id, COUNT(*) as order_count, SUM(total) as total_spent FROM orders GROUP BY user_id) SELECT u.name, us.order_count, us.total_spent FROM users u JOIN user_stats us ON u.id = us.user_id ORDER BY us.total_spent DESC")
        ]
        
        for title, query in examples:
            with st.container():
                st.write(f"**{title}:**")
                if st.button(f"Use this query", key=f"example_{title.replace(' ', '_').lower()}"):
                    st.session_state.sql_input = query
                    st.rerun()
            st.code(query, language="sql")
        
        st.divider()
        st.write("**Performance Tips:**")
        st.write("• **Redis Cache**: Fast for repeated queries, data already in memory")  
        st.write("• **Direct Parquet**: Uses ranked window functions for deduplication, may be slower but always current")
        st.write("• **Partitioned Files**: Each update creates a new partition file with timestamp")
        st.write("• **Primary Key Ranking**: Latest records selected by _modified_time for each primary key")


def render_write_operations():
    """Render write operations interface with data editor"""
    st.subheader("✏️ Write Operations")
    
    engine = get_engine()
    if not engine:
        st.error("FlashDuck engine not available")
        return
    
    st.info("💡 Edit tables directly with the data editor. Add new rows or modify existing ones, then save changes to Parquet files.")
    
    # Table selector
    available_tables = ["users", "products", "orders"]
    selected_table = st.selectbox(
        "Select table to edit:",
        available_tables,
        key="write_table_selector"
    )
    
    if selected_table:
        try:
            # Load current table data
            current_df = engine.cache_manager.load_table_snapshot(selected_table)
            
            if current_df is None or current_df.empty:
                st.warning(f"No data found for table '{selected_table}'. You can add new records below.")
                # Create empty dataframe with proper schema
                if selected_table == "users":
                    current_df = pd.DataFrame(columns=["id", "name", "email", "age", "city", "active"])
                elif selected_table == "products":
                    current_df = pd.DataFrame(columns=["id", "name", "price", "category", "in_stock", "rating"])
                elif selected_table == "orders":
                    current_df = pd.DataFrame(columns=["order_id", "user_id", "product_id", "quantity", "total", "date", "status"])
            else:
                # Remove metadata columns for editing
                display_df = current_df.drop(columns=['_source_file', '_modified_time'], errors='ignore')
                current_df = display_df.copy()
            
            # Configure column types for data editor
            column_config = {}
            if selected_table == "users":
                column_config = {
                    "id": st.column_config.NumberColumn("ID", help="Unique user ID", min_value=1, step=1),
                    "name": st.column_config.TextColumn("Name", help="Full name", max_chars=100),
                    "email": st.column_config.TextColumn("Email", help="Email address"),
                    "age": st.column_config.NumberColumn("Age", help="Age in years", min_value=1, max_value=120, step=1),
                    "city": st.column_config.TextColumn("City", help="City of residence"),
                    "active": st.column_config.CheckboxColumn("Active", help="User is active")
                }
            elif selected_table == "products":
                column_config = {
                    "id": st.column_config.NumberColumn("ID", help="Product ID", min_value=1, step=1),
                    "name": st.column_config.TextColumn("Name", help="Product name", max_chars=100),
                    "price": st.column_config.NumberColumn("Price", help="Product price", min_value=0.01, step=0.01, format="$%.2f"),
                    "category": st.column_config.TextColumn("Category", help="Product category"),
                    "in_stock": st.column_config.CheckboxColumn("In Stock", help="Available for purchase"),
                    "rating": st.column_config.NumberColumn("Rating", help="Product rating", min_value=0.0, max_value=5.0, step=0.1)
                }
            elif selected_table == "orders":
                column_config = {
                    "order_id": st.column_config.NumberColumn("Order ID", help="Unique order ID", min_value=1, step=1),
                    "user_id": st.column_config.NumberColumn("User ID", help="Customer ID", min_value=1, step=1),
                    "product_id": st.column_config.NumberColumn("Product ID", help="Ordered product ID", min_value=1, step=1),
                    "quantity": st.column_config.NumberColumn("Quantity", help="Number of items", min_value=1, step=1),
                    "total": st.column_config.NumberColumn("Total", help="Order total", min_value=0.01, step=0.01, format="$%.2f"),
                    "date": st.column_config.DateColumn("Date", help="Order date"),
                    "status": st.column_config.SelectboxColumn("Status", help="Order status", 
                                                             options=["pending", "processing", "shipped", "completed", "cancelled"])
                }
            
            # Data editor
            st.write(f"**Edit {selected_table.title()} Table:**")
            edited_df = st.data_editor(
                current_df,
                column_config=column_config,
                num_rows="dynamic",  # Allow adding new rows
                use_container_width=True,
                key=f"data_editor_{selected_table}"
            )
            
            # Show primary key info
            primary_key = engine.config.table_primary_keys.get(selected_table, "id")
            st.info(f"🔑 Primary key: **{primary_key}** (duplicates will be deduplicated automatically)")
            
            # Save changes button
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                if st.button("💾 Save Changes to Parquet File", use_container_width=True):
                    if edited_df.empty:
                        st.warning("No data to save")
                    else:
                        try:
                            # Get the original data to compare against
                            original_df = engine.cache_manager.load_table_snapshot(selected_table)
                            
                            if original_df is not None and not original_df.empty:
                                # Remove metadata columns for comparison
                                original_clean = original_df.drop(columns=['_source_file', '_modified_time'], errors='ignore')
                                
                                # Find only the changed/new rows
                                primary_key = engine.config.table_primary_keys.get(selected_table, "id")
                                
                                if primary_key in edited_df.columns and primary_key in original_clean.columns:
                                    # Create comparison sets for finding differences
                                    # Handle NaN values by filtering them out
                                    edited_keys = set(edited_df[primary_key].dropna().values)
                                    original_keys = set(original_clean[primary_key].dropna().values)
                                    
                                    # Find new records (keys that are in edited but not in original)
                                    new_keys = edited_keys - original_keys
                                    
                                    # Find potentially changed records (keys that exist in both)
                                    existing_keys = edited_keys & original_keys
                                    
                                    changed_rows = []
                                    
                                    # Add all new records
                                    if new_keys:
                                        new_rows = edited_df[edited_df[primary_key].isin(new_keys)]
                                        changed_rows.append(new_rows)
                                        st.info(f"🆕 Found {len(new_rows)} new records: {list(new_keys)}")
                                    
                                    # Also check for rows with NaN/null primary keys (completely new rows from data_editor)
                                    null_key_rows = edited_df[edited_df[primary_key].isna()]
                                    if not null_key_rows.empty:
                                        changed_rows.append(null_key_rows)
                                        st.info(f"🆕 Found {len(null_key_rows)} new records with missing primary keys")
                                    
                                    # Check existing records for changes with robust comparison
                                    modified_count = 0
                                    for key in existing_keys:
                                        edited_row = edited_df[edited_df[primary_key] == key].iloc[0]
                                        original_row = original_clean[original_clean[primary_key] == key].iloc[0]
                                        
                                        # More robust comparison that handles data type differences
                                        row_changed = False
                                        for col in edited_df.columns:
                                            if col != primary_key and col in original_clean.columns:
                                                # Normalize values for comparison
                                                edited_val = edited_row[col]
                                                original_val = original_row[col]
                                                
                                                # Handle NaN values
                                                if pd.isna(edited_val) and pd.isna(original_val):
                                                    continue  # Both NaN, no change
                                                elif pd.isna(edited_val) or pd.isna(original_val):
                                                    row_changed = True  # One NaN, one not
                                                    break
                                                
                                                # Normalize string representation for comparison
                                                edited_str = str(edited_val).strip().lower()
                                                original_str = str(original_val).strip().lower()
                                                
                                                # For boolean values, normalize
                                                if edited_str in ['true', 'false'] and original_str in ['true', 'false']:
                                                    if edited_str != original_str:
                                                        row_changed = True
                                                        break
                                                # For numeric values, try numeric comparison
                                                elif edited_str != original_str:
                                                    try:
                                                        if float(edited_val) != float(original_val):
                                                            row_changed = True
                                                            break
                                                    except (ValueError, TypeError):
                                                        # If not numeric, use string comparison
                                                        if edited_str != original_str:
                                                            row_changed = True
                                                            break
                                        
                                        if row_changed:
                                            changed_rows.append(edited_df[edited_df[primary_key] == key])
                                            modified_count += 1
                                            st.info(f"🔄 Record {key} was modified in column(s)")
                                        # (Record unchanged - no output needed for cleaner UI)
                                    
                                    if modified_count > 0:
                                        st.info(f"✏️ Found {modified_count} modified records")
                                    
                                    if changed_rows:
                                        # Combine all changed rows
                                        changes_df = pd.concat(changed_rows, ignore_index=True)
                                    else:
                                        st.info("ℹ️ No changes detected - nothing to save")
                                        return
                                else:
                                    # Fallback: if primary key comparison fails, save all data
                                    st.warning("⚠️ Cannot determine changes, saving all data")
                                    changes_df = edited_df.copy()
                            else:
                                # No original data, save everything as new
                                st.info("🆕 No existing data found, saving all records as new")
                                changes_df = edited_df.copy()
                            
                            # Apply proper data types to the changes with error handling
                            try:
                                if selected_table == "users":
                                    if "id" in changes_df.columns:
                                        changes_df["id"] = pd.to_numeric(changes_df["id"], errors='coerce').astype("int32")
                                    if "age" in changes_df.columns:
                                        changes_df["age"] = pd.to_numeric(changes_df["age"], errors='coerce').astype("int32")
                                    if "active" in changes_df.columns:
                                        changes_df["active"] = changes_df["active"].astype("bool")
                                elif selected_table == "products":
                                    if "id" in changes_df.columns:
                                        changes_df["id"] = pd.to_numeric(changes_df["id"], errors='coerce').astype("int32")
                                    if "price" in changes_df.columns:
                                        changes_df["price"] = pd.to_numeric(changes_df["price"], errors='coerce').astype("float64")
                                    if "in_stock" in changes_df.columns:
                                        changes_df["in_stock"] = changes_df["in_stock"].astype("bool")
                                    if "rating" in changes_df.columns:
                                        changes_df["rating"] = pd.to_numeric(changes_df["rating"], errors='coerce').astype("float32")
                                elif selected_table == "orders":
                                    if "order_id" in changes_df.columns:
                                        changes_df["order_id"] = pd.to_numeric(changes_df["order_id"], errors='coerce').astype("int32")
                                    if "user_id" in changes_df.columns:
                                        changes_df["user_id"] = pd.to_numeric(changes_df["user_id"], errors='coerce').astype("int32")
                                    if "product_id" in changes_df.columns:
                                        changes_df["product_id"] = pd.to_numeric(changes_df["product_id"], errors='coerce').astype("int32")
                                    if "quantity" in changes_df.columns:
                                        changes_df["quantity"] = pd.to_numeric(changes_df["quantity"], errors='coerce').astype("int32")
                                    if "total" in changes_df.columns:
                                        changes_df["total"] = pd.to_numeric(changes_df["total"], errors='coerce').astype("float64")
                                    if "date" in changes_df.columns:
                                        changes_df["date"] = pd.to_datetime(changes_df["date"], errors='coerce')
                                
                                # Show data types and records for debugging
                                st.write(f"📋 Data types after conversion: {dict(changes_df.dtypes)}")
                                st.write(f"📊 Records to save: {len(changes_df)} rows")
                                
                                # Show which specific records are being saved
                                if primary_key in changes_df.columns:
                                    saved_keys = changes_df[primary_key].tolist()
                                    st.write(f"🔑 Primary keys being saved: {saved_keys}")
                                
                                # Show preview of records being saved
                                if len(changes_df) <= 5:
                                    st.write("📋 Preview of records to save:")
                                    st.dataframe(changes_df, use_container_width=True)
                                
                            except Exception as type_error:
                                st.error(f"⚠️ Data type conversion error: {type_error}")
                                st.write("Using original data types...")
                                # Continue with original types
                            
                            # Only write the changed/new records
                            records = changes_df.to_dict('records')
                            success = engine.file_monitor.create_table_update(selected_table, records)
                            
                            if success:
                                st.success(f"✅ Successfully saved {len(records)} changed/new records to {selected_table} partition file!")
                                st.rerun()  # Refresh to show updated data
                            else:
                                st.error("❌ Failed to save changes")
                                
                        except Exception as e:
                            st.error(f"❌ Error saving changes: {e}")
            
            with col2:
                if st.button("🔄 Refresh Data", use_container_width=True):
                    st.rerun()
                    
            with col3:
                if st.button("🗑️ Clear Table", use_container_width=True):
                    st.warning("⚠️ This would clear all data - not implemented for safety")
            
            # Show data statistics
            if not edited_df.empty:
                st.divider()
                stats_cols = st.columns(4)
                with stats_cols[0]:
                    st.metric("Total Rows", len(edited_df))
                with stats_cols[1]:
                    st.metric("Columns", len(edited_df.columns))
                with stats_cols[2]:
                    if primary_key in edited_df.columns:
                        unique_keys = edited_df[primary_key].nunique()
                        st.metric(f"Unique {primary_key}s", unique_keys)
                with stats_cols[3]:
                    # Check for duplicates
                    if primary_key in edited_df.columns:
                        duplicates = len(edited_df) - edited_df[primary_key].nunique()
                        st.metric("Duplicates", duplicates)
                        if duplicates > 0:
                            st.warning(f"⚠️ {duplicates} duplicate {primary_key}(s) found - latest will be kept")
            
        except Exception as e:
            st.error(f"❌ Error loading table data: {e}")
            
    st.divider()
    
    # Quick actions
    st.write("**Quick Actions:**")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Refresh All Tables", use_container_width=True):
            try:
                success = engine.file_monitor.force_refresh()
                if success:
                    st.success("✅ All tables refreshed from Parquet files")
                else:
                    st.error("❌ Failed to refresh tables")
            except Exception as e:
                st.error(f"❌ Error refreshing: {e}")
    
    with col2:
        if st.button("📊 Show File Info", use_container_width=True):
            try:
                file_info = engine.file_monitor.get_file_info()
                st.json(file_info)
            except Exception as e:
                st.error(f"❌ Error getting file info: {e}")


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


def render_parquet_files():
    """Render parquet files explorer interface"""
    st.subheader("📁 Parquet Files")
    
    engine = get_engine()
    if not engine:
        st.error("FlashDuck engine not available")
        return
    
    st.info("💡 View all Parquet files in the monitored directory with metadata and preview capabilities")
    
    try:
        # Get file information
        file_info = engine.file_monitor.get_file_info()
        
        if "error" in file_info:
            st.error(f"❌ Error getting file info: {file_info['error']}")
            return
        
        # Show directory info
        st.write(f"**Monitored Directory:** `{file_info['directory']}`")
        st.write(f"**File Format:** `{file_info.get('file_format', 'parquet')}`")
        
        files = file_info.get("files", [])
        
        if not files:
            st.warning("No Parquet files found in the monitored directory")
            return
        
        st.write(f"**Found {len(files)} Parquet files:**")
        
        # Create a DataFrame for better display
        files_data = []
        for file in files:
            # Get file size in human readable format
            size_bytes = file.get("size_bytes", 0)
            if size_bytes > 1024 * 1024:
                size_display = f"{size_bytes / (1024 * 1024):.2f} MB"
            elif size_bytes > 1024:
                size_display = f"{size_bytes / 1024:.2f} KB"
            else:
                size_display = f"{size_bytes} bytes"
            
            # Format modified time
            import datetime
            modified_time = file.get("modified_time", 0)
            if modified_time:
                modified_display = datetime.datetime.fromtimestamp(modified_time).strftime("%Y-%m-%d %H:%M:%S")
            else:
                modified_display = "Unknown"
            
            files_data.append({
                "File Name": file.get("name", ""),
                "Size": size_display,
                "Modified": modified_display,
                "Path": file.get("path", "")
            })
        
        files_df = pd.DataFrame(files_data)
        
        # Display files table
        st.dataframe(files_df, use_container_width=True)
        
        st.divider()
        
        # File preview section
        st.write("**File Preview:**")
        
        # File selector for preview
        file_names = [f["name"] for f in files]
        selected_file = st.selectbox(
            "Select a file to preview:",
            file_names,
            key="parquet_file_selector"
        )
        
        if selected_file:
            try:
                # Get table name (remove .parquet extension)
                table_name = selected_file.replace(".parquet", "")
                
                # Load data preview
                preview_df = engine.cache_manager.load_table_snapshot(table_name)
                
                if preview_df is not None and not preview_df.empty:
                    # File details
                    selected_file_info = next(f for f in files if f["name"] == selected_file)
                    
                    # Show file metadata
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Rows", len(preview_df))
                    with col2:
                        st.metric("Columns", len(preview_df.columns))
                    with col3:
                        st.metric("File Size", f"{selected_file_info.get('size_bytes', 0) / 1024:.1f} KB")
                    with col4:
                        primary_key = engine.config.table_primary_keys.get(table_name, "N/A")
                        st.metric("Primary Key", primary_key)
                    
                    # Data preview
                    st.write(f"**Data Preview (first 20 rows):**")
                    preview_data = preview_df.head(20)
                    st.dataframe(preview_data, use_container_width=True)
                    
                    # Column information
                    with st.expander("📊 Column Information", expanded=False):
                        col_info = []
                        for col in preview_df.columns:
                            dtype = str(preview_df[col].dtype)
                            null_count = preview_df[col].isnull().sum()
                            unique_count = preview_df[col].nunique()
                            
                            col_info.append({
                                "Column": col,
                                "Data Type": dtype,
                                "Non-null Count": len(preview_df) - null_count,
                                "Unique Values": unique_count,
                                "Sample Value": str(preview_df[col].iloc[0]) if len(preview_df) > 0 else "N/A"
                            })
                        
                        col_df = pd.DataFrame(col_info)
                        st.dataframe(col_df, use_container_width=True)
                    
                    # Raw parquet file info (if available)
                    with st.expander("🔍 Raw Parquet Metadata", expanded=False):
                        try:
                            import pyarrow.parquet as pq
                            from pathlib import Path
                            
                            file_path = Path(file_info['directory']) / selected_file
                            if file_path.exists():
                                parquet_file = pq.ParquetFile(file_path)
                                metadata = parquet_file.metadata
                                
                                st.write(f"**Schema Version:** {parquet_file.schema_arrow}")
                                st.write(f"**Number of Row Groups:** {metadata.num_row_groups}")
                                st.write(f"**Total Rows:** {metadata.num_rows}")
                                st.write(f"**Serialized Size:** {metadata.serialized_size} bytes")
                                
                                # Column statistics
                                st.write("**Column Statistics:**")
                                for i in range(metadata.num_columns):
                                    col_meta = metadata.row_group(0).column(i)
                                    st.write(f"• {col_meta.path_in_schema}: {col_meta.statistics}")
                            else:
                                st.warning("Parquet file not found for detailed metadata")
                                
                        except Exception as meta_e:
                            st.warning(f"Could not load raw Parquet metadata: {meta_e}")
                
                else:
                    st.warning(f"No data found for file: {selected_file}")
                    
            except Exception as e:
                st.error(f"❌ Error previewing file '{selected_file}': {e}")
        
        st.divider()
        
        # File operations
        st.write("**File Operations:**")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔄 Refresh File List", use_container_width=True):
                st.rerun()
        
        with col2:
            if st.button("📊 Show Directory Stats", use_container_width=True):
                total_size = sum(f.get("size_bytes", 0) for f in files)
                total_size_mb = total_size / (1024 * 1024)
                
                st.success(f"📁 {len(files)} files, {total_size_mb:.2f} MB total")
        
        with col3:
            if st.button("🗂️ Open Directory Info", use_container_width=True):
                st.json(file_info)
                
    except Exception as e:
        st.error(f"❌ Error loading Parquet files: {e}")


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
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📈 Status Overview", 
        "🔍 Data Explorer", 
        "🗃️ SQL Interface", 
        "✏️ Write Operations",
        "🗄️ Redis Cache",
        "📁 Parquet Files"
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
    
    with tab6:
        render_parquet_files()
    
    # Footer
    st.divider()
    st.markdown("""
    <div style='text-align: center; color: #666; font-size: 0.8em;'>
        🦆 FlashDuck v0.1.0 - High-performance data management with DuckDB and Redis
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()

"""
Command line interface for FlashDuck
"""

import json
import click
import sys
from typing import Dict, Any
from .core import FlashDuckEngine
from .config import Config


@click.group()
@click.option('--table', default='default_table', help='Table name')
@click.option('--db-root', default='./shared_db', help='Database root directory')
@click.option('--cache-db', default=None, help='Path to DuckDB cache file')
@click.option('--pending-writes', default=None,
              help='Directory for pending Parquet writes')
@click.option('--verbose', '-v', is_flag=True, help='Verbose logging')
@click.pass_context
def cli(ctx, table, db_root, cache_db, pending_writes, verbose):
    """FlashDuck CLI - High-performance data management with DuckDB"""
    # Create config
    config = Config(
        table_name=table,
        db_root=db_root,
        cache_db_path=cache_db,
        pending_writes_dir=pending_writes,
    )
    
    # Setup context
    ctx.ensure_object(dict)
    ctx.obj['config'] = config
    ctx.obj['verbose'] = verbose


@cli.command()
@click.pass_context
def start(ctx):
    """Start the FlashDuck engine"""
    config = ctx.obj['config']
    
    try:
        with FlashDuckEngine(config) as engine:
            click.echo(f"✅ FlashDuck engine started for table: {config.table_name}")
            click.echo(f"📁 Database root: {config.db_root}")
            click.echo("Press Ctrl+C to stop...")
            
            # Keep running until interrupted
            try:
                import threading
                threading.Event().wait()
            except KeyboardInterrupt:
                click.echo("\n🛑 Stopping engine...")
                
    except Exception as e:
        click.echo(f"❌ Failed to start engine: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument('query')
@click.option('--format', 'output_format', default='table', 
              type=click.Choice(['table', 'json', 'csv']),
              help='Output format')
@click.pass_context
def sql(ctx, query, output_format):
    """Execute SQL query"""
    config = ctx.obj['config']
    
    try:
        engine = FlashDuckEngine(config)
        result = engine.sql(query)
        
        if not result['success']:
            click.echo(f"❌ Query failed: {result['error']}", err=True)
            sys.exit(1)
        
        if output_format == 'json':
            click.echo(json.dumps(result, indent=2))
        elif output_format == 'csv':
            import pandas as pd
            df = pd.DataFrame(result['data'])
            click.echo(df.to_csv(index=False))
        else:  # table format
            import pandas as pd
            if result['data']:
                df = pd.DataFrame(result['data'])
                click.echo(df.to_string(index=False))
            else:
                click.echo("No data returned")
                
        if ctx.obj['verbose']:
            click.echo(f"\n📊 Returned {result['rows']} rows", err=True)
            
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def status(ctx):
    """Show engine status"""
    config = ctx.obj['config']
    
    try:
        engine = FlashDuckEngine(config)
        status = engine.get_status()
        
        # Format status output
        click.echo("🔍 FlashDuck Status")
        click.echo("=" * 50)
        
        # Engine status
        engine_status = status.get('engine', {})
        click.echo(f"Engine Running: {'✅' if engine_status.get('running') else '❌'}")
        click.echo(f"Table Name: {engine_status.get('table_name', 'N/A')}")
        click.echo(f"DB Root: {engine_status.get('db_root', 'N/A')}")

        # Cache status
        cache_status = status.get('cache', {})
        if cache_status:
            click.echo(f"DuckDB Connected: {'✅' if cache_status.get('connected') else '❌'}")
            click.echo(f"Cache Rows: {cache_status.get('rows', 0):,}")
            click.echo(f"Cache Columns: {cache_status.get('columns', 0)}")
            click.echo(f"Cache Size: {cache_status.get('size_bytes', 0):,} bytes")

        # Pending writes status
        pending_status = status.get('pending_writes', {})
        if pending_status:
            click.echo(f"Pending Writes: {pending_status.get('count', 0)}")

        # File status
        file_status = status.get('files', {})
        click.echo(f"Files Monitored: {file_status.get('file_count', 0)}")
        
        # Parquet status
        parquet_status = status.get('parquet', {})
        if parquet_status.get('exists'):
            click.echo(f"Parquet Size: {parquet_status.get('size_bytes', 0):,} bytes")
            click.echo(f"Parquet Rows: {parquet_status.get('rows', 0):,}")
        
        if ctx.obj['verbose']:
            click.echo("\nDetailed Status:")
            click.echo(json.dumps(status, indent=2))
            
    except Exception as e:
        click.echo(f"❌ Error getting status: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument('record_id')
@click.argument('data')
@click.pass_context
def upsert(ctx, record_id, data):
    """Enqueue upsert operation"""
    config = ctx.obj['config']
    
    try:
        # Parse JSON data
        try:
            data_dict = json.loads(data)
        except json.JSONDecodeError as e:
            click.echo(f"❌ Invalid JSON data: {e}", err=True)
            sys.exit(1)
        
        engine = FlashDuckEngine(config)
        file_path = engine.enqueue_upsert(record_id, data_dict)

        click.echo(f"✅ Upsert written to: {file_path}")
        click.echo(f"Record ID: {record_id}")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument('record_id')
@click.pass_context
def delete(ctx, record_id):
    """Enqueue delete operation"""
    config = ctx.obj['config']
    
    try:
        engine = FlashDuckEngine(config)
        file_path = engine.enqueue_delete(record_id)

        click.echo(f"✅ Delete written to: {file_path}")
        click.echo(f"Record ID: {record_id}")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def refresh(ctx):
    """Force refresh cache from files"""
    config = ctx.obj['config']
    
    try:
        engine = FlashDuckEngine(config)
        success = engine.force_refresh()
        
        if success:
            click.echo("✅ Cache refreshed successfully")
            
            # Show updated info
            info = engine.get_table_info()
            if info.get('exists'):
                click.echo(f"📊 Loaded {info['rows']:,} rows, {info['columns']} columns")
        else:
            click.echo("❌ Failed to refresh cache", err=True)
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def sample(ctx):
    """Show sample data"""
    config = ctx.obj['config']
    
    try:
        engine = FlashDuckEngine(config)
        result = engine.get_sample_data(10)
        
        if not result['success']:
            click.echo(f"❌ Failed to get sample data: {result['error']}", err=True)
            sys.exit(1)
        
        if result['data']:
            import pandas as pd
            df = pd.DataFrame(result['data'])
            click.echo("📋 Sample Data (first 10 rows):")
            click.echo(df.to_string(index=False))
        else:
            click.echo("📭 No data available")
            
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def export(ctx):
    """Export current cache to Parquet file"""
    config = ctx.obj['config']
    
    try:
        engine = FlashDuckEngine(config)
        success = engine.write_parquet()
        
        if success:
            parquet_info = engine.parquet_writer.get_parquet_info()
            click.echo("✅ Parquet file exported successfully")
            click.echo(f"📄 File: {parquet_info['path']}")
            if 'size_bytes' in parquet_info:
                click.echo(f"📊 Size: {parquet_info['size_bytes']:,} bytes")
        else:
            click.echo("❌ Failed to export Parquet file", err=True)
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli()

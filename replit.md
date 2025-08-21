# FlashDuck Documentation

## Overview

FlashDuck is a high-performance data management system that combines DuckDB's analytical SQL capabilities with Redis caching for real-time data access. The system monitors file system changes in a shared directory, maintains synchronized Redis cache snapshots, and provides SQL querying through DuckDB while generating consolidated Parquet exports for persistence.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Core Components

**Engine Orchestration**: The FlashDuckEngine serves as the main coordinator, managing background threads for file monitoring, cache updates, and Parquet generation. It implements a pub/sub pattern where file changes trigger cache updates, which in turn trigger Parquet writes.

**File System Monitoring**: A background FileMonitor continuously scans the source directory for file changes (creates, deletes, renames) and rebuilds the complete dataset snapshot when changes are detected. This avoids expensive file-by-file watching and instead relies on periodic scans with filename comparison.

**Multi-Format Cache Strategy**: The CacheManager stores complete table snapshots in Redis using configurable formats (Arrow IPC, Parquet bytes, or JSON) optimized for different use cases. Arrow IPC is preferred for fast DuckDB ingestion with minimal serialization overhead.

**Query Processing**: The QueryEngine loads cached snapshots directly into DuckDB for SQL execution, bypassing disk I/O entirely. It validates SQL queries to ensure read-only operations and supports multiple output formats (JSON, CSV, Arrow).

**Consolidated Export**: The ParquetWriter generates single consolidated Parquet files from cache snapshots with atomic writes, compression options, and optional debouncing to prevent excessive writes during rapid changes.

### Data Flow Architecture

**Source of Truth**: Raw data files in a shared directory serve as the authoritative source, allowing manual file management while providing automated synchronization.

**Cache-First Reads**: All queries execute against Redis-cached data rather than scanning individual files, providing consistent sub-millisecond response times regardless of dataset size.

**Write Queue Processing**: Future write operations will use Redis Streams for queuing upsert/delete operations, which are then applied to source files and trigger cache refreshes.

**Schema Evolution**: The system supports union-based schema evolution, automatically merging schemas from heterogeneous files and padding missing columns with nulls.

### Deployment Patterns

**Standalone Mode**: Single-process deployment with all components running in background threads, suitable for development and small-scale production.

**Component Separation**: Individual components can be deployed separately for horizontal scaling, with shared Redis coordination and configurable polling intervals.

**CLI and Web Interfaces**: Both command-line tools and Streamlit web interface provide operational access, with the web interface offering real-time monitoring and interactive querying.

## External Dependencies

**Redis**: Primary caching layer storing table snapshots in binary format (Arrow IPC/Parquet). Requires Redis 4.0+ for binary string support and Redis Streams for write queuing.

**DuckDB**: SQL query execution engine providing OLAP capabilities on cached data. Uses in-memory tables created from Arrow/Parquet data without persistent storage.

**Apache Arrow/PyArrow**: High-performance columnar data format for efficient serialization between Redis cache and DuckDB. Provides zero-copy operations and cross-language compatibility.

**Pandas**: Data manipulation and DataFrame operations for schema merging, file loading, and data transformation between formats.

**Streamlit**: Web-based demo interface with real-time monitoring, query execution, and data visualization capabilities.

**Click**: Command-line interface framework providing operational commands for engine management, data querying, and write operations.
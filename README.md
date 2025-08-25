# 🚀 FlashDuck: Real-Time Data Sync & Query Engine
## ⚡ Flash-fast sync. Duck-simple queries.

![FlashDuck Logo](https://via.placeholder.com/200x100/FF6B6B/FFFFFF?text=FlashDuck)

**High-performance data management powered by DuckDB**

FlashDuck is a lightweight data management solution built on DuckDB. It keeps a local DuckDB cache synchronized with source files so you can query fresh data instantly—perfect for real-time applications and analytics.

Real-time sync: Folder-based or application-level changes are captured and reflected in the DuckDB cache.

Single-file Parquet consolidation: Updates are automatically consolidated into a clean Parquet file for durability and downstream use.

Embedded queries: DuckDB runs SQL directly against the cached tables for sub-second analytics without batch ETL.

Lightweight & embeddable: Runs anywhere—no external services or clusters required.

Flexible integration: Use it as a cache, an analytics engine, or both. Ingest files, query with standard SQL, export with Parquet.

FlashDuck is designed for developers and analysts who want the simplicity of DuckDB with automatic synchronization—without the overhead of a full data warehouse or lakehouse.

## Requirements

- Python 3.8 or later
- DuckDB 1.0 or later
- Pandas 2.0 or later

## 🚀 Features

- **High-Performance Querying**: Execute SQL queries on cached data using DuckDB
- **Real-time File Monitoring**: Automatic detection and processing of file changes
- **DuckDB Caching**: Persistent local cache with configurable formats
- **Parquet Export**: Consolidated Parquet file generation with compression
- **Schema Evolution**: Flexible handling of changing data structures
- **Write Operations**: Queue-based upsert/delete operations tracked via local Parquet files
- **CLI Interface**: Command-line tools for common operations
- **Streamlit Demo**: Interactive web interface showcasing all features

## 🏗️ Architecture
![Architecture Diagram](https://via.placeholder.com/800x400/FF6B6B/FFFFFF?text=FlashDuck+Architecture+Diagram)

## 📦 Installation

```bash
pip install flashduck
```

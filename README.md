# 🚀 FlashDuck: Real-Time Data Sync & Query Engine
## ⚡ Flash-fast sync. Duck-simple queries.

![FlashDuck Logo](https://via.placeholder.com/200x100/FF6B6B/FFFFFF?text=FlashDuck)

**High-performance data management combining DuckDB and Redis**

FlashDuck is a cutting-edge data management solution that combines the speed of Redis with the analytical power of DuckDB. It allows you to keep your data fresh and query it instantly, making it ideal for real-time applications and analytics.

FlashDuck combines the in-memory speed of Redis with the analytical power of DuckDB, giving you a seamless way to synchronize data and query it instantly.

Real-time sync: Folder-based or application-level changes are streamed into Redis, keeping your cache always current.

Single-file Parquet consolidation: Updates are automatically consolidated into a clean Parquet file for durability and downstream use.

In-memory queries: DuckDB runs SQL directly against Redis-stored snapshots, so you get sub-second analytics without waiting for batch ETL.

Lightweight & embeddable: Runs anywhere—no heavy cluster setup. Perfect for edge, cloud, or local development.

Flexible integration: Use it as a cache, an analytics engine, or both. Ingest via Redis Streams, query with standard SQL, export with Parquet.

FlashDuck is designed for developers, analysts, and teams who want the freshness of Redis and the analytical depth of DuckDB—without the overhead of a full data warehouse or lakehouse.

## 🚀 Features

- **High-Performance Querying**: Execute SQL queries on cached data using DuckDB
- **Real-time File Monitoring**: Automatic detection and processing of file changes
- **Redis Caching**: Lightning-fast in-memory data access with configurable formats
- **Parquet Export**: Consolidated Parquet file generation with compression
- **Schema Evolution**: Flexible handling of changing data structures
- **Write Operations**: Queue-based upsert/delete operations via Redis Streams
- **CLI Interface**: Command-line tools for common operations
- **Streamlit Demo**: Interactive web interface showcasing all features

## 🏗️ Architecture
![Architecture Diagram](https://via.placeholder.com/800x400/FF6B6B/FFFFFF?text=FlashDuck+Architecture+Diagram)

## 📦 Installation

```bash
pip install flashduck
```

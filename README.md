# Python Persistent Key-Value Store

A persistent, network-available Key-Value store implemented in pure Python (Standard Library only), following an LSM-Tree architecture.

## Features

- **LSM-Tree Architecture**: High write throughput.
- **Crash Durability**: Write-Ahead Log (WAL) with `fsync`.
- **Range Queries**: Efficient scanning using sorted in-memory structures and SSTables.
- **HTTP API**: Simple REST-like interface.

## Requirements

- Python 3.7+ (No external pip packages)

## Quick Start

### Start Server

```bash
export PYTHONPATH=$PYTHONPATH:.
python3 app/server.py
```

### Usage

**Put**

```bash
curl -X POST http://localhost:8080/put -d '{"key": "foo", "value": "bar"}'
```

**Get**

```bash
curl "http://localhost:8080/get?key=foo"
```

**Range Scan**

```bash
curl "http://localhost:8080/range?start=a&end=z"
```

## Architecture

1. **Memtable**: Uses `dict` for O(1) lookups and `bisect` for sorted range scans.
2. **WAL**: Append-only log for durability.
3. **SSTables**: Flushes sorted data to disk when Memtable fills up.
4. **Compaction**: (Size-tiered - simplified MVP implementation)

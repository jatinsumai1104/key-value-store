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

**1. Put (Insert/Update)**

```bash
curl -X POST http://localhost:8080/put \
     -d '{"key": "user:1", "value": "Alice"}'
```

**2. Batch Put (High Throughput)**

```bash
curl -X POST http://localhost:8080/batchput \
     -d '{"items": [{"key": "user:2", "value": "Bob"}, {"key": "user:3", "value": "Charlie"}]}'
```

**3. Get (Read)**

```bash
curl "http://localhost:8080/get?key=user:1"
```

**4. Range Scan**
Scan keys between start (inclusive) and end (exclusive).

```bash
curl "http://localhost:8080/range?start=user:&end=user:~&limit=10"
```

**5. Delete**

```bash
curl -X DELETE "http://localhost:8080/delete?key=user:1"
```

## Architecture

1. **Memtable**: Uses `dict` for O(1) lookups and `bisect` for sorted range scans.
2. **WAL**: Append-only log for durability.
3. **SSTables**: Flushes sorted data to disk when Memtable fills up.

## Trade-offs & Limitations

- **No Background Compaction**: To keep the implementation simple, we do not merge old SSTables.
  - **Consequence**: Disk usage increases indefinitely (space amplification).
  - **Consequence**: Read latency increases linearly with the number of updates (read amplification).
- **Single Threaded Writer**: We use a `threading.RLock`, so writes are serialized.
- **In-Memory Index**: We load all keys into RAM. For datasets larger than RAM, a sparse index or Bloom Filters would be required.

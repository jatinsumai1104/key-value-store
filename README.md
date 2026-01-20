# Python Persistent Key-Value Store

A persistent, network-available Key-Value store implemented in pure Python (Standard Library only), following an LSM-Tree architecture with RAFT consensus for distributed replication.

## Features

- **LSM-Tree Architecture**: High write throughput.
- **Crash Durability**: Write-Ahead Log (WAL) with `fsync`.
- **Range Queries**: Efficient scanning using sorted in-memory structures and SSTables.
- **HTTP API**: Simple REST-like interface.
- **RAFT Consensus**: Distributed replication with automatic leader election and crash recovery.

## Requirements

- Python 3.7+ (No external pip packages)

## Quick Start

### Option 1: Standalone Mode (Single Node)

```bash
python3 app/server.py
```

### Option 2: Distributed Cluster (3 Nodes with RAFT)

Use the provided script to start a 3-node cluster:

```bash
./start_cluster.sh
```

This starts nodes on ports 9001, 9002, and 9003. Data is stored in `app/data/node-X/`.

To stop the cluster:

```bash
./start_cluster.sh stop
# If processes hang, force kill:
pkill -9 -f "server.py"
```

**Manual cluster startup:**

```bash
# Terminal 1 - Node 1
python3 app/server.py --node-id node-1 --port 9001 --peers "node-2:localhost:9002,node-3:localhost:9003"

# Terminal 2 - Node 2
python3 app/server.py --node-id node-2 --port 9002 --peers "node-1:localhost:9001,node-3:localhost:9003"

# Terminal 3 - Node 3
python3 app/server.py --node-id node-3 --port 9003 --peers "node-1:localhost:9001,node-2:localhost:9002"
```

### Server Options

| Option | Default | Description |
|--------|---------|-------------|
| `--node-id` | `standalone` | Unique node identifier |
| `--port` | `9001` | HTTP server port |
| `--host` | `localhost` | Server host |
| `--peers` | `""` | Comma-separated peer list (format: `id:host:port`) |
| `--data-dir` | `app/data` | Directory for persistent storage |

## Usage

### Basic Operations

**1. Put (Insert/Update)**

```bash
curl -X POST http://localhost:9001/put \
     -H "Content-Type: application/json" \
     -d '{"key": "user:1", "value": "Alice"}'
```

**2. Batch Put (High Throughput)**

```bash
curl -X POST http://localhost:9001/batchput \
     -H "Content-Type: application/json" \
     -d '{"items": [{"key": "user:2", "value": "Bob"}, {"key": "user:3", "value": "Charlie"}]}'
```

**3. Get (Read)**

```bash
curl "http://localhost:9001/get?key=user:1"
```

**4. Range Scan**

Scan keys between start (inclusive) and end (exclusive).

```bash
curl "http://localhost:9001/range?start=user:&end=user:~&limit=10"
```

**5. Delete**

```bash
curl -X DELETE "http://localhost:9001/delete?key=user:1"
```

### RAFT Status

Check the cluster status and leader information:

```bash
curl "http://localhost:9001/raft/status"
```

Response:
```json
{
  "node_id": "node-1",
  "state": "leader",
  "term": 1,
  "leader_id": "node-1",
  "commit_index": 5,
  "last_applied": 5,
  "log_length": 5
}
```

### Handling Non-Leader Requests

Writes to followers return `503 Service Unavailable` with leader information:

```json
{
  "error": "not leader",
  "leader": "localhost:9001"
}
```

## Architecture

### Storage Layer (LSM-Tree)

1. **Memtable**: Uses `dict` for O(1) lookups and `bisect` for sorted range scans.
2. **WAL**: Append-only log for durability.
3. **SSTables**: Flushes sorted data to disk when Memtable fills up.

### Consensus Layer (RAFT)

1. **Leader Election**: Automatic leader election with randomized timeouts (500-1000ms).
2. **Log Replication**: Leader replicates entries to followers via AppendEntries RPC.
3. **Crash Recovery**: Persists `current_term`, `voted_for`, `commit_index`, and `last_applied`. Replays committed log entries on restart.

## Running Tests

### Standalone KVStore Tests
```bash
python3 app/test/manual_test.py
```
Tests: Basic operations, WAL persistence, range scans, SSTable flush

### RAFT Cluster Tests
```bash
python3 app/test/raft_test.py
```
Tests: Leader election, log replication, crash recovery, leader failover

### Performance Benchmark
```bash
# Standalone benchmark
python3 app/server.py &
python3 app/test/benchmark.py --mode standalone

# Cluster benchmark
./start_cluster.sh
python3 app/test/benchmark.py --mode cluster
./start_cluster.sh stop

# Run both modes
python3 app/test/benchmark.py --mode both --ops 1000
```

Options:
- `--mode`: `standalone`, `cluster`, or `both`
- `--ops`: Number of operations per test (default: 1000)
- `--host`: Server host (default: localhost)
- `--port`: Standalone port (default: 9001)

## Performance Benchmarks

Benchmark results from running 1000 operations per test on a local machine (MacOS, Apple Silicon).

| Metric | Standalone Mode | Cluster Mode (3-node RAFT) |
|--------|-------|-------|
| Write Throughput | 4,089 ops/sec | 82 ops/sec |
| Read Throughput | 4,063 ops/sec | 135 ops/sec |
| Batch Write Throughput | 40,167 items/sec | 7,048 items/sec |
| Write Latency (P99) | 0.52 ms | 14.06 ms |
| Read Latency (P99) | 0.30 ms | 1.55 ms |


Cluster mode shows **~50x lower write throughput** compared to standalone. This is expected due to:

- Cluster writes are slower due to RAFT consensus (majority replication required)
- AppendEntries sent to peers one at a time, not in parallel
- Reads go through leader only for strong consistency
- Batch operations amortize the consensus overhead significantly
- Results vary based on hardware, disk speed, and network conditions

## Trade-offs & Limitations

### Storage Layer

- **No Background Compaction**: Old SSTables are not merged.
  - *Consequence*: Disk usage increases indefinitely (space amplification).
  - *Consequence*: Read latency increases with the number of updates (read amplification).
- **Single Threaded Writer**: Writes are serialized via `threading.RLock`.
- **In-Memory Index**: All keys are loaded into RAM. Large datasets require sparse indices or Bloom filters.

### RAFT Consensus

- **Strong Consistency with Latency Cost**: All reads and writes go through the leader, adding network latency.
- **Frequent Disk Writes**: State is persisted after each commit/apply for durability, which may impact throughput.
- **No Snapshotting**: Log grows unbounded. Production systems need log compaction via snapshots.
- **No Membership Changes**: Cluster topology is fixed at startup. Adding/removing nodes requires restart.
- **No Read Replicas**: Followers cannot serve reads. Could be improved with lease-based reads or read indices.

### Network

- **HTTP/1.1 Transport**: Simple but not optimal for high-throughput RPC. gRPC or custom binary protocol would be faster.
- **No Connection Pooling**: Each RPC creates a new connection.

## Architecture Diagram

### System Overview (3-Node RAFT Cluster)

```
                                    ┌─────────────────┐
                                    │     Client      │
                                    └────────┬────────┘
                                             │ HTTP Request
                                             ▼
                    ┌────────────────────────────────────────────────┐
                    │                 RAFT Cluster                   │
                    │                                                │
                    │   ┌──────────┐  ┌──────────┐  ┌──────────┐     │
                    │   │  Node 1  │  │  Node 2  │  │  Node 3  │     │
                    │   │ (Leader) │◄─┤(Follower)│  │(Follower)│     │
                    │   └────┬─────┘  └────┬─────┘  └────┬─────┘     │
                    │        │             │             │           │
                    │        │ AppendEntries (Replication)           │
                    │        ├─────────────┼─────────────┤           │
                    │        ▼             ▼             ▼           │
                    │   ┌─────────┐   ┌─────────┐   ┌─────────┐      │
                    │   │ KVStore │   │ KVStore │   │ KVStore │      │
                    │   └─────────┘   └─────────┘   └─────────┘      │
                    └────────────────────────────────────────────────┘
```

### Storage Engine (LSM-Tree per Node)

```
    Write Path                              Read Path
    ──────────                              ─────────
        │                                       │
        ▼                                       ▼
   ┌─────────┐                            ┌─────────┐
   │   WAL   │ ◄── fsync (durability)     │ Client  │
   │ (Disk)  │                            │ Request │
   └────┬────┘                            └────┬────┘
        │                                      │
        ▼                                      ▼
   ┌─────────────────────────────┐       ┌─────────────┐
   │         Memtable            │ ◄─────│ 1. Check    │
   │  (In-Memory: Dict + List)   │       │ Memtable    │
   │                             │       └────┬────----┘
   │  ┌───────────────────────┐  │            │ Miss
   │  │ key1: val1            │  │            ▼
   │  │ key2: val2            │  │       ┌──────────────┐
   │  │ key3: val3            │  │       │ 2. Check     │
   │  └───────────────────────┘  │       │ SSTables     │
   └──────────────┬──────────────┘       │ (New→Old)    |
                  │                      └────┬─────────┘
                  │ Flush (when full)          │
                  ▼                            ▼
   ┌─────────────────────────────────────────────────┐
   │                   SSTables (Disk)               │
   │                                                 │
   │  ┌─────────────┐ ┌─────────────┐ ┌───────────┐  │
   │  │ SST-3 (New) │ │   SST-2     │ │ SST-1(Old)│  │
   │  │ .data+.index│ │ .data+.index│ │.data+.index  │
   │  └─────────────┘ └─────────────┘ └───────────┘  │
   └─────────────────────────────────────────────────┘
```

### RAFT State Machine

```
                     ┌─────────────────────────────────────┐
                     │            RAFT Node                │
                     │                                     │
   Vote Request ────►│  ┌─────────────────────────────┐    │
                     │  │      Persistent State       │    │
   AppendEntries ───►│  │  ─────────────────────────  │    │
                     │  │  current_term │ voted_for   │    │
                     │  │  commit_index │ last_applied│    │
                     │  └─────────────────────────────┘    │
                     │                 │                   │
                     │                 ▼                   │
                     │  ┌─────────────────────────────┐    │
                     │  │      RAFT Log (Binary)      │    │
                     │  │  ┌─────┬─────┬─────┬─────┐  │    │
                     │  │  │ E1  │ E2  │ E3  │ ... │  │    │
                     │  │  │T:1  │T:1  │T:2  │     │  │    │
                     │  │  └─────┴─────┴─────┴─────┘  │    │
                     │  └─────────────────────────────┘    │
                     │                 │                   │
                     │                 │ Apply committed   │
                     │                 ▼                   │
                     │  ┌─────────────────────────────┐    │
                     │  │    State Machine (KVStore)  │    │
                     │  └─────────────────────────────┘    │
                     └─────────────────────────────────────┘
```

### Data Flow (Write Operation)

```
Client                Leader                 Followers
  │                     │                       │
  │ PUT {key, value}    │                       │
  ├────────────────────►│                       │
  │                     │                       │
  │               1. Append to RAFT Log         │
  │               2. fsync to disk              │
  │                     │                       │
  │                     │ AppendEntries RPC     │
  │                     ├──────────────────────►│
  │                     │                       │
  │                     │◄── Success (majority) │
  │                     │                       │
  │               3. Mark committed             │
  │               4. Apply to KVStore           │
  │                     │                       │
  │◄── 200 OK ──────────│                       │
  │                     │                       │
```

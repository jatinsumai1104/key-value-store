#!/usr/bin/env python3
"""Performance benchmark for KV Store (standalone and cluster modes)"""

import http.client
import json
import time
import statistics
import argparse
import sys
import random
import string

# Test configs
STANDALONE_PORT = 9001
CLUSTER_PORTS = [9001, 9002, 9003]
NUM_OPS = 1000
BATCH_SIZE = 100
NUM_BATCHES = 10
NUM_RANGE_QUERIES = 10

# Timeout settings
STANDALONE_TIMEOUT = 5.0
CLUSTER_TIMEOUT = 30.0  # Longer timeout for RAFT consensus

# Global timeout (set based on mode)
REQUEST_TIMEOUT = STANDALONE_TIMEOUT


def http_get(host: str, port: int, path: str, timeout: float = None) -> tuple[int, dict]:
    """Make GET request, return (status_code, response_dict)"""
    if timeout is None:
        timeout = REQUEST_TIMEOUT
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        data = resp.read().decode()
        return resp.status, json.loads(data) if data else {}
    except Exception as e:
        return 0, {"error": str(e)}
    finally:
        conn.close()


def http_post(host: str, port: int, path: str, body: dict, timeout: float = None) -> tuple[int, dict]:
    """Make POST request, return (status_code, response_dict)"""
    if timeout is None:
        timeout = REQUEST_TIMEOUT
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        conn.request("POST", path, json.dumps(body), {"Content-Type": "application/json"})
        resp = conn.getresponse()
        data = resp.read().decode()
        return resp.status, json.loads(data) if data else {}
    except Exception as e:
        return 0, {"error": str(e)}
    finally:
        conn.close()


def find_leader(ports: list[int], host: str = "localhost") -> int | None:
    """Find the leader node in cluster mode"""
    for port in ports:
        try:
            status, data = http_get(host, port, "/raft/status", timeout=2.0)
            if status == 200 and data.get("state") == "leader":
                return port
        except:
            continue
    return None


def wait_for_leader(ports: list[int], host: str = "localhost", max_wait: float = 10.0) -> int | None:
    """Wait for leader election to complete"""
    start = time.time()
    while time.time() - start < max_wait:
        leader = find_leader(ports, host)
        if leader:
            return leader
        time.sleep(0.5)
    return None


def random_key(prefix: str = "bench") -> str:
    """Generate random key"""
    return f"{prefix}:{random.randint(0, 999999):06d}"


def random_value(size: int = 100) -> str:
    """Generate random value of given size"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))


def benchmark_writes(host: str, port: int, num_ops: int) -> dict:
    """Benchmark sequential PUT operations"""
    latencies = []
    success = 0

    for i in range(num_ops):
        key = f"bench:write:{i:06d}"
        value = random_value(100)

        start = time.perf_counter()
        status, _ = http_post(host, port, "/put", {"key": key, "value": value})
        elapsed = time.perf_counter() - start

        if status == 200:
            success += 1
            latencies.append(elapsed * 1000)  # Convert to ms

    if not latencies:
        return {"error": "No successful operations"}

    return {
        "ops": num_ops,
        "success": success,
        "throughput": success / sum(latencies) * 1000 if latencies else 0,
        "avg_latency_ms": statistics.mean(latencies),
        "p50_latency_ms": statistics.median(latencies),
        "p99_latency_ms": statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else max(latencies),
        "min_latency_ms": min(latencies),
        "max_latency_ms": max(latencies),
    }


def benchmark_reads(host: str, port: int, num_ops: int) -> dict:
    """Benchmark sequential GET operations"""
    # First, write keys to read
    for i in range(num_ops):
        key = f"bench:read:{i:06d}"
        http_post(host, port, "/put", {"key": key, "value": f"value{i}"})

    latencies = []
    success = 0

    for i in range(num_ops):
        key = f"bench:read:{i:06d}"

        start = time.perf_counter()
        status, data = http_get(host, port, f"/get?key={key}")
        elapsed = time.perf_counter() - start

        if status == 200 and data.get("value"):
            success += 1
            latencies.append(elapsed * 1000)

    if not latencies:
        return {"error": "No successful operations"}

    return {
        "ops": num_ops,
        "success": success,
        "throughput": success / sum(latencies) * 1000 if latencies else 0,
        "avg_latency_ms": statistics.mean(latencies),
        "p50_latency_ms": statistics.median(latencies),
        "p99_latency_ms": statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else max(latencies),
        "min_latency_ms": min(latencies),
        "max_latency_ms": max(latencies),
    }


def benchmark_batch_writes(host: str, port: int, num_batches: int, batch_size: int) -> dict:
    """Benchmark batch PUT operations"""
    latencies = []
    success = 0
    total_items = 0

    for b in range(num_batches):
        items = [
            {"key": f"bench:batch:{b}:{i:06d}", "value": random_value(100)}
            for i in range(batch_size)
        ]

        start = time.perf_counter()
        status, data = http_post(host, port, "/batchput", {"items": items})
        elapsed = time.perf_counter() - start

        if status == 200:
            success += 1
            total_items += batch_size
            latencies.append(elapsed * 1000)

    if not latencies:
        return {"error": "No successful operations"}

    total_time_sec = sum(latencies) / 1000

    return {
        "batches": num_batches,
        "batch_size": batch_size,
        "total_items": total_items,
        "success_batches": success,
        "batch_throughput": success / total_time_sec if total_time_sec > 0 else 0,
        "item_throughput": total_items / total_time_sec if total_time_sec > 0 else 0,
        "avg_latency_ms": statistics.mean(latencies),
        "p99_latency_ms": statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else max(latencies),
    }


def benchmark_range_queries(host: str, port: int, num_queries: int) -> dict:
    """Benchmark range scan operations"""
    # First, write some keys for range queries
    for i in range(100):
        http_post(host, port, "/put", {"key": f"range:{i:03d}", "value": f"value{i}"})

    latencies = []
    success = 0
    total_items = 0

    for _ in range(num_queries):
        start = time.perf_counter()
        status, data = http_get(host, port, "/range?start=range:&end=range:~&limit=100")
        elapsed = time.perf_counter() - start

        if status == 200:
            success += 1
            total_items += data.get("count", 0)
            latencies.append(elapsed * 1000)

    if not latencies:
        return {"error": "No successful operations"}

    return {
        "queries": num_queries,
        "success": success,
        "avg_items_returned": total_items / success if success > 0 else 0,
        "throughput": success / sum(latencies) * 1000 if latencies else 0,
        "avg_latency_ms": statistics.mean(latencies),
        "p99_latency_ms": statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else max(latencies),
    }


def print_result(name: str, result: dict):
    """Print benchmark result in formatted way"""
    print(f"\n{'='*60}")
    print(f" {name}")
    print(f"{'='*60}")

    if "error" in result:
        print(f"  ERROR: {result['error']}")
        return

    for key, value in result.items():
        if isinstance(value, float):
            print(f"  {key:.<30} {value:>12.2f}")
        else:
            print(f"  {key:.<30} {value:>12}")


def print_summary_table(results: dict):
    """Print summary table for README"""
    print("\n")
    print("## Benchmark Results Summary")
    print()
    print("| Metric | Standalone | Cluster (3-node) |")
    print("|--------|------------|------------------|")

    standalone = results.get("standalone", {})
    cluster = results.get("cluster", {})

    # Write throughput
    sw = standalone.get("writes", {}).get("throughput", "-")
    cw = cluster.get("writes", {}).get("throughput", "-")
    sw_str = f"{sw:.0f} ops/sec" if isinstance(sw, float) else sw
    cw_str = f"{cw:.0f} ops/sec" if isinstance(cw, float) else cw
    print(f"| Write Throughput | {sw_str} | {cw_str} |")

    # Read throughput
    sr = standalone.get("reads", {}).get("throughput", "-")
    cr = cluster.get("reads", {}).get("throughput", "-")
    sr_str = f"{sr:.0f} ops/sec" if isinstance(sr, float) else sr
    cr_str = f"{cr:.0f} ops/sec" if isinstance(cr, float) else cr
    print(f"| Read Throughput | {sr_str} | {cr_str} |")

    # Batch write throughput
    sb = standalone.get("batch_writes", {}).get("item_throughput", "-")
    cb = cluster.get("batch_writes", {}).get("item_throughput", "-")
    sb_str = f"{sb:.0f} items/sec" if isinstance(sb, float) else sb
    cb_str = f"{cb:.0f} items/sec" if isinstance(cb, float) else cb
    print(f"| Batch Write Throughput | {sb_str} | {cb_str} |")

    # Write latency P99
    swl = standalone.get("writes", {}).get("p99_latency_ms", "-")
    cwl = cluster.get("writes", {}).get("p99_latency_ms", "-")
    swl_str = f"{swl:.2f} ms" if isinstance(swl, float) else swl
    cwl_str = f"{cwl:.2f} ms" if isinstance(cwl, float) else cwl
    print(f"| Write Latency (P99) | {swl_str} | {cwl_str} |")

    # Read latency P99
    srl = standalone.get("reads", {}).get("p99_latency_ms", "-")
    crl = cluster.get("reads", {}).get("p99_latency_ms", "-")
    srl_str = f"{srl:.2f} ms" if isinstance(srl, float) else srl
    crl_str = f"{crl:.2f} ms" if isinstance(crl, float) else crl
    print(f"| Read Latency (P99) | {srl_str} | {crl_str} |")


def benchmark_standalone(host: str = "localhost", port: int = STANDALONE_PORT):
    """Run all benchmarks in standalone mode"""
    global REQUEST_TIMEOUT
    REQUEST_TIMEOUT = STANDALONE_TIMEOUT

    print("\n" + "="*60)
    print(" STANDALONE MODE BENCHMARK")
    print("="*60)

    # Check if server is running
    status, data = http_get(host, port, "/raft/status")
    if status != 200:
        print(f"ERROR: Cannot connect to server at {host}:{port}")
        print("Please start the server first: python3 app/server.py")
        return None

    print(f"Connected to server at {host}:{port}")
    print(f"Running {NUM_OPS} operations per test...")

    results = {}

    # Sequential writes
    print("\nRunning write benchmark...")
    results["writes"] = benchmark_writes(host, port, NUM_OPS)
    print_result("Sequential Writes", results["writes"])

    # Sequential reads
    print("\nRunning read benchmark...")
    results["reads"] = benchmark_reads(host, port, NUM_OPS)
    print_result("Sequential Reads", results["reads"])

    # Batch writes
    print("\nRunning batch write benchmark...")
    results["batch_writes"] = benchmark_batch_writes(host, port, NUM_BATCHES, BATCH_SIZE)
    print_result("Batch Writes", results["batch_writes"])

    # Range queries
    print("\nRunning range query benchmark...")
    results["range"] = benchmark_range_queries(host, port, NUM_RANGE_QUERIES)
    print_result("Range Queries", results["range"])

    return results


def benchmark_cluster(host: str = "localhost", ports: list[int] = None):
    """Run all benchmarks in cluster mode"""
    global REQUEST_TIMEOUT
    REQUEST_TIMEOUT = CLUSTER_TIMEOUT

    if ports is None:
        ports = CLUSTER_PORTS

    print("\n" + "="*60)
    print(" CLUSTER MODE BENCHMARK (3-node RAFT)")
    print("="*60)

    # Wait for leader
    print("Waiting for leader election...")
    leader_port = wait_for_leader(ports, host, max_wait=15.0)

    if not leader_port:
        print("ERROR: No leader found. Please start the cluster first:")
        print("  ./start_cluster.sh")
        return None

    print(f"Leader found at {host}:{leader_port}")

    # Wait for cluster to stabilize (let heartbeats establish leadership)
    print("Waiting for cluster to stabilize...")
    time.sleep(2.0)

    # Verify leader is still the leader after stabilization
    leader_port = wait_for_leader(ports, host, max_wait=5.0)
    if not leader_port:
        print("ERROR: Leader changed during stabilization. Cluster may be unstable.")
        return None

    print(f"Cluster stable. Leader at {host}:{leader_port}")
    print(f"Running {NUM_OPS} operations per test (timeout: {REQUEST_TIMEOUT}s)...")

    results = {}

    # Sequential writes (with RAFT replication)
    print("\nRunning write benchmark (with replication)...")
    results["writes"] = benchmark_writes(host, leader_port, NUM_OPS)
    print_result("Sequential Writes (Replicated)", results["writes"])

    # Sequential reads
    print("\nRunning read benchmark...")
    results["reads"] = benchmark_reads(host, leader_port, NUM_OPS)
    print_result("Sequential Reads", results["reads"])

    # Batch writes
    print("\nRunning batch write benchmark...")
    results["batch_writes"] = benchmark_batch_writes(host, leader_port, NUM_BATCHES, BATCH_SIZE)
    print_result("Batch Writes (Replicated)", results["batch_writes"])

    # Range queries
    print("\nRunning range query benchmark...")
    results["range"] = benchmark_range_queries(host, leader_port, NUM_RANGE_QUERIES)
    print_result("Range Queries", results["range"])

    return results


def main():
    global NUM_OPS

    parser = argparse.ArgumentParser(description="KV Store Performance Benchmark")
    parser.add_argument("--mode", choices=["standalone", "cluster", "both"],
                        default="standalone", help="Benchmark mode")
    parser.add_argument("--host", default="localhost", help="Server host")
    parser.add_argument("--port", type=int, default=STANDALONE_PORT, help="Standalone port")
    parser.add_argument("--ops", type=int, default=NUM_OPS, help="Number of operations")
    args = parser.parse_args()

    NUM_OPS = args.ops

    results = {}

    if args.mode in ["standalone", "both"]:
        results["standalone"] = benchmark_standalone(args.host, args.port)

    if args.mode in ["cluster", "both"]:
        results["cluster"] = benchmark_cluster(args.host)

    # Print summary
    # if args.mode == "both" and results.get("standalone") and results.get("cluster"):
    print_summary_table(results)

    print("\nBenchmark complete!")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
RAFT Manual Test Script

Tests the RAFT consensus implementation with a 3-node cluster.
Uses only Python standard libraries.
"""

import http.client
import json
import os
import shutil
import signal
import subprocess
import sys
import time

# Test configuration
PORTS = [9081, 9082, 9083]
NODE_IDS = ["node-1", "node-2", "node-3"]
TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "test/data")
SERVER_SCRIPT = os.path.join(os.path.dirname(os.path.dirname(__file__)), "server.py")

# ANSI color codes
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"

# Global process list
processes = []


def print_pass(msg):
    print(f"{GREEN}[PASS]{RESET} {msg}")


def print_fail(msg):
    print(f"{RED}[FAIL]{RESET} {msg}")


def print_info(msg):
    print(f"{BLUE}[INFO]{RESET} {msg}")


def print_test(msg):
    print(f"\n{YELLOW}{'='*60}{RESET}")
    print(f"{YELLOW}TEST: {msg}{RESET}")
    print(f"{YELLOW}{'='*60}{RESET}")


# ========== HTTP Helpers ==========

def http_get(port, path, timeout=5):
    """Make GET request, return (status_code, response_dict or None)."""
    try:
        conn = http.client.HTTPConnection("localhost", port, timeout=timeout)
        conn.request("GET", path)
        resp = conn.getresponse()
        body = resp.read().decode("utf-8")
        conn.close()
        try:
            data = json.loads(body) if body else None
        except json.JSONDecodeError:
            data = None
        return resp.status, data
    except Exception as e:
        return None, None


def http_post(port, path, body, timeout=5):
    """Make POST request, return (status_code, response_dict or None)."""
    try:
        conn = http.client.HTTPConnection("localhost", port, timeout=timeout)
        headers = {"Content-Type": "application/json"}
        conn.request("POST", path, json.dumps(body), headers)
        resp = conn.getresponse()
        resp_body = resp.read().decode("utf-8")
        conn.close()
        try:
            data = json.loads(resp_body) if resp_body else None
        except json.JSONDecodeError:
            data = None
        return resp.status, data
    except Exception as e:
        return None, None


def http_delete(port, path, timeout=5):
    """Make DELETE request, return (status_code, response_dict or None)."""
    try:
        conn = http.client.HTTPConnection("localhost", port, timeout=timeout)
        conn.request("DELETE", path)
        resp = conn.getresponse()
        body = resp.read().decode("utf-8")
        conn.close()
        try:
            data = json.loads(body) if body else None
        except json.JSONDecodeError:
            data = None
        return resp.status, data
    except Exception as e:
        return None, None


# ========== Cluster Management ==========

def cleanup_data():
    """Remove test data directory."""
    print(TEST_DATA_DIR, )
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR)


def start_cluster():
    """Start a 3-node RAFT cluster."""
    global processes
    processes = []
    cleanup_data()
    os.makedirs(TEST_DATA_DIR, exist_ok=True)

    for i, (node_id, port) in enumerate(zip(NODE_IDS, PORTS)):
        # Build peers string (all nodes except self)
        peers = []
        for j, (other_id, other_port) in enumerate(zip(NODE_IDS, PORTS)):
            if i != j:
                peers.append(f"{other_id}:localhost:{other_port}")
        peers_str = ",".join(peers)

        data_dir = os.path.join(TEST_DATA_DIR, node_id)
        os.makedirs(data_dir, exist_ok=True)

        cmd = [
            sys.executable,
            SERVER_SCRIPT,
            "--node-id", node_id,
            "--port", str(port),
            "--data-dir", data_dir,
            "--peers", peers_str
        ]

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=os.path.dirname(SERVER_SCRIPT)
        )
        processes.append(proc)
        print_info(f"Started {node_id} on port {port} (PID: {proc.pid})")

    # Give servers time to start
    time.sleep(0.5)


def stop_cluster():
    """Stop all cluster nodes."""
    global processes
    for proc in processes:
        try:
            proc.terminate()
            proc.wait(timeout=2)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
    processes = []
    print_info("Cluster stopped")


def stop_node(port):
    """Stop a specific node by port."""
    global processes
    idx = PORTS.index(port)
    proc = processes[idx]
    try:
        proc.terminate()
        proc.wait(timeout=2)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
    print_info(f"Stopped node on port {port} (PID: {proc.pid})")


def restart_node(port):
    """Restart a specific node."""
    global processes
    idx = PORTS.index(port)
    node_id = NODE_IDS[idx]

    # Build peers string
    peers = []
    for j, (other_id, other_port) in enumerate(zip(NODE_IDS, PORTS)):
        if idx != j:
            peers.append(f"{other_id}:localhost:{other_port}")
    peers_str = ",".join(peers)

    data_dir = os.path.join(TEST_DATA_DIR, node_id)

    cmd = [
        sys.executable,
        SERVER_SCRIPT,
        "--node-id", node_id,
        "--port", str(port),
        "--data-dir", data_dir,
        "--peers", peers_str
    ]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=os.path.dirname(SERVER_SCRIPT)
    )
    processes[idx] = proc
    print_info(f"Restarted {node_id} on port {port} (PID: {proc.pid})")


def restart_cluster():
    """Restart all nodes without wiping data."""
    global processes
    processes = []

    for i, (node_id, port) in enumerate(zip(NODE_IDS, PORTS)):
        # Build peers string (all nodes except self)
        peers = []
        for j, (other_id, other_port) in enumerate(zip(NODE_IDS, PORTS)):
            if i != j:
                peers.append(f"{other_id}:localhost:{other_port}")
        peers_str = ",".join(peers)

        data_dir = os.path.join(TEST_DATA_DIR, node_id)

        cmd = [
            sys.executable,
            SERVER_SCRIPT,
            "--node-id", node_id,
            "--port", str(port),
            "--data-dir", data_dir,
            "--peers", peers_str
        ]

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=os.path.dirname(SERVER_SCRIPT)
        )
        processes.append(proc)
        print_info(f"Started {node_id} on port {port} (PID: {proc.pid})")


def get_node_status(port):
    """Get RAFT status from a node."""
    status, data = http_get(port, "/raft/status")
    return data


def wait_for_leader(timeout=10):
    """Wait until a leader is elected. Returns leader port or None."""
    start = time.time()
    while time.time() - start < timeout:
        for port in PORTS:
            status = get_node_status(port)
            if status and status.get("state") == "leader":
                return port
        time.sleep(0.2)
    return None


def find_leader():
    """Find the current leader port. Returns (leader_port, leader_id) or (None, None)."""
    for port in PORTS:
        status = get_node_status(port)
        if status and status.get("state") == "leader":
            return port, status.get("node_id")
    return None, None


def find_follower():
    """Find a follower port. Returns port or None."""
    for port in PORTS:
        status = get_node_status(port)
        if status and status.get("state") == "follower":
            return port
    return None


# ========== Test Cases ==========

def test_leader_election():
    """Test 1: Leader Election"""
    print_test("Leader Election")

    start_cluster()
    time.sleep(0.5)

    # Wait for leader
    leader_port = wait_for_leader(timeout=10)
    if leader_port is None:
        print_fail("No leader elected within timeout")
        return False

    print_info(f"Leader elected on port {leader_port}")

    # Verify exactly one leader
    leaders = []
    leader_id = None
    for port in PORTS:
        status = get_node_status(port)
        if status:
            print_info(f"Port {port}: state={status.get('state')}, term={status.get('term')}, leader_id={status.get('leader_id')}")
            if status.get("state") == "leader":
                leaders.append(port)
                leader_id = status.get("node_id")

    if len(leaders) != 1:
        print_fail(f"Expected 1 leader, found {len(leaders)}")
        return False

    print_pass("Exactly one leader exists")

    # Verify all nodes agree on leader
    for port in PORTS:
        status = get_node_status(port)
        if status and status.get("leader_id") != leader_id:
            print_fail(f"Node on port {port} disagrees on leader")
            return False

    print_pass("All nodes agree on the leader")
    return True


def test_basic_operations():
    """Test 2: Basic Operations via Leader"""
    print_test("Basic Operations via Leader")

    leader_port, _ = find_leader()
    if leader_port is None:
        print_fail("No leader found")
        return False

    # PUT
    status, data = http_post(leader_port, "/put", {"key": "test_key1", "value": "test_value1"})
    if status != 200:
        print_fail(f"PUT failed: status={status}, data={data}")
        return False
    print_pass("PUT succeeded")

    # GET
    status, data = http_get(leader_port, "/get?key=test_key1")
    if status != 200 or data.get("value") != "test_value1":
        print_fail(f"GET failed: status={status}, data={data}")
        return False
    print_pass("GET succeeded")

    # BATCHPUT
    items = [
        {"key": "batch_key1", "value": "batch_val1"},
        {"key": "batch_key2", "value": "batch_val2"},
        {"key": "batch_key3", "value": "batch_val3"}
    ]
    status, data = http_post(leader_port, "/batchput", {"items": items})
    if status != 200:
        print_fail(f"BATCHPUT failed: status={status}, data={data}")
        return False
    print_pass("BATCHPUT succeeded")

    # RANGE
    status, data = http_get(leader_port, "/range?start=batch_key1&end=batch_key4")
    if status != 200 or data.get("count", 0) < 3:
        print_fail(f"RANGE failed: status={status}, data={data}")
        return False
    print_pass(f"RANGE succeeded: found {data.get('count')} items")

    # DELETE
    status, data = http_delete(leader_port, "/delete?key=test_key1")
    if status != 200:
        print_fail(f"DELETE failed: status={status}, data={data}")
        return False
    print_pass("DELETE succeeded")

    # Verify DELETE
    status, data = http_get(leader_port, "/get?key=test_key1")
    if status != 404:
        print_fail(f"Key should be deleted: status={status}, data={data}")
        return False
    print_pass("DELETE verified (key not found)")

    return True


def test_leader_forwarding():
    """Test 3: Leader Forwarding (503 on follower write)"""
    print_test("Leader Forwarding")

    follower_port = find_follower()
    if follower_port is None:
        print_fail("No follower found")
        return False

    print_info(f"Attempting write to follower on port {follower_port}")

    # Attempt write to follower
    status, data = http_post(follower_port, "/put", {"key": "follower_test", "value": "should_fail"})

    if status != 503:
        print_fail(f"Expected 503, got {status}")
        return False

    if data is None or "error" not in data:
        print_fail(f"Expected error response, got {data}")
        return False

    print_pass(f"Got 503 response with leader address: {data.get('leader')}")
    return True


def test_log_replication():
    """Test 4: Log Replication"""
    print_test("Log Replication")

    leader_port, _ = find_leader()
    if leader_port is None:
        print_fail("No leader found")
        return False

    # Write some data to leader
    for i in range(5):
        status, _ = http_post(leader_port, "/put", {"key": f"repl_key{i}", "value": f"repl_val{i}"})
        if status != 200:
            print_fail(f"PUT repl_key{i} failed")
            return False

    print_info("Wrote 5 keys to leader")

    # Give time for replication
    time.sleep(1)

    # Check log length and commit_index on all nodes
    statuses = {}
    for port in PORTS:
        status = get_node_status(port)
        if status:
            statuses[port] = status
            print_info(f"Port {port}: log_length={status.get('log_length')}, commit_index={status.get('commit_index')}")

    # Verify all nodes have same log length
    log_lengths = [s.get("log_length", -1) for s in statuses.values()]
    if len(set(log_lengths)) != 1:
        print_fail(f"Log lengths differ: {log_lengths}")
        return False
    print_pass(f"All nodes have same log length: {log_lengths[0]}")

    # Verify commit_index matches (or is very close)
    commit_indices = [s.get("commit_index", -1) for s in statuses.values()]
    if max(commit_indices) - min(commit_indices) > 1:
        print_fail(f"Commit indices differ significantly: {commit_indices}")
        return False
    print_pass(f"Commit indices match: {commit_indices}")

    return True


def test_crash_recovery():
    """Test 5: Crash Recovery"""
    print_test("Crash Recovery")

    leader_port, _ = find_leader()
    if leader_port is None:
        print_fail("No leader found")
        return False

    # Write data before crash
    test_key = "crash_test_key"
    test_value = "crash_test_value"
    status, _ = http_post(leader_port, "/put", {"key": test_key, "value": test_value})
    if status != 200:
        print_fail("PUT before crash failed")
        return False
    print_info(f"Wrote {test_key}={test_value}")

    # Give time for replication
    time.sleep(1)

    # Stop all nodes
    print_info("Stopping all nodes...")
    stop_cluster()
    time.sleep(1)

    # Restart all nodes (without wiping data)
    print_info("Restarting all nodes...")
    restart_cluster()
    time.sleep(1)

    # Wait for new leader
    leader_port = wait_for_leader(timeout=10)
    if leader_port is None:
        print_fail("No leader elected after restart")
        return False
    print_pass(f"New leader elected on port {leader_port}")

    # Verify data persists
    status, data = http_get(leader_port, f"/get?key={test_key}")
    if status != 200 or data.get("value") != test_value:
        print_fail(f"Data not persisted: status={status}, data={data}")
        return False
    print_pass(f"Data persisted: {test_key}={data.get('value')}")

    return True


def test_leader_failover():
    """Test 6: Leader Failover"""
    print_test("Leader Failover")

    leader_port, leader_id = find_leader()
    if leader_port is None:
        print_fail("No leader found")
        return False

    print_info(f"Current leader: {leader_id} on port {leader_port}")

    # Write data before failover
    pre_failover_key = "pre_failover_key"
    pre_failover_value = "pre_failover_value"
    status, _ = http_post(leader_port, "/put", {"key": pre_failover_key, "value": pre_failover_value})
    if status != 200:
        print_fail("PUT before failover failed")
        return False
    print_info(f"Wrote {pre_failover_key}={pre_failover_value}")

    # Give time for replication
    time.sleep(1)

    # Kill the leader
    print_info(f"Killing leader on port {leader_port}...")
    stop_node(leader_port)
    time.sleep(0.5)

    # Wait for new leader (should be elected from remaining nodes)
    remaining_ports = [p for p in PORTS if p != leader_port]

    new_leader_port = None
    start = time.time()
    while time.time() - start < 10:
        for port in remaining_ports:
            status = get_node_status(port)
            if status and status.get("state") == "leader":
                new_leader_port = port
                break
        if new_leader_port:
            break
        time.sleep(0.2)

    if new_leader_port is None:
        print_fail("No new leader elected after failover")
        return False

    new_status = get_node_status(new_leader_port)
    print_pass(f"New leader elected: {new_status.get('node_id')} on port {new_leader_port}")

    # Verify data is still accessible on new leader
    status, data = http_get(new_leader_port, f"/get?key={pre_failover_key}")
    if status != 200 or data.get("value") != pre_failover_value:
        print_fail(f"Data not accessible after failover: status={status}, data={data}")
        return False
    print_pass(f"Pre-failover data accessible: {pre_failover_key}={data.get('value')}")

    # Verify writes work on new leader
    post_failover_key = "post_failover_key"
    post_failover_value = "post_failover_value"
    status, _ = http_post(new_leader_port, "/put", {"key": post_failover_key, "value": post_failover_value})
    if status != 200:
        print_fail("PUT on new leader failed")
        return False
    print_pass(f"Writes work on new leader")

    # Verify read
    status, data = http_get(new_leader_port, f"/get?key={post_failover_key}")
    if status != 200 or data.get("value") != post_failover_value:
        print_fail(f"Read after write failed: status={status}, data={data}")
        return False
    print_pass(f"Read after write succeeded: {post_failover_key}={data.get('value')}")

    return True


# ========== Main ==========

def main():
    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"{BLUE}RAFT Consensus Manual Test Suite{RESET}")
    print(f"{BLUE}{'='*60}{RESET}")

    results = {}

    try:
        # Test 1: Leader Election
        results["Leader Election"] = test_leader_election()

        if results["Leader Election"]:
            # Test 2: Basic Operations (requires running cluster)
            results["Basic Operations"] = test_basic_operations()

            # Test 3: Leader Forwarding
            results["Leader Forwarding"] = test_leader_forwarding()

            # Test 4: Log Replication
            results["Log Replication"] = test_log_replication()

            # Test 5: Crash Recovery
            results["Crash Recovery"] = test_crash_recovery()

            # Test 6: Leader Failover
            results["Leader Failover"] = test_leader_failover()
        else:
            print_info("Skipping remaining tests due to leader election failure")

    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"\n{RED}Error: {e}{RESET}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup
        stop_cluster()
        cleanup_data()

    # Summary
    print(f"\n{BLUE}{'='*60}{RESET}")
    print(f"{BLUE}Test Summary{RESET}")
    print(f"{BLUE}{'='*60}{RESET}")

    passed = 0
    failed = 0
    for name, result in results.items():
        if result:
            print(f"{GREEN}[PASS]{RESET} {name}")
            passed += 1
        else:
            print(f"{RED}[FAIL]{RESET} {name}")
            failed += 1

    print(f"\n{passed} passed, {failed} failed")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

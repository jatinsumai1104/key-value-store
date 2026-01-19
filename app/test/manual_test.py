import os
import sys
import shutil
import time

# Add parent directory to path so we can import 'store'
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from store.kvstore import KVStore

TEST_DATA_DIR = "app/test/data"

def cleanup():
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR)


def test_basic_ops():
    print("--- Test Basic Ops ---")
    store = KVStore(TEST_DATA_DIR, memtable_size_limit=1024)

    print("Put key1=val1")
    store.put("key1", "val1")

    print("Get key1:", store.get("key1"))
    assert store.get("key1") == "val1"

    print("Put key2=val2")
    store.put("key2", "val2")

    print("Delete key1")
    store.delete("key1")

    print("Get key1 (should be None):", store.get("key1"))
    assert store.get("key1") is None

    store.close()
    print("Passed.")


def test_persistence():
    print("\n--- Test Persistence (WAL Recovery) ---")
    # Reopen same store
    store = KVStore(TEST_DATA_DIR)

    print("Get key2 (should be val2):", store.get("key2"))
    assert store.get("key2") == "val2"

    print("Get key1 (should be None, was deleted):", store.get("key1"))
    assert store.get("key1") is None

    store.close()
    print("Passed.")


def test_flushing_and_sstables():
    print("\n--- Test Flushing & SSTables ---")
    # Tiny memtable to force flushing
    store = KVStore(TEST_DATA_DIR, memtable_size_limit=10)

    print("Inserting items to trigger flush...")
    # "a"*10 is 10 bytes.
    store.put("long_key", "a" * 20)  # Should trigger flush?
    # Logic is check AFTER put.
    # memtable size updated.

    store.put("k3", "v3")
    store.put("k4", "v4")

    # Force flush manually to be sure
    store.flush()

    print("Checking if data exists in SSTable...")
    # sstables list should not be empty
    assert len(store.sstables) >= 1
    print(f"SSTables count: {len(store.sstables)}")

    val = store.get("long_key")
    print(f"Get long_key from SSTable: {len(val)} bytes")
    assert val == "a" * 20

    store.close()
    print("Passed.")


def test_range_scan():
    print("\n--- Test Range Scan ---")
    store = KVStore(TEST_DATA_DIR)

    # Clear for clean slate or just add known keys
    # k3, k4, long_key exist.
    # key2 exists.

    print("Scan 'k' to 'l'")
    items = store.range_scan("k", "l")
    print("Items:", items)
    # Should include k3, k4, key2 (if persistent)

    keys = [k for k, v in items]
    assert "k3" in keys
    assert "k4" in keys

    print("Passed.")


if __name__ == "__main__":
    cleanup()
    test_basic_ops()
    test_persistence()
    test_flushing_and_sstables()
    test_range_scan()
    cleanup()

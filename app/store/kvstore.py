import os
import glob
import time
import threading
from .wal import WAL
from .memtable import Memtable
from .sstable import SSTableWriter, SSTableReader, TOMBSTONE


class KVStore:
    """
    Persistent Key-Value Store.
    Architecture: WAL -> Memtable -> SSTables (LSM-Tree)
    """

    def __init__(self, data_dir="app/data", memtable_size_limit=1024 * 1024):
        self.data_dir = data_dir
        self.memtable_size_limit = memtable_size_limit

        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)

        self.wal_path = os.path.join(data_dir, "wal.log")
        self.memtable = Memtable()
        self.wal = WAL(self.wal_path)

        self.sstables = []  # List of SSTableReaders (Newest -> Oldest)
        self.lock = threading.RLock()  # Single writer, safe readers

        # 1. Load SSTables
        self._load_sstables()

        # 2. Replay WAL
        self._replay_wal()

    def _load_sstables(self):
        """Loads existing SSTables from disk."""
        # Pattern: sst-[timestamp].data
        files = glob.glob(os.path.join(self.data_dir, "sst-*.data"))
        files.sort(reverse=True)  # Newest first

        for f in files:
            # format: sst-[timestamp]-[counter].data
            # We just need the base name to find the index
            base_name = os.path.basename(f).replace(".data", "")
            index_path = os.path.join(self.data_dir, f"{base_name}.index")
            try:
                reader = SSTableReader(f, index_path)
                self.sstables.append(reader)
            except Exception as e:
                print(f"Error loading SSTable {f}: {e}")

    def _replay_wal(self):
        """Replays WAL into Memtable."""
        count = 0
        for key, value in self.wal.replay():
            self.memtable.put(key, value)
            count += 1
        print(f"Recovered {count} records from WAL.")

    """
    Within lock: Append to WAL, update Memtable, and check flush.
    """
    def put(self, key: str, value: str):
        with self.lock:
            self.wal.append(key, value, WAL.OP_PUT)
            self.memtable.put(key, value)
            self.check_and_flush()

    def get(self, key: str):
        # OPTIMIZATION: Check Memtable first
        with self.lock:
            val = self.memtable.get(key)
            if val is not None:
                return val  # Found in Memtable

        # Check SSTables (Newest to Oldest)
        with self.lock:
            readers = list(self.sstables)

        for reader in readers:
            val = reader.get(key)
            if val is not None:
                if val is TOMBSTONE:
                    return None  # Key was deleted
                return val  # Found value

        return None

    def delete(self, key: str):
        with self.lock:
            self.wal.append(key, None, WAL.OP_DELETE)
            self.memtable.delete(key)  # Sets value to None
            self.check_and_flush()

    def batch_put(self, items: list):
        """
        items: list of (key, value) tuples
        """
        with self.lock:
            # Optimize: can write one big WAL block? For now, loop.
            for k, v in items:
                self.wal.append(k, v, WAL.OP_PUT)
                self.memtable.put(k, v)

            self.check_and_flush()

    def check_and_flush(self):
        """
        Checks Memtable size and flushes if needed.
        """
        if self.memtable.size_bytes > self.memtable_size_limit:
            self.flush()

    def flush(self):
        """
        Flushes Memtable to a new SSTable.
        """
        # Ensure we have data
        if not self.memtable.data:
            return

        # Ensure unique timestamp even in same ms
        timestamp = int(time.time() * 1000)
        # Check against previous implementation detail: collision caused overwrite.
        # Simple fix: Add uniqueness or sleep (bad).
        # Better: use a counter for this instance.
        if not hasattr(self, "_flush_counter"):
            self._flush_counter = 0
        self._flush_counter += 1

        base_name = f"sst-{timestamp}-{self._flush_counter}"
        data_path = os.path.join(self.data_dir, f"{base_name}.data")
        index_path = os.path.join(self.data_dir, f"{base_name}.index")

        # 1. Get sorted items
        items = self.memtable.flush()

        # 2. Write to disk
        writer = SSTableWriter(data_path, index_path)
        writer.write(items)
        writer.close()

        # 3. Add to active readers (Newest first)
        reader = SSTableReader(data_path, index_path)
        self.sstables.insert(0, reader)

        # 4. Clear WAL
        self.wal.clear()
        print(f"Flushed {len(items)} items to {base_name}")

    def range_scan(self, start_key, end_key):
        """
        Merges results from Memtable and all SSTables.
        """
        # This is a classic "Merge Iterator" problem.
        # For Phase 1 simplicity: Collect all keys, sort, and return valid ones.
        # Not distinct "streaming" iterator, but functional.

        results = {}

        # 1. Scan SSTables (Oldest to Newest, so Newest overwrites)
        # We process in reverse order of self.sstables (which is Newest->Oldest)
        # So: Oldest -> Newest

        with self.lock:
            snapshot_sstables = list(reversed(self.sstables))
            # Scan Memtable
            mem_items = list(self.memtable.scan(start_key, end_key))

        # Iterating SSTables
        for reader in snapshot_sstables:
            for k, v in reader.scan(start_key, end_key):
                if v is TOMBSTONE:
                    results[k] = (
                        None  # Mark as deleted (tombstone wins over older values)
                    )
                else:
                    results[k] = v

        # Apply Memtable on top
        for k, v in mem_items:
            results[k] = v

        # Filter deletes and sort
        final_output = []
        for k in sorted(results.keys()):
            if results[k] is not None:
                final_output.append((k, results[k]))

        return final_output

    def close(self):
        self.flush()
        with self.lock:
            for reader in self.sstables:
                reader.close()

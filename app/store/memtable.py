import bisect


class Memtable:
    """
    In-memory storage engine.
    Uses a Dict for O(1) lookups and a sorted List for O(log N) range queries (via bisect).
    """

    def __init__(self):
        self.data = {}  # HashMap: Key -> Value
        self.sorted_keys = []  # Sorted List of Keys
        self.size_bytes = 0  # Approximate size in bytes

    def put(self, key: str, value: str):
        """Inserts or updates a key."""
        if key not in self.data:
            bisect.insort(self.sorted_keys, key)
            self.size_bytes += len(key)

        # If updating, subtract old value size
        if key in self.data:
            old_val = self.data[key]
            if old_val:  # could be None for tombstone
                self.size_bytes -= len(old_val)

        self.data[key] = value
        if value:
            self.size_bytes += len(value)

    def get(self, key: str):
        """Returns value or None if not found/deleted."""
        return self.data.get(key)

    def delete(self, key: str):
        """Deletes a key (adds tombstone)."""
        self.put(key, None)

    def scan(self, start_key: str, end_key: str):
        """
        Yields (key, value) for keys in [start_key, end_key).
        """
        # Find start index
        idx = bisect.bisect_left(self.sorted_keys, start_key)

        while idx < len(self.sorted_keys):
            key = self.sorted_keys[idx]
            if end_key and key >= end_key:
                break

            val = self.data[key]
            if val is not None:  # Skip tombstones
                yield key, val

            idx += 1

    def flush(self):
        """Returns all data sorted and clears the memtable."""
        # This returns data for flushing to SSTable
        # Since keys are already sorted in sorted_keys, we can just iterate
        snapshot = []
        for k in self.sorted_keys:
            v = self.data[k]
            # When flushing, we might want to keep tombstones to propagate deletes to
            # older SSTables during compaction. For this simplified version, let's keep them.
            snapshot.append((k, v))

        # Reset
        self.data = {}
        self.sorted_keys = []
        self.size_bytes = 0

        return snapshot

TOMBSTONE = object()

import struct
import os
import bisect


class SSTableWriter:
    """
    Writes sorted key-value pairs to disk (SSTable).
    Creates two files:
    - .data: [KeyLen][Key][ValLen][Value]...
    - .index: [KeyLen][Key][Offset]...
    """

    def __init__(self, data_path, index_path):
        self.data_file = open(data_path, "wb")
        self.index_file = open(index_path, "wb")
        self.current_offset = 0

    def write(self, items):
        """
        items: list of (key, value) sorted by key.
        """
        for key, value in items:
            key_bytes = key.encode("utf-8")
            if value is None:
                # Tombstone: ValLen = -1
                val_bytes = b""
                val_len = -1
            else:
                val_bytes = value.encode("utf-8")
                val_len = len(val_bytes)

            # Write to Data File
            # Format: KeyLen(I), Key(s), ValLen(i), Value(s)
            entry = struct.pack(
                f">I{len(key_bytes)}si{len(val_bytes)}s",
                len(key_bytes),
                key_bytes,
                val_len,
                val_bytes,
            )
            self.data_file.write(entry)

            # Write key -> offset to Index File
            # We index EVERY key for simplicity (Dense Index).
            # Optimization: Can do Sparse Index (every Nth key) if RAM is tight.
            # Format: KeyLen(I), Key(s), Offset(Q - unsigned long long)
            index_entry = struct.pack(
                f">I{len(key_bytes)}sQ", len(key_bytes), key_bytes, self.current_offset
            )
            self.index_file.write(index_entry)

            self.current_offset += len(entry)

    def close(self):
        self.data_file.close()
        self.index_file.close()


class SSTableReader:
    """
    Reads from an SSTable using an in-memory index.
    """

    def __init__(self, data_path, index_path):
        self.data_path = data_path
        self.index_keys = []  # Sorted list of keys in index
        self.index_offsets = []  # Corresponding list of offsets

        # Load index into memory
        self._load_index(index_path)

        # Open data file for random access
        self.data_file = open(data_path, "rb")

    def _load_index(self, index_path):
        if not os.path.exists(index_path):
            return

        with open(index_path, "rb") as f:
            while True:
                # Read KeyLen (4 bytes)
                header = f.read(4)
                if not header:
                    break
                key_len = struct.unpack(">I", header)[0]

                # Read Key
                key_bytes = f.read(key_len)
                key = key_bytes.decode("utf-8")

                # Read Offset (8 bytes)
                offset_bytes = f.read(8)
                offset = struct.unpack(">Q", offset_bytes)[0]

                self.index_keys.append(key)
                self.index_offsets.append(offset)

    def get(self, key):
        """
        Binary search in index, then seek in file.
        Returns:
            - Value (str) if found and not deleted.
            - TOMBSTONE object if found and deleted.
            - None if not found in this SSTable.
        """
        # Binary search for key
        idx = bisect.bisect_left(self.index_keys, key)
        if idx < len(self.index_keys) and self.index_keys[idx] == key:
            # Found in index
            offset = self.index_offsets[idx]
            return self._read_at(offset)
        return None  # Not found

    def _read_at(self, offset):
        self.data_file.seek(offset)

        # Format: KeyLen(I), Key(s), ValLen(i), Value(s)
        # We already know the Key, but we need to read past it
        header = self.data_file.read(4)
        key_len = struct.unpack(">I", header)[0]
        self.data_file.read(key_len)  # Skip key

        val_len_bytes = self.data_file.read(4)
        val_len = struct.unpack(">i", val_len_bytes)[0]

        if val_len == -1:
            return TOMBSTONE  # Found, but deleted

        val_bytes = self.data_file.read(val_len)
        return val_bytes.decode("utf-8")

    def scan(self, start_key, end_key):
        """
        Yields (key, value) pairs within range using the index.
        """
        idx = bisect.bisect_left(self.index_keys, start_key)

        while idx < len(self.index_keys):
            key = self.index_keys[idx]
            if end_key and key >= end_key:
                break

            # Read value from disk
            # Optimization: Could just read sequentially from file instead of seeking
            # if we are scanning heavily. For now, random seek is easier to code.
            val = self.get(key)
            yield key, val
            idx += 1

    def close(self):
        self.data_file.close()

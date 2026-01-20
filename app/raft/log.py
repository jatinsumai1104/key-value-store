"""
RAFT Log - Persistent log storage with binary format.

Binary format per entry:
[Term: 4B][Index: 4B][Op: 1B][KeyLen: 4B][ValLen: 4B][Key: var][Value: var]

Op codes: 0=put, 1=delete, 2=batch_put
For batch_put, the key/value contains JSON encoded items.
"""

import os
import struct
import json
from dataclasses import dataclass
from typing import List, Optional, Dict, Any


# Operation codes
OP_PUT = 0
OP_DELETE = 1
OP_BATCH_PUT = 2


@dataclass
class LogEntry:
    """A single entry in the RAFT log."""
    term: int
    index: int
    command: Dict[str, Any]  # {"op": "put"|"delete"|"batch_put", "key": str, "value": str, ...}

    def to_bytes(self) -> bytes:
        """Serialize entry to binary format."""
        op_str = self.command.get("op", "put")
        if op_str == "put":
            op = OP_PUT
            key = self.command.get("key", "").encode("utf-8")
            value = self.command.get("value", "").encode("utf-8")
        elif op_str == "delete":
            op = OP_DELETE
            key = self.command.get("key", "").encode("utf-8")
            value = b""
        elif op_str == "batch_put":
            op = OP_BATCH_PUT
            key = b"Batch Put"
            value = json.dumps(self.command.get("items", [])).encode("utf-8")
        else:
            raise ValueError(f"Unknown operation: {op_str}")

        # Header: term(4) + index(4) + op(1) + key_len(4) + val_len(4) = 17 bytes
        header = struct.pack(">IIBII", self.term, self.index, op, len(key), len(value))
        return header + key + value

    @classmethod
    def from_bytes(cls, data: bytes, offset: int = 0) -> tuple["LogEntry", int]:
        """Deserialize entry from binary format. Returns (entry, bytes_consumed)."""
        header_size = 17
        term, index, op, key_len, val_len = struct.unpack(
            ">IIBII", data[offset:offset + header_size]
        )

        key_start = offset + header_size
        key = data[key_start:key_start + key_len].decode("utf-8")
        val_start = key_start + key_len
        value = data[val_start:val_start + val_len].decode("utf-8")

        if op == OP_PUT:
            command = {"op": "put", "key": key, "value": value}
        elif op == OP_DELETE:
            command = {"op": "delete", "key": key}
        elif op == OP_BATCH_PUT:
            items = json.loads(value)
            command = {"op": "batch_put", "items": items}
        else:
            raise ValueError(f"Unknown op code: {op}")

        entry = cls(term=term, index=index, command=command)
        bytes_consumed = header_size + key_len + val_len
        return entry, bytes_consumed


class RaftLog:
    """Persistent RAFT log with binary format."""

    def __init__(self, path: str):
        self.path = path
        self.entries: List[LogEntry] = []
        self._load()

    def _load(self) -> None:
        """Load log entries from disk."""
        if not os.path.exists(self.path):
            return

        with open(self.path, "rb") as f:
            data = f.read()

        offset = 0
        while offset < len(data):
            try:
                entry, consumed = LogEntry.from_bytes(data, offset)
                self.entries.append(entry)
                offset += consumed
            except Exception as e:
                print(f"Error loading log entry at offset {offset}: {e}")
                break

    def _persist(self) -> None:
        """Write all entries to disk with fsync."""
        with open(self.path, "wb") as f:
            for entry in self.entries:
                f.write(entry.to_bytes())
            f.flush()
            os.fsync(f.fileno())

    def append(self, entry: LogEntry) -> None:
        """Append a new entry and persist to disk."""
        self.entries.append(entry)
        # Append mode for efficiency
        with open(self.path, "ab") as f:
            f.write(entry.to_bytes())
            f.flush()
            os.fsync(f.fileno())

    def get(self, index: int) -> Optional[LogEntry]:
        """Get entry by index (1-indexed as per RAFT spec)."""
        if index < 1 or index > len(self.entries):
            return None
        return self.entries[index - 1]

    def get_entries_from(self, start_index: int) -> List[LogEntry]:
        """Get all entries starting from index (inclusive, 1-indexed)."""
        if start_index < 1:
            start_index = 1
        if start_index > len(self.entries):
            return []
        return self.entries[start_index - 1:]

    def truncate_from(self, index: int) -> None:
        """Remove all entries from index onwards (1-indexed). Used for conflict resolution."""
        if index < 1:
            return
        self.entries = self.entries[:index - 1]
        self._persist()

    def last_index(self) -> int:
        """Return the index of the last entry, or 0 if empty."""
        return len(self.entries)

    def last_term(self) -> int:
        """Return the term of the last entry, or 0 if empty."""
        if not self.entries:
            return 0
        return self.entries[-1].term

    def get_term_at(self, index: int) -> int:
        """Get the term of entry at given index, or 0 if not found."""
        entry = self.get(index)
        return entry.term if entry else 0

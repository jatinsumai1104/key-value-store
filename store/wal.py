
import os
import struct
import time

class WAL:
    """
    Write-Ahead Log (WAL) implementation for crash durability.
    Format: [Op: 1B][KeyLen: 4B][ValLen: 4B][Key: var][Value: var]
    """
    OP_PUT = 0
    OP_DELETE = 1

    def __init__(self, path):
        self.path = path
        # Open in append binary mode
        self.file = open(self.path, "ab")
        self.file.flush()

    def append(self, key: str, value: str, op=OP_PUT):
        """
        Appends an entry to the WAL file and fsyncs to disk.
        """
        key_bytes = key.encode('utf-8')
        val_bytes = value.encode('utf-8') if value else b''
        
        # Structure: Op (1) + KeyLen (4) + ValLen (4) + Key + Value
        # value_len is -1 for delete (tombstone) or just len(val_bytes)
        
        val_len = -1

        if op != self.OP_DELETE:
            val_len = len(val_bytes)

        entry = struct.pack(
            f">BIi{len(key_bytes)}s{len(val_bytes)}s",
            op,
            len(key_bytes),
            val_len,
            key_bytes,
            val_bytes
        )

        self.file.write(entry)
        self.file.flush()
        os.fsync(self.file.fileno())

    def replay(self):
        """
        Generator that yields (key, value) pairs from the WAL for recovery.
        Value is None for deletes.
        """
        # Read from beginning
        if not os.path.exists(self.path):
            return

        with open(self.path, "rb") as f:
            while True:
                # Read header: Op(1) + KeyLen(4) + ValLen(4) = 9 bytes
                header = f.read(9)
                if not header or len(header) < 9:
                    break
                
                op, key_len, val_len = struct.unpack(">BIi", header)
                
                key_bytes = f.read(key_len)
                if len(key_bytes) < key_len:
                    break # corrupted tail
                
                val_bytes = b''
                if val_len > 0:
                    val_bytes = f.read(val_len)
                    if len(val_bytes) < val_len:
                        break # corrupted tail
                
                key = key_bytes.decode('utf-8')
                value = val_bytes.decode('utf-8') if val_len != -1 else None
                
                yield key, value

    def clear(self):
        """Clears the WAL (used after flush to SSTable)."""
        self.file.close()
        # Truncate file
        with open(self.path, "wb") as f:
            pass
        # Reopen
        self.file = open(self.path, "ab")

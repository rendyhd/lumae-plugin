import struct

MAX_ENTRIES = 15
MIN_DB = -128
MAX_DB = 127
MAX_CHUNK_INDEX = 0xFFFF


def encode_ramp(entries):
    if len(entries) > MAX_ENTRIES:
        raise ValueError(f"ramp has {len(entries)} entries, max is {MAX_ENTRIES}")

    out = bytearray()
    for db, chunk_index in entries:
        db_i = int(db)
        chunk_i = int(chunk_index)
        if db_i < MIN_DB or db_i > MAX_DB:
            raise ValueError(f"db {db_i} out of int8 range")
        if chunk_i < 0 or chunk_i > MAX_CHUNK_INDEX:
            raise ValueError(f"chunk_index {chunk_i} out of uint16 range")
        out.extend(struct.pack("<bH", db_i, chunk_i))
    return bytes(out)

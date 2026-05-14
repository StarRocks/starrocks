#!/usr/bin/env python3
"""
Generate a multi-record, multi-block Avro test file for split boundary tests.
The file has 100 records split across multiple blocks (sync_interval set small
so we get many blocks even for a small file).
"""
import json
import io
import os
import sys

import avro.schema
import avro.datafile
import avro.io

SCHEMA_JSON = json.dumps({
    "type": "record",
    "name": "SplitTestRecord",
    "fields": [
        {"name": "id",    "type": "int"},
        {"name": "name",  "type": "string"}
    ]
})

schema = avro.schema.parse(SCHEMA_JSON)

out_path = sys.argv[1] if len(sys.argv) > 1 else "multiblock.avro"

# Write with a very small sync_interval so every ~5 records land in a new block.
# avro-python3 1.10.x exposes sync_interval as a constructor kwarg.
buf = io.BytesIO()
writer = avro.datafile.DataFileWriter(buf, avro.io.DatumWriter(), schema)
# Write 10 records per block (10 blocks total) by flushing after every 10 records.
# Each flush() call calls _WriteBlock() which writes one Avro block + sync marker.
for i in range(100):
    writer.append({"id": i, "name": f"name_{i}"})
    if (i + 1) % 10 == 0:
        writer.flush()
writer.flush()

raw = buf.getvalue()
with open(out_path, "wb") as f:
    f.write(raw)

print(f"Written {len(raw)} bytes to {out_path}")

# Verify by reading back
buf.seek(0)
reader = avro.datafile.DataFileReader(io.BytesIO(raw), avro.io.DatumReader())
records = list(reader)
reader.close()
print(f"Read back {len(records)} records")
assert len(records) == 100, f"Expected 100, got {len(records)}"
assert records[0] == {"id": 0, "name": "name_0"}
assert records[99] == {"id": 99, "name": "name_99"}
print("Verification OK")

#!/usr/bin/env python3
# Generates variant_shredding_empty_object_conflict.parquet: a shredded variant column `data` with one scalar field `x`.
# Row 0 is valid (x only in typed_value). Row 1 is invalid per the shredding spec: the top-level `value` is a scalar,
# while the object typed_value group is present with every field null (an empty partially shredded object, like
# apache/parquet-testing case 128).
# Usage: python3 gen_variant_shredding_empty_object_conflict.py
import pyarrow as pa
import pyarrow.parquet as pq

field_x = pa.struct([pa.field("value", pa.binary(), nullable=True), pa.field("typed_value", pa.int64(), nullable=True)])
typed_value = pa.struct([pa.field("x", field_x, nullable=True)])
variant = pa.struct([
    pa.field("metadata", pa.binary(), nullable=False),
    pa.field("value", pa.binary(), nullable=True),
    pa.field("typed_value", typed_value, nullable=True),
])
schema = pa.schema([pa.field("data", variant, nullable=True)])

EMPTY_METADATA = b"\x01\x00\x00"  # variant metadata version 1, empty dictionary
INT8_5 = b"\x0c\x05"  # variant value: primitive int8 = 5

rows = [
    {"metadata": EMPTY_METADATA, "value": None, "typed_value": {"x": {"value": None, "typed_value": 7}}},
    {"metadata": EMPTY_METADATA, "value": INT8_5, "typed_value": {"x": None}},
]
table = pa.Table.from_pylist([{"data": r} for r in rows], schema=schema)
pq.write_table(table, "variant_shredding_empty_object_conflict.parquet", compression="NONE")
print(pq.ParquetFile("variant_shredding_empty_object_conflict.parquet").schema)

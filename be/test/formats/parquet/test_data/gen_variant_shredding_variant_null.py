#!/usr/bin/env python3
# Generates variant_shredding_variant_null.parquet: a shredded variant `data` whose typed_value is an INT32.
#   row 0: metadata set, value null, typed_value null   a variant null (the group is present but holds nothing)
#   row 1: metadata set, value null, typed_value 5      5
#   row 2: whole `data` null                            SQL NULL
#   row 3: metadata set, value = variant null (0x00)    a variant null, written explicitly
# The Parquet Variant shredding spec says a variant with neither value nor typed_value is a variant null; only a null
# group (a null metadata) is a SQL NULL. Usage: python3 gen_variant_shredding_variant_null.py
import pyarrow as pa
import pyarrow.parquet as pq

variant = pa.struct([
    pa.field("metadata", pa.binary(), nullable=False),
    pa.field("value", pa.binary(), nullable=True),
    pa.field("typed_value", pa.int32(), nullable=True),
])
schema = pa.schema([pa.field("data", variant, nullable=True)])

META = b"\x01\x00\x00"  # variant metadata version 1, empty dictionary
rows = [
    {"data": {"metadata": META, "value": None, "typed_value": None}},
    {"data": {"metadata": META, "value": None, "typed_value": 5}},
    {"data": None},
    {"data": {"metadata": META, "value": b"\x00", "typed_value": None}},
]
pq.write_table(pa.Table.from_pylist(rows, schema=schema), "variant_shredding_variant_null.parquet", compression="NONE")
print(pq.ParquetFile("variant_shredding_variant_null.parquet").schema)

#!/usr/bin/env python3
# Generates variant_shredding_empty_object.parquet: shredded variant `data` with a scalar field x and an object field p.
#   row 0: {}                  typed_value group present, every child null   (expected JSON: {})
#   row 1: SQL NULL            whole `data` null                              (expected: NULL)
#   row 2: {"x":1}             normal                                          (expected JSON: {"x":1})
#   row 3: {"p":{}}            p shredded as an object that is present but empty (expected JSON: {"p":{}})
# Used by a characterization test that prints what the reader returns for each row. Usage: python3 gen_variant_shredding_empty_object.py
import pyarrow as pa
import pyarrow.parquet as pq

x_type = pa.struct([pa.field("value", pa.binary(), nullable=True), pa.field("typed_value", pa.int64(), nullable=True)])
a_type = pa.struct([pa.field("value", pa.binary(), nullable=True), pa.field("typed_value", pa.int64(), nullable=True)])
p_typed = pa.struct([pa.field("a", a_type, nullable=True)])
p_type = pa.struct([pa.field("value", pa.binary(), nullable=True), pa.field("typed_value", p_typed, nullable=True)])
typed_value = pa.struct([pa.field("x", x_type, nullable=True), pa.field("p", p_type, nullable=True)])
variant = pa.struct([
    pa.field("metadata", pa.binary(), nullable=False),
    pa.field("value", pa.binary(), nullable=True),
    pa.field("typed_value", typed_value, nullable=True),
])
schema = pa.schema([pa.field("data", variant, nullable=True)])

META = b"\x01\x00\x00"  # variant metadata version 1, empty dictionary
rows = [
    {"data": {"metadata": META, "value": None, "typed_value": {"x": None, "p": None}}},
    {"data": None},
    {"data": {"metadata": META, "value": None, "typed_value": {"x": {"value": None, "typed_value": 1}, "p": None}}},
    {"data": {"metadata": META, "value": None,
              "typed_value": {"x": None, "p": {"value": None, "typed_value": {"a": None}}}}},
]
pq.write_table(pa.Table.from_pylist(rows, schema=schema), "variant_shredding_empty_object.parquet", compression="NONE")
print(pq.ParquetFile("variant_shredding_empty_object.parquet").schema)

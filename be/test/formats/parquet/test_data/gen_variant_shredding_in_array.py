#!/usr/bin/env python3
# Generates variant_shredding_in_array.parquet: a column `arr` that is a list of shredded variants whose typed_value is an
# object with one shredded field x. Rows: [{"x":1},{"x":2}] and [{"x":3}]. Usage: python3 gen_variant_shredding_in_array.py
import pyarrow as pa
import pyarrow.parquet as pq

x_type = pa.struct([pa.field("value", pa.binary(), nullable=True), pa.field("typed_value", pa.int64(), nullable=True)])
variant = pa.struct([
    pa.field("metadata", pa.binary(), nullable=False),
    pa.field("value", pa.binary(), nullable=True),
    pa.field("typed_value", pa.struct([pa.field("x", x_type, nullable=True)]), nullable=True),
])
schema = pa.schema([pa.field("arr", pa.list_(pa.field("element", variant, nullable=True)), nullable=True)])
META = b"\x01\x00\x00"


def element(x):
    return {"metadata": META, "value": None, "typed_value": {"x": {"value": None, "typed_value": x}}}


rows = [{"arr": [element(1), element(2)]}, {"arr": [element(3)]}]
pq.write_table(pa.Table.from_pylist(rows, schema=schema), "variant_shredding_in_array.parquet", compression="NONE")
print(pq.ParquetFile("variant_shredding_in_array.parquet").schema)

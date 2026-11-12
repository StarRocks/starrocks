#!/usr/bin/env python3
# Generates variant_shredding_field_fallback.parquet: a shredded object variant `data` with one shredded INT64 field a.
#   row 0: a = 1       typed_value
#   row 1: a = 2       typed_value
#   row 2: a = "x"     does not fit INT64, so it is kept in the field's own `value` column (the fallback)
# Reading only the path a takes the typed-only fast path for rows 0 and 1, but row 2 has a fallback value, so the batch
# is read again together with the base payload. Usage: python3 gen_variant_shredding_field_fallback.py
import pyarrow as pa
import pyarrow.parquet as pq

a_type = pa.struct([pa.field("value", pa.binary(), nullable=True), pa.field("typed_value", pa.int64(), nullable=True)])
typed_value = pa.struct([pa.field("a", a_type, nullable=True)])
variant = pa.struct([
    pa.field("metadata", pa.binary(), nullable=False),
    pa.field("value", pa.binary(), nullable=True),
    pa.field("typed_value", typed_value, nullable=True),
])
schema = pa.schema([pa.field("data", variant, nullable=True)])

META = b"\x01\x00\x00"  # variant metadata version 1, empty dictionary
SHORT_STRING_X = b"\x05x"  # variant short string of length 1


def row(a_value, a_typed):
    return {"data": {"metadata": META, "value": None,
                     "typed_value": {"a": {"value": a_value, "typed_value": a_typed}}}}


rows = [row(None, 1), row(None, 2), row(SHORT_STRING_X, None)]
pq.write_table(pa.Table.from_pylist(rows, schema=schema), "variant_shredding_field_fallback.parquet", compression="NONE")
print(pq.ParquetFile("variant_shredding_field_fallback.parquet").schema)

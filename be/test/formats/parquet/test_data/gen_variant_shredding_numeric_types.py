#!/usr/bin/env python3
# Generates two files with a shredded variant column `data` that has one field n, shredded with a different type:
#   variant_shredding_n_int64.parquet  : n is INT64:  9007199254740993 (2^53 + 1) and 9223372036854775807 (INT64_MAX)
#   variant_shredding_n_double.parquet : n is DOUBLE: 1.5
# A double holds 53 bits, so reading both files into one column must not turn the INT64 values into doubles.
# Usage: python3 gen_variant_shredding_numeric_types.py
import pyarrow as pa
import pyarrow.parquet as pq

EMPTY_METADATA = b"\x01\x00\x00"  # variant metadata version 1, empty dictionary


def write(file_name, arrow_type, values):
    field_n = pa.struct([pa.field("value", pa.binary(), nullable=True), pa.field("typed_value", arrow_type, nullable=True)])
    typed_value = pa.struct([pa.field("n", field_n, nullable=True)])
    variant = pa.struct([
        pa.field("metadata", pa.binary(), nullable=False),
        pa.field("value", pa.binary(), nullable=True),
        pa.field("typed_value", typed_value, nullable=True),
    ])
    schema = pa.schema([pa.field("data", variant, nullable=True)])
    rows = [{"metadata": EMPTY_METADATA, "value": None, "typed_value": {"n": {"value": None, "typed_value": v}}}
            for v in values]
    pq.write_table(pa.Table.from_pylist([{"data": r} for r in rows], schema=schema), file_name, compression="NONE")
    print(file_name, [str(f) for f in pq.ParquetFile(file_name).schema_arrow.field("data").type.field("typed_value").type])


write("variant_shredding_n_int64.parquet", pa.int64(), [9007199254740993, 9223372036854775807])
write("variant_shredding_n_double.parquet", pa.float64(), [1.5])

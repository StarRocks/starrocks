#!/usr/bin/env python3
# Generates variant_shredding_timestamps_in_array.parquet: a column `arr` that is a list of shredded variants whose
# typed_value is an object with shredded fields n (INT64), ts (TIMESTAMP(isAdjustedToUTC=true, MICROS)) and ns
# (TIMESTAMP(isAdjustedToUTC=true, NANOS)). Rows:
#   0: [{n=1, ts=2025-01-02 03:04:05.123456 UTC, ns=2025-01-02 03:04:05.123456789 UTC}, {n=2, ts and ns as above + 1s}]
#   1: [{n=3, ts, ns as in row 0, and a residual field extra=7 in the object's value}]
# Usage: python3 gen_variant_shredding_timestamps_in_array.py
import calendar
import pyarrow as pa
import pyarrow.parquet as pq

EPOCH_S = calendar.timegm((2025, 1, 2, 3, 4, 5, 0, 0, 0))
ts_type = pa.timestamp("us", tz="UTC")
ns_type = pa.timestamp("ns", tz="UTC")


def leaf(typed_type):
    return pa.struct([pa.field("value", pa.binary(), nullable=True), pa.field("typed_value", typed_type, nullable=True)])


variant = pa.struct([
    pa.field("metadata", pa.binary(), nullable=False),
    pa.field("value", pa.binary(), nullable=True),
    pa.field("typed_value", pa.struct([
        pa.field("n", leaf(pa.int64()), nullable=True),
        pa.field("ts", leaf(ts_type), nullable=True),
        pa.field("ns", leaf(ns_type), nullable=True),
    ]), nullable=True),
])
schema = pa.schema([pa.field("arr", pa.list_(pa.field("element", variant, nullable=True)), nullable=True)])
EMPTY_META = b"\x01\x00\x00"
EXTRA_META = b"\x01\x01\x00\x05extra"  # dictionary with the single key "extra"
EXTRA_OBJECT = b"\x02\x01\x00\x00\x02\x0c\x07"  # {"extra": 7} with field id 0 and an INT8 value


def element(n, seconds_offset, metadata=EMPTY_META, value=None):
    seconds = EPOCH_S + seconds_offset
    return {"metadata": metadata, "value": value, "typed_value": {
        "n": {"value": None, "typed_value": n},
        "ts": {"value": None, "typed_value": seconds * 10**6 + 123456},
        "ns": {"value": None, "typed_value": seconds * 10**9 + 123456789},
    }}


rows = [{"arr": [element(1, 0), element(2, 1)]}, {"arr": [element(3, 0, EXTRA_META, EXTRA_OBJECT)]}]
pq.write_table(pa.Table.from_pylist(rows, schema=schema), "variant_shredding_timestamps_in_array.parquet",
               compression="NONE")
print(pq.ParquetFile("variant_shredding_timestamps_in_array.parquet").schema)

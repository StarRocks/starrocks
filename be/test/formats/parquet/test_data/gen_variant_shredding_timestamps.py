#!/usr/bin/env python3
# Generates variant_shredding_timestamps.parquet: a shredded variant column `data` with four timestamp fields, one row.
#   ts_utc_us : TIMESTAMP(isAdjustedToUTC=true,  MICROS)   2025-01-02 03:04:05.123456 UTC
#   ts_ntz_us : TIMESTAMP(isAdjustedToUTC=false, MICROS)   2025-01-02 03:04:05.123456
#   ts_utc_ns : TIMESTAMP(isAdjustedToUTC=true,  NANOS)    2025-01-02 03:04:05.123456789 UTC
#   ts_ntz_ms : TIMESTAMP(isAdjustedToUTC=false, MILLIS)   2025-01-02 03:04:05.123
# Used by FileReaderTest.test_read_variant_shredding_timestamps_keep_type_and_instant. Usage: python3 gen_variant_shredding_timestamps.py
import calendar
import pyarrow as pa
import pyarrow.parquet as pq

EPOCH_S = calendar.timegm((2025, 1, 2, 3, 4, 5, 0, 0, 0))
leaf_types = {
    "ts_utc_us": pa.timestamp("us", tz="UTC"),
    "ts_ntz_us": pa.timestamp("us"),
    "ts_utc_ns": pa.timestamp("ns", tz="UTC"),
    "ts_ntz_ms": pa.timestamp("ms"),
}
leaf_values = {
    "ts_utc_us": EPOCH_S * 10**6 + 123456,
    "ts_ntz_us": EPOCH_S * 10**6 + 123456,
    "ts_utc_ns": EPOCH_S * 10**9 + 123456789,
    "ts_ntz_ms": EPOCH_S * 10**3 + 123,
}

def build_field(name):
    typed = pa.array([leaf_values[name]], type=leaf_types[name])
    value = pa.array([None], type=pa.binary())
    return pa.StructArray.from_arrays([value, typed], fields=[
        pa.field("value", pa.binary(), nullable=True), pa.field("typed_value", leaf_types[name], nullable=True)])

typed_value = pa.StructArray.from_arrays([build_field(n) for n in leaf_types],
                                         fields=[pa.field(n, build_field(n).type, nullable=True) for n in leaf_types])
metadata = pa.array([b"\x01\x00\x00"], type=pa.binary())  # variant metadata: version 1, empty dictionary
value = pa.array([None], type=pa.binary())
data = pa.StructArray.from_arrays([metadata, value, typed_value], fields=[
    pa.field("metadata", pa.binary(), nullable=False),
    pa.field("value", pa.binary(), nullable=True),
    pa.field("typed_value", typed_value.type, nullable=True)])
table = pa.Table.from_arrays([data], names=["data"])
pq.write_table(table, "variant_shredding_timestamps.parquet", compression="NONE", version="2.6")
print(pq.ParquetFile("variant_shredding_timestamps.parquet").schema)

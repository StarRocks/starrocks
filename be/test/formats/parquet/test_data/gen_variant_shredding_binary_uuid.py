#!/usr/bin/env python3
# Generates variant_shredding_binary_uuid.parquet: shredded variant `data` with an int field n, a BINARY field b and a UUID field u.
#   row 0: n=5, b=0x0102, u=<uuid>   (all three shredded)
#   row 1: n=6 only                  (control row)
# Used by FileReaderTest.test_read_variant_shredding_binary_uuid_keeps_other_fields: n must survive next to binary/uuid data.
# Usage: python3 gen_variant_shredding_binary_uuid.py
import uuid
import pyarrow as pa
import pyarrow.parquet as pq

def leaf(typed_array):
    value = pa.array([None] * len(typed_array), type=pa.binary())
    return pa.StructArray.from_arrays(
        [value, typed_array],
        fields=[pa.field("value", pa.binary(), nullable=True), pa.field("typed_value", typed_array.type, nullable=True)])

uuid_bytes = uuid.UUID("12345678-1234-5678-1234-567812345678").bytes
n_leaf = leaf(pa.array([5, 6], type=pa.int64()))
b_leaf = leaf(pa.array([b"\x01\x02", None], type=pa.binary()))
u_leaf = leaf(pa.ExtensionArray.from_storage(pa.uuid(), pa.array([uuid_bytes, None], type=pa.binary(16))))

typed_value = pa.StructArray.from_arrays(
    [n_leaf, b_leaf, u_leaf],
    fields=[pa.field("n", n_leaf.type, nullable=True), pa.field("b", b_leaf.type, nullable=True),
            pa.field("u", u_leaf.type, nullable=True)])
metadata = pa.array([b"\x01\x00\x00"] * 2, type=pa.binary())  # variant metadata: version 1, empty dictionary
value = pa.array([None, None], type=pa.binary())
data = pa.StructArray.from_arrays(
    [metadata, value, typed_value],
    fields=[pa.field("metadata", pa.binary(), nullable=False), pa.field("value", pa.binary(), nullable=True),
            pa.field("typed_value", typed_value.type, nullable=True)])
pq.write_table(pa.Table.from_arrays([data], names=["data"]), "variant_shredding_binary_uuid.parquet", compression="NONE")
print(pq.ParquetFile("variant_shredding_binary_uuid.parquet").schema)

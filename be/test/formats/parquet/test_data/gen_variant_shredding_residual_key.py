#!/usr/bin/env python3
# Generates variant_shredding_residual_key.parquet: a shredded object variant `data` with the shredded fields a (int64) and
# p (an object with the shredded field c), plus a residual `value` object that holds some of the same keys.
# Metadata dictionary: a=0, b=1, c=2, d=3. Expected result of a whole-variant read, one per row:
#   row 0: residual {a:1, b:2}, a=10, p = residual {c:1, d:2} + c=20   {"a":10,"b":2,"p":{"c":20,"d":2}}   the shredded value wins
#   row 1: residual {a:1, b:2}, a not set                                {"b":2}   the key is removed, a has no value
#   row 2: residual {b:2}, a=7                                           {"a":7,"b":2}   no clash
#   row 3: residual {a:1, b:"xy"} with the values stored out of field order (b first), a=30      {"a":30,"b":"xy"}
# A second file, variant_shredding_residual_corrupt.parquet, has one row whose residual has the 32-bit offsets [0, 8, 0xfffffff0, 4]:
# the offset table of a corrupt object. Reading it must fail with an error, not read outside the value.
# The Parquet Variant shredding spec says a key must not be in both `value` and `typed_value`; for such data the shredded
# field owns the key, as in the Iceberg reader. Usage: python3 gen_variant_shredding_residual_key.py
import pyarrow as pa
import pyarrow.parquet as pq


def leaf(typed):
    return pa.struct([pa.field("value", pa.binary(), nullable=True), pa.field("typed_value", typed, nullable=True)])


p_typed = pa.struct([pa.field("c", leaf(pa.int64()), nullable=True)])
typed_value = pa.struct([pa.field("a", leaf(pa.int64()), nullable=True),
                         pa.field("p", leaf(p_typed), nullable=True)])
variant = pa.struct([
    pa.field("metadata", pa.binary(), nullable=False),
    pa.field("value", pa.binary(), nullable=True),
    pa.field("typed_value", typed_value, nullable=True),
])
schema = pa.schema([pa.field("data", variant, nullable=True)])

META = b"\x11\x04\x00\x01\x02\x03\x04abcd"  # version 1, sorted, 4 names: a b c d


def int8(v):  # variant primitive INT8
    return bytes([0x0C, v])


def obj(fields):  # fields: [(field id, encoded value)] in field-name order, one-byte ids and offsets
    values = b"".join(v for _, v in fields)
    offsets, pos = [], 0
    for _, v in fields:
        offsets.append(pos)
        pos += len(v)
    offsets.append(pos)
    return bytes([0x02, len(fields)]) + bytes(i for i, _ in fields) + bytes(offsets) + values


res_ab = obj([(0, int8(1)), (1, int8(2))])
res_b = obj([(1, int8(2))])
res_cd = obj([(2, int8(1)), (3, int8(2))])


def obj_unordered(fields, data_order):
    """An object whose field values are stored in `data_order` (indexes into fields), not in field order."""
    chunks = {k: fields[k][1] for k in data_order}
    positions, pos = {}, 0
    for k in data_order:
        positions[k] = pos
        pos += len(chunks[k])
    offsets = [positions[k] for k in range(len(fields))] + [pos]
    return (bytes([0x02, len(fields)]) + bytes(i for i, _ in fields) + bytes(offsets)
            + b"".join(chunks[k] for k in data_order))


SHORT_STRING_XY = bytes([0x09]) + b"xy"
res_unordered = obj_unordered([(0, int8(1)), (1, SHORT_STRING_XY)], data_order=[1, 0])  # b first, then a
# Three fields a, b, c with 4-byte offsets [0, 8, 0xfffffff0, 4] and four bytes of data: the second and third offsets lie far
# beyond the end of the value. 0x0E is an object header with one-byte ids and four-byte offsets.
res_corrupt = (bytes([0x0E, 3, 0, 1, 2]) + b"".join(v.to_bytes(4, "little") for v in (0, 8, 0xFFFFFFF0, 4))
               + int8(1) + int8(2))


def row(value, a, p):
    return {"data": {"metadata": META, "value": value,
                     "typed_value": {"a": a, "p": p}}}


rows = [
    row(res_ab, {"value": None, "typed_value": 10},
        {"value": res_cd, "typed_value": {"c": {"value": None, "typed_value": 20}}}),
    row(res_ab, None, None),
    row(res_b, {"value": None, "typed_value": 7}, None),
    row(res_unordered, {"value": None, "typed_value": 30}, None),
]
pq.write_table(pa.Table.from_pylist(rows, schema=schema), "variant_shredding_residual_key.parquet", compression="NONE")
print(pq.ParquetFile("variant_shredding_residual_key.parquet").schema)

corrupt_rows = [row(res_corrupt, {"value": None, "typed_value": 5}, None)]
pq.write_table(pa.Table.from_pylist(corrupt_rows, schema=schema), "variant_shredding_residual_corrupt.parquet",
               compression="NONE")

#!/usr/bin/env python3
# Generates variant_shredding_residual_key.parquet: a shredded object variant `data` with the shredded fields a (int64) and
# p (an object with the shredded field c), plus a residual `value` object that holds some of the same keys.
# Metadata dictionary: a=0, b=1, c=2, d=3. Expected result of a whole-variant read, one per row:
#   row 0: residual {a:1, b:2}, a=10, p = residual {c:1, d:2} + c=20   {"a":10,"b":2,"p":{"c":20,"d":2}}   the shredded value wins
#   row 1: residual {a:1, b:2}, a not set                                {"b":2}   the key is removed, a has no value
#   row 2: residual {b:2}, a=7                                           {"a":7,"b":2}   no clash
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


def row(value, a, p):
    return {"data": {"metadata": META, "value": value,
                     "typed_value": {"a": a, "p": p}}}


rows = [
    row(res_ab, {"value": None, "typed_value": 10},
        {"value": res_cd, "typed_value": {"c": {"value": None, "typed_value": 20}}}),
    row(res_ab, None, None),
    row(res_b, {"value": None, "typed_value": 7}, None),
]
pq.write_table(pa.Table.from_pylist(rows, schema=schema), "variant_shredding_residual_key.parquet", compression="NONE")
print(pq.ParquetFile("variant_shredding_residual_key.parquet").schema)

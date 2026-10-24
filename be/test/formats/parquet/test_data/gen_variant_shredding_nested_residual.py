#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generates variant_shredding_nested_residual.parquet.

The `data` column is a shredded variant whose `commit` object is only partially shredded: `commit.collection`
lives in typed_value and the other fields of `commit` live in the residual `data.typed_value.commit.value`.
`note` is a shredded field with only a `value` column (no typed_value), so it lives in that column alone.

    data: struct<metadata, value, typed_value: struct<
        kind:   struct<value, typed_value: string>,
        commit: struct<value, typed_value: struct<collection: struct<value, typed_value: string>>>,
        note:   struct<value>>>

Rows (as JSON):
    0: {"commit":{"collection":"post","record":{"text":"hello"}},"kind":"commit"}
    1: {"commit":{"collection":"like"},"kind":"commit"}
    2: {"extra":1,"kind":"identity","note":"n1"}
    3: NULL
    4: {"commit":{"rev":"r1"}}

Usage: python3 gen_variant_shredding_nested_residual.py   (needs pyarrow)
"""

import os
import struct

import pyarrow as pa
import pyarrow.parquet as pq

KEYS = sorted(["collection", "commit", "extra", "kind", "note", "record", "rev", "text"])
KEY_ID = {k: i for i, k in enumerate(KEYS)}


def encode_metadata():
    # header: version 1, sorted_strings, offset_size 1 byte.
    out = bytearray([0x01 | 0x10, len(KEYS)])
    offset = 0
    out.append(offset)
    for key in KEYS:
        offset += len(key.encode())
        out.append(offset)
    for key in KEYS:
        out += key.encode()
    return bytes(out)


def encode_value(v):
    if isinstance(v, str):
        data = v.encode()
        assert len(data) < 64
        return bytes([(len(data) << 2) | 0x01]) + data  # short string
    if isinstance(v, int):
        return bytes([(6 << 2) | 0x00]) + struct.pack("<q", v)  # int64
    if isinstance(v, dict):
        keys = sorted(v.keys())
        values = [encode_value(v[k]) for k in keys]
        # header: field_offset_size 1 byte, field_id_size 1 byte, not large.
        out = bytearray([0x02, len(keys)])
        out += bytes(KEY_ID[k] for k in keys)
        offset = 0
        out.append(offset)
        for value in values:
            offset += len(value)
            out.append(offset)
        assert offset < 256
        for value in values:
            out += value
        return bytes(out)
    raise TypeError(v)


METADATA = encode_metadata()

# (top-level residual, kind, commit residual, commit.collection, commit present); None for a null row.
ROWS = [
    (None, "commit", {"record": {"text": "hello"}}, "post", True),
    (None, "commit", None, "like", True),
    ({"extra": 1}, "identity", None, None, False),  # also carries note = "n1"
    None,
    # The only payload of the row is the residual of `commit`.
    (None, None, {"rev": "r1"}, None, True),
]


def build_data_column():
    leaf_type = lambda typed: pa.struct([("value", pa.binary()), ("typed_value", typed)])
    commit_type = pa.struct(
        [("value", pa.binary()), ("typed_value", pa.struct([("collection", leaf_type(pa.string()))]))]
    )
    note_type = pa.struct([("value", pa.binary())])
    typed_type = pa.struct([("kind", leaf_type(pa.string())), ("commit", commit_type), ("note", note_type)])
    data_type = pa.struct(
        [pa.field("metadata", pa.binary(), nullable=False), ("value", pa.binary()), ("typed_value", typed_type)]
    )

    values = []
    for row in ROWS:
        if row is None:
            values.append(None)
            continue
        top_residual, kind, commit_residual, collection, has_commit = row
        # A missing field has both value and typed_value null.
        commit = {"value": None, "typed_value": None}
        if has_commit:
            commit = {
                "value": encode_value(commit_residual) if commit_residual is not None else None,
                "typed_value": {"collection": {"value": None, "typed_value": collection}},
            }
        values.append(
            {
                "metadata": METADATA,
                "value": encode_value(top_residual) if top_residual is not None else None,
                "typed_value": {
                    "kind": {"value": None, "typed_value": kind},
                    "commit": commit,
                    "note": {"value": encode_value("n1") if kind == "identity" else None},
                },
            }
        )
    return pa.array(values, type=data_type)


def main():
    table = pa.table({"id": pa.array(range(len(ROWS)), type=pa.int64()), "data": build_data_column()})
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "variant_shredding_nested_residual.parquet")
    pq.write_table(table, out, write_statistics=True)


if __name__ == "__main__":
    main()

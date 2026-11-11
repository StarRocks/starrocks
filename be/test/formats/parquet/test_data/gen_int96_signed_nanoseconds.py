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

"""Generate the static INT96 fixture (PyArrow 25.0.1; not needed to run BE tests)."""

from pathlib import Path
import struct

import pyarrow as pa
import pyarrow.parquet as pq

# Epoch nanoseconds: pre-epoch fractional seconds and a normal positive value.
values = {
    "before_epoch_us": -1000,
    "before_epoch_half_second": -500000000,
    "after_epoch": 123456789000,
}
table = pa.table({
    name: pa.array([value, value, None, value, value], type=pa.timestamp("ns"))
    for name, value in values.items()
})
path = Path(__file__).parent / "column_converter/int96_signed_nanoseconds.parquet"
pq.write_table(
    table, path, use_deprecated_int96_timestamps=True, use_dictionary=False,
    compression="NONE", write_statistics=False, write_page_checksum=False,
)

# Recent Arrow normalizes INT96; reproduce the equivalent signed nanos encoding
# emitted by older writers. PLAIN pages are uncompressed, without checksums or stats.
raw = path.read_bytes()
for nanos in (-1000, -500000000):
    canonical = struct.pack("<qI", 86400000000000 + nanos, 2440587)
    signed = struct.pack("<qI", nanos, 2440588)
    assert raw.count(canonical) == 4
    raw = raw.replace(canonical, signed)
    assert raw.count(signed) == 4
path.write_bytes(raw)
assert pq.read_table(path).equals(table)
print(path)

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

"""Regenerate the small INT64 (not INT96) timestamp fixture using PyArrow."""
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

columns = {"id": pa.array(range(1, 11), type=pa.int32())}
for unit, scale in [("ms", 1000), ("us", 1000000), ("ns", 1000000000)]:
    # Fractional epochs, second/day boundaries, positive controls and NULL.
    values = [-1, -scale, -scale - 1, -86400 * scale - 1,
              0, 1, scale + 1, None, -28800 * scale - 1, 1609459200 * scale + 1]
    for tz, prefix in [(None, "local"), ("UTC", "utc")]:
        columns[f"{prefix}_{unit}"] = pa.array(values, type=pa.timestamp(unit, tz=tz))
pq.write_table(pa.table(columns), Path(__file__).with_name("negative_timestamps.parquet"),
               version="2.6", use_deprecated_int96_timestamps=False, compression="NONE")

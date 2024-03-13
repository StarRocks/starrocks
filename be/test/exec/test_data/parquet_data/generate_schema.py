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
#
import pyarrow as pa
import pyarrow.parquet as pq

# file 1
columns = [
    "col1",
]
data = [
    pa.array([1, 2, 3], type=pa.int64()),
]

table = pa.Table.from_arrays(data, columns)
pq.write_table(table, "./schema1.parquet")

# file 2
columns = [
    "col2",
    "col3",
    "col4",
]
data = [
    pa.array(["4", "5", "6"], type=pa.string()),
    pa.array([7, 8, 9], type=pa.int64()),
    pa.array([10.1, 11.2, 12.3], type=pa.float64()),
]

table = pa.Table.from_arrays(data, columns)
pq.write_table(table, "./schema2.parquet")

# file 3
columns = [
    "col2",
    "col3",
    "col4",
]
data = [
    pa.array([13, 14, 15], type=pa.int32()),
    pa.array([16, 17, 18], type=pa.int32()),
    pa.array([19.1, 20.2, 21.3], type=pa.float32()),
]

table = pa.Table.from_arrays(data, columns)
pq.write_table(table, "./schema3.parquet")

# file 4 
columns = [
    "col1",
    "COL1",
]
data = [
    pa.array([22, 23, 24], type=pa.int32()),
    pa.array([25, 26, 27], type=pa.int32()),
]

table = pa.Table.from_arrays(data, columns)
pq.write_table(table, "./schema4.parquet")
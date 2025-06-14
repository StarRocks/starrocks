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

from pyarrow import json
import pyarrow as pa
import pyarrow.parquet as pq

# Generate parquet file for testing

output = "./nested.parquet"

data = [
    pa.array([1, 2, 3], type=pa.int64()),
    pa.array([[1, 2], [3, 4], [5, 6]], type=pa.list_(pa.int32())),
    pa.array([[[1, 2], [1,2]], [[3, 4],[3,4]], [[5,6],[5, 6]]], type=pa.list_(pa.list_(pa.int32()))),
    pa.array([[("s1", 1), ("s2", 3)], [("s2", 2)], [("s3", 3)]], type=pa.map_(pa.string(), pa.int32())),
    pa.array([[("s1", (("s2", 1), ("s3", 3)))], [("s4", (("s5", 2),))], [("s6", (("s7", 3),))]], type=pa.map_(pa.string(), pa.map_(pa.string(), pa.int32()))),
    pa.array([[[("s1", 1), ("s2", 3)], [("s3", 4)]], [[("s4", 2)],[("s5", 7)]], [[("s6", 3)], [("s7", 0), ("s8", 10)]]], type=pa.list_(pa.map_(pa.string(), pa.int32()))),
    pa.array([[("s1", [1, 2]), ("s2", [3,4,5])], [("s2", [2])], [("s3", [3, 10])]], type=pa.map_(pa.string(), pa.list_(pa.int32()))),
]

columns = [
    "col_int",
    "col_list_int",
    "col_list_list_int",
    "col_map_string_int",
    "col_map_map_string_int",
    "col_list_map_string_int",
    "col_map_string_list_int",
]
table = pa.Table.from_arrays(data, columns)
pq.write_table(table, output)
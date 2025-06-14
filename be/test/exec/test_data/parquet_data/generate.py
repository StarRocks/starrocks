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

# input = './data_json.json'
# table = json.read_json(input)

output = "./data_json.parquet"

data = [
    pa.array([1, 2, 3], type=pa.int8()),
    pa.array([1, 2, 3], type=pa.int16()),
    pa.array([1, 2, 3], type=pa.int32()),
    pa.array([1, 2, 3], type=pa.int64()),

    pa.array([1, 2, 3], type=pa.uint8()),
    pa.array([1, 2, 3], type=pa.uint16()),
    pa.array([1, 2, 3], type=pa.uint32()),
    pa.array([1, 2, 3], type=pa.uint64()),
    
    pa.array([1659962123000, 1659962124000, 1659962125000], type=pa.timestamp('ms', tz='America/New_York')),
    pa.array([1659962123000, 1659962124000, 1659962125000], type=pa.timestamp('ms')),

    pa.array([1.1, 2.1, 3.1], type=pa.float32()),
    pa.array([1.1, 2.1, 3.1], type=pa.float64()),

    pa.array([True, False, True], type=pa.bool_()),
    pa.array(["s1", "s2", "s3"], type=pa.string()),

    pa.array([[1, 2], [3, 4], [5, 6]], type=pa.list_(pa.int32())),
    pa.array([[("s1", 1), ("s2", 3)], [("s2", 2)], [("s3", 3)]], type=pa.map_(pa.string(), pa.int32())),
    pa.array(
        [
            [(1659962123000, 1)],
            [(1659962124000, 2)],
            [(1659962125000, 3)],
        ],
        type=pa.map_(pa.timestamp('ms', tz='America/New_York'), pa.int32()),
    ),

    pa.array(
        [
            {"s0": 1, "s1": "string1"},
            {"s0": 2, "s1": "string2"},
            {"s0": 3, "s1": "string3"},
        ],
        type=pa.struct([("s0", pa.int32()), ("s1", pa.string())]),
    ),
    pa.array(
        [
            [[1, 2, 3], [7, 8, 9], [10, 11, 12]],
            [[4, 5, 6], [7, 8, 9], [12, 13, 14]],
            [[4, 5, 6], [7, 8, 9], [12, 13, 14]],
        ],
        type=pa.list_(pa.list_(pa.int32())),
    ),
    pa.array(
        [
            [("s1", [1, 2]), ("s2", [3, 4])],
            [("s1", [5, 6])],
            [("s1", [5, 6])],
        ],
        type=pa.map_(pa.string(), pa.list_(pa.int32())),
    ),
    pa.array(
        [
            [{"s0": 1, "s1": "string1"}, {"s0": 2, "s1": "string2"}],
            [{"s0": 1, "s1": "string1"}],
            [{"s0": 1, "s1": "string3"}],
        ],
        type=pa.list_(pa.struct([("s0", pa.int32()), ("s1", pa.string())])),
    ),
    pa.array(
        [
            {
                "s0": 1,
                "s1": {"s2": 3},
            },
            {
                "s0": 2,
                "s1": {"s2": 4},
            },
            {
                "s0": 3,
                "s1": {"s2": 5},
            },
        ],
        type=pa.struct([("s0", pa.int32()), ("s1", pa.struct([("s2", pa.int32())]))]),
    ),

    pa.array(
        [
            {"s0": 1, "s1": "string1"},
            {"s0": 2, "s1": "string2"},
            {"s0": 3, "s1": "string3"},
        ],
        type=pa.struct([("s0", pa.int32()), ("s1", pa.string())]),
    ),

    pa.array(["{\"s1\": 1 }", "{\"s2\": 2}", "{\"s3\": 3}"], type=pa.string()),
]

columns = [
    "col_json_int8",
    "col_json_int16",
    "col_json_int32",
    "col_json_int64",
    "col_json_uint8",
    "col_json_uint16",
    "col_json_uint32",
    "col_json_uint64",
    
    "col_json_timestamp",
    "col_json_timestamp_not_normalized",

    "col_json_float32",
    "col_json_float64",

    "col_json_bool",
    "col_json_string",

    "col_json_list",
    "col_json_map",
    "col_json_map_timestamp",
    "col_json_struct",
    "col_json_list_list",
    "col_json_map_list",
    "col_json_list_struct",
    "col_json_struct_struct",
    
    "col_json_struct_string",
    "col_json_json_string",
]
table = pa.Table.from_arrays(data, columns)
pq.write_table(table, output)

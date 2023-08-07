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

output = "./issue_16375.parquet"

data = [
    pa.array(range(4097), type=pa.int32()),
    pa.array(
        [[["1"]] for i in range(4097)],
        type=pa.list_(pa.list_(pa.string())),
    ),
]

columns = [
    "c0",
    "c1"
]
table = pa.Table.from_arrays(data, columns)
pq.write_table(table, output)

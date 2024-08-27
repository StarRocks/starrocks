#!/bin/bash

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

import pandas as pd
from pyarrow import orc

output = "timestamp.orc"

# Create a data frame
df = pd.DataFrame(
    {
        "col_int": [1,2,3,4],
        "col_datetime": [
            pd.Timestamp("2006-01-02 15:04:05", tz = "Asia/Shanghai"),
            pd.Timestamp("2006-01-02 15:04:05.9", tz = "Asia/Shanghai"),
            pd.Timestamp("2006-01-02 15:04:05.999999", tz = "Asia/Shanghai"),
            pd.Timestamp("2006-01-02 15:04:05.999999999", tz = "Asia/Shanghai"),
        ]
    }
)

# Convert the dataframe to a PyArrow table
table = orc.Table.from_pandas(df)

# Write the table to an ORC file
with open(output, 'wb') as f:
    orc.write_table(table, f)
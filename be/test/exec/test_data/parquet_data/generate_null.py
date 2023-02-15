# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

import pyarrow as pa
import pyarrow.parquet as pq

# Generate parquet file for testing
output = "./data_null.parquet"

data = [
    pa.array([None, None, None], type=pa.null()),
    pa.array([None, None, None], type=pa.null()),
]

columns = [
    "col_int_null",
    "col_string_null",
]
table = pa.Table.from_arrays(data, columns)
pq.write_table(table, output)

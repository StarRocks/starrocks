from pyarrow import json
import pyarrow as pa
import pyarrow.parquet as pq

# input = './data_json.json'
# table = json.read_json(input)

output = "./data_json.parquet"

schema = pa.schema(
    [
        ("col_json_int", pa.int32()),
        ("col_json_bool", pa.bool_()),
        ("col_json_string", pa.string()),
        ("col_json_list", pa.list_(pa.int32())),
        ("col_json_map", pa.map_(pa.string(), pa.int32())),
        ("col_json_struct", pa.struct([("s0", pa.int32()), ("s1", pa.string())])),
    ]
)

data = [
    pa.array([1, 2, 3], type=pa.int32()),
    pa.array([True, False, True], type=pa.bool_()),
    pa.array(["s1", "s2", "s3"], type=pa.string()),
    pa.array([[1, 2], [3, 4], [5, 6]], type=pa.list_(pa.int32())),
    pa.array([[("s1", 1)], [("s2", 2)], [("s3", 3)]], type=pa.map_(pa.string(), pa.int32())),
    pa.array(
        [
            {"s0": 1, "s1": "string1"},
            {"s0": 2, "s1": "string2"},
            {"s0": 3, "s1": "string3"},
        ],
        type=pa.struct([("s0", pa.int32()), ("s1", pa.string())]),
    ),
]

columns = ["col_json_int", "col_json_bool", "col_json_string", "col_json_list", "col_json_map", "col_json_struct"]
# batches = pa.RecordBatch.from_arrays(data, columns)
# print(batches)
# table = pa.Table.from_batches(batches)
table = pa.Table.from_arrays(data, columns)

pq.write_table(table, output)

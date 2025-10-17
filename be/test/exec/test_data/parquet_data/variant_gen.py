# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This program uses Apache Spark to generate example binary Variant data
#
# Requirements
# pip install pyarrow
# pip install pyspark
#
# Last run with Spark 4.0 preview 2:
# https://spark.apache.org/news/spark-4.0.0-preview2.html

from pyspark.sql import SparkSession
import pyarrow.parquet as pq
import os
import json

# Initialize Spark session and create variant data via SQL
spark = SparkSession.builder \
    .appName("PySpark SQL Example") \
    .config("spark.sql.parquet.compression.codec", "uncompressed") \
    .getOrCreate()

# recursively cleanup the spark-warehouse directory
if os.path.exists('spark-warehouse'):
    for root, dirs, files in os.walk('spark-warehouse', topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))


# Create a table with variant and insert various types into it
#
# This writes data files into spark-warehouse/output
sql = """
CREATE TABLE T (name VARCHAR(2000), col_variant VARIANT);

-------------------------------
-- Primitive type (basic_type=0)
-------------------------------
-- One row with a value from each type listed in 
-- https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#encoding-types
--
-- Spark Types: https://spark.apache.org/docs/latest/sql-ref-datatypes.html
-- Note: must use explicit typecasts as Spark returns an error for implicit casts
INSERT INTO T VALUES ('primitive_null', NULL);
INSERT INTO T VALUES ('primitive_boolean_true', true::Variant);
INSERT INTO T VALUES ('primitive_boolean_false', false::Variant);
INSERT INTO T VALUES ('primitive_int8', 42::Byte::Variant);
INSERT INTO T VALUES ('primitive_int16', 1234::Short::Variant);
INSERT INTO T VALUES ('primitive_int32', 123456::Integer::Variant);
INSERT INTO T VALUES ('primitive_int64', 1234567890123456789::Long::Variant); -- must be be too large to fit in int32: https://github.com/apache/parquet-testing/issues/82
INSERT INTO T VALUES ('primitive_double', 1234567890.1234::Double::Variant);
INSERT INTO T VALUES ('primitive_decimal4', 12.34::Decimal(8,2)::Variant);
INSERT INTO T VALUES ('primitive_decimal8', 12345678.90::Decimal(12,2)::Variant);
INSERT INTO T VALUES ('primitive_decimal16', 12345678912345678.90::Decimal(30,2)::Variant);
INSERT INTO T VALUES ('primitive_date', '2025-04-16'::Date::Variant);
INSERT INTO T VALUES ('primitive_timestamp', '2025-04-16T12:34:56.78'::Timestamp::Variant);
INSERT INTO T VALUES ('primitive_timestampntz', '2025-04-16T12:34:56.78'::Timestamp_NTZ::Variant);
INSERT INTO T VALUES ('primitive_float', 1234567890.1234::Float::Variant);
INSERT INTO T VALUES ('primitive_binary', X'31337deadbeefcafe'::Variant);
INSERT INTO T VALUES ('primitive_string', 'This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes several non ascii characters such as 🐢, 💖, ♥️, 🎣 and 🤦!!'::Variant);

-- https://github.com/apache/parquet-testing/issues/79
-- is not clear how to create the following types using Spark SQL
-- TODO TimeNTZ                    (Type ID 17)
-- TODO 'timestamp with timezone (NANOS)'  (Type ID 18)
-- TODO 'timestamp with time zone (NANOS)' (Type ID 19)
-- TODO 'UUID'                     (Type ID 20)

-------------------------------
-- Short string (basic_type=1)
-------------------------------
INSERT INTO T VALUES ('short_string', 'Less than 64 bytes (❤️ with utf8)'::Variant);

-------------------------------
-- Object (basic_type=2)
-------------------------------
-- Use parse_json to create Variant, as spark does not seem to support casting structs --> Variant.
INSERT INTO T VALUES ('object_empty', parse_json('{}')::Variant);
INSERT INTO T VALUES ('object_primitive', parse_json('{"int_field" : 1, "double_field": 1.23456789, "boolean_true_field": true, "boolean_false_field": false, "string_field": "Apache Parquet", "null_field": null, "timestamp_field": "2025-04-16T12:34:56.78"}')::Variant);
INSERT INTO T VALUES ('object_nested', parse_json('{ "id" : 1, "species" : { "name": "lava monster", "population": 6789}, "observation" : { "time": "12:34:56", "location": "In the Volcano", "value" : { "temperature": 123, "humidity": 456 } } }')::Variant);

-- https://github.com/apache/parquet-testing/issues/77
-- TODO create example variant objects with fields that non-json types (like timestamp, date, etc)
-- Casting from "STRUCT<...>" to "VARIANT"" is not yet supported
-- INSERT INTO T VALUES ('object_primitive', struct(1234.56::Double as double_field, true as boolean_true_field, false as boolean_false_field, '2025-04-16T12:34:56.78'::Timestamp as timestamp_field, 'Apache Parquet' as string_field, null as null_field)::Variant);
--TODO objects with more than 2**8 distinct fields (that require using more than one byte for field offset)
--TODO objects with more than 2**16 distinct fields (that require using more than 2 bytes for field offset)
--TODO objects with more than 2**24 distinct fields (that require using more than 3 bytes for field offset)

-------------------------------
-- Array (basic_type=3)
-------------------------------
INSERT INTO T VALUES ('array_empty', parse_json('[]')::Variant);
INSERT INTO T VALUES ('array_primitive', parse_json('[2, 1, 5, 9]')::Variant);
INSERT INTO T VALUES ('array_nested', parse_json('[ { "id": 1, "thing": { "names": ["Contrarian", "Spider"] } }, null, { "id": 2, "type": "if", "names": ["Apple", "Ray", null] } ]')::Variant);

-- https://github.com/apache/parquet-testing/issues/78
-- TODO arrays with more than 2**8 distinct elements (that require using more than one byte for count)
-- TODO arrays where the total length of all values is greater than 2**8, 2**16, and 2**24 bytes (that require using more than one byte for the offsets)

-------------------------------
-- Output the value to a new table that also has the JSON representation of the variant column
-------------------------------
DROP TABLE IF EXISTS output;
CREATE TABLE output AS SELECT name, col_variant, to_json(col_variant) as json_col FROM T;
"""

for statement in sql.split("\n"):
    statement = statement.strip()
    if not statement or statement.startswith("--"):
        continue
    print("Running SQL:", statement)
    spark.sql(statement)

mypath = 'spark-warehouse/output'
parquet_files = [f for f in os.listdir(mypath) if f.endswith('.parquet')]

# Create parquet_data directory if it doesn't exist
output_dir = '.'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# extract the values from the parquet files and create individual files
data_dictionary = {}
for f in parquet_files:
    table = pq.read_table(os.path.join(mypath, f))
    for row in range(len(table)):
        name = table[0][row]
        # variants are stored as StructArrays with two fields:
        # metadata, and value
        col_variant = table[1][row]
        json_value = table[2][row]

        # Add the JSON representation to the data dictionary
        name = name.as_py()
        json_value = json_value.as_py()

        if json_value is not None:
            data_dictionary[name] = json.loads(json_value)
        else:
            data_dictionary[name] = None

        # Create individual parquet file for each variant type
        # Extract the original schema and create a new table with just this row
        # This preserves the original Spark-generated schema including nullability
        # single_row_table = table.slice(row, 1)
        #
        # # Write uncompressed parquet file
        # output_file = os.path.join(output_dir, f"{name}.parquet")
        # pq.write_table(single_row_table, output_file, compression='none')
        # print(f"Created: {output_file}")

# Write a single file with all the data
variant_output = 'variant_all'
spark.sql("SELECT * FROM output").repartition(1).write.parquet(variant_output, mode='overwrite', compression='none')

# Find the generated parquet file and rename it
variant_files = [f for f in os.listdir(variant_output) if f.endswith('.parquet')]
if variant_files:
    original_file = os.path.join(variant_output, variant_files[0])
    new_variant_file = os.path.join(output_dir, 'variant.parquet')

    # Copy the file to the new location
    import shutil
    shutil.copy2(original_file, new_variant_file)
    print(f"Created variant.parquet with all data")

# Clean up temporary directories
def cleanup_directory(dir_path):
    """Recursively delete a directory and all its contents"""
    if os.path.exists(dir_path):
        print(f"Cleaning up {dir_path}...")
        for root, dirs, files in os.walk(dir_path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(dir_path)
        print(f"Deleted {dir_path}")

# Delete spark-warehouse directory
cleanup_directory('spark-warehouse')

# Delete artifacts directory
cleanup_directory('artifacts')

# Delete variant_all directory
cleanup_directory('variant_all')

with open(f"variant_data_dictionary.json", "w") as f:
    f.write(json.dumps(data_dictionary, sort_keys = True, indent=4))

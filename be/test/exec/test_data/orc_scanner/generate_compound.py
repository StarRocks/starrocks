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

import pyorc

# Define the ORC type for the columns
orc_type = pyorc.TypeDescription.from_string(
    "struct<col_int:int,col_list_int:array<int>,col_list_list_int:array<array<int>>,col_map_string_int:map<string,int>,col_map_string_map_string_int:map<string,map<string,int>>,col_struct_string_int:struct<field_string:string,field_int:int>,col_struct_struct_string_int_string:struct<field_struct:struct<field_string1:string,field_int:int>,field_string2:string>>"
)

# Open a file to write
with open("compound.orc", "wb") as data:
    writer = pyorc.Writer(data, orc_type)

    # Write rows to the ORC file
    writer.write((
        1,
        [2, 3],
        [[4,5], [6,7]],
        {"key8": 9, "key10": 11},
        {"key12":{"key13": 14, "key15": 16}},
        ("value17", 18),
        (("value19", 20), "value21")
    ))

    # Close the writer to ensure everything is written to the file
    writer.close()

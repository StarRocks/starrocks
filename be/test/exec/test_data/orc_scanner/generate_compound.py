#!/bin/bash
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

import pyorc

# Define the ORC type for the columns
orc_type = pyorc.TypeDescription.from_string(
    "struct<col_int:int,col_list_int:array<int>,col_map_int_bool:map<int,double>,col_struct_int_double:struct<field_int:int,field_double:double>>"
)

# Open a file to write
with open("compound.orc", "wb") as data:
    writer = pyorc.Writer(data, orc_type)

    # Write rows to the ORC file
    writer.write((
        1,
        [2, 3],
        {4: 5.1, 6: 7.1},
        (8, 3.14)
    ))

    # Close the writer to ensure everything is written to the file
    writer.close()

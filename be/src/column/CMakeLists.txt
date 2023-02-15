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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/column")

add_library(Column STATIC
        array_column.cpp
        adaptive_nullable_column.cpp
        chunk.cpp
        chunk_extra_data.cpp
        column.cpp
        column_helper.cpp
        column_viewer.cpp
        const_column.cpp
        datum.cpp
        datum_convert.cpp
        datum_tuple.cpp
        field.cpp
        fixed_length_column_base.cpp
        fixed_length_column.cpp
        nullable_column.cpp
        schema.cpp
        binary_column.cpp
        object_column.cpp
        decimalv3_column.cpp
        column_visitor.cpp
        column_visitor_mutable.cpp
        json_column.cpp
        map_column.cpp
        struct_column.cpp
        stream_chunk.cpp
)

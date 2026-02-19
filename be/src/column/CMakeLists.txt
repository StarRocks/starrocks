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

ADD_BE_LIB(ColumnCore
        array_column.cpp
        array_view_column.cpp
        map_column.cpp
        struct_column.cpp
        column.cpp
        column_filter_range.cpp
        column_visitor.cpp
        column_visitor_mutable.cpp
        mysql_row_buffer.cpp
        german_string.cpp
        object_column.cpp
        variant_column.cpp
        fixed_length_column_base.cpp
        fixed_length_column.cpp
        binary_column.cpp
        const_column.cpp
        nullable_column.cpp
        decimalv3_column.cpp
        json_column.cpp
        adaptive_nullable_column.cpp
        column_helper.cpp
        type_traits.cpp
        column_view/column_view_base.cpp
        column_view/column_view_helper.cpp
        column_hash/column_hash.cpp
)

ADD_BE_LIB(ChunkCore
        field.cpp
        schema.cpp
        chunk.cpp
)

ADD_BE_LIB(Column
        chunk_extra_data.cpp
        column_access_path.cpp
        chunk_slice.cpp
        column_helper_time.cpp
        append_with_mask.cpp
        column_viewer.cpp
        datum_convert.cpp
        datum_tuple.cpp
        stream_chunk.cpp
)

target_link_libraries(ChunkCore PUBLIC ColumnCore)
target_link_libraries(Column PUBLIC ColumnCore ChunkCore)

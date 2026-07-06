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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/storage_primitive")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/storage_primitive")

ADD_BE_LIB(StoragePrimitive
    aggregate_type.cpp
    aggregate_iterator.cpp
    chunk_aggregator.cpp
    chunk_iterator.cpp
    column_aggregate_func.cpp
    merge_iterator.cpp
    column_and_predicate.cpp
    column_expr_predicate.cpp
    column_in_predicate.cpp
    column_not_in_predicate.cpp
    column_or_predicate.cpp
    column_null_predicate.cpp
    column_placeholder_predicate.cpp
    column_predicate_cmp.cpp
    column_predicate_dict_conjuct.cpp
    column_value_range.cpp
    conjunctive_predicates.cpp
    disjunctive_predicates.cpp
    edit_version.cpp
    empty_iterator.cpp
    flat_json_config.cpp
    key_coder.cpp
    projection_iterator.cpp
    range.cpp
    predicate_parser.cpp
    primary_key_encoder.cpp
    row_source_mask_buffer.cpp
    schema_helper.cpp
    predicate_tree/predicate_tree.cpp
    storage_ids.cpp
    tablet_column.cpp
    tablet_info.cpp
    tablet_column_util.cpp
    tablet_column_type_utils.cpp
    union_iterator.cpp
)

target_link_libraries(StoragePrimitive PUBLIC
    Expr
    Runtime
    ColumnSortCore
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
)

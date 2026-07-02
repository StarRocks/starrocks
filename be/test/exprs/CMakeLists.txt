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

set(BE_TEST_MACOS_LINK_STUBS)
if (APPLE)
    list(APPEND BE_TEST_MACOS_LINK_STUBS ../base/macos_link_stubs.cpp)
endif()

set(EXPR_CORE_TEST_FILES
    ${BE_TEST_MACOS_LINK_STUBS}
    agg/aggregate_factory_core_test.cpp
    ai_functions_test.cpp
    arithmetic_expr_test.cpp
    array_functions_test.cpp
    array_expr_test.cpp
    case_expr_test.cpp
    cast_expr_core_test.cpp
    function_context_core_test.cpp
    expr_core_test.cpp
    expr_factory_core_test.cpp
    expr_context_core_test.cpp
    binary_functions_test.cpp
    geography_functions_test.cpp
    hash_functions_test.cpp
    http_request_functions_test.cpp
    hyperloglog_functions_test.cpp
    in_const_predicate_test.cpp
    in_predicate_test.cpp
    is_null_predicate_test.cpp
    binary_predicate_core_test.cpp
    array_element_expr_test.cpp
    es_functions_test.cpp
    function_helper_test.cpp
    gin_functions_test.cpp
    json_functions_test.cpp
    map_expr_test.cpp
    map_element_expr_test.cpp
    map_functions_test.cpp
    ngram_search_test.cpp
    percentile_functions_test.cpp
    condition_expr_core_test.cpp
    compound_predicate_core_test.cpp
    json_each_core_test.cpp
    string_fn_blake3_test.cpp
    string_fn_concat_test.cpp
    string_fn_field.cpp
    string_fn_format_bytes_test.cpp
    string_fn_locate_test.cpp
    string_fn_money_format_decimal_test.cpp
    string_fn_ngram_test.cpp
    string_fn_pad_test.cpp
    string_fn_regexp_replace_test.cpp
    string_fn_repeat_test.cpp
    string_fn_reverse_test.cpp
    string_fn_sm3_test.cpp
    string_fn_space_test.cpp
    string_fn_substr_test.cpp
    string_fn_test.cpp
    string_fn_trim_test.cpp
    string_fn_url_extract_parameter_test.cpp
    struct_functions_test.cpp
    subfield_expr_test.cpp
    vectorized_literal_test.cpp
)

set(EXPR_CORE_TEST_LINK_LIBS
    ExprCore
    Platform
    Geo
    Runtime
    ColumnSortCore
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
    ${STARROCKS_DEPENDENCIES}
    ${STARROCKS_STATIC_LINK_LIBS}
    ${WL_LINK_DYNAMIC} ${STARROCKS_SYSTEM_LINK_LIBS} ${WRAP_LINKER_FLAGS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

add_library(starrocks_expr_core_test_objs OBJECT ${EXPR_CORE_TEST_FILES})
target_link_libraries(starrocks_expr_core_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_expr_core_test_objs PROPERTIES
    COMPILE_FLAGS "-fno-access-control -DSTARROCKS_EXPR_CORE_TEST_NO_JIT")
add_executable(expr_core_test expr_core_test_main.cpp $<TARGET_OBJECTS:starrocks_expr_core_test_objs>)
target_link_libraries(expr_core_test ${EXPR_CORE_TEST_LINK_LIBS})

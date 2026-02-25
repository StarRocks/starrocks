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

set(EXPR_CORE_TEST_FILES
    ../base/cxa_throw_wrap.cpp
    arithmetic_expr_test.cpp
    array_expr_test.cpp
    case_expr_test.cpp
    function_context_core_test.cpp
    expr_core_test.cpp
    expr_context_core_test.cpp
    hash_functions_test.cpp
    hyperloglog_functions_test.cpp
    in_const_predicate_test.cpp
    in_predicate_test.cpp
    is_null_predicate_test.cpp
    array_element_expr_test.cpp
    es_functions_test.cpp
    function_helper_test.cpp
    gin_functions_test.cpp
    map_expr_test.cpp
    map_element_expr_test.cpp
    map_functions_test.cpp
    compound_predicate_core_test.cpp
    json_each_core_test.cpp
    struct_functions_test.cpp
    subfield_expr_test.cpp
    variant_path_parser_test.cpp
    vectorized_literal_test.cpp
)

set(EXPR_CORE_TEST_LINK_LIBS
    ExprCore
    RuntimeCore
    ChunkCore
    ColumnCore
    TypesCore
    Common
    Base
    Gutil
    StarRocksGen
    ${STARROCKS_DEPENDENCIES}
    ${STARROCKS_STATIC_LINK_LIBS}
    ${WL_LINK_DYNAMIC} -lresolv -liberty -lc -lm -ldl -rdynamic -pthread ${WRAP_LINKER_FLAGS}
    ${WL_START_GROUP}
    gmock
    gtest
    ${WL_END_GROUP}
)

add_library(starrocks_expr_core_test_objs OBJECT ${EXPR_CORE_TEST_FILES})
target_link_libraries(starrocks_expr_core_test_objs PRIVATE StarRocksPCH)
set_target_properties(starrocks_expr_core_test_objs PROPERTIES
    COMPILE_FLAGS "-fno-access-control -DSTARROCKS_EXPR_CORE_TEST_NO_JIT")
add_executable(expr_core_test $<TARGET_OBJECTS:starrocks_expr_core_test_objs>)
target_link_libraries(expr_core_test ${EXPR_CORE_TEST_LINK_LIBS} gtest_main)

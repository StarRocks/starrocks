# This file is made available under Elastic License 2.0.
# This file is based on code available under the Apache license here:
#   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/CMakeLists.txt

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

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/exprs")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/exprs")

set(EXPR_FILES 
  agg/factory/aggregate_factory.cpp
  agg/factory/aggregate_resolver_approx.cpp
  agg/factory/aggregate_resolver_avg.cpp
  agg/factory/aggregate_resolver_minmaxany.cpp
  agg/factory/aggregate_resolver_others.cpp
  agg/factory/aggregate_resolver_sumcount.cpp
  agg/factory/aggregate_resolver_utility.cpp
  agg/factory/aggregate_resolver_variance.cpp
  agg/factory/aggregate_resolver_window.cpp
  anyval_util.cpp
  base64.cpp
  expr_context.cpp
  expr.cpp
  table_function/table_function_factory.cpp
  table_function/json_each.cpp
  vectorized/arithmetic_expr.cpp
  vectorized/array_element_expr.cpp
  vectorized/array_expr.cpp
  vectorized/array_functions.cpp
  vectorized/binary_predicate.cpp
  vectorized/bitmap_functions.cpp
  vectorized/case_expr.cpp
  vectorized/cast_expr.cpp
  vectorized/cast_expr_array.cpp
  vectorized/column_ref.cpp
  vectorized/placeholder_ref.cpp
  vectorized/dictmapping_expr.cpp
  vectorized/compound_predicate.cpp
  vectorized/condition_expr.cpp
  vectorized/encryption_functions.cpp
  vectorized/es_functions.cpp
  vectorized/find_in_set.cpp
  vectorized/function_call_expr.cpp
  vectorized/function_helper.cpp
  vectorized/geo_functions.cpp
  vectorized/grouping_sets_functions.cpp
  vectorized/hyperloglog_functions.cpp
  vectorized/in_const_predicate.cpp
  vectorized/info_func.cpp
  vectorized/in_predicate.cpp
  vectorized/is_null_predicate.cpp
  vectorized/json_functions.cpp
  vectorized/jsonpath.cpp
  vectorized/like_predicate.cpp
  vectorized/literal.cpp
  vectorized/locate.cpp
  vectorized/map_element_expr.cpp
  vectorized/map_functions.cpp
  vectorized/math_functions.cpp
  vectorized/percentile_functions.cpp
  vectorized/runtime_filter_bank.cpp
  vectorized/runtime_filter.cpp
  vectorized/split.cpp
  vectorized/split_part.cpp
  vectorized/string_functions.cpp
  vectorized/time_functions.cpp
  vectorized/utility_functions.cpp
  agg/java_udaf_function.cpp
  agg/java_window_function.cpp
  table_function/java_udtf_function.cpp
  vectorized/java_function_call_expr.cpp
  vectorized/lambda_function.cpp
  vectorized/array_map_expr.cpp
  vectorized/subfield_expr.cpp
)

add_library(Exprs ${EXPR_FILES})

# simdjson Runtime Implement Dispatch: https://github.com/simdjson/simdjson/blob/master/doc/implementation-selection.md#runtime-cpu-detection
# Unset architecture-specific flags to avoid breaking implement runtime dispatch.
if ("${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86" OR "${CMAKE_BUILD_TARGET_ARCH}" STREQUAL "x86_64")
  set_source_files_properties(vectorized/json_functions.cpp PROPERTIES COMPILE_FLAGS -mno-avx)
  set_source_files_properties(vectorized/json_functions.cpp PROPERTIES COMPILE_FLAGS -mno-avx2)
endif()

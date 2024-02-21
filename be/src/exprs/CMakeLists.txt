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

if (CLANG_TIDY_PATH)
    set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif()

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
  agg/factory/aggregate_resolver_stream.cpp
  agg/factory/aggregate_resolver_utility.cpp
  agg/factory/aggregate_resolver_variance.cpp
  agg/factory/aggregate_resolver_window.cpp
  jit/ir_helper.cpp
  jit/jit_engine.cpp
  jit/jit_expr.cpp
  anyval_util.cpp
  base64.cpp
  binary_functions.cpp
  expr_context.cpp
  expr.cpp
  function_context.cpp
  table_function/table_function_factory.cpp
  table_function/json_each.cpp
  table_function/list_rowsets.cpp
  arithmetic_expr.cpp
  array_element_expr.cpp
  array_expr.cpp
  array_functions.cpp
  binary_predicate.cpp
  bitmap_functions.cpp
  case_expr.cpp
  cast_expr.cpp
  cast_expr_array.cpp
  cast_expr_json.cpp
  cast_nested.cpp
  column_ref.cpp
  placeholder_ref.cpp
  dictmapping_expr.cpp
  compound_predicate.cpp
  condition_expr.cpp
  encryption_functions.cpp
  es_functions.cpp
  find_in_set.cpp
  function_call_expr.cpp
  function_helper.cpp
  geo_functions.cpp
  grouping_sets_functions.cpp
  hyperloglog_functions.cpp
  in_const_predicate.cpp
  info_func.cpp
  in_predicate.cpp
  is_null_predicate.cpp
  json_functions.cpp
  jsonpath.cpp
  like_predicate.cpp
  literal.cpp
  locate.cpp
  map_element_expr.cpp
  map_functions.cpp
  struct_functions.cpp
  math_functions.cpp
  percentile_functions.cpp
  runtime_filter_bank.cpp
  runtime_filter.cpp
  split.cpp
  split_part.cpp
  str_to_map.cpp
  string_functions.cpp
  time_functions.cpp
  utility_functions.cpp
  agg/java_udaf_function.cpp
  agg/java_window_function.cpp
  table_function/java_udtf_function.cpp
  java_function_call_expr.cpp
  lambda_function.cpp
  array_map_expr.cpp
  subfield_expr.cpp
  map_apply_expr.cpp
  map_expr.cpp
  substring_index.cpp
  dict_query_expr.cpp
  runtime_filter_layout.cpp
  dictionary_get_expr.cpp
  ngram.cpp
)

add_library(Exprs ${EXPR_FILES})

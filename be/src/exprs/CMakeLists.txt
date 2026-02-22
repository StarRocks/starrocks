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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/exprs")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/exprs")

set(EXPR_CORE_FILES
  function_context.cpp
  expr_context.cpp
  expr.cpp
  array_element_expr.cpp
  array_expr.cpp
  base64.cpp
  bit_functions.cpp
  cast_nested.cpp
  chunk_predicate_evaluator.cpp
  column_ref.cpp
  compound_predicate.cpp
  es_functions.cpp
  expr_executor.cpp
  function_helper.cpp
  gin_functions.cpp
  grouping_sets_functions.cpp
  info_func.cpp
  jsonpath.cpp
  lambda_function.cpp
  literal.cpp
  map_expr.cpp
  map_element_expr.cpp
  map_functions.cpp
  match_expr.cpp
  placeholder_ref.cpp
  regexp_split.cpp
  runtime_filter_layout.cpp
  struct_functions.cpp
  subfield_expr.cpp
  table_function/json_each.cpp
  variant_path_parser.cpp
)

if(STARROCKS_JIT_ENABLE)
  set(EXPR_CORE_FILES ${EXPR_CORE_FILES}
      jit/ir_helper.cpp
      jit/expr_jit_codegen.cpp)
endif()

ADD_BE_LIB(ExprCore ${EXPR_CORE_FILES})
target_link_libraries(ExprCore PUBLIC RuntimeCore)

set(EXPR_FILES
  agg/data_sketch/ds_theta.cpp
  agg/factory/aggregate_factory.cpp
  agg/factory/aggregate_resolver_approx.cpp
  agg/factory/aggregate_resolver_avg.cpp
  agg/factory/aggregate_resolver_boolean.cpp
  agg/factory/aggregate_resolver_hypothesis_testing.cpp
  agg/factory/aggregate_resolver_minmaxany.cpp
  agg/factory/aggregate_resolver_others.cpp
  agg/factory/aggregate_resolver_sumcount.cpp
  agg/factory/aggregate_resolver_distinct.cpp
  agg/factory/aggregate_resolver_stream.cpp
  agg/factory/aggregate_resolver_utility.cpp
  agg/factory/aggregate_resolver_variance.cpp
  agg/factory/aggregate_resolver_window.cpp
  agg/combinator/agg_state_utils.cpp
  binary_functions.cpp
  expr_factory.cpp
  table_function/table_function_factory.cpp
  table_function/list_rowsets.cpp
  arithmetic_expr.cpp
  array_functions.cpp
  arrow_function_call.cpp
  binary_predicate.cpp
  bitmap_functions.cpp
  case_expr.cpp
  cast_expr.cpp
  cast_expr_array.cpp
  cast_expr_json.cpp
  cast_expr_struct.cpp
  cast_expr_map.cpp
  dictmapping_expr.cpp
  condition_expr.cpp
  encryption_functions.cpp
  find_in_set.cpp
  function_call_expr.cpp
  geo_functions.cpp
  hyperloglog_functions.cpp
  in_const_predicate.cpp
  inet_aton.cpp
  in_predicate.cpp
  is_null_predicate.cpp
  json_functions.cpp
  variant_functions.cpp
  like_predicate.cpp
  locate.cpp
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
  array_map_expr.cpp
  array_sort_lambda_expr.cpp
  map_apply_expr.cpp
  substring_index.cpp
  dict_query_expr.cpp
  dictionary_get_expr.cpp
  ngram.cpp
  hash_functions.cpp
  ai_functions.cpp
)

if(STARROCKS_JIT_ENABLE)
  set(EXPR_FILES ${EXPR_FILES}
      jit/expr_jit_rewriter.cpp
      jit/expr_jit_pass.cpp
      jit/jit_engine.cpp
      jit/jit_expr.cpp)

endif()

ADD_BE_LIB(Exprs ${EXPR_FILES})
target_link_libraries(Exprs PUBLIC ExprCore)

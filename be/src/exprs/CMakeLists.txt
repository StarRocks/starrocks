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

add_library(Exprs
  agg/aggregate_factory.cpp
  table_function/table_function_factory.cpp
  base64.cpp
  expr.cpp
  expr_context.cpp
  slot_ref.cpp
  anyval_util.cpp
  vectorized/arithmetic_expr.cpp
  vectorized/array_expr.cpp
  vectorized/array_element_expr.cpp
  vectorized/array_functions.cpp
  vectorized/compound_predicate.cpp
  vectorized/binary_predicate.cpp
  vectorized/literal.cpp
  vectorized/cast_expr.cpp
  vectorized/function_call_expr.cpp
  vectorized/function_helper.cpp
  vectorized/math_functions.cpp
  vectorized/string_functions.cpp
  vectorized/time_functions.cpp
  vectorized/condition_expr.cpp
  vectorized/like_predicate.cpp
  vectorized/in_predicate.cpp
  vectorized/case_expr.cpp
  vectorized/is_null_predicate.cpp
  vectorized/hyperloglog_functions.cpp
  vectorized/bitmap_functions.cpp
  vectorized/json_functions.cpp
  vectorized/percentile_functions.cpp
  vectorized/encryption_functions.cpp
  vectorized/geo_functions.cpp
  vectorized/find_in_set.cpp
  vectorized/locate.cpp
  vectorized/split.cpp
  vectorized/split_part.cpp
  vectorized/column_ref.cpp
  vectorized/grouping_sets_functions.cpp
  vectorized/es_functions.cpp
  vectorized/utility_functions.cpp
  vectorized/info_func.cpp
  vectorized/runtime_filter.cpp
  vectorized/runtime_filter_bank.cpp
)

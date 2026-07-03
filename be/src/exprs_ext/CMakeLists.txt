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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/exprs_ext")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/exprs_ext")

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(EXPR_DICT_FILES
  dict/expr_factory_dict_exprs_extension.cpp
  dict/dictmapping_expr.cpp
  dict/dict_query_expr.cpp
  dict/dictionary_get_expr.cpp
)

ADD_BE_LIB(ExprDict ${EXPR_DICT_FILES})
target_link_libraries(ExprDict PUBLIC Expr Runtime)
target_link_libraries(ExprDict PRIVATE ComputeEnv Storage Platform StoragePrimitive)

set(EXPR_TABLE_FUNCTION_FILES
  table_function/list_rowsets_factory_extension.cpp
  table_function/list_rowsets.cpp
)

ADD_BE_LIB(ExprTableFunction ${EXPR_TABLE_FUNCTION_FILES})
target_link_libraries(ExprTableFunction PUBLIC Expr Runtime)
target_link_libraries(ExprTableFunction PRIVATE Storage)

set(EXPR_UTILITY_FILES
  utility/utility_functions.cpp
)

ADD_BE_LIB(ExprUtility ${EXPR_UTILITY_FILES})
target_link_libraries(ExprUtility PUBLIC Expr Runtime)
target_link_libraries(ExprUtility PRIVATE ExecRuntime Platform StoragePrimitive)

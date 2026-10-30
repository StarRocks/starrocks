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

set(EXPR_EXTENSION_LIBS
    ExprDict
    ExprTableFunction
    ExprUtility
)

# Expression extensions that are not part of this tree: a checkout may add
# extension_targets.local.cmake next to this file and list(APPEND
# EXPR_EXTENSION_LIBS ...) the targets it declares in CMakeLists.local.cmake, so
# they are linked and force-loaded like the ones above without editing this file.
include(${CMAKE_CURRENT_LIST_DIR}/extension_targets.local.cmake OPTIONAL)

set(EXPR_FORCE_LOAD_LIBS ${EXPR_EXTENSION_LIBS} Expr)

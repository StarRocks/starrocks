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

option(WITH_CLANG_TIDY "Use clang-tidy static analyzer" OFF)

if(${WITH_CLANG_TIDY})
  find_program(CLANG_TIDY_CACHE_BIN NAMES "clang-tidy-cache")
  find_program(
    CLANG_TIDY_BIN NAMES "clang-tidy-17" "clang-tidy-16" "clang-tidy-15"
                         "clang-tidy-14" "clang-tidy-13" "clang-tidy")

  if(CLANG_TIDY_CACHE_BIN)
    set(CLANG_TIDY_PATH
        "${CLANG_TIDY_CACHE_BIN};${CLANG_TIDY_BIN}"
        CACHE STRING "Cache for clang-tidy")
  else()
    set(CLANG_TIDY_PATH "${CLANG_TIDY_BIN}")
  endif()

  if(CLANG_TIDY_PATH)
    message(STATUS "Find clang-tidy: ${CLANG_TIDY_PATH}.")
  else()
    message(FATAL_ERROR "clang-tidy is not found")
  endif()
endif()

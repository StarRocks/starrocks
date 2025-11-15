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

# check glibc version, assign to GLIBC_VERSION

set(CHECK_C_SOURCE_CODE
    "
#include <stdio.h>
#include <gnu/libc-version.h>
int main() {
    const char* version = gnu_get_libc_version();
    printf(\"%s\\n\", version);
    return 0;
}
")
file(WRITE ${CMAKE_BINARY_DIR}/check_c_source.c "${CHECK_C_SOURCE_CODE}")
execute_process(
  COMMAND ${CMAKE_C_COMPILER} ${CMAKE_BINARY_DIR}/check_c_source.c -o
          ${CMAKE_BINARY_DIR}/check_c_source RESULT_VARIABLE compile_result)
if(compile_result EQUAL 0)
  execute_process(
    COMMAND ${CMAKE_BINARY_DIR}/check_c_source
    OUTPUT_VARIABLE GLIBC_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  message(STATUS "GLIBC version: ${GLIBC_VERSION}")
else()
  message(FATAL_ERROR "Failed to get the glibc version")
endif()

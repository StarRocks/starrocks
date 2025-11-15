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

if (ENABLE_MULTI_DYNAMIC_LIBS)
    execute_process(
        COMMAND ${CMAKE_CXX_COMPILER} --print-file-name=libstdc++.so
        OUTPUT_VARIABLE LIBSTDCPP_SO
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )

    get_filename_component(LIBSTDCPP_DIR ${LIBSTDCPP_SO} DIRECTORY)

    message(STATUS "libstdc++ dir: ${LIBSTDCPP_DIR}")

    file(GLOB STDCPP_ALL "${LIBSTDCPP_DIR}/libstdc++.so*")
    list(FILTER STDCPP_ALL EXCLUDE REGEX "\\.py$")

    install(FILES ${STDCPP_ALL} DESTINATION ${OUTPUT_DIR}/lib)


    execute_process(
        COMMAND ${CMAKE_CXX_COMPILER} --print-file-name=libgcc_s.so.1
        OUTPUT_VARIABLE LIBGCC_SO
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )

    get_filename_component(LIBGCC_DIR ${LIBGCC_SO} DIRECTORY)
    file(GLOB LIBGCC_ALL "${LIBGCC_DIR}/libgcc_s.so*")

    install(FILES ${LIBGCC_ALL} DESTINATION ${OUTPUT_DIR}/lib)
endif()
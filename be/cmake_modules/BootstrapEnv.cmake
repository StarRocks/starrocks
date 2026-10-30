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

execute_process(COMMAND uname -s
                OUTPUT_VARIABLE STARROCKS_HOST_SYSTEM_NAME
                OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND uname -m
                OUTPUT_VARIABLE STARROCKS_HOST_SYSTEM_PROCESSOR
                OUTPUT_STRIP_TRAILING_WHITESPACE)

get_filename_component(STARROCKS_BE_DIR "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
get_filename_component(STARROCKS_HOME_DIR "${STARROCKS_BE_DIR}/.." ABSOLUTE)

macro(starrocks_set_env_default ENV_NAME ENV_VALUE ENV_REASON)
    if (NOT DEFINED ENV{${ENV_NAME}} OR "$ENV{${ENV_NAME}}" STREQUAL "")
        set(ENV{${ENV_NAME}} "${ENV_VALUE}")
        message(STATUS "Preconfig default: ${ENV_NAME}=${ENV_VALUE} (${ENV_REASON})")
    endif()
endmacro()

if (STARROCKS_HOST_SYSTEM_NAME STREQUAL "Darwin" AND STARROCKS_HOST_SYSTEM_PROCESSOR STREQUAL "arm64")
    include("${CMAKE_CURRENT_LIST_DIR}/BootstrapDarwinEnv.cmake")
elseif (STARROCKS_HOST_SYSTEM_NAME STREQUAL "Linux")
    include("${CMAKE_CURRENT_LIST_DIR}/BootstrapLinuxEnv.cmake")
endif()

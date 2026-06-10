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

set(STARROCKS_LINUX_THIRDPARTY "${STARROCKS_HOME_DIR}/thirdparty")
set(STARROCKS_LINUX_THIRDPARTY_REASON "repo-local thirdparty")
if (EXISTS "/var/local/thirdparty/installed")
    set(STARROCKS_LINUX_THIRDPARTY "/var/local/thirdparty")
    set(STARROCKS_LINUX_THIRDPARTY_REASON "shared /var/local/thirdparty")
endif()

starrocks_set_env_default(STARROCKS_HOME "${STARROCKS_HOME_DIR}" "repo root")
starrocks_set_env_default(STARROCKS_THIRDPARTY "${STARROCKS_LINUX_THIRDPARTY}" "${STARROCKS_LINUX_THIRDPARTY_REASON}")

find_program(STARROCKS_LINUX_GCC_EXECUTABLE NAMES gcc)
if (NOT STARROCKS_HAS_CMAKE_COMPILER_OVERRIDE AND STARROCKS_LINUX_GCC_EXECUTABLE)
    get_filename_component(STARROCKS_LINUX_GCC_EXECUTABLE_REALPATH "${STARROCKS_LINUX_GCC_EXECUTABLE}" REALPATH)
    get_filename_component(STARROCKS_LINUX_GCC_BIN_DIR "${STARROCKS_LINUX_GCC_EXECUTABLE_REALPATH}" DIRECTORY)
    get_filename_component(STARROCKS_LINUX_GCC_HOME "${STARROCKS_LINUX_GCC_BIN_DIR}/.." ABSOLUTE)
    if (EXISTS "${STARROCKS_LINUX_GCC_HOME}/bin/gcc" AND EXISTS "${STARROCKS_LINUX_GCC_HOME}/bin/g++")
        starrocks_set_env_default(STARROCKS_GCC_HOME "${STARROCKS_LINUX_GCC_HOME}" "system GCC")
    endif()
endif()

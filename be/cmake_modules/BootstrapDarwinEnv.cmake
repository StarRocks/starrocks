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

set(STARROCKS_DARWIN_THIRDPARTY "${STARROCKS_HOME_DIR}/thirdparty")
set(STARROCKS_DARWIN_LLVM_HOME "/opt/homebrew/opt/llvm")

starrocks_set_env_default(STARROCKS_HOME "${STARROCKS_HOME_DIR}" "repo root")
starrocks_set_env_default(STARROCKS_THIRDPARTY "${STARROCKS_DARWIN_THIRDPARTY}" "repo-local thirdparty")

if (NOT STARROCKS_HAS_CMAKE_COMPILER_OVERRIDE AND EXISTS "${STARROCKS_DARWIN_LLVM_HOME}/bin/clang")
    starrocks_set_env_default(STARROCKS_LLVM_HOME "${STARROCKS_DARWIN_LLVM_HOME}" "Homebrew LLVM")
endif()

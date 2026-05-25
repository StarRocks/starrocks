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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/compute_env")

set(COMPUTE_ENV_SORTING_FILES
    sorting/compare_column.cpp
    sorting/merge.cpp
    sorting/merge_cascade.cpp
    sorting/merge_column.cpp
    sorting/merge_path.cpp
    sorting/sort_column.cpp
    sorting/sort_cursor.cpp
    sorting/sort_permute.cpp
)

ADD_BE_LIB(ComputeEnv
    compute_env.cpp
    pipeline/driver_limiter.cpp
    pipeline/observer.cpp
    ${COMPUTE_ENV_SORTING_FILES}
)

target_link_libraries(ComputeEnv PUBLIC
    ExecPrimitive
    ExprCore
    RuntimeEnv
    RuntimeCore
    Platform
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
)

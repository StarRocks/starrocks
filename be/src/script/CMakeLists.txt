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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/script")

set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/script")

set(EXEC_FILES
    script.cpp
    ../thirdparty/wren/wren.c
)

add_library(Script STATIC ${EXEC_FILES})

target_include_directories(Script PRIVATE ${SRC_DIR}/script ${SRC_DIR}/thirdparty/wren ${SRC_DIR}/thirdparty/wrenbind17)

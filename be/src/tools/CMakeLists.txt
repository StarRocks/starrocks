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

if (CLANG_TIDY_PATH)
    set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif()

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/tools")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/tools")

add_library(Tools STATIC
    meta_tool.cpp
)

# libmockjvm will be build as shared lib. we use it to mock a libjvm.so to 
# make sure BE can start without real java runtime
add_library(mockjvm SHARED mock_jvm.c)
# define `A` SUNWprivate_1.1 symbol to mockjvm to suppress some warning 
target_link_options(mockjvm PRIVATE "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/mock_jvm.lds")
install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/)
install(TARGETS mockjvm
    DESTINATION ${OUTPUT_DIR}/lib/)

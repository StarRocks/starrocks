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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/service")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/service")

add_subdirectory(service_be)

set(EXEC_FILES
    backend_options.cpp
    backend_base.cpp
    internal_service.cpp
    staros_worker.cpp
    mem_hook.cpp
    service.cpp
)

add_library(Service
        ${EXEC_FILES}
)

# only build starrocks_be when TEST is off
if (NOT ${MAKE_TEST} STREQUAL "ON")
    add_executable(starrocks_be
        starrocks_main.cpp
        )

    # This permits libraries loaded by dlopen to link to the symbols in the program.
    # set_target_properties(starrocks_be PROPERTIES LINK_FLAGS -pthread)

    target_link_libraries(starrocks_be
        ${STARROCKS_LINK_LIBS}
        )

    install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/)

    install(TARGETS starrocks_be
        DESTINATION ${OUTPUT_DIR}/lib/)
endif()

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

set(SCRIPT_TEST_FILES
    ../base/cxa_throw_wrap.cpp
    command_executor_test.cpp
    key_cache_script_test.cpp
)

set(SCRIPT_TEST_LINK_LIBS
    Script
    ${TEST_LINK_LIBS}
)

add_executable(script_test ${SCRIPT_TEST_FILES})
target_link_libraries(script_test ${SCRIPT_TEST_LINK_LIBS} gtest_main)
STARROCKS_FORCE_LOAD_LIBS(script_test Exprs ExprCore)
set_target_properties(script_test PROPERTIES COMPILE_FLAGS "-fno-access-control")

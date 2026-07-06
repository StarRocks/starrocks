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

add_executable(module_bootstrap_test module_bootstrap_test.cpp)
target_link_be_test_cxa_throw_wrap(module_bootstrap_test)
target_link_libraries(module_bootstrap_test ModuleBootstrap ${TEST_LINK_LIBS} gtest_main)
set_target_properties(module_bootstrap_test PROPERTIES COMPILE_FLAGS "-fno-access-control")

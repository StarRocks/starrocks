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

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/test/testutil")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/test/testutil")

# TestUtil contains helper implementations used only by unit tests. Keep it
# under be/test and link it through TEST_LINK_LIBS instead of production links.
add_library(TestUtil
    chunk_assert.cpp
    desc_tbl_builder.cc
    function_utils.cpp
    schema_test_helper.cpp
    tablet_test_helper.cpp
    exprs_test_helper.cpp
)

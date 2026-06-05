// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include "exec/pipeline/primitives/fragment_runtime_state.h"

namespace starrocks::pipeline {

TEST(FragmentRuntimeStateTest, StoresFragmentInstanceId) {
    FragmentRuntimeState state;
    TUniqueId fragment_instance_id;
    fragment_instance_id.hi = 3;
    fragment_instance_id.lo = 4;

    state.set_fragment_instance_id(fragment_instance_id);

    EXPECT_EQ(3, state.fragment_instance_id().hi);
    EXPECT_EQ(4, state.fragment_instance_id().lo);
}

} // namespace starrocks::pipeline

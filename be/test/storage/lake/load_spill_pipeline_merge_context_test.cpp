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

#include "storage/lake/load_spill_pipeline_merge_context.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(LoadSpillPipelineMergeContextTest, quit_flag_is_mutable) {
    LoadSpillPipelineMergeContext context(nullptr);

    auto* quit_flag = context.quit_flag();
    ASSERT_NE(quit_flag, nullptr);
    ASSERT_FALSE(quit_flag->load());

    quit_flag->store(true);
    ASSERT_TRUE(quit_flag->load());
}

TEST(LoadSpillPipelineMergeContextTest, tracks_ready_slot_ranges) {
    LoadSpillPipelineMergeContext context(nullptr);

    ASSERT_FALSE(context.is_slot_ready(0, 2));

    context.mark_slot_ready(0);
    context.mark_slot_ready(1);
    ASSERT_TRUE(context.is_slot_ready(0, 1));
    ASSERT_FALSE(context.is_slot_ready(0, 2));

    context.mark_slot_ready(2);
    ASSERT_TRUE(context.is_slot_ready(0, 2));
    ASSERT_TRUE(context.is_slot_ready(-1, 0));
}

TEST(LoadSpillPipelineMergeContextTest, add_merge_task_accepts_empty_task_handle) {
    LoadSpillPipelineMergeContext context(nullptr);

    context.add_merge_task(nullptr);
}

} // namespace starrocks

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

#include "exec/workgroup/mem_tracker_manager.h"

#include <gtest/gtest.h>

#include "base/testutil/parallel_test.h"
#include "exec/workgroup/work_group.h"

namespace starrocks::workgroup {
PARALLEL_TEST(MemTrackerMangerTest, test_mem_tracker_for_default_mem_pool) {
    MemTrackerManager manager;
    const auto work_group{std::make_shared<WorkGroup>("default_wg", 123, WorkGroup::DEFAULT_VERSION, 1, 0.5, 0, 0.9,
                                                      WorkGroupType::WG_DEFAULT, WorkGroup::DEFAULT_MEM_POOL)};

    const auto tracker{manager.register_workgroup(work_group)};
    ASSERT_EQ(tracker, GlobalEnv::GetInstance()->query_pool_mem_tracker_shared());
}

PARALLEL_TEST(MemTrackerMangerTest, test_mem_tracker_for_custom_mem_pool) {
    MemTrackerManager manager;
    const auto work_group1{std::make_shared<WorkGroup>("wg_1", 123, WorkGroup::DEFAULT_VERSION, 1, 0.5, 0, 0.9,
                                                       WorkGroupType::WG_DEFAULT, "test_pool")};
    const auto work_group2{std::make_shared<WorkGroup>("wg_2", 134, WorkGroup::DEFAULT_VERSION, 1, 0.5, 0, 0.9,
                                                       WorkGroupType::WG_DEFAULT, "test_pool")};
    const auto work_group3{std::make_shared<WorkGroup>("wg_2", 134, WorkGroup::DEFAULT_VERSION, 1, 0.5, 0, 0.9,
                                                       WorkGroupType::WG_DEFAULT, "other_pool")};

    const MemTrackerPtr parent_workgroup_1 = manager.register_workgroup(work_group1);
    const MemTrackerPtr parent_workgroup_2 = manager.register_workgroup(work_group2);
    const MemTrackerPtr parent_workgroup_3 = manager.register_workgroup(work_group3);

    ASSERT_EQ(parent_workgroup_1, parent_workgroup_2);
    ASSERT_NE(parent_workgroup_1, parent_workgroup_3);
}
PARALLEL_TEST(MemTrackerMangerTest, test_mem_tracker_for_custom_mem_pool_overwrite) {
    MemTrackerManager manager;
    const auto work_group1{std::make_shared<WorkGroup>("wg_1", 123, WorkGroup::DEFAULT_VERSION, 1, 0.5, 0, 0.9,
                                                       WorkGroupType::WG_DEFAULT, "test_pool")};
    const auto work_group2{std::make_shared<WorkGroup>("wg_2", 134, WorkGroup::DEFAULT_VERSION, 1, 0.7, 0, 0.9,
                                                       WorkGroupType::WG_DEFAULT, "test_pool")};

    const MemTrackerPtr parent_workgroup1 = manager.register_workgroup(work_group1);
    const MemTrackerPtr parent_workgroup2 = manager.register_workgroup(work_group2);

    ASSERT_NE(parent_workgroup1, parent_workgroup2);
}
} // namespace starrocks::workgroup

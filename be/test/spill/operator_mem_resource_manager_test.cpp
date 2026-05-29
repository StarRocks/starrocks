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

#include "exec/spill/operator_mem_resource_manager.h"

#include <gtest/gtest.h>

#include <algorithm>

#include "base/uid_util.h"
#include "runtime/runtime_state.h"

namespace starrocks::spill {
class OperatorMemoryResourceManagerTest : public ::testing::Test {
public:
    void SetUp() override { dummy_query_id = generate_uuid(); }

protected:
    RuntimeState _dummy_state;
    TUniqueId dummy_query_id;
    GlobalSpillManager _global_mgr;
    DirManager _dir_mgr;
};

TEST_F(OperatorMemoryResourceManagerTest, test_inc) {
    QuerySpillManager query_spill_manager_1(dummy_query_id, &_global_mgr, &_dir_mgr);

    OperatorMemoryResourceManager op_mem_res_mgr;
    op_mem_res_mgr.prepare(&query_spill_manager_1, true, true, 128);
    EXPECT_EQ(_global_mgr.spillable_operators(), 1);
    EXPECT_EQ(_global_mgr.spill_expected_reserved_bytes(), 128);

    OperatorMemoryResourceManager op_mem_res_mgr_2;
    op_mem_res_mgr_2.prepare(&query_spill_manager_1, false, true, 256);
    EXPECT_EQ(_global_mgr.spillable_operators(), 1);
    EXPECT_EQ(_global_mgr.spill_expected_reserved_bytes(), 384);

    op_mem_res_mgr.close();
    EXPECT_EQ(_global_mgr.spillable_operators(), 0);
    EXPECT_EQ(_global_mgr.spill_expected_reserved_bytes(), 256);

    op_mem_res_mgr_2.close();
    EXPECT_EQ(_global_mgr.spillable_operators(), 0);
    EXPECT_EQ(_global_mgr.spill_expected_reserved_bytes(), 0);
}

TEST_F(OperatorMemoryResourceManagerTest, test_low_memory_transition_is_idempotent) {
    QuerySpillManager query_spill_manager(dummy_query_id, &_global_mgr, &_dir_mgr);
    OperatorMemoryResourceManager op_mem_res_mgr;
    op_mem_res_mgr.prepare(&query_spill_manager, false, true, 64);

    EXPECT_TRUE(op_mem_res_mgr.releaseable());
    EXPECT_TRUE(op_mem_res_mgr.enter_low_memory_mode());
    EXPECT_FALSE(op_mem_res_mgr.releaseable());
    EXPECT_FALSE(op_mem_res_mgr.enter_low_memory_mode());
}

TEST_F(OperatorMemoryResourceManagerTest, test_destructor_releases_reserved_resources) {
    QuerySpillManager query_spill_manager(dummy_query_id, &_global_mgr, &_dir_mgr);
    {
        OperatorMemoryResourceManager op_mem_res_mgr;
        op_mem_res_mgr.prepare(&query_spill_manager, true, true, 128);
        EXPECT_EQ(_global_mgr.spillable_operators(), 1);
        EXPECT_EQ(_global_mgr.spill_expected_reserved_bytes(), 128);
    }

    EXPECT_EQ(_global_mgr.spillable_operators(), 0);
    EXPECT_EQ(_global_mgr.spill_expected_reserved_bytes(), 0);
}

TEST_F(OperatorMemoryResourceManagerTest, test_compute_available_memory_bytes_matches_runtime_state) {
    const size_t expected =
            std::min<size_t>(std::max<size_t>(_dummy_state.spill_mem_table_size() * _dummy_state.spill_mem_table_num(),
                                              _dummy_state.spill_operator_min_bytes()),
                             _dummy_state.spill_operator_max_bytes());

    EXPECT_EQ(expected, OperatorMemoryResourceManager::compute_available_memory_bytes(_dummy_state));
}

} // namespace starrocks::spill

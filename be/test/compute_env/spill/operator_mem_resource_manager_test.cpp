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

#include "compute_env/spill/operator_mem_resource_manager.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>

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

// Regression for a heap-use-after-free during fragment teardown.
//
// OperatorMemoryResourceManager stores a RAW pointer to the QueryContext-owned
// QuerySpillManager. When an operator is closed while its query is still alive
// (the normal teardown ordering), close() must drop that raw pointer so the
// manager's destructor -- which runs reset() again -- does not dereference the
// spill manager after the QueryContext has been reclaimed. On branches where
// close() does NOT drop the pointer, the destructor's reset() dereferences freed
// memory (ASAN: heap-use-after-free in ResGuard::reset()).
TEST_F(OperatorMemoryResourceManagerTest, test_close_drops_query_spill_manager_pointer) {
    auto query_spill_manager = std::make_unique<QuerySpillManager>(dummy_query_id, &_global_mgr, &_dir_mgr);

    OperatorMemoryResourceManager op_mem_res_mgr;
    op_mem_res_mgr.prepare(query_spill_manager.get(), true, true, 128);
    EXPECT_EQ(query_spill_manager.get(), op_mem_res_mgr.query_spill_manager());

    // Operator closes while the query (and its spill manager) is still alive.
    op_mem_res_mgr.close();
    EXPECT_EQ(nullptr, op_mem_res_mgr.query_spill_manager());
    EXPECT_EQ(_global_mgr.spillable_operators(), 0);
    EXPECT_EQ(_global_mgr.spill_expected_reserved_bytes(), 0);

    // Query teardown frees the spill manager that the operator pointed into.
    query_spill_manager.reset();

    // op_mem_res_mgr's destructor runs close()->reset() again at scope exit;
    // because the raw pointer was dropped above, this is a no-op rather than a UAF.
}

} // namespace starrocks::spill

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

#include "base/testutil/parallel_test.h"
#include "exec/workgroup/work_group.h"
#include "runtime/mem_tracker.h"

namespace starrocks::workgroup {
TWorkGroup create_twg(const int64_t id, const int64_t version, const std::string& name, const std::string& mem_pool,
                      const double mem_limit) {
    TWorkGroup twg;
    twg.__set_id(id);
    twg.__set_version(version);
    twg.__set_name(name);
    twg.__set_mem_pool(mem_pool);
    twg.__set_mem_limit(mem_limit);
    return twg;
}

TWorkGroupOp make_twg_op(const TWorkGroup& twg, const TWorkGroupOpType::type op_type) {
    TWorkGroupOp op;
    op.__set_workgroup(twg);
    op.__set_op_type(op_type);
    return op;
}

PARALLEL_TEST(WorkGroupManagerTest, add_workgroups_different_mem_pools) {
    PipelineExecutorSetConfig config{10, 1, 1, 1, CpuUtil::CpuIds{}, false, false, nullptr};
    auto _manager = std::make_unique<WorkGroupManager>(config);

    {
        auto wg1 = std::make_shared<WorkGroup>(create_twg(102, 1, "wg", "test_pool", 0.5));
        auto wg2 = std::make_shared<WorkGroup>(create_twg(103, 1, "wg2", "test_pool1", 0.5));
        auto wg3 = std::make_shared<WorkGroup>(create_twg(104, 1, "wg3", WorkGroup::DEFAULT_MEM_POOL, 0.5));

        _manager->add_workgroup(wg1);
        _manager->add_workgroup(wg2);
        _manager->add_workgroup(wg3);

        auto workgroups = _manager->list_workgroups();

        ASSERT_EQ(3, workgroups.size());
        EXPECT_NE(wg2->mem_tracker()->parent(), wg1->mem_tracker()->parent());
        EXPECT_NE(wg2->mem_tracker()->parent(), wg3->mem_tracker()->parent());
        EXPECT_NE(wg1->mem_tracker()->parent(), wg3->mem_tracker()->parent());

        EXPECT_EQ(wg1->mem_tracker()->parent()->type(), MemTrackerType::RESOURCE_GROUP_SHARED_MEMORY_POOL);
        EXPECT_EQ(wg2->mem_tracker()->parent()->type(), MemTrackerType::RESOURCE_GROUP_SHARED_MEMORY_POOL);
        EXPECT_EQ(wg3->mem_tracker()->parent()->type(), MemTrackerType::QUERY_POOL);
    }
    _manager->destroy();
}

PARALLEL_TEST(WorkGroupManagerTest, add_workgroups_same_mem_pools) {
    PipelineExecutorSetConfig config{10, 1, 1, 1, CpuUtil::CpuIds{}, false, false, nullptr};
    auto _manager = std::make_unique<WorkGroupManager>(config);

    {
        auto wg1 = std::make_shared<WorkGroup>(create_twg(105, 1, "wg5", "test_pool", 0.5));
        auto wg2 = std::make_shared<WorkGroup>(create_twg(106, 1, "wg6", "test_pool", 0.5));
        auto wg3 = std::make_shared<WorkGroup>(create_twg(107, 1, "wg7", WorkGroup::DEFAULT_MEM_POOL, 0.5));

        _manager->add_workgroup(wg1);
        _manager->add_workgroup(wg2);
        _manager->add_workgroup(wg3);

        auto workgroups = _manager->list_workgroups();

        ASSERT_EQ(3, workgroups.size());
        EXPECT_EQ(wg2->mem_tracker()->parent(), wg1->mem_tracker()->parent());
        EXPECT_EQ(wg2->mem_tracker()->parent()->type(), MemTrackerType::RESOURCE_GROUP_SHARED_MEMORY_POOL);
        EXPECT_EQ(wg2->mem_limit_bytes(), wg2->mem_tracker()->parent()->limit());

        EXPECT_NE(wg2->mem_tracker()->parent(), wg3->mem_tracker()->parent());
        EXPECT_EQ(wg3->mem_tracker()->parent()->type(), MemTrackerType::QUERY_POOL);
    }
    _manager->destroy();
}

PARALLEL_TEST(WorkGroupManagerTest, test_if_unused_memory_pools_are_cleaned_up) {
    PipelineExecutorSetConfig config{10, 1, 1, 1, CpuUtil::CpuIds{}, false, false, nullptr};
    auto _manager = std::make_unique<WorkGroupManager>(config);
    _manager->set_workgroup_expiration_time(std::chrono::seconds(0));
    {
        auto twg1 = create_twg(110, 1, "wg110", "test_pool_2", 0.5);
        auto twg2 = create_twg(111, 1, "wg111", "test_pool_2", 0.5);
        auto twg3 = create_twg(112, 1, "wg112", WorkGroup::DEFAULT_MEM_POOL, 0.5);

        std::vector create_operations{
                make_twg_op(twg1, TWorkGroupOpType::WORKGROUP_OP_CREATE),
                make_twg_op(twg2, TWorkGroupOpType::WORKGROUP_OP_CREATE),
                make_twg_op(twg3, TWorkGroupOpType::WORKGROUP_OP_CREATE),
        };

        _manager->apply(create_operations);

        EXPECT_EQ(_manager->list_memory_pools().size(), 2);

        // Version must be strictly larger, otherwise workgroup will not be deleted
        twg1.version++;
        twg2.version++;

        std::vector delete_operations{make_twg_op(twg1, TWorkGroupOpType::WORKGROUP_OP_DELETE),
                                      make_twg_op(twg2, TWorkGroupOpType::WORKGROUP_OP_DELETE)};

        _manager->apply(delete_operations);
        std::this_thread::sleep_for(std::chrono::seconds(1));
        // The expired workgroups will only get erased in the next call to apply
        _manager->apply({});

        EXPECT_EQ(_manager->list_memory_pools().size(), 1);
    }
    _manager->destroy();
}
} // namespace starrocks::workgroup

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

#include "runtime/descriptors.h"

#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks {

class HiveTableDescriptorTest : public ::testing::Test {
public:
    void SetUp() override {
        _exec_env = ExecEnv::GetInstance();
        _query_pool = std::make_unique<ObjectPool>();

        TTableDescriptor tdesc;
        tdesc.id = 1;
        tdesc.tableType = TTableType::HDFS_TABLE;
        _table_desc = _query_pool->add(new HdfsTableDescriptor(tdesc, _query_pool.get()));
    }

    std::shared_ptr<RuntimeState> _make_runtime_state() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        auto rs = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, _exec_env);
        TUniqueId query_id;
        rs->init_mem_trackers(query_id);
        return rs;
    }

    static THdfsPartition _make_partition_value() {
        THdfsPartition p;
        p.file_format = THdfsFileFormat::TEXT;
        p.location.suffix = "";
        return p;
    }

protected:
    ExecEnv* _exec_env = nullptr;
    std::unique_ptr<ObjectPool> _query_pool;
    HdfsTableDescriptor* _table_desc = nullptr;
};

// Regression test for a heap-use-after-free in HiveTableDescriptor::add_partition_value.
//
// When two fragment instances of the same query share one HiveTableDescriptor (e.g. a
// Hive/Iceberg table referenced multiple times via CTE), each fragment instance calls
// add_partition_value, which stores the partition descriptor pointer in the
// HiveTableDescriptor::_partition_id_to_desc_map shared across fragments.
//
// The previous implementation used `runtime_state->obj_pool()` (per-fragment) to allocate
// the descriptor. When fragment A finished and its pool destructed, the inserted
// descriptor was freed, but the pointer remained in the shared map. Fragment B then read
// the dangling pointer in the duplicate-check comparison, causing UAF.
//
// The fix uses `RuntimeStateHelper::global_obj_pool(runtime_state)` (per-query) so the descriptor's
// lifetime matches the shared map. This test simulates that fixed behavior: both
// fragments share the same query-level ObjectPool, so dropping a fragment's
// RuntimeState does not free the partition descriptor. Under ASAN this exercises the
// memory paths and must complete without errors.
TEST_F(HiveTableDescriptorTest, AddPartitionValueLifetimeAcrossPools) {
    constexpr int64_t partition_id = 42;
    THdfsPartition thrift_partition = _make_partition_value();

    {
        // Fragment instance A: allocate from the shared query-level pool, then finish.
        auto rs_a = _make_runtime_state();
        ASSERT_OK(_table_desc->add_partition_value(rs_a.get(), _query_pool.get(), partition_id, thrift_partition));
    }
    // rs_a is destroyed here. Because the partition descriptor was allocated from
    // _query_pool (the long-lived per-query pool), the map entry remains valid.

    {
        // Fragment instance B: same partition_id. add_partition_value finds the existing
        // entry, compares thrift_partition_key_exprs against the (still-alive) old entry,
        // and returns OK without inserting a duplicate.
        auto rs_b = _make_runtime_state();
        ASSERT_OK(_table_desc->add_partition_value(rs_b.get(), _query_pool.get(), partition_id, thrift_partition));
    }

    // The partition descriptor must still be accessible after both fragment instances
    // have been destroyed; it is owned by the query-level pool.
    HdfsPartitionDescriptor* partition = _table_desc->get_partition(partition_id);
    ASSERT_NE(nullptr, partition);
}

} // namespace starrocks
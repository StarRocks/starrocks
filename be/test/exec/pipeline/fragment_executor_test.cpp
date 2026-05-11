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

#include "base/testutil/assert.h"
#include "common/config.h"
#include "exec/pipeline/query_context.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/casts.h"
#include "runtime/descriptors.h"
#include "runtime/descriptors_ext.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/runtime_state_helper.h"

namespace starrocks::pipeline {

// Forward declaration: the helper lives in fragment_executor.cpp. It is non-static so
// this test can call it directly and pin the contract that partition descriptors are
// allocated from the query-level ObjectPool (so they outlive any single fragment).
Status add_scan_ranges_partition_values(RuntimeState* runtime_state, const std::vector<TScanRangeParams>& scan_ranges);

class FragmentExecutorPartitionTest : public ::testing::Test {
public:
    void SetUp() override { _exec_env = ExecEnv::GetInstance(); }

    std::shared_ptr<RuntimeState> _make_runtime_state(QueryContext* query_ctx, DescriptorTbl* desc_tbl) {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        auto rs = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, _exec_env);
        TUniqueId query_id;
        rs->init_mem_trackers(query_id);
        rs->set_query_ctx(query_ctx);
        rs->set_desc_tbl(desc_tbl);
        return rs;
    }

    static TScanRangeParams _make_scan_range(TTableId table_id, int64_t partition_id) {
        TScanRangeParams params;
        TScanRange& sr = params.scan_range;
        sr.__isset.hdfs_scan_range = true;
        sr.hdfs_scan_range.__isset.table_id = true;
        sr.hdfs_scan_range.table_id = table_id;
        sr.hdfs_scan_range.__isset.partition_id = true;
        sr.hdfs_scan_range.partition_id = partition_id;
        sr.hdfs_scan_range.__isset.partition_value = true;
        sr.hdfs_scan_range.partition_value.file_format = THdfsFileFormat::TEXT;
        sr.hdfs_scan_range.partition_value.location.suffix = "";
        return params;
    }

protected:
    ExecEnv* _exec_env = nullptr;
};

// Regression test for a heap-use-after-free in add_scan_ranges_partition_values.
//
// HiveTableDescriptor::_partition_id_to_desc_map is shared across all fragment
// instances of the same query. The partition descriptors stored in that map must
// therefore outlive any single fragment instance.
//
// Before the fix, add_scan_ranges_partition_values allocated partition descriptors
// from `runtime_state->obj_pool()` (per-fragment). When one fragment finished and its
// pool destructed, the descriptors it inserted into the shared map were freed,
// leaving dangling pointers. A sibling fragment then hit UAF on the duplicate-check
// comparison `partition->thrift_partition_key_exprs() != old_partition->...`.
//
// The fix switched to `RuntimeStateHelper::global_obj_pool(runtime_state)`
// (per-query). This test
// asserts that contract: after a fragment-level RuntimeState (and its per-fragment
// pool) is destroyed, the partition descriptor it inserted remains alive in the
// query-level pool. Under ASAN, reading the descriptor's heap fields after the
// fragment's pool dies would otherwise be reported as heap-use-after-free.
TEST_F(FragmentExecutorPartitionTest, PartitionDescriptorOutlivesFragmentPool) {
    constexpr TTableId kTableId = 100;
    constexpr int64_t kPartitionId = 42;

    // Build a TDescriptorTable that contains a single HDFS table descriptor.
    TDescriptorTable thrift_tbl;
    {
        TTableDescriptor tt;
        tt.id = kTableId;
        tt.tableType = TTableType::HDFS_TABLE;
        thrift_tbl.tableDescriptors.push_back(tt);
    }

    // The QueryContext owns the per-query ObjectPool; the partition descriptor must
    // ultimately end up here so it survives fragment teardown.
    auto query_ctx = std::make_shared<QueryContext>();

    // Build the DescriptorTbl in the query-level pool, matching the production path
    // taken by FragmentExecutor._prepare_runtime_state for is_cached==false.
    DescriptorTbl* desc_tbl = nullptr;
    {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        RuntimeState bootstrap_rs(fragment_id, query_options, query_globals, _exec_env);
        ASSERT_OK(DescriptorTbl::create(&bootstrap_rs, query_ctx->object_pool(), thrift_tbl, &desc_tbl,
                                        config::vector_chunk_size));
    }

    auto* table_desc = desc_tbl->get_table_descriptor(kTableId);
    ASSERT_NE(nullptr, table_desc);
    auto* hive_table = down_cast<HiveTableDescriptor*>(table_desc);

    std::vector<TScanRangeParams> scan_ranges = {_make_scan_range(kTableId, kPartitionId)};

    // Run add_scan_ranges_partition_values inside a fragment-level scope. The
    // RuntimeState (and its per-fragment obj_pool) is destroyed when the scope exits.
    {
        auto rs = _make_runtime_state(query_ctx.get(), desc_tbl);
        // Sanity check: the contract being asserted only holds when the two pools
        // really differ. If a future refactor unified them, the assertion below
        // wouldn't tell us anything useful, so guard it.
        ASSERT_NE(rs->obj_pool(), RuntimeStateHelper::global_obj_pool(rs.get()));

        ASSERT_OK(add_scan_ranges_partition_values(rs.get(), scan_ranges));
    }

    // After the fragment is gone, the partition descriptor must still be accessible
    // and its heap-owned fields must not have been freed.
    HdfsPartitionDescriptor* partition = hive_table->get_partition(kPartitionId);
    ASSERT_NE(nullptr, partition);
    // Touching the vector trips ASAN if the descriptor backing memory was freed.
    (void)partition->thrift_partition_key_exprs().size();
}

} // namespace starrocks::pipeline
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

#include "orchestration/fragment_executor.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "common/config_exec_fwd.h"
#include "connector/hive/hive_connector.h"
#include "exec/connector_scan_node.h"
#include "exec/exec_env.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/runtime/fragment_context.h"
#include "exec/runtime/group_execution/execution_group.h"
#include "exec/runtime/group_execution/execution_group_builder.h"
#include "exec/runtime/pipeline.h"
#include "exec/runtime_compat/runtime_state_helper.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/casts.h"
#include "runtime/descriptors.h"
#include "runtime/descriptors_ext.h"
#include "runtime/runtime_state.h"

namespace starrocks::orchestration {

using pipeline::QueryContext;

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
        rs->set_query_ctx(query_ctx, &query_ctx->query_runtime_state(), query_ctx->object_pool());
        rs->set_desc_tbl(desc_tbl);
        return rs;
    }

    static TScanRangeParams _make_scan_range(TTableId table_id, int64_t partition_id) {
        TScanRangeParams params;
        TScanRange& sr = params.scan_range;
        sr.__isset.hdfs_scan_range = true;
        sr.hdfs_scan_range.__set_table_id(table_id);
        sr.hdfs_scan_range.__set_partition_id(partition_id);
        THdfsPartition pv;
        pv.__set_file_format(THdfsFileFormat::TEXT);
        THdfsPartitionLocation loc;
        loc.__set_suffix("");
        pv.__set_location(loc);
        sr.hdfs_scan_range.__set_partition_value(pv);
        return params;
    }

protected:
    ExecEnv* _exec_env = nullptr;
};

// Regression test for a heap-use-after-free in add_scan_ranges_partition_values.
//
// HiveTableDescriptor::_partition_id_to_desc_map is shared across all fragment
// instances of the same query (the descriptor itself lives in the query-level
// ObjectPool via DescriptorTbl). The partition descriptors stored in that map
// must therefore outlive any single fragment instance.
//
// Before the fix, add_scan_ranges_partition_values allocated partition descriptors
// from `runtime_state->obj_pool()` (per-fragment). When one fragment finished and
// tore down its pool, the descriptors it inserted into the shared map were freed,
// leaving dangling pointers. A sibling fragment then hit UAF on the duplicate-check
// path `partition->thrift_partition_key_exprs() != old_partition->...`.
//
// The fix switches the allocation to `RuntimeStateHelper::global_obj_pool(rs)`
// (query-level). Together with the earlier refactor that made HdfsPartitionDescriptor
// thrift-only (no fragment-bound ExprContexts on the descriptor), this lets the
// descriptor live for the whole query without smuggling fragment-scoped state.
//
// This test asserts: after a fragment-level RuntimeState (and its per-fragment
// pool) is destroyed, the partition descriptor it inserted via this helper remains
// alive in the query-level pool and its thrift fields are still readable. Under
// ASAN, reading those fields after the fragment's pool dies would otherwise be
// reported as heap-use-after-free.
TEST_F(FragmentExecutorPartitionTest, PartitionDescriptorOutlivesFragmentPool) {
    constexpr TTableId kTableId = 100;
    constexpr int64_t kPartitionId = 42;

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
    // RuntimeState (and its per-fragment obj_pool) is destroyed when the scope
    // exits, simulating one fragment finishing while a sibling continues.
    {
        auto rs = _make_runtime_state(query_ctx.get(), desc_tbl);
        // Sanity: the contract being asserted only holds when the two pools really
        // differ. If a future refactor unified them, the assertion below would not
        // tell us anything useful, so guard it.
        ASSERT_NE(rs->obj_pool(), RuntimeStateHelper::global_obj_pool(rs.get()));

        ASSERT_OK(FragmentExecutor::add_scan_ranges_partition_values(rs.get(), scan_ranges));
    }

    // After the fragment is gone, the partition descriptor must still be reachable
    // through the shared map and its heap-owned fields must not have been freed.
    HdfsPartitionDescriptor* partition = hive_table->get_partition(kPartitionId);
    ASSERT_NE(nullptr, partition);
    // Reading the vector trips ASAN if the descriptor's backing memory was freed.
    (void)partition->thrift_partition_key_exprs().size();
}

namespace {
// Minimal concrete ScanOperatorFactory: apply_refreshed_cloud_configurations only reads
// plan_node_id() and scan_node(), so the create/prepare hooks are never exercised.
class StubScanOperatorFactory final : public pipeline::ScanOperatorFactory {
public:
    StubScanOperatorFactory(int32_t id, ScanNode* scan_node) : pipeline::ScanOperatorFactory(id, scan_node) {}
    Status do_prepare(RuntimeState*) override { return Status::OK(); }
    void do_close(RuntimeState*) override {}
    pipeline::OperatorPtr do_create(int32_t, int32_t) override { return nullptr; }
};
} // namespace

// A refreshed cloud configuration keyed by scan-node id reaches the matching connector scan
// provider through the pipeline -> scan factory -> ConnectorScanNode -> provider chain.
TEST_F(FragmentExecutorPartitionTest, AppliesRefreshedCloudConfigurationToConnectorProvider) {
    constexpr int32_t kPlanNodeId = 7;

    TUniqueId fragment_id;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    auto runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, _exec_env);
    TUniqueId mem_id;
    runtime_state->init_mem_trackers(mem_id);

    TDescriptorTable thrift_tbl;
    DescriptorTbl* descs = nullptr;
    ASSERT_OK(DescriptorTbl::create(runtime_state.get(), runtime_state->obj_pool(), thrift_tbl, &descs,
                                    config::vector_chunk_size));

    TPlanNode tnode;
    tnode.__set_node_id(kPlanNodeId);
    tnode.__set_node_type(TPlanNodeType::HDFS_SCAN_NODE);
    tnode.__set_row_tuples(std::vector<TTupleId>{});
    tnode.__set_limit(-1);
    TConnectorScanNode connector_scan_node;
    connector_scan_node.connector_name = connector::Connector::HIVE;
    tnode.__set_connector_scan_node(connector_scan_node);

    auto scan_node = std::make_shared<ConnectorScanNode>(runtime_state->obj_pool(), tnode, *descs);
    // Install a HiveDataSourceProvider directly rather than via the connector registry, which may
    // not be force-loaded in this test binary. -fno-access-control lets the test set the member.
    THdfsScanNode hdfs_scan_node;
    auto hive_provider = std::make_unique<connector::HiveDataSourceProvider>(kPlanNodeId, hdfs_scan_node);
    auto* hive_provider_raw = hive_provider.get();
    scan_node->_data_source_provider = std::move(hive_provider);

    auto scan_factory = std::make_shared<StubScanOperatorFactory>(0, scan_node.get());

    // iterate_pipeline() walks the execution groups, not the raw pipeline list, so the pipeline
    // must be registered in a non-empty group (set_pipelines drops empty groups).
    auto fragment_ctx = std::make_shared<pipeline::FragmentContext>();
    auto pipe = std::make_shared<pipeline::Pipeline>(0, pipeline::OpFactories{scan_factory}, nullptr);
    pipeline::Pipelines pipelines;
    pipelines.emplace_back(pipe);
    auto exec_group = pipeline::ExecutionGroupBuilder::create_normal_exec_group();
    exec_group->add_pipeline(pipe.get());
    pipeline::ExecutionGroups exec_groups;
    exec_groups.emplace_back(std::move(exec_group));
    fragment_ctx->set_pipelines(std::move(exec_groups), std::move(pipelines));

    TCloudConfiguration refreshed;
    refreshed.__set_cloud_properties({{"fs.gs.temporary.access.token", "fresh"}});
    std::map<TPlanNodeId, TCloudConfiguration> node_to_cc{{kPlanNodeId, refreshed}};

    FragmentExecutor::apply_refreshed_cloud_configurations(fragment_ctx.get(), node_to_cc);

    TCloudConfiguration holder;
    const TCloudConfiguration* effective = hive_provider_raw->effective_cloud_configuration(&holder);
    ASSERT_EQ(effective, &holder);
    EXPECT_EQ(effective->cloud_properties.at("fs.gs.temporary.access.token"), "fresh");
}

} // namespace starrocks::orchestration

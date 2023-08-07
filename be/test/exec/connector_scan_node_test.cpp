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

#include "exec/connector_scan_node.h"

#include <gtest/gtest.h>

#include "exec/pipeline/scan/morsel.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks {

class ConnectorScanNodeTest : public ::testing::Test {
public:
    void SetUp() override {
        config::enable_system_metrics = false;
        config::enable_metric_calculator = false;

        _exec_env = ExecEnv::GetInstance();

        create_runtime_state();
        _object_pool = _runtime_state->obj_pool();
    }
    void TearDown() override {}

protected:
    void create_runtime_state();
    DescriptorTbl* create_table_desc();

    std::shared_ptr<TPlanNode> create_tplan_node_cloud();
    std::vector<TScanRangeParams> create_scan_ranges_cloud(size_t num);

    std::shared_ptr<TPlanNode> create_tplan_node_hive();
    std::vector<TScanRangeParams> create_scan_ranges_hive(size_t num);

    std::shared_ptr<RuntimeState> _runtime_state = nullptr;
    OlapTableDescriptor* _table_desc = nullptr;
    ObjectPool* _object_pool = nullptr;
    std::shared_ptr<MemTracker> _mem_tracker = nullptr;
    ExecEnv* _exec_env = nullptr;
};

void ConnectorScanNodeTest::create_runtime_state() {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, _exec_env);
    TUniqueId id;
    _mem_tracker = std::make_shared<MemTracker>(-1, "connector scan");
    _runtime_state->init_mem_trackers(id);
}

DescriptorTbl* ConnectorScanNodeTest::create_table_desc() {
    /// Init DescriptorTable
    TDescriptorTableBuilder desc_tbl_builder;
    TTupleDescriptorBuilder tuple_desc_builder;
    TypeDescriptor t(TYPE_INT);
    TSlotDescriptorBuilder slot_desc_builder;
    slot_desc_builder.type(t).length(t.len).precision(t.precision).scale(t.scale).nullable(true);
    tuple_desc_builder.add_slot(slot_desc_builder.build());
    tuple_desc_builder.build(&desc_tbl_builder);

    DescriptorTbl* tbl = nullptr;
    DescriptorTbl::create(_runtime_state.get(), _object_pool, desc_tbl_builder.desc_tbl(), &tbl,
                          config::vector_chunk_size);

    _runtime_state->set_desc_tbl(tbl);
    return tbl;
}

std::shared_ptr<TPlanNode> ConnectorScanNodeTest::create_tplan_node_cloud() {
    std::vector<::starrocks::TTupleId> tuple_ids{0};
    std::vector<bool> nullable_tuples{true};

    auto tnode = std::make_shared<TPlanNode>();
    tnode->__set_node_id(1);
    tnode->__set_node_type(TPlanNodeType::LAKE_SCAN_NODE);
    tnode->__set_row_tuples(tuple_ids);
    tnode->__set_nullable_tuples(nullable_tuples);
    tnode->__set_limit(-1);

    TConnectorScanNode connector_scan_node;
    connector_scan_node.connector_name = connector::Connector::LAKE;
    tnode->__set_connector_scan_node(connector_scan_node);

    return tnode;
}

std::vector<TScanRangeParams> ConnectorScanNodeTest::create_scan_ranges_cloud(size_t num) {
    std::vector<TScanRangeParams> scan_ranges;

    for (int i = 0; i < num; i++) {
        TInternalScanRange internal_scan_range;
        internal_scan_range.__set_tablet_id(i);
        internal_scan_range.__set_version("1");

        TScanRange scan_range;
        scan_range.__set_internal_scan_range(internal_scan_range);

        TScanRangeParams param;
        param.__set_scan_range(scan_range);
        scan_ranges.push_back(param);
    }

    return scan_ranges;
}

TEST_F(ConnectorScanNodeTest, test_convert_scan_range_to_morsel_queue_factory_cloud) {
    auto* descs = create_table_desc();
    auto tnode = create_tplan_node_cloud();
    auto scan_node = std::make_shared<starrocks::ConnectorScanNode>(_object_pool, *tnode, *descs);
    ASSERT_OK(scan_node->init(*tnode, _runtime_state.get()));

    bool enable_tablet_internal_parallel = false;
    auto tablet_internal_parallel_mode = TTabletInternalParallelMode::type::AUTO;
    std::map<int32_t, std::vector<TScanRangeParams>> no_scan_ranges_per_driver_seq;

    // dop is 1 and not so much morsels
    int pipeline_dop = 1;
    auto scan_ranges = create_scan_ranges_cloud(1);
    ASSIGN_OR_ABORT(auto morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(morsel_queue_factory->is_shared());

    // dop is 2 and not so much morsels
    pipeline_dop = 2;
    scan_ranges = create_scan_ranges_cloud(pipeline_dop * scan_node->io_tasks_per_scan_operator());
    ASSIGN_OR_ABORT(morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_FALSE(morsel_queue_factory->is_shared());

    // dop is 2 and so much morsels
    pipeline_dop = 2;
    scan_ranges = create_scan_ranges_cloud(pipeline_dop * scan_node->io_tasks_per_scan_operator() + 1);
    ASSIGN_OR_ABORT(morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(morsel_queue_factory->is_shared());
}

std::shared_ptr<TPlanNode> ConnectorScanNodeTest::create_tplan_node_hive() {
    std::vector<::starrocks::TTupleId> tuple_ids{0};
    std::vector<bool> nullable_tuples{true};

    auto tnode = std::make_shared<TPlanNode>();
    tnode->__set_node_id(1);
    tnode->__set_node_type(TPlanNodeType::HDFS_SCAN_NODE);
    tnode->__set_row_tuples(tuple_ids);
    tnode->__set_nullable_tuples(nullable_tuples);
    tnode->__set_limit(-1);

    TConnectorScanNode connector_scan_node;
    connector_scan_node.connector_name = connector::Connector::HIVE;
    tnode->__set_connector_scan_node(connector_scan_node);

    return tnode;
}

std::vector<TScanRangeParams> ConnectorScanNodeTest::create_scan_ranges_hive(size_t num) {
    std::vector<TScanRangeParams> scan_ranges;

    for (int i = 0; i < num; i++) {
        THdfsScanRange hdfs_scan_range;
        hdfs_scan_range.__set_full_path("file");
        hdfs_scan_range.__set_offset(i);
        hdfs_scan_range.__set_length(1);

        TScanRange scan_range;
        scan_range.__set_hdfs_scan_range(hdfs_scan_range);

        TScanRangeParams param;
        param.__set_scan_range(scan_range);
        scan_ranges.push_back(param);
    }

    return scan_ranges;
}

TEST_F(ConnectorScanNodeTest, test_convert_scan_range_to_morsel_queue_factory_hive) {
    auto* descs = create_table_desc();
    auto tnode = create_tplan_node_hive();
    auto scan_node = std::make_shared<starrocks::ConnectorScanNode>(_object_pool, *tnode, *descs);
    ASSERT_OK(scan_node->init(*tnode, _runtime_state.get()));

    bool enable_tablet_internal_parallel = false;
    auto tablet_internal_parallel_mode = TTabletInternalParallelMode::type::AUTO;
    std::map<int32_t, std::vector<TScanRangeParams>> no_scan_ranges_per_driver_seq;

    // dop is 1 and not so much morsels
    int pipeline_dop = 1;
    auto scan_ranges = create_scan_ranges_hive(1);
    ASSIGN_OR_ABORT(auto morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(morsel_queue_factory->is_shared());

    // dop is 2 and not so much morsels
    pipeline_dop = 2;
    scan_ranges = create_scan_ranges_hive(pipeline_dop * scan_node->io_tasks_per_scan_operator());
    ASSIGN_OR_ABORT(morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(morsel_queue_factory->is_shared());

    // dop is 2 and so much morsels
    pipeline_dop = 2;
    scan_ranges = create_scan_ranges_hive(pipeline_dop * scan_node->io_tasks_per_scan_operator() + 1);
    ASSIGN_OR_ABORT(morsel_queue_factory,
                    scan_node->convert_scan_range_to_morsel_queue_factory(
                            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), pipeline_dop,
                            enable_tablet_internal_parallel, tablet_internal_parallel_mode));
    ASSERT_TRUE(morsel_queue_factory->is_shared());
}

} // namespace starrocks

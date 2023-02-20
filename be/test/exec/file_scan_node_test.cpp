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

// #include "exec/file_scan_node.h"
#include <gtest/gtest.h>

#include <memory>

#include "column/column_helper.h"
#include "exec/connector_scan_node.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/storage_engine.h"
#include "util/disk_info.h"
#include "util/mem_info.h"

//TODO: test multi thread
//TODO: test runtime filter
namespace starrocks {
class FileScanNodeTest : public ::testing::Test {
public:
    void SetUp() override {
        config::enable_system_metrics = false;
        config::enable_metric_calculator = false;

        _exec_env = ExecEnv::GetInstance();

        _create_runtime_state();
        _pool = _runtime_state->obj_pool();
    }
    void TearDown() override {}

private:
    void _create_runtime_state();
    std::shared_ptr<TPlanNode> _create_tplan_node();
    DescriptorTbl* _create_table_desc(const std::vector<TypeDescriptor>& types);
    std::vector<TScanRangeParams> _create_csv_scan_ranges(const std::vector<TypeDescriptor>& types,
                                                          const string& multi_row_delimiter = "\n",
                                                          const string& multi_column_separator = "|");
    static ChunkPtr _create_chunk(const std::vector<TypeDescriptor>& types);

    std::shared_ptr<RuntimeState> _runtime_state = nullptr;
    OlapTableDescriptor* _table_desc = nullptr;
    ObjectPool* _pool = nullptr;
    std::shared_ptr<MemTracker> _mem_tracker = nullptr;
    ExecEnv* _exec_env = nullptr;
    std::string _file = "./be/test/exec/test_data/csv_scanner/csv_file1";
};

ChunkPtr FileScanNodeTest::_create_chunk(const std::vector<TypeDescriptor>& types) {
    ChunkPtr chunk = std::make_shared<Chunk>();
    for (int i = 0; i < types.size(); i++) {
        chunk->append_column(ColumnHelper::create_column(types[i], true), i);
    }
    return chunk;
}

std::vector<TScanRangeParams> FileScanNodeTest::_create_csv_scan_ranges(const std::vector<TypeDescriptor>& types,
                                                                        const string& multi_row_delimiter,
                                                                        const string& multi_column_separator) {
    /// TBrokerScanRangeParams
    TBrokerScanRangeParams* params = _pool->add(new TBrokerScanRangeParams());
    params->__set_multi_row_delimiter(multi_row_delimiter);
    params->__set_multi_column_separator(multi_column_separator);
    params->strict_mode = true;
    params->dest_tuple_id = 0;
    params->src_tuple_id = 0;
    for (int i = 0; i < types.size(); i++) {
        params->expr_of_dest_slot[i] = TExpr();
        params->expr_of_dest_slot[i].nodes.emplace_back(TExprNode());
        params->expr_of_dest_slot[i].nodes[0].__set_type(types[i].to_thrift());
        params->expr_of_dest_slot[i].nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
        params->expr_of_dest_slot[i].nodes[0].__set_is_nullable(true);
        params->expr_of_dest_slot[i].nodes[0].__set_slot_ref(TSlotRef());
        params->expr_of_dest_slot[i].nodes[0].slot_ref.__set_slot_id(i);
    }

    for (int i = 0; i < types.size(); i++) {
        params->src_slot_ids.emplace_back(i);
    }

    std::vector<TBrokerRangeDesc>* ranges = _pool->add(new vector<TBrokerRangeDesc>());

    TBrokerRangeDesc* range = _pool->add(new TBrokerRangeDesc());
    range->__set_path(_file);
    range->__set_start_offset(0);
    range->__set_num_of_columns_from_file(types.size());
    ranges->push_back(*range);

    TBrokerScanRange* broker_scan_range = _pool->add(new TBrokerScanRange());
    broker_scan_range->params = *params;
    broker_scan_range->ranges = *ranges;

    TScanRange scan_range;
    scan_range.__set_broker_scan_range(*broker_scan_range);

    TScanRangeParams param;
    param.__set_scan_range(scan_range);

    return std::vector<TScanRangeParams>{param};
}

void FileScanNodeTest::_create_runtime_state() {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, _exec_env);
    TUniqueId id;
    _mem_tracker = std::make_shared<MemTracker>(-1, "olap scanner test");
    _runtime_state->init_mem_trackers(id);
}

std::shared_ptr<TPlanNode> FileScanNodeTest::_create_tplan_node() {
    std::vector<::starrocks::TTupleId> tuple_ids{0};
    std::vector<bool> nullable_tuples{true};

    auto tnode = std::make_shared<TPlanNode>();

    tnode->__set_node_id(1);
    tnode->__set_node_type(TPlanNodeType::FILE_SCAN_NODE);
    tnode->__set_row_tuples(tuple_ids);
    tnode->__set_nullable_tuples(nullable_tuples);
    tnode->__set_limit(-1);

    TConnectorScanNode connector_scan_node;
    connector_scan_node.connector_name = connector::Connector::FILE;
    tnode->__set_connector_scan_node(connector_scan_node);

    return tnode;
}

DescriptorTbl* FileScanNodeTest::_create_table_desc(const std::vector<TypeDescriptor>& types) {
    /// Init DescriptorTable
    TDescriptorTableBuilder desc_tbl_builder;
    TTupleDescriptorBuilder tuple_desc_builder;
    for (auto& t : types) {
        TSlotDescriptorBuilder slot_desc_builder;
        slot_desc_builder.type(t).length(t.len).precision(t.precision).scale(t.scale).nullable(true);
        tuple_desc_builder.add_slot(slot_desc_builder.build());
    }
    tuple_desc_builder.build(&desc_tbl_builder);

    DescriptorTbl* tbl = nullptr;
    DescriptorTbl::create(_runtime_state.get(), _pool, desc_tbl_builder.desc_tbl(), &tbl, config::vector_chunk_size);

    _runtime_state->set_desc_tbl(tbl);
    return tbl;
}

TEST_F(FileScanNodeTest, CSVBasic) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_VARCHAR);
    types.emplace_back(TYPE_DATE);
    types.emplace_back(TYPE_VARCHAR);

    auto tnode = _create_tplan_node();
    auto* descs = _create_table_desc(types);
    auto file_scan_node = std::make_shared<ConnectorScanNode>(_pool, *tnode, *descs);

    Status status = file_scan_node->init(*tnode, _runtime_state.get());
    ASSERT_TRUE(status.ok());

    status = file_scan_node->prepare(_runtime_state.get());
    ASSERT_TRUE(status.ok());

    auto scan_ranges = _create_csv_scan_ranges(types);
    status = file_scan_node->set_scan_ranges(scan_ranges);
    ASSERT_TRUE(status.ok());

    status = file_scan_node->open(_runtime_state.get());
    ASSERT_TRUE(status.ok());

    auto chunk = _create_chunk(types);
    bool eos = false;

    status = file_scan_node->get_next(_runtime_state.get(), &chunk, &eos);
    ASSERT_TRUE(status.ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(chunk->num_rows(), 3);

    status = file_scan_node->close(_runtime_state.get());
    ASSERT_TRUE(status.ok());
}

} // namespace starrocks

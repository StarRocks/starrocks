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

#include "exec/exec_factory.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>

#include "base/testutil/assert.h"
#include "common/config.h"
#include "common/object_pool.h"
#include "exec/aggregate/aggregate_blocking_node.h"
#include "exec/aggregate/aggregate_streaming_node.h"
#include "exec/aggregate/distinct_blocking_node.h"
#include "exec/aggregate/distinct_streaming_node.h"
#include "exec/connector_scan_node.h"
#include "exec/exec_node.h"
#include "exec/file_scan_node.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class ExecFactoryTest : public ::testing::Test {
public:
    ExecFactoryTest() : _runtime_state(TQueryGlobals()) {}

protected:
    void SetUp() override {
        TDescriptorTableBuilder desc_tbl_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        TSlotDescriptorBuilder slot_desc_builder;
        slot_desc_builder.type(TYPE_INT).nullable(true);
        tuple_desc_builder.add_slot(slot_desc_builder.build());
        tuple_desc_builder.build(&desc_tbl_builder);

        ASSERT_TRUE(DescriptorTbl::create(&_runtime_state, &_object_pool, desc_tbl_builder.desc_tbl(), &_desc_tbl,
                                          config::vector_chunk_size)
                            .ok());
        _runtime_state.set_desc_tbl(_desc_tbl);
        std::vector<TupleDescriptor*> tuple_descs;
        _desc_tbl->get_tuple_descs(&tuple_descs);
        ASSERT_EQ(1, tuple_descs.size());
        _tuple_id = tuple_descs[0]->id();
    }

    TPlanNode make_base_plan_node(TPlanNodeType::type node_type, int node_id = 1, int num_children = 0) const {
        TPlanNode tnode;
        tnode.__set_node_id(node_id);
        tnode.__set_node_type(node_type);
        tnode.__set_num_children(num_children);
        tnode.__set_limit(-1);
        tnode.row_tuples.push_back(_tuple_id);
        return tnode;
    }

    TPlanNode make_aggregation_node(bool streaming_preagg, bool has_aggregate_functions) const {
        TPlanNode tnode = make_base_plan_node(TPlanNodeType::AGGREGATION_NODE);
        TAggregationNode agg_node;
        agg_node.__set_use_streaming_preaggregation(streaming_preagg);
        if (has_aggregate_functions) {
            agg_node.aggregate_functions.emplace_back();
        }
        tnode.__set_agg_node(agg_node);
        return tnode;
    }

    TPlanNode make_file_scan_node(bool enable_pipeline_load) const {
        TPlanNode tnode = make_base_plan_node(TPlanNodeType::FILE_SCAN_NODE);
        TFileScanNode file_scan_node;
        file_scan_node.__set_tuple_id(_tuple_id);
        if (enable_pipeline_load) {
            file_scan_node.__set_enable_pipeline_load(true);
        }
        tnode.__set_file_scan_node(file_scan_node);
        return tnode;
    }

    template <typename NodeType>
    void assert_node_instance(const TPlanNode& tnode) {
        ExecNode* node = nullptr;
        ASSERT_OK(ExecFactory::create_vectorized_node(&_runtime_state, &_object_pool, tnode, *_desc_tbl, &node));
        ASSERT_NE(node, nullptr);
        ASSERT_NE(dynamic_cast<NodeType*>(node), nullptr);
    }

protected:
    RuntimeState _runtime_state;
    ObjectPool _object_pool;
    DescriptorTbl* _desc_tbl = nullptr;
    TTupleId _tuple_id = 0;
};

TEST_F(ExecFactoryTest, test_aggregation_node_mapping) {
    assert_node_instance<DistinctStreamingNode>(make_aggregation_node(true, false));
    assert_node_instance<AggregateStreamingNode>(make_aggregation_node(true, true));
    assert_node_instance<DistinctBlockingNode>(make_aggregation_node(false, false));
    assert_node_instance<AggregateBlockingNode>(make_aggregation_node(false, true));
}

TEST_F(ExecFactoryTest, test_file_scan_node_mapping) {
    assert_node_instance<FileScanNode>(make_file_scan_node(false));
    assert_node_instance<ConnectorScanNode>(make_file_scan_node(true));
}

TEST_F(ExecFactoryTest, test_unsupported_node_type) {
    TPlanNode tnode = make_base_plan_node(TPlanNodeType::CSV_SCAN_NODE);
    ExecNode* node = nullptr;
    Status st = ExecFactory::create_vectorized_node(&_runtime_state, &_object_pool, tnode, *_desc_tbl, &node);
    ASSERT_ERROR(st);
    ASSERT_TRUE(st.is_internal_error());
    ASSERT_NE(st.message().find("Vectorized engine not support node"), std::string::npos);
}

TEST_F(ExecFactoryTest, test_stream_scan_invalid_source_type) {
    TPlanNode tnode = make_base_plan_node(TPlanNodeType::STREAM_SCAN_NODE);
    TStreamScanNode stream_scan_node;
    stream_scan_node.__set_source_type(static_cast<StreamSourceType::type>(-1));
    tnode.__set_stream_scan_node(stream_scan_node);

    ExecNode* node = nullptr;
    Status st = ExecFactory::create_vectorized_node(&_runtime_state, &_object_pool, tnode, *_desc_tbl, &node);
    ASSERT_ERROR(st);
    ASSERT_TRUE(st.is_internal_error());
    ASSERT_NE(st.message().find("Stream scan node does not support source type"), std::string::npos);
}

TEST_F(ExecFactoryTest, test_create_tree_empty_plan) {
    TPlan plan;
    ExecNode* root = reinterpret_cast<ExecNode*>(0x1);
    ASSERT_OK(ExecFactory::create_tree(&_runtime_state, &_object_pool, plan, *_desc_tbl, &root));
    ASSERT_EQ(root, nullptr);
}

TEST_F(ExecFactoryTest, test_create_tree_valid_simple_tree) {
    TPlan plan;
    plan.nodes.emplace_back(make_base_plan_node(TPlanNodeType::SELECT_NODE, 10, 1));
    plan.nodes.emplace_back(make_base_plan_node(TPlanNodeType::EMPTY_SET_NODE, 11, 0));

    ExecNode* root = nullptr;
    ASSERT_OK(ExecFactory::create_tree(&_runtime_state, &_object_pool, plan, *_desc_tbl, &root));
    ASSERT_NE(root, nullptr);
    ASSERT_EQ(root->type(), TPlanNodeType::SELECT_NODE);
    ASSERT_EQ(root->children().size(), 1);
    ASSERT_NE(root->children()[0], nullptr);
    ASSERT_EQ(root->children()[0]->type(), TPlanNodeType::EMPTY_SET_NODE);
}

TEST_F(ExecFactoryTest, test_create_tree_invalid_tuple_id) {
    TPlanNode tnode = make_base_plan_node(TPlanNodeType::SELECT_NODE);
    tnode.row_tuples.clear();
    tnode.row_tuples.push_back(_tuple_id + 1000);

    TPlan plan;
    plan.nodes.emplace_back(std::move(tnode));

    ExecNode* root = nullptr;
    Status st = ExecFactory::create_tree(&_runtime_state, &_object_pool, plan, *_desc_tbl, &root);
    ASSERT_ERROR(st);
    ASSERT_TRUE(st.is_internal_error());
    ASSERT_NE(st.message().find("Tuple ids are not in descs"), std::string::npos);
}

TEST_F(ExecFactoryTest, test_create_tree_malformed_or_partial_tree) {
    {
        TPlan malformed_plan;
        malformed_plan.nodes.emplace_back(make_base_plan_node(TPlanNodeType::SELECT_NODE, 20, 1));

        ExecNode* root = nullptr;
        Status st = ExecFactory::create_tree(&_runtime_state, &_object_pool, malformed_plan, *_desc_tbl, &root);
        ASSERT_ERROR(st);
        ASSERT_TRUE(st.is_internal_error());
        ASSERT_NE(st.message().find("Failed to reconstruct plan tree from thrift."), std::string::npos);
    }

    {
        TPlan partial_plan;
        partial_plan.nodes.emplace_back(make_base_plan_node(TPlanNodeType::SELECT_NODE, 30, 0));
        partial_plan.nodes.emplace_back(make_base_plan_node(TPlanNodeType::EMPTY_SET_NODE, 31, 0));

        ExecNode* root = nullptr;
        Status st = ExecFactory::create_tree(&_runtime_state, &_object_pool, partial_plan, *_desc_tbl, &root);
        ASSERT_ERROR(st);
        ASSERT_TRUE(st.is_internal_error());
        ASSERT_NE(st.message().find("Plan tree only partially reconstructed"), std::string::npos);
    }
}

} // namespace starrocks

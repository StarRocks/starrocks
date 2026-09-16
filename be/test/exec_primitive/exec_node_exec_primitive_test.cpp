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
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "common/object_pool.h"
#include "exec_primitive/exec_node.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {

class ExecNodeExecPrimitiveTest : public ::testing::Test {
public:
    ExecNodeExecPrimitiveTest() : _runtime_state(TQueryGlobals()) {}

protected:
    void SetUp() override {
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.__set_id(0);
        t_tuple_desc.__set_byteSize(0);

        auto* tuple_desc = _object_pool.add(new TupleDescriptor(t_tuple_desc));
        _desc_tbl = _object_pool.add(new DescriptorTbl());
        _desc_tbl->_tuple_desc_map.emplace(tuple_desc->id(), tuple_desc);
        _tuple_id = tuple_desc->id();
    }

    TPlanNode make_plan_node(TPlanNodeType::type node_type, int node_id) const {
        TPlanNode tnode;
        tnode.__set_node_id(node_id);
        tnode.__set_node_type(node_type);
        tnode.__set_num_children(0);
        tnode.__set_limit(-1);
        tnode.row_tuples.push_back(_tuple_id);
        return tnode;
    }

    RuntimeState _runtime_state;
    ObjectPool _object_pool;
    DescriptorTbl* _desc_tbl = nullptr;
    TTupleId _tuple_id = 0;
};

TEST_F(ExecNodeExecPrimitiveTest, TreeHelpersAndDefaultPipelineDecomposition) {
    auto root_node = make_plan_node(TPlanNodeType::SELECT_NODE, 10);
    auto child_node = make_plan_node(TPlanNodeType::EMPTY_SET_NODE, 11);
    ExecNode root(&_object_pool, root_node, *_desc_tbl);
    ExecNode child(&_object_pool, child_node, *_desc_tbl);
    root.add_child(&child);

    std::vector<ExecNode*> select_nodes;
    root.collect_nodes(TPlanNodeType::SELECT_NODE, &select_nodes);
    ASSERT_EQ(1, select_nodes.size());
    EXPECT_EQ(&root, select_nodes[0]);
    EXPECT_EQ(root.children().size(), 1);
    EXPECT_EQ(root.children()[0], &child);
    EXPECT_NE(root.debug_string().find("SELECT_NODE"), std::string::npos);
    EXPECT_NE(root.debug_string().find("EMPTY_SET_NODE"), std::string::npos);

    auto factories = root.decompose_to_pipeline(nullptr);
    ASSERT_TRUE(factories.ok());
    EXPECT_TRUE(factories.value().empty());
}

} // namespace
} // namespace starrocks

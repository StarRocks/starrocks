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

#include "query_orchestration/query_orchestrator.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "base/url_coding.h"
#include "common/config_thrift_server_fwd.h"
#include "common/util/thrift_util.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Planner_types.h"
#include "gen_cpp/QueryPlanExtra_types.h"
#include "gen_cpp/StarrocksExternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/exec_env.h"

namespace starrocks::query_orchestration {

class QueryOrchestratorTest : public testing::Test {
protected:
    static void SetUpTestSuite() {
        config::thrift_max_message_size = 1073741824;
        config::thrift_max_frame_size = 16384000;
        config::thrift_max_recursion_depth = 64;
    }

    static TExprNode make_slot_ref_node(TSlotId slot_id, TTupleId tuple_id, TPrimitiveType::type primitive_type,
                                        int32_t varchar_len = -1) {
        TExprNode node;
        node.node_type = TExprNodeType::SLOT_REF;
        node.num_children = 0;
        node.output_scale = -1;
        node.__set_is_nullable(true);

        TScalarType scalar_type;
        scalar_type.type = primitive_type;
        if (primitive_type == TPrimitiveType::VARCHAR || primitive_type == TPrimitiveType::CHAR) {
            scalar_type.__set_len(varchar_len);
        }
        TTypeNode type_node;
        type_node.type = TTypeNodeType::SCALAR;
        type_node.__set_scalar_type(scalar_type);
        node.type.types.push_back(type_node);

        TSlotRef slot_ref;
        slot_ref.slot_id = slot_id;
        slot_ref.tuple_id = tuple_id;
        node.__set_slot_ref(slot_ref);
        return node;
    }

    static TSlotDescriptor make_slot(TSlotId id, TTupleId parent, TPrimitiveType::type primitive_type,
                                     const std::string& col_name, int32_t varchar_len = -1) {
        TSlotDescriptor slot;
        slot.__set_id(id);
        slot.__set_parent(parent);
        TScalarType scalar_type;
        scalar_type.type = primitive_type;
        if (primitive_type == TPrimitiveType::VARCHAR || primitive_type == TPrimitiveType::CHAR) {
            scalar_type.__set_len(varchar_len);
        }
        TTypeNode type_node;
        type_node.type = TTypeNodeType::SCALAR;
        type_node.__set_scalar_type(scalar_type);
        TTypeDesc type_desc;
        type_desc.types.push_back(type_node);
        slot.__set_slotType(type_desc);
        slot.__set_colName(col_name);
        slot.__set_isNullable(true);
        return slot;
    }
};

// Reproduces the scenario where an output slot has an empty col_name (e.g. the
// VARCHAR slot generated for the DECODE_NODE output tuple). Before switching to
// get_slot_descriptor(), the lookup went through _slot_with_column_name_map,
// which dropped slots with empty col_name and produced "slot descriptor is null".
TEST_F(QueryOrchestratorTest, ExecExternalPlanFragmentLooksUpSlotWithEmptyColName) {
    TQueryPlanInfo query_plan_info;

    // tuple 0: scan tuple (k1 INT, k2 INT for dict-encoded k2)
    // tuple 1: decode output tuple (k1 INT, "" VARCHAR - empty colName)
    TTupleDescriptor tuple0;
    tuple0.__set_id(0);
    TTupleDescriptor tuple1;
    tuple1.__set_id(1);
    query_plan_info.desc_tbl.tupleDescriptors = {tuple0, tuple1};

    query_plan_info.desc_tbl.__set_slotDescriptors({
            make_slot(/*id=*/1, /*parent=*/0, TPrimitiveType::INT, "k1"),
            make_slot(/*id=*/10, /*parent=*/0, TPrimitiveType::INT, "k2"),
            make_slot(/*id=*/1, /*parent=*/1, TPrimitiveType::INT, "k1"),
            // The slot under test: tuple 1, slot id 2, VARCHAR, empty col_name.
            make_slot(/*id=*/2, /*parent=*/1, TPrimitiveType::VARCHAR, "", /*varchar_len=*/65533),
    });

    TExpr expr_k1;
    expr_k1.nodes.push_back(make_slot_ref_node(/*slot_id=*/1, /*tuple_id=*/1, TPrimitiveType::INT));
    TExpr expr_k2;
    expr_k2.nodes.push_back(
            make_slot_ref_node(/*slot_id=*/2, /*tuple_id=*/1, TPrimitiveType::VARCHAR, /*varchar_len=*/65533));

    query_plan_info.plan_fragment.__set_output_exprs({expr_k1, expr_k2});
    TDataPartition partition;
    partition.type = TPartitionType::RANDOM;
    query_plan_info.plan_fragment.partition = partition;

    query_plan_info.query_id.__set_hi(1);
    query_plan_info.query_id.__set_lo(2);
    query_plan_info.tablet_info = {};
    query_plan_info.__set_output_names({"k1", "k2"});

    // Serialize via binary protocol (matches deserialize_thrift_msg in
    // exec_external_plan_fragment) and base64-encode.
    ThriftSerializer serializer(/*compact=*/false, /*initial_buffer_size=*/1024);
    std::string serialized;
    ASSERT_OK(serializer.serialize(&query_plan_info, &serialized));
    std::string opaqued;
    base64_encode(serialized, &opaqued);

    TScanOpenParams open_params;
    open_params.cluster = "default_cluster";
    open_params.database = "test_db";
    open_params.table = "t1";
    // Unknown tablet id forces the function to return NotFound *after* the slot
    // descriptor lookup loop completes, so selected_columns is populated but the
    // heavy FragmentExecutor::prepare path is skipped.
    open_params.tablet_ids = {99999};
    open_params.opaqued_query_plan = opaqued;
    open_params.__set_batch_size(1024);

    TUniqueId fragment_instance_id;
    fragment_instance_id.__set_hi(3);
    fragment_instance_id.__set_lo(4);

    QueryOrchestrator query_orchestrator(ExecEnv::GetInstance());
    std::vector<TScanColumnDesc> selected_columns;
    TUniqueId out_query_id;
    Status st = query_orchestrator.exec_external_plan_fragment(open_params, fragment_instance_id, &selected_columns,
                                                               &out_query_id);

    // Slot lookup succeeded for the empty-colName slot (otherwise we would get
    // InvalidArgument "slot descriptor is null" with empty selected_columns).
    ASSERT_EQ(2u, selected_columns.size());
    EXPECT_EQ("k1", selected_columns[0].name);
    EXPECT_EQ(TPrimitiveType::INT, selected_columns[0].type);
    EXPECT_EQ("k2", selected_columns[1].name);
    EXPECT_EQ(TPrimitiveType::VARCHAR, selected_columns[1].type);

    EXPECT_EQ(query_plan_info.query_id, out_query_id);
    EXPECT_TRUE(st.is_not_found()) << "expected NotFound from unknown tablet, got: " << st;
}

} // namespace starrocks::query_orchestration

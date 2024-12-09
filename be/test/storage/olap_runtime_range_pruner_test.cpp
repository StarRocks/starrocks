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

#include "storage/olap_runtime_range_pruner.hpp"

#include <gtest/gtest.h>

#include "exprs/runtime_filter_bank.h"
#include "gen_cpp/RuntimeFilter_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/column_predicate.h"
#include "storage/predicate_parser.h"
#include "testutil/exprs_test_helper.h"
#include "testutil/schema_test_helper.h"

namespace starrocks {

class OlapRuntimeRangePrunerTest : public ::testing::Test {
public:
    void SetUp() override {
        _tablet_schema = SchemaTestHelper::gen_schema_of_dup(1, 3, 1);
        _predicate_parser = std::make_unique<PredicateParser>(_tablet_schema);
    }

    static StatusOr<std::shared_ptr<RuntimeFilterProbeDescriptor>> gen_runtime_filter_desc(TPlanNodeId node_id,
                                                                                           ObjectPool* pool,
                                                                                           RuntimeState* runtime_state);

protected:
    ObjectPool _pool;
    RuntimeState _runtime_state;
    TPlanNodeId _node_id;
    TabletSchemaSPtr _tablet_schema;
    std::unique_ptr<PredicateParser> _predicate_parser;
};

StatusOr<std::shared_ptr<RuntimeFilterProbeDescriptor>> OlapRuntimeRangePrunerTest::gen_runtime_filter_desc(
        TPlanNodeId node_id, ObjectPool* pool, RuntimeState* runtime_state) {
    TRuntimeFilterDescription tRuntimeFilterDescription;
    tRuntimeFilterDescription.__set_filter_id(1);
    tRuntimeFilterDescription.__set_has_remote_targets(false);
    tRuntimeFilterDescription.__set_build_plan_node_id(node_id);
    tRuntimeFilterDescription.__set_build_join_mode(TRuntimeFilterBuildJoinMode::BORADCAST);
    tRuntimeFilterDescription.__set_filter_type(TRuntimeFilterBuildType::TOPN_FILTER);

    TExpr col_ref = ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(1, true);
    tRuntimeFilterDescription.__isset.plan_node_id_to_target_expr = true;
    tRuntimeFilterDescription.plan_node_id_to_target_expr.emplace(node_id, col_ref);

    auto runtime_filter_desc = std::make_shared<RuntimeFilterProbeDescriptor>();
    RETURN_IF_ERROR(runtime_filter_desc->init(pool, tRuntimeFilterDescription, node_id, runtime_state));

    return runtime_filter_desc;
}

TEST_F(OlapRuntimeRangePrunerTest, update_1) {
    SlotDescriptor slot(0, "c0", TypeDescriptor(TYPE_INT));

    auto ret = gen_runtime_filter_desc(_node_id, &_pool, &_runtime_state);
    ASSERT_TRUE(ret.ok());
    auto runtime_filter_desc = ret.value();

    UnarrivedRuntimeFilterList unarrivedRuntimeFilterList;
    unarrivedRuntimeFilterList.add_unarrived_rf(runtime_filter_desc.get(), &slot, 1);
    OlapRuntimeScanRangePruner pruner(_predicate_parser.get(), unarrivedRuntimeFilterList);

    size_t pred_size = 0;
    std::string pred_1;
    std::string pred_2;

    // init
    Status st = pruner.update_range_if_arrived(
            nullptr,
            [&pred_size](auto vid, const PredicateList& predicates) {
                pred_size = predicates.size();
                return Status::OK();
            },
            100000);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(pred_size, 0);

    // version 1
    RuntimeBloomFilter<TYPE_INT> _rf;
    _rf.insert(10);
    _rf.insert(20);
    runtime_filter_desc->set_runtime_filter(&_rf);

    st = pruner.update_range_if_arrived(
            nullptr,
            [&pred_size, &pred_1, &pred_2](auto vid, const PredicateList& predicates) {
                pred_size = predicates.size();
                pred_1 = predicates[0]->debug_string();
                pred_2 = predicates[1]->debug_string();
                return Status::OK();
            },
            200000);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(pred_size, 2);
    ASSERT_EQ(pred_1, "(columnId(0)>=10)");
    ASSERT_EQ(pred_2, "(columnId(0)<=20)");

    // version 2 & 3
    _rf.update_min_max<true>(11);
    _rf.update_min_max<false>(15);
    st = pruner.update_range_if_arrived(
            nullptr,
            [&pred_size, &pred_1, &pred_2](auto vid, const PredicateList& predicates) {
                pred_size = predicates.size();
                pred_1 = predicates[0]->debug_string();
                pred_2 = predicates[1]->debug_string();
                return Status::OK();
            },
            300000);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(pred_size, 2);
    ASSERT_EQ(pred_1, "(columnId(0)>=11)");
    ASSERT_EQ(pred_2, "(columnId(0)<=15)");
}
} // namespace starrocks

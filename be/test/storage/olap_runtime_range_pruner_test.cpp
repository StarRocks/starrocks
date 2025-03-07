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

#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_bank.h"
#include "gen_cpp/RuntimeFilter_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/column_predicate.h"
#include "storage/predicate_parser.h"
#include "storage/runtime_range_pruner.hpp"
#include "testutil/exprs_test_helper.h"
#include "testutil/schema_test_helper.h"
#include "types/logical_type.h"

namespace starrocks {

class OlapRuntimeRangePrunerTest : public ::testing::Test {
public:
    void SetUp() override {
        _tablet_schema = SchemaTestHelper::gen_schema_of_dup(TYPE_INT, 1, 3, 1);
        _predicate_parser = std::make_unique<OlapPredicateParser>(_tablet_schema);
    }

protected:
    StatusOr<std::shared_ptr<RuntimeFilterProbeDescriptor>> _gen_runtime_filter_desc();

    using Int32Decoder = detail::RuntimeColumnPredicateBuilder::DummyDecoder<int32_t>;
    using Int32RuntimeFilter = ComposedRuntimeFilter<TYPE_INT>;
    using Decimal32RuntimeFilter = ComposedRuntimeFilter<TYPE_DECIMAL32>;

    const TypeDescriptor TYPE_DECIMAL32_DESC = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 5, 4);

    ObjectPool _pool;
    RuntimeState _runtime_state;
    TPlanNodeId _node_id = 0;
    TabletSchemaSPtr _tablet_schema;
    std::unique_ptr<OlapPredicateParser> _predicate_parser;
};

StatusOr<std::shared_ptr<RuntimeFilterProbeDescriptor>> OlapRuntimeRangePrunerTest::_gen_runtime_filter_desc() {
    TRuntimeFilterDescription desc;
    desc.__set_filter_id(1);
    desc.__set_has_remote_targets(false);
    desc.__set_build_plan_node_id(_node_id);
    desc.__set_build_join_mode(TRuntimeFilterBuildJoinMode::BORADCAST);
    desc.__set_filter_type(TRuntimeFilterBuildType::TOPN_FILTER);

    TExpr col_ref = ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(1, true);
    desc.__isset.plan_node_id_to_target_expr = true;
    desc.plan_node_id_to_target_expr.emplace(_node_id, col_ref);

    auto runtime_filter_desc = std::make_shared<RuntimeFilterProbeDescriptor>();
    RETURN_IF_ERROR(runtime_filter_desc->init(&_pool, desc, _node_id, &_runtime_state));

    return runtime_filter_desc;
}

TEST_F(OlapRuntimeRangePrunerTest, min_max_parser) {
    Int32Decoder decoder(nullptr);

    Int32RuntimeFilter rf;
    rf.insert(10);
    rf.insert(20);

    detail::RuntimeColumnPredicateBuilder::MinMaxParser<MinMaxRuntimeFilter<TYPE_INT>, Int32Decoder> parser(
            &rf.min_max_filter(), &decoder);
    ColumnPtr min_column = parser.min_const_column<TYPE_INT>(TYPE_INT_DESC, &_pool);
    ColumnPtr max_column = parser.max_const_column<TYPE_INT>(TYPE_INT_DESC, &_pool);
    ASSERT_EQ(min_column->debug_string(), "CONST: 10 Size : 1");
    ASSERT_EQ(max_column->debug_string(), "CONST: 20 Size : 1");
}

TEST_F(OlapRuntimeRangePrunerTest, min_max_parser_for_decimal) {
    Int32Decoder decoder(nullptr);

    Decimal32RuntimeFilter rf;
    rf.insert(10);
    rf.insert(20);

    detail::RuntimeColumnPredicateBuilder::MinMaxParser<MinMaxRuntimeFilter<TYPE_DECIMAL32>, Int32Decoder> parser(
            &rf.min_max_filter(), &decoder);
    ColumnPtr min_column = parser.min_const_column<TYPE_DECIMAL32>(TYPE_DECIMAL32_DESC, &_pool);
    ColumnPtr max_column = parser.max_const_column<TYPE_DECIMAL32>(TYPE_DECIMAL32_DESC, &_pool);
    ASSERT_EQ(min_column->debug_string(), "CONST: 0.0010 Size : 1");
    ASSERT_EQ(max_column->debug_string(), "CONST: 0.0020 Size : 1");
}

TEST_F(OlapRuntimeRangePrunerTest, update_1) {
    SlotDescriptor slot(0, "c0", TYPE_INT_DESC);

    ASSIGN_OR_ASSERT_FAIL(auto runtime_filter_desc, _gen_runtime_filter_desc());

    UnarrivedRuntimeFilterList unarrivedRuntimeFilterList;
    unarrivedRuntimeFilterList.add_unarrived_rf(runtime_filter_desc.get(), &slot, 1);
    RuntimeScanRangePruner pruner(_predicate_parser.get(), unarrivedRuntimeFilterList);

    size_t pred_size = 0;
    std::string pred_1;
    std::string pred_2;

    // init
    ASSERT_OK(pruner.update_range_if_arrived(
            nullptr,
            [&pred_size](auto vid, const PredicateList& predicates) {
                pred_size = predicates.size();
                return Status::OK();
            },
            false, 100000));
    ASSERT_EQ(pred_size, 0);

    // version 1
    MinMaxRuntimeFilter<TYPE_INT> _rf;
    _rf.insert(10);
    _rf.insert(20);
    runtime_filter_desc->set_runtime_filter(&_rf);

    ASSERT_OK(pruner.update_range_if_arrived(
            nullptr,
            [&pred_size, &pred_1, &pred_2](auto vid, const PredicateList& predicates) {
                pred_size = predicates.size();
                pred_1 = predicates[0]->debug_string();
                pred_2 = predicates[1]->debug_string();
                return Status::OK();
            },
            false, 200000));
    ASSERT_EQ(pred_size, 2);
    ASSERT_EQ(pred_1, "(columnId(0)>=10)");
    ASSERT_EQ(pred_2, "(columnId(0)<=20)");

    // version 2 & 3
    _rf.update_min_max<true>(11);
    _rf.update_min_max<false>(15);
    ASSERT_OK(pruner.update_range_if_arrived(
            nullptr,
            [&pred_size, &pred_1, &pred_2](auto vid, const PredicateList& predicates) {
                pred_size = predicates.size();
                pred_1 = predicates[0]->debug_string();
                pred_2 = predicates[1]->debug_string();
                return Status::OK();
            },
            false, 300000));
    ASSERT_EQ(pred_size, 2);
    ASSERT_EQ(pred_1, "(columnId(0)>=11)");
    ASSERT_EQ(pred_2, "(columnId(0)<=15)");
}

TEST_F(OlapRuntimeRangePrunerTest, update_has_null) {
    SlotDescriptor slot(0, "c0", TYPE_INT_DESC);

    ASSIGN_OR_ASSERT_FAIL(auto runtime_filter_desc, _gen_runtime_filter_desc());

    UnarrivedRuntimeFilterList unarrivedRuntimeFilterList;
    unarrivedRuntimeFilterList.add_unarrived_rf(runtime_filter_desc.get(), &slot, 1);
    RuntimeScanRangePruner pruner(_predicate_parser.get(), unarrivedRuntimeFilterList);

    size_t pred_size = 0;
    std::string pred;

    // init
    ASSERT_OK(pruner.update_range_if_arrived(
            nullptr,
            [&pred_size](auto vid, const PredicateList& predicates) {
                pred_size = predicates.size();
                return Status::OK();
            },
            false, 100000));
    ASSERT_EQ(pred_size, 0);

    // version 1
    MinMaxRuntimeFilter<TYPE_INT> _rf;
    _rf.insert(10);
    _rf.insert(20);
    _rf.insert_null();
    runtime_filter_desc->set_runtime_filter(&_rf);

    ASSERT_OK(pruner.update_range_if_arrived(
            nullptr,
            [&pred_size, &pred](auto vid, const PredicateList& predicates) {
                pred_size = predicates.size();
                pred = predicates[0]->debug_string();
                return Status::OK();
            },
            false, 200000));
    ASSERT_EQ(pred_size, 1);
    ASSERT_EQ(pred, "OR(0:AND(0:(columnId(0)>=10), 1:(columnId(0)<=20)), 1:(ColumnId(0) IS NULL))");

    // version 2 & 3
    _rf.update_min_max<true>(11);
    _rf.update_min_max<false>(15);
    _rf.insert_null();
    ASSERT_OK(pruner.update_range_if_arrived(
            nullptr,
            [&pred_size, &pred](auto vid, const PredicateList& predicates) {
                pred_size = predicates.size();
                pred = predicates[0]->debug_string();
                return Status::OK();
            },
            false, 300000));
    ASSERT_EQ(pred_size, 1);
    ASSERT_EQ(pred, "OR(0:AND(0:(columnId(0)>=11), 1:(columnId(0)<=15)), 1:(ColumnId(0) IS NULL))");
}
} // namespace starrocks

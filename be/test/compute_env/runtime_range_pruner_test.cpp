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

#include "compute_env/runtime_range_pruner.hpp"

#include <gtest/gtest.h>

#include <limits>
#include <vector>

#include "exec_primitive/runtime_filter/runtime_filter_probe.h"
#include "gen_cpp/RuntimeFilter_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_filter.h"
#include "runtime/runtime_state.h"
#include "storage_primitive/column_expr_predicate.h"
#include "storage_primitive/column_predicate_factory.h"
#include "storage_primitive/predicate_parser.h"
#include "testutil/exprs_test_helper.h"
#include "types/logical_type.h"

namespace starrocks {

class RuntimeRangePrunerTest : public ::testing::Test {
protected:
    StatusOr<std::shared_ptr<RuntimeFilterProbeDescriptor>> _gen_runtime_filter_desc();
    StatusOr<std::shared_ptr<RuntimeFilterProbeDescriptor>> _gen_runtime_filter_desc(const TExpr& probe_expr);
    static TExpr _arithmetic_probe_expr(TExprOpcode::type opcode, int32_t literal_value);

    using Int32Decoder = detail::RuntimeColumnPredicateBuilder::DummyDecoder<int32_t>;
    using Int32RuntimeFilter = ComposedRuntimeBloomFilter<TYPE_INT>;
    using Decimal32RuntimeFilter = ComposedRuntimeBloomFilter<TYPE_DECIMAL32>;

    const TypeDescriptor TYPE_DECIMAL32_DESC = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 5, 4);

    ObjectPool _pool;
    RuntimeState _runtime_state;
    TPlanNodeId _node_id = 0;
};

StatusOr<std::shared_ptr<RuntimeFilterProbeDescriptor>> RuntimeRangePrunerTest::_gen_runtime_filter_desc() {
    return _gen_runtime_filter_desc(ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(1, true));
}

StatusOr<std::shared_ptr<RuntimeFilterProbeDescriptor>> RuntimeRangePrunerTest::_gen_runtime_filter_desc(
        const TExpr& probe_expr) {
    TRuntimeFilterDescription desc;
    desc.__set_filter_id(1);
    desc.__set_has_remote_targets(false);
    desc.__set_build_plan_node_id(_node_id);
    desc.__set_build_join_mode(TRuntimeFilterBuildJoinMode::BROADCAST);
    desc.__set_filter_type(TRuntimeFilterBuildType::TOPN_FILTER);

    desc.__isset.plan_node_id_to_target_expr = true;
    desc.plan_node_id_to_target_expr.emplace(_node_id, probe_expr);

    auto runtime_filter_desc = std::make_shared<RuntimeFilterProbeDescriptor>();
    RETURN_IF_ERROR(runtime_filter_desc->init(&_pool, desc, _node_id, &_runtime_state));

    return runtime_filter_desc;
}

// `c0 <opcode> <literal_value>`, flagged monotonic the way the FE flags arithmetic: from the shape
// of the expression alone, ignoring that the BE evaluates it in wrapping arithmetic.
TExpr RuntimeRangePrunerTest::_arithmetic_probe_expr(TExprOpcode::type opcode, int32_t literal_value) {
    TExprNode arithmetic_node;
    arithmetic_node.node_type = TExprNodeType::ARITHMETIC_EXPR;
    arithmetic_node.num_children = 2;
    arithmetic_node.type = TYPE_INT_DESC.to_thrift();
    arithmetic_node.__set_opcode(opcode);
    arithmetic_node.__set_child_type(TPrimitiveType::INT);
    arithmetic_node.__set_is_nullable(true);
    arithmetic_node.__set_is_monotonic(true);

    TExprNode literal_node;
    literal_node.node_type = TExprNodeType::INT_LITERAL;
    literal_node.num_children = 0;
    literal_node.type = TYPE_INT_DESC.to_thrift();
    literal_node.__set_is_nullable(false);
    TIntLiteral literal;
    literal.value = literal_value;
    literal_node.__set_int_literal(literal);

    TExpr probe_expr;
    probe_expr.nodes.emplace_back(arithmetic_node);
    probe_expr.nodes.emplace_back(ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(0, true).nodes[0]);
    probe_expr.nodes.emplace_back(literal_node);
    return probe_expr;
}

TEST_F(RuntimeRangePrunerTest, min_max_parser) {
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

TEST_F(RuntimeRangePrunerTest, min_max_parser_for_decimal) {
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

// The bounds go onto the column, not onto the expression: `c0 + 1` in [10, 20] means `c0` in [9, 19].
TEST_F(RuntimeRangePrunerTest, additive_probe_expr_inverts_bounds_onto_the_column) {
    SlotDescriptor slot(0, "c0", TYPE_INT_DESC);
    std::vector<SlotDescriptor*> slot_descs{&slot};
    ConnectorPredicateParser predicate_parser(&slot_descs);

    ASSIGN_OR_ASSERT_FAIL(auto runtime_filter_desc,
                          _gen_runtime_filter_desc(_arithmetic_probe_expr(TExprOpcode::ADD, 1)));
    ASSERT_OK(runtime_filter_desc->probe_expr_ctx()->prepare(&_runtime_state));
    ASSERT_OK(runtime_filter_desc->probe_expr_ctx()->open(&_runtime_state));

    MinMaxRuntimeFilter<TYPE_INT> rf;
    rf.insert(10);
    rf.insert(20);
    runtime_filter_desc->set_runtime_filter(&rf);

    UnarrivedRuntimeFilterList unarrived_runtime_filters;
    unarrived_runtime_filters.add_unarrived_rf(runtime_filter_desc.get(), &slot, 1);
    RuntimeScanRangePruner pruner(&predicate_parser, unarrived_runtime_filters);

    std::vector<PredicateType> predicate_types;
    std::vector<std::string> predicate_strings;
    ASSERT_OK(pruner.update_range_if_arrived(
            nullptr,
            [&](auto, const PredicateList& predicates) {
                for (const auto* predicate : predicates) {
                    predicate_types.emplace_back(predicate->type());
                    predicate_strings.emplace_back(predicate->debug_string());
                }
                return Status::OK();
            },
            false, 200000));
    ASSERT_EQ(2, predicate_types.size());
    EXPECT_EQ(PredicateType::kGE, predicate_types[0]);
    EXPECT_EQ(PredicateType::kLE, predicate_types[1]);
    EXPECT_EQ("(columnId(0)>=9)", predicate_strings[0]);
    EXPECT_EQ("(columnId(0)<=19)", predicate_strings[1]);

    runtime_filter_desc->close(&_runtime_state);
}

// An overflowing inversion emits nothing, never an empty range: the preimage wraps around the type
// and is no longer an interval, and pruning on it would drop rows the join wanted.
TEST_F(RuntimeRangePrunerTest, inversion_that_overflows_emits_no_predicate) {
    SlotDescriptor slot(0, "c0", TYPE_INT_DESC);
    std::vector<SlotDescriptor*> slot_descs{&slot};
    ConnectorPredicateParser predicate_parser(&slot_descs);

    ASSIGN_OR_ASSERT_FAIL(auto runtime_filter_desc,
                          _gen_runtime_filter_desc(_arithmetic_probe_expr(TExprOpcode::ADD, 1)));
    ASSERT_OK(runtime_filter_desc->probe_expr_ctx()->prepare(&_runtime_state));
    ASSERT_OK(runtime_filter_desc->probe_expr_ctx()->open(&_runtime_state));

    MinMaxRuntimeFilter<TYPE_INT> rf;
    rf.insert(std::numeric_limits<int32_t>::min());
    rf.insert(20);
    runtime_filter_desc->set_runtime_filter(&rf);

    UnarrivedRuntimeFilterList unarrived_runtime_filters;
    unarrived_runtime_filters.add_unarrived_rf(runtime_filter_desc.get(), &slot, 1);
    RuntimeScanRangePruner pruner(&predicate_parser, unarrived_runtime_filters);

    size_t predicates_built = 0;
    ASSERT_OK(pruner.update_range_if_arrived(
            nullptr,
            [&](auto, const PredicateList& predicates) {
                predicates_built += predicates.size();
                return Status::OK();
            },
            false, 200000));
    EXPECT_EQ(0, predicates_built);

    runtime_filter_desc->close(&_runtime_state);
}

// Multiplication is not invertible under wrapping -- wrap(2 * c0) puts two column values on every
// key -- so the pruner declines it even though the FE marked it monotonic.
TEST_F(RuntimeRangePrunerTest, multiplicative_probe_expr_emits_no_predicate) {
    SlotDescriptor slot(0, "c0", TYPE_INT_DESC);
    std::vector<SlotDescriptor*> slot_descs{&slot};
    ConnectorPredicateParser predicate_parser(&slot_descs);

    ASSIGN_OR_ASSERT_FAIL(auto runtime_filter_desc,
                          _gen_runtime_filter_desc(_arithmetic_probe_expr(TExprOpcode::MULTIPLY, 2)));
    ASSERT_OK(runtime_filter_desc->probe_expr_ctx()->prepare(&_runtime_state));
    ASSERT_OK(runtime_filter_desc->probe_expr_ctx()->open(&_runtime_state));

    MinMaxRuntimeFilter<TYPE_INT> rf;
    rf.insert(10);
    rf.insert(20);
    runtime_filter_desc->set_runtime_filter(&rf);

    UnarrivedRuntimeFilterList unarrived_runtime_filters;
    unarrived_runtime_filters.add_unarrived_rf(runtime_filter_desc.get(), &slot, 1);
    RuntimeScanRangePruner pruner(&predicate_parser, unarrived_runtime_filters);

    size_t predicates_built = 0;
    ASSERT_OK(pruner.update_range_if_arrived(
            nullptr,
            [&](auto, const PredicateList& predicates) {
                predicates_built += predicates.size();
                return Status::OK();
            },
            false, 200000));
    EXPECT_EQ(0, predicates_built);

    runtime_filter_desc->close(&_runtime_state);
}

// The predicates built for a dict encoded column must carry decoded strings, not dict codes.
TEST_F(RuntimeRangePrunerTest, dict_encoded_slot_ref_decodes_codes) {
    SlotDescriptor slot(0, "c0", TypeDescriptor::create_varchar_type(10));
    std::vector<SlotDescriptor*> slot_descs{&slot};
    ConnectorPredicateParser predicate_parser(&slot_descs);

    ASSIGN_OR_ASSERT_FAIL(auto runtime_filter_desc,
                          _gen_runtime_filter_desc(ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(0, true)));

    MinMaxRuntimeFilter<TYPE_INT> rf;
    rf.insert(100);
    rf.insert(200);
    runtime_filter_desc->set_runtime_filter(&rf);

    GlobalDictMap dict{{Slice("apple"), 100}, {Slice("melon"), 200}};
    ColumnIdToGlobalDictMap dict_maps;
    dict_maps[0] = &dict;

    UnarrivedRuntimeFilterList unarrived_runtime_filters;
    unarrived_runtime_filters.add_unarrived_rf(runtime_filter_desc.get(), &slot, 1);
    RuntimeScanRangePruner pruner(&predicate_parser, unarrived_runtime_filters);

    std::vector<std::string> predicate_strings;
    ASSERT_OK(pruner.update_range_if_arrived(
            &dict_maps,
            [&](auto, const PredicateList& predicates) {
                for (const auto* predicate : predicates) {
                    predicate_strings.emplace_back(predicate->debug_string());
                }
                return Status::OK();
            },
            false, 200000));
    ASSERT_EQ(2, predicate_strings.size());
    EXPECT_NE(predicate_strings[0].find("apple"), std::string::npos) << predicate_strings[0];
    EXPECT_NE(predicate_strings[1].find("melon"), std::string::npos) << predicate_strings[1];
}

// No index filtering for a monotonic expr over a dict encoded column.
TEST_F(RuntimeRangePrunerTest, monotonic_expr_on_dict_column_falls_back) {
    SlotDescriptor slot(0, "c0", TypeDescriptor::create_varchar_type(10));
    std::vector<SlotDescriptor*> slot_descs{&slot};
    ConnectorPredicateParser predicate_parser(&slot_descs);

    TExprNode add_node;
    add_node.node_type = TExprNodeType::ARITHMETIC_EXPR;
    add_node.num_children = 2;
    add_node.type = TYPE_INT_DESC.to_thrift();
    add_node.__set_opcode(TExprOpcode::ADD);
    add_node.__set_child_type(TPrimitiveType::INT);
    add_node.__set_is_nullable(true);
    add_node.__set_is_monotonic(true);

    TExpr slot_expr = ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(0, true);
    TExprNode literal_node;
    literal_node.node_type = TExprNodeType::INT_LITERAL;
    literal_node.num_children = 0;
    literal_node.type = TYPE_INT_DESC.to_thrift();
    literal_node.__set_is_nullable(false);
    TIntLiteral literal;
    literal.value = 1;
    literal_node.__set_int_literal(literal);

    TExpr probe_expr;
    probe_expr.nodes.emplace_back(add_node);
    probe_expr.nodes.emplace_back(slot_expr.nodes[0]);
    probe_expr.nodes.emplace_back(literal_node);
    ASSIGN_OR_ASSERT_FAIL(auto runtime_filter_desc, _gen_runtime_filter_desc(probe_expr));
    ASSERT_OK(runtime_filter_desc->probe_expr_ctx()->prepare(&_runtime_state));
    ASSERT_OK(runtime_filter_desc->probe_expr_ctx()->open(&_runtime_state));

    MinMaxRuntimeFilter<TYPE_INT> rf;
    rf.insert(100);
    rf.insert(200);
    runtime_filter_desc->set_runtime_filter(&rf);

    GlobalDictMap dict{{Slice("apple"), 100}, {Slice("melon"), 200}};
    ColumnIdToGlobalDictMap dict_maps;
    dict_maps[0] = &dict;

    UnarrivedRuntimeFilterList unarrived_runtime_filters;
    unarrived_runtime_filters.add_unarrived_rf(runtime_filter_desc.get(), &slot, 1);
    RuntimeScanRangePruner pruner(&predicate_parser, unarrived_runtime_filters);

    size_t updater_called = 0;
    ASSERT_OK(pruner.update_range_if_arrived(
            &dict_maps,
            [&](auto, const PredicateList& predicates) {
                updater_called += predicates.size();
                return Status::OK();
            },
            false, 200000));
    ASSERT_EQ(0, updater_called);

    runtime_filter_desc->close(&_runtime_state);
}

TEST_F(RuntimeRangePrunerTest, update_1) {
    SlotDescriptor slot(0, "c0", TYPE_INT_DESC);
    std::vector<SlotDescriptor*> slot_descs{&slot};
    ConnectorPredicateParser predicate_parser(&slot_descs);

    ASSIGN_OR_ASSERT_FAIL(auto runtime_filter_desc, _gen_runtime_filter_desc());

    UnarrivedRuntimeFilterList unarrivedRuntimeFilterList;
    unarrivedRuntimeFilterList.add_unarrived_rf(runtime_filter_desc.get(), &slot, 1);
    RuntimeScanRangePruner pruner(&predicate_parser, unarrivedRuntimeFilterList);

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

TEST_F(RuntimeRangePrunerTest, update_has_null) {
    SlotDescriptor slot(0, "c0", TYPE_INT_DESC);
    std::vector<SlotDescriptor*> slot_descs{&slot};
    ConnectorPredicateParser predicate_parser(&slot_descs);

    ASSIGN_OR_ASSERT_FAIL(auto runtime_filter_desc, _gen_runtime_filter_desc());

    UnarrivedRuntimeFilterList unarrivedRuntimeFilterList;
    unarrivedRuntimeFilterList.add_unarrived_rf(runtime_filter_desc.get(), &slot, 1);
    RuntimeScanRangePruner pruner(&predicate_parser, unarrivedRuntimeFilterList);

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

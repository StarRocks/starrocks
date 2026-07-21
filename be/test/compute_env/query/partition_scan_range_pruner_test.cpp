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

#include "compute_env/query/partition_scan_range_pruner.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/nullable_column.h"
#include "common/object_pool.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "types/type_descriptor.h"

namespace starrocks {

namespace {

TExpr make_int_literal(int32_t value) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::INT_LITERAL);
    node.__set_type(TypeDescriptor(TYPE_INT).to_thrift());
    node.__set_num_children(0);
    TIntLiteral literal;
    literal.__set_value(value);
    node.__set_int_literal(literal);
    TExpr expr;
    expr.nodes.emplace_back(std::move(node));
    return expr;
}

TExpr make_int_null_literal() {
    TExprNode node;
    node.__set_node_type(TExprNodeType::NULL_LITERAL);
    node.__set_type(TypeDescriptor(TYPE_INT).to_thrift());
    node.__set_num_children(0);
    node.__set_is_nullable(true);
    TExpr expr;
    expr.nodes.emplace_back(std::move(node));
    return expr;
}

TExpr make_int_eq_predicate(TTupleId tuple_id, TSlotId slot_id, int32_t value) {
    TExprNode predicate;
    predicate.__set_node_type(TExprNodeType::BINARY_PRED);
    predicate.__set_opcode(TExprOpcode::EQ);
    predicate.__set_child_type(TPrimitiveType::INT);
    predicate.__set_type(TypeDescriptor(TYPE_BOOLEAN).to_thrift());
    predicate.__set_num_children(2);

    TExprNode slot;
    slot.__set_node_type(TExprNodeType::SLOT_REF);
    slot.__set_type(TypeDescriptor(TYPE_INT).to_thrift());
    slot.__set_num_children(0);
    TSlotRef slot_ref;
    slot_ref.__set_tuple_id(tuple_id);
    slot_ref.__set_slot_id(slot_id);
    slot.__set_slot_ref(slot_ref);

    TExpr expr;
    expr.nodes.emplace_back(std::move(predicate));
    expr.nodes.emplace_back(std::move(slot));
    auto literal = make_int_literal(value);
    expr.nodes.emplace_back(std::move(literal.nodes.front()));
    return expr;
}

TScanRangeParams make_scan_range(const TKeyRange& partition_range) {
    TInternalScanRange internal_range;
    internal_range.__set_partition_column_ranges({partition_range});
    TScanRange scan_range;
    scan_range.__set_internal_scan_range(internal_range);
    TScanRangeParams params;
    params.__set_scan_range(scan_range);
    return params;
}

} // namespace

TEST(PartitionScanRangePrunerTest, BuildsInclusiveIntegerRangeWithNull) {
    RuntimeState state;
    ObjectPool pool;
    TSlotDescriptor thrift_slot =
            TSlotDescriptorBuilder().type(LogicalType::TYPE_INT).column_name("p").column_pos(0).nullable(true).build();
    SlotDescriptor slot(thrift_slot);

    TKeyRange range;
    range.__set_column_type(TPrimitiveType::INT);
    range.__set_column_name("p");
    range.__set_begin_key(2);
    range.__set_end_key(4);
    range.__set_has_null(true);

    auto result = build_partition_col_values(&slot, range, &pool, &state);
    ASSERT_TRUE(result.ok()) << result.status();
    auto column = std::move(result).value();
    ASSERT_GE(column->size(), 4);
    const size_t offset = column->size() - 4;
    EXPECT_EQ(2, column->get(offset).get_int32());
    EXPECT_EQ(3, column->get(offset + 1).get_int32());
    EXPECT_EQ(4, column->get(offset + 2).get_int32());
    EXPECT_TRUE(column->get(offset + 3).is_null());
}

TEST(PartitionScanRangePrunerTest, BuildsInclusiveDateRange) {
    RuntimeState state;
    ObjectPool pool;
    TSlotDescriptor thrift_slot = TSlotDescriptorBuilder()
                                          .type(LogicalType::TYPE_DATE)
                                          .column_name("p")
                                          .column_pos(0)
                                          .nullable(false)
                                          .build();
    SlotDescriptor slot(thrift_slot);

    TKeyRange range;
    range.__set_column_type(TPrimitiveType::DATE);
    range.__set_column_name("p");
    range.__set_begin_key(19880730);
    range.__set_end_key(19880801);

    auto result = build_partition_col_values(&slot, range, &pool, &state);
    ASSERT_TRUE(result.ok()) << result.status();
    auto column = std::move(result).value();
    ASSERT_GE(column->size(), 3);
    const size_t offset = column->size() - 3;
    EXPECT_EQ(date::from_date(1988, 7, 30), column->get(offset).get_date().julian());
    EXPECT_EQ(date::from_date(1988, 7, 31), column->get(offset + 1).get_date().julian());
    EXPECT_EQ(date::from_date(1988, 8, 1), column->get(offset + 2).get_date().julian());
}

TEST(PartitionScanRangePrunerTest, BuildsLiteralList) {
    RuntimeState state;
    ObjectPool pool;
    TSlotDescriptor thrift_slot =
            TSlotDescriptorBuilder().type(LogicalType::TYPE_INT).column_name("p").column_pos(0).nullable(true).build();
    SlotDescriptor slot(thrift_slot);

    TKeyRange range;
    range.__set_column_type(TPrimitiveType::INT);
    range.__set_column_name("p");
    range.__set_list_values({make_int_literal(2), make_int_null_literal(), make_int_literal(5)});

    auto result = build_partition_col_values(&slot, range, &pool, &state);
    ASSERT_TRUE(result.ok()) << result.status();
    auto column = std::move(result).value();
    ASSERT_GE(column->size(), 3);
    const size_t offset = column->size() - 3;
    EXPECT_EQ(2, column->get(offset).get_int32());
    EXPECT_TRUE(column->get(offset + 1).is_null());
    EXPECT_EQ(5, column->get(offset + 2).get_int32());
}

TEST(PartitionScanRangePrunerTest, EmptyConjunctsRetainAllRanges) {
    RuntimeState state;
    std::vector<TScanRangeParams> scan_ranges(2);
    std::vector<TScanRangeParams> retained;

    ASSERT_OK(prune_scan_ranges_by_partition_conjuncts(&state, nullptr, {}, scan_ranges, &retained));
    EXPECT_EQ(2, retained.size());
}

TEST(PartitionScanRangePrunerTest, PrunesRangesThatCannotMatch) {
    RuntimeState state;
    TDescriptorTableBuilder descriptor_builder;
    TTupleDescriptorBuilder tuple_builder;
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(LogicalType::TYPE_INT)
                                   .column_name("p")
                                   .column_pos(0)
                                   .nullable(false)
                                   .build());
    tuple_builder.build(&descriptor_builder);

    DescriptorTbl* descriptor_table = nullptr;
    ASSERT_OK(DescriptorTbl::create(&state, state.obj_pool(), descriptor_builder.desc_tbl(), &descriptor_table, 4096));
    state.set_desc_tbl(descriptor_table);
    auto* tuple = descriptor_table->get_tuple_descriptor(0);
    ASSERT_NE(nullptr, tuple);
    ASSERT_EQ(1, tuple->slots().size());

    auto predicate = make_int_eq_predicate(tuple->id(), tuple->slots()[0]->id(), 3);
    ExprContext* predicate_ctx = nullptr;
    ASSERT_OK(ExprFactory::create_expr_tree(state.obj_pool(), predicate, &predicate_ctx, &state));
    std::vector<ExprContext*> predicate_ctxs{predicate_ctx};
    ASSERT_OK(ExprExecutor::prepare(predicate_ctxs, &state));
    ASSERT_OK(ExprExecutor::open(predicate_ctxs, &state));

    TKeyRange rejected_range;
    rejected_range.__set_column_type(TPrimitiveType::INT);
    rejected_range.__set_column_name("p");
    rejected_range.__set_begin_key(1);
    rejected_range.__set_end_key(2);

    TKeyRange retained_range = rejected_range;
    retained_range.__set_begin_key(2);
    retained_range.__set_end_key(4);

    TKeyRange unknown_column_range = rejected_range;
    unknown_column_range.__set_column_name("unknown");

    std::vector<TScanRangeParams> scan_ranges{make_scan_range(rejected_range), make_scan_range(retained_range),
                                              make_scan_range(unknown_column_range)};
    std::vector<TScanRangeParams> retained;
    ASSERT_OK(prune_scan_ranges_by_partition_conjuncts(&state, tuple, {predicate_ctx}, scan_ranges, &retained));
    ASSERT_EQ(2, retained.size());
    EXPECT_EQ(2, retained[0].scan_range.internal_scan_range.partition_column_ranges[0].begin_key);
    EXPECT_EQ("unknown", retained[1].scan_range.internal_scan_range.partition_column_ranges[0].column_name);

    ExprExecutor::close(predicate_ctxs, &state);
}

} // namespace starrocks

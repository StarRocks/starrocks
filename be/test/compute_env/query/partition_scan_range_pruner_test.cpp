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

#include <optional>

#include "base/testutil/assert.h"
#include "column/column_viewer.h"
#include "column/nullable_column.h"
#include "common/object_pool.h"
#include "exec_primitive/runtime_filter/runtime_filter_probe.h"
#include "exprs/column_ref.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "exprs/in_const_predicate.hpp"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_filter.h"
#include "runtime/runtime_filter_factory.h"
#include "runtime/runtime_filter_layout.h"
#include "runtime/runtime_in_filter.h"
#include "runtime/runtime_state.h"
#include "testutil/exprs_test_helper.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"
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

TExpr make_string_literal(const std::string& value) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::STRING_LITERAL);
    node.__set_type(TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH).to_thrift());
    node.__set_num_children(0);
    TStringLiteral literal;
    literal.__set_value(value);
    node.__set_string_literal(literal);
    TExpr expr;
    expr.nodes.emplace_back(std::move(node));
    return expr;
}

TExpr make_largeint_literal(const std::string& value) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::LARGE_INT_LITERAL);
    node.__set_type(TypeDescriptor(TYPE_LARGEINT).to_thrift());
    node.__set_num_children(0);
    TLargeIntLiteral literal;
    literal.__set_value(value);
    node.__set_large_int_literal(literal);
    TExpr expr;
    expr.nodes.emplace_back(std::move(node));
    return expr;
}

TExpr make_date_literal(LogicalType type, const std::string& value) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::DATE_LITERAL);
    node.__set_type(TypeDescriptor(type).to_thrift());
    node.__set_num_children(0);
    TDateLiteral literal;
    literal.__set_value(value);
    node.__set_date_literal(literal);
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

ColumnPtr make_boundary_int_column(std::initializer_list<int32_t> values, bool nullable, bool append_null = false) {
    auto column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), nullable);
    for (int32_t value : values) {
        column->append_datum(Datum(value));
    }
    if (append_null) {
        column->append_nulls(1);
    }
    return column;
}

// Build the filter exactly like HashJoiner does: a bare ColumnRef probe and the values inserted
// through VectorizedInConstPredicateBuilder, so the predicate template follows the probe slot type
// (a CHAR probe yields VectorizedInConstPredicate<TYPE_CHAR>).
template <LogicalType LT>
auto make_runtime_in_filter(RuntimeState* state, TSlotId slot_id, const std::vector<TExpr>& values) {
    auto* pool = state->obj_pool();
    auto column = ColumnHelper::create_column(TypeDescriptor(LT), true);
    for (const auto& literal : values) {
        ExprContext* ctx = nullptr;
        CHECK_OK(ExprFactory::create_expr_tree(pool, literal, &ctx, state));
        CHECK_OK(ctx->prepare(state));
        CHECK_OK(ctx->open(state));
        auto value = ctx->root()->evaluate_const(ctx);
        CHECK_OK(value.status());
        column->append(*ColumnHelper::unpack_and_duplicate_const_column(1, value.value()), 0, 1);
        ctx->close(state);
    }
    VectorizedInConstPredicateBuilder builder(state, pool, pool->add(new ColumnRef(TypeDescriptor(LT), slot_id)));
    builder.use_as_join_runtime_filter();
    CHECK_OK(builder.create());
    builder.add_values(column, 0);
    // String values are stored as Slices into `column`; keep it alive for the test the same way
    // HashJoiner keeps _string_key_columns.
    pool->add(new ColumnPtr(column));
    auto close = [state](ExprContext* ctx) { ctx->close(state); };
    std::unique_ptr<ExprContext, decltype(close)> filter(builder.get_in_const_predicate(), close);
    CHECK_OK(filter->prepare(state));
    CHECK_OK(filter->open(state));
    return filter;
}

auto make_runtime_in_filter(RuntimeState* state, TSlotId slot_id, std::initializer_list<int32_t> values) {
    std::vector<TExpr> literals;
    for (int32_t value : values) {
        literals.push_back(make_int_literal(value));
    }
    return make_runtime_in_filter<TYPE_INT>(state, slot_id, literals);
}

bool in_filter_prunes(const RuntimeFilterPartitionBoundary& boundary, ExprContext* in_filter) {
    RuntimeFilterPartitionBoundaryMap boundaries;
    boundaries[boundary.slot_id].emplace_back(boundary);
    RuntimeFilterPartitionPruner pruner(boundaries);
    pruner.prune_by_in_filters({in_filter});
    return pruner.pruned_partition_count() > 0;
}

// A null filter represents a descriptor whose filter has not arrived.
RuntimeFilterProbeDescriptor* add_bloom_descriptor(ObjectPool* pool, RuntimeState* state,
                                                   RuntimeFilterProbeCollector* collector, int32_t filter_id,
                                                   SlotId slot_id, const RuntimeFilter* filter,
                                                   const TypeDescriptor& type = TypeDescriptor(TYPE_INT)) {
    TRuntimeFilterDescription desc;
    desc.__set_filter_id(filter_id);
    desc.__set_has_remote_targets(false);
    desc.__set_build_plan_node_id(0);
    desc.__set_build_join_mode(TRuntimeFilterBuildJoinMode::BROADCAST);
    desc.__isset.plan_node_id_to_target_expr = true;
    auto probe_expr = ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(slot_id, true);
    probe_expr.nodes[0].__set_type(type.to_thrift());
    desc.plan_node_id_to_target_expr.emplace(0, std::move(probe_expr));
    auto* descriptor = pool->add(new RuntimeFilterProbeDescriptor());
    CHECK(descriptor->init(pool, desc, 0, state).ok());
    if (filter != nullptr) {
        descriptor->set_runtime_filter(filter);
    }
    collector->add_descriptor(descriptor);
    return descriptor;
}

bool bloom_filter_prunes(const RuntimeFilterPartitionBoundary& boundary, const RuntimeFilter& filter,
                         RuntimeState* state) {
    ObjectPool pool;
    RuntimeFilterProbeCollector collector;
    add_bloom_descriptor(&pool, state, &collector, /*filter_id=*/1, boundary.slot_id, &filter, boundary.column_type);
    RuntimeFilterPartitionBoundaryMap boundaries;
    boundaries[boundary.slot_id].emplace_back(boundary);
    RuntimeFilterPartitionPruner pruner(boundaries);
    pruner.prune_by_bloom_filters(collector, state);
    return pruner.pruned_partition_count() > 0;
}

void make_tuple(RuntimeState* state, TupleDescriptor** tuple, bool nullable = true,
                const TypeDescriptor& type = TypeDescriptor(TYPE_INT)) {
    TDescriptorTableBuilder descriptor_builder;
    TTupleDescriptorBuilder tuple_builder;
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(type).column_name("p").column_pos(0).nullable(nullable).build());
    tuple_builder.build(&descriptor_builder);

    DescriptorTbl* descriptor_table = nullptr;
    ASSERT_OK(DescriptorTbl::create(state, state->obj_pool(), descriptor_builder.desc_tbl(), &descriptor_table, 4096));
    state->set_desc_tbl(descriptor_table);
    *tuple = descriptor_table->get_tuple_descriptor(0);
    ASSERT_NE(nullptr, *tuple);
}

TPartitionBoundary make_list_boundary(TSlotId slot_id, std::vector<int64_t> physical_ids,
                                      std::initializer_list<int32_t> values, bool contains_null = false) {
    TPartitionBoundary boundary;
    boundary.__set_physical_partition_ids(std::move(physical_ids));
    boundary.__set_slot_id(slot_id);
    std::vector<TExpr> literals;
    for (int32_t value : values) {
        literals.emplace_back(make_int_literal(value));
    }
    boundary.__set_list_values(literals);
    boundary.__set_contains_null(contains_null);
    return boundary;
}

TPartitionBoundary make_range_boundary(TSlotId slot_id, std::vector<int64_t> physical_ids, std::optional<int32_t> lower,
                                       std::optional<int32_t> upper, bool contains_null = false,
                                       bool upper_closed = false) {
    TPartitionBoundary boundary;
    boundary.__set_physical_partition_ids(std::move(physical_ids));
    boundary.__set_slot_id(slot_id);
    if (lower.has_value()) {
        boundary.__set_range_lower(make_int_literal(*lower));
    }
    if (upper.has_value()) {
        boundary.__set_range_upper(make_int_literal(*upper));
    }
    boundary.__set_contains_null(contains_null);
    if (upper_closed) {
        boundary.__set_range_upper_closed(true);
    }
    return boundary;
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

TEST(PartitionScanRangePrunerTest, ParsesRuntimeFilterPartitionBoundaries) {
    RuntimeState state;
    TupleDescriptor* tuple = nullptr;
    make_tuple(&state, &tuple);
    auto* slot = tuple->slots()[0];

    auto list_boundary = make_list_boundary(slot->id(), {10, 11}, {2, 5}, /*contains_null=*/true);
    auto range_boundary = make_range_boundary(slot->id(), {12}, 2, 5, /*contains_null=*/false,
                                              /*upper_closed=*/true);

    auto boundaries = parse_partition_boundaries(tuple, {list_boundary, range_boundary});

    ASSERT_EQ(1, boundaries.size());
    const auto& slot_boundaries = boundaries.at(slot->id());
    ASSERT_EQ(2, slot_boundaries.size());

    const auto& parsed_list = slot_boundaries[0];
    EXPECT_FALSE(parsed_list.is_range());
    EXPECT_EQ((std::vector<int64_t>{10, 11}), parsed_list.physical_partition_ids);
    ASSERT_EQ(2, parsed_list.list_values->size());
    EXPECT_EQ(2, parsed_list.list_values->get(0).get_int32());
    EXPECT_EQ(5, parsed_list.list_values->get(1).get_int32());

    const auto& parsed_range = slot_boundaries[1];
    EXPECT_TRUE(parsed_range.is_range());
    EXPECT_EQ((std::vector<int64_t>{12}), parsed_range.physical_partition_ids);
    ASSERT_NE(nullptr, parsed_range.lower_bound);
    ASSERT_NE(nullptr, parsed_range.upper_bound);
    ASSERT_EQ(1, parsed_range.lower_bound->size());
    ASSERT_EQ(1, parsed_range.upper_bound->size());
    ASSERT_FALSE(parsed_range.lower_bound->is_null(0));
    ASSERT_FALSE(parsed_range.upper_bound->is_null(0));
    EXPECT_EQ(2, parsed_range.lower_bound->get(0).get_int32());
    EXPECT_EQ(5, parsed_range.upper_bound->get(0).get_int32());
    EXPECT_TRUE(parsed_range.upper_bound_closed);
}

// A boundary column holds exactly the values it was built from: the pruner reads a RANGE bound at row 0, so a
// column padded with leading default rows would compare against the wrong bound and prune live partitions.
TEST(PartitionScanRangePrunerTest, BoundaryColumnsHoldOnlyTheirOwnValues) {
    RuntimeState state;
    TupleDescriptor* tuple = nullptr;
    make_tuple(&state, &tuple, true, TypeDescriptor(TYPE_INT));
    auto* slot = tuple->slots()[0];

    TPartitionBoundary range_boundary;
    range_boundary.__set_physical_partition_ids({11});
    range_boundary.__set_slot_id(slot->id());
    range_boundary.__set_range_lower(make_int_literal(100));
    range_boundary.__set_range_upper(make_int_literal(200));

    TPartitionBoundary list_boundary;
    list_boundary.__set_physical_partition_ids({12});
    list_boundary.__set_slot_id(slot->id());
    list_boundary.__set_list_values({make_int_literal(7), make_int_literal(9)});

    auto boundaries = parse_partition_boundaries(tuple, {range_boundary, list_boundary});
    ASSERT_EQ(1, boundaries.size());
    ASSERT_EQ(2, boundaries.at(slot->id()).size());

    const auto& range = boundaries.at(slot->id())[0];
    ASSERT_EQ(1, range.lower_bound->size());
    ASSERT_EQ(1, range.upper_bound->size());
    EXPECT_EQ(100, range.lower_bound->get(0).get_int32());
    EXPECT_EQ(200, range.upper_bound->get(0).get_int32());

    const auto& list = boundaries.at(slot->id())[1];
    ASSERT_EQ(2, list.list_values->size());
    EXPECT_EQ(7, list.list_values->get(0).get_int32());
    EXPECT_EQ(9, list.list_values->get(1).get_int32());
}

// Integer and date boundaries travel as plain numbers. Each type must decode to the same column the literal
// encoding produces, because both reach the same pruning comparison.
TEST(PartitionScanRangePrunerTest, CompactValuesMatchTheirLiteralForm) {
    auto list_of_ints = [](TSlotId slot_id, std::vector<int64_t> values) {
        TPartitionBoundary boundary;
        boundary.__set_physical_partition_ids({10});
        boundary.__set_slot_id(slot_id);
        boundary.__set_list_int_values(std::move(values));
        return boundary;
    };
    {
        // Every integer width, including negatives and the type's own limits.
        struct Case {
            LogicalType type;
            std::vector<int64_t> values;
        };
        std::vector<Case> cases = {{TYPE_TINYINT, {-128, 0, 127}},
                                   {TYPE_SMALLINT, {-32768, -1, 32767}},
                                   {TYPE_INT, {-2147483648LL, 7, 2147483647LL}},
                                   {TYPE_BIGINT, {-9223372036854775807LL - 1, 0, 9223372036854775807LL}}};
        for (const auto& c : cases) {
            RuntimeState state;
            TupleDescriptor* tuple = nullptr;
            make_tuple(&state, &tuple, true, TypeDescriptor(c.type));
            auto* slot = tuple->slots()[0];
            auto boundaries = parse_partition_boundaries(tuple, {list_of_ints(slot->id(), c.values)});
            const auto& column = boundaries.at(slot->id())[0].list_values;
            ASSERT_EQ(c.values.size(), column->size()) << c.type;
            for (size_t i = 0; i < c.values.size(); ++i) {
                const Datum datum = column->get(i);
                int64_t decoded = 0;
                switch (c.type) {
                case TYPE_TINYINT:
                    decoded = datum.get_int8();
                    break;
                case TYPE_SMALLINT:
                    decoded = datum.get_int16();
                    break;
                case TYPE_INT:
                    decoded = datum.get_int32();
                    break;
                default:
                    decoded = datum.get_int64();
                    break;
                }
                EXPECT_EQ(c.values[i], decoded) << c.type << " at " << i;
            }
        }
    }
    {
        // DATE uses yyyymmdd.
        RuntimeState state;
        TupleDescriptor* tuple = nullptr;
        make_tuple(&state, &tuple, true, TypeDescriptor(TYPE_DATE));
        auto* slot = tuple->slots()[0];
        auto boundaries = parse_partition_boundaries(tuple, {list_of_ints(slot->id(), {20250315})});
        DateValue expected;
        ASSERT_TRUE(expected.from_string("2025-03-15", 10));
        EXPECT_EQ(expected, boundaries.at(slot->id())[0].list_values->get(0).get_date());
    }
    {
        // DATETIME literals preserve microseconds.
        RuntimeState state;
        TupleDescriptor* tuple = nullptr;
        make_tuple(&state, &tuple, true, TypeDescriptor(TYPE_DATETIME));
        auto* slot = tuple->slots()[0];
        const std::string value = "2025-03-15 12:30:45.123456";
        TPartitionBoundary boundary;
        boundary.__set_physical_partition_ids({10});
        boundary.__set_slot_id(slot->id());
        boundary.__set_list_values({make_date_literal(TYPE_DATETIME, value)});
        auto boundaries = parse_partition_boundaries(tuple, {boundary});
        ASSERT_EQ(1, boundaries.size());
        const auto& column = boundaries.at(slot->id())[0].list_values;
        ASSERT_NE(nullptr, column);
        ASSERT_EQ(1, column->size());
        TimestampValue expected;
        ASSERT_TRUE(expected.from_string(value.data(), value.size()));
        EXPECT_EQ(expected, column->get(0).get_timestamp());
    }
    {
        // A NULL-only partition declares an empty list; the boundary is still a LIST one.
        RuntimeState state;
        TupleDescriptor* tuple = nullptr;
        make_tuple(&state, &tuple, true, TypeDescriptor(TYPE_INT));
        auto* slot = tuple->slots()[0];
        auto boundary = list_of_ints(slot->id(), {});
        boundary.__set_contains_null(true);
        auto boundaries = parse_partition_boundaries(tuple, {boundary});
        const auto& parsed = boundaries.at(slot->id())[0];
        ASSERT_NE(nullptr, parsed.list_values);
        EXPECT_EQ(0, parsed.list_values->size());
        EXPECT_FALSE(parsed.is_range());
        EXPECT_TRUE(parsed.contains_null);
    }
    {
        // A column the encoding does not cover drops its boundary instead of pruning on an empty column.
        RuntimeState state;
        TupleDescriptor* tuple = nullptr;
        make_tuple(&state, &tuple, true, TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH));
        auto* slot = tuple->slots()[0];
        EXPECT_TRUE(parse_partition_boundaries(tuple, {list_of_ints(slot->id(), {7})}).empty());
    }
}

// Boundary values of every partition key type are decoded straight from their thrift literal nodes.
TEST(PartitionScanRangePrunerTest, BoundaryLiteralsOfEveryPartitionKeyType) {
    auto boundary_of = [](TSlotId slot_id, std::vector<TExpr> values) {
        TPartitionBoundary boundary;
        boundary.__set_physical_partition_ids({10});
        boundary.__set_slot_id(slot_id);
        boundary.__set_list_values(std::move(values));
        return boundary;
    };

    {
        // Integer key, NULL included.
        RuntimeState state;
        TupleDescriptor* tuple = nullptr;
        make_tuple(&state, &tuple, true, TypeDescriptor(TYPE_INT));
        auto* slot = tuple->slots()[0];
        auto boundaries = parse_partition_boundaries(
                tuple, {boundary_of(slot->id(), {make_int_literal(2), make_int_null_literal(), make_int_literal(5)})});
        ASSERT_EQ(1, boundaries.size());
        const auto& values = boundaries.at(slot->id())[0].list_values;
        ASSERT_EQ(3, values->size());
        EXPECT_EQ(2, values->get(0).get_int32());
        EXPECT_TRUE(values->get(1).is_null());
        EXPECT_EQ(5, values->get(2).get_int32());
    }
    {
        // DATE key, the most common RANGE partition column.
        RuntimeState state;
        TupleDescriptor* tuple = nullptr;
        make_tuple(&state, &tuple, true, TypeDescriptor(TYPE_DATE));
        auto* slot = tuple->slots()[0];
        auto boundaries = parse_partition_boundaries(
                tuple, {boundary_of(slot->id(), {make_date_literal(TYPE_DATE, "2025-03-15")})});
        ASSERT_EQ(1, boundaries.size());
        const auto& values = boundaries.at(slot->id())[0].list_values;
        ASSERT_EQ(1, values->size());
        DateValue expected;
        ASSERT_TRUE(expected.from_string("2025-03-15", 10));
        EXPECT_EQ(expected, values->get(0).get_date());
    }
    {
        // VARCHAR key, a LIST partition column.
        RuntimeState state;
        TupleDescriptor* tuple = nullptr;
        make_tuple(&state, &tuple, true, TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH));
        auto* slot = tuple->slots()[0];
        auto boundaries = parse_partition_boundaries(
                tuple, {boundary_of(slot->id(), {make_string_literal("beijing"), make_string_literal("shanghai")})});
        ASSERT_EQ(1, boundaries.size());
        const auto& values = boundaries.at(slot->id())[0].list_values;
        ASSERT_EQ(2, values->size());
        EXPECT_EQ("beijing", values->get(0).get_slice().to_string());
        EXPECT_EQ("shanghai", values->get(1).get_slice().to_string());
    }
    {
        // LARGEINT key: its literal carries the value as text, and the decoder keeps that rule.
        RuntimeState state;
        TupleDescriptor* tuple = nullptr;
        make_tuple(&state, &tuple, true, TypeDescriptor(TYPE_LARGEINT));
        auto* slot = tuple->slots()[0];
        auto boundaries = parse_partition_boundaries(
                tuple, {boundary_of(slot->id(), {make_largeint_literal("12345"), make_largeint_literal("67890")})});
        ASSERT_EQ(1, boundaries.size());
        const auto& values = boundaries.at(slot->id())[0].list_values;
        ASSERT_EQ(2, values->size());
        EXPECT_EQ(12345, values->get(0).get_int128());
        EXPECT_EQ(67890, values->get(1).get_int128());
    }
}

// FE serializes CHAR literals as VARCHAR.
TEST(PartitionScanRangePrunerTest, ParsesCharBoundaryFromVarcharLiteral) {
    RuntimeState state;
    TupleDescriptor* tuple = nullptr;
    make_tuple(&state, &tuple, false, TypeDescriptor(TYPE_CHAR));
    auto* slot = tuple->slots()[0];

    auto literal = make_string_literal("beijing");

    TPartitionBoundary list_boundary;
    list_boundary.__set_physical_partition_ids({10});
    list_boundary.__set_slot_id(slot->id());
    list_boundary.__set_list_values({literal});

    TPartitionBoundary range_boundary;
    range_boundary.__set_physical_partition_ids({11});
    range_boundary.__set_slot_id(slot->id());
    range_boundary.__set_range_lower(literal);

    auto boundaries = parse_partition_boundaries(tuple, {list_boundary, range_boundary});
    ASSERT_EQ(1, boundaries.size());
    ASSERT_EQ(2, boundaries.at(slot->id()).size());
    EXPECT_EQ(1, boundaries.at(slot->id())[0].list_values->size());
    ASSERT_NE(nullptr, boundaries.at(slot->id())[1].lower_bound);
}

// FE serializes VARBINARY literals as VARCHAR.
TEST(PartitionScanRangePrunerTest, ParsesVarbinaryBoundaryFromVarcharLiteral) {
    RuntimeState state;
    TupleDescriptor* tuple = nullptr;
    make_tuple(&state, &tuple, false, TypeDescriptor::create_varbinary_type(TypeDescriptor::MAX_VARCHAR_LENGTH));
    auto* slot = tuple->slots()[0];

    auto literal = make_string_literal("payload");

    TPartitionBoundary list_boundary;
    list_boundary.__set_physical_partition_ids({10});
    list_boundary.__set_slot_id(slot->id());
    list_boundary.__set_list_values({literal});

    auto boundaries = parse_partition_boundaries(tuple, {list_boundary});
    ASSERT_EQ(1, boundaries.size());
    ASSERT_EQ(1, boundaries.at(slot->id()).size());
    const auto& parsed = boundaries.at(slot->id())[0];
    ASSERT_NE(nullptr, parsed.list_values);
    ASSERT_EQ(1, parsed.list_values->size());
    EXPECT_EQ("payload", parsed.list_values->get(0).get_slice().to_string());
}

TEST(PartitionScanRangePrunerTest, ParseIsFailOpenOnMalformedBoundaries) {
    RuntimeState state;
    TupleDescriptor* tuple = nullptr;
    make_tuple(&state, &tuple, /*nullable=*/false);
    auto* slot = tuple->slots()[0];

    // Unknown slot id: dropped.
    auto unknown_slot = make_list_boundary(999, {10}, {2});
    // list_values decides the kind: stray range fields are ignored, the entry is a LIST.
    auto list_wins = make_list_boundary(slot->id(), {11}, {2});
    list_wins.__set_range_lower(make_int_literal(1));
    // A well-formed entry parsed regardless of the bad ones around it.
    auto good = make_range_boundary(slot->id(), {14}, 3, std::nullopt);

    auto boundaries = parse_partition_boundaries(tuple, {unknown_slot, list_wins, good});

    ASSERT_EQ(1, boundaries.size());
    const auto& slot_boundaries = boundaries.at(slot->id());
    ASSERT_EQ(2, slot_boundaries.size());
    EXPECT_FALSE(slot_boundaries[0].is_range());
    EXPECT_EQ((std::vector<int64_t>{11}), slot_boundaries[0].physical_partition_ids);
    EXPECT_TRUE(slot_boundaries[1].is_range());
    EXPECT_EQ((std::vector<int64_t>{14}), slot_boundaries[1].physical_partition_ids);
}

TEST(PartitionScanRangePrunerTest, ListBoundaryUsesMembershipFilter) {
    RuntimeState state;
    RuntimeFilterPartitionBoundary boundary;
    boundary.slot_id = 1;
    boundary.column_type = TypeDescriptor(TYPE_INT);
    boundary.list_values = make_boundary_int_column({2, 5}, true);
    boundary.physical_partition_ids = {10};

    ComposedRuntimeBloomFilter<TYPE_INT> filter;
    filter.membership_filter().init(16);
    filter.insert(7);
    EXPECT_TRUE(bloom_filter_prunes(boundary, filter, &state));

    filter.insert(2);
    EXPECT_FALSE(bloom_filter_prunes(boundary, filter, &state));
}

TEST(PartitionScanRangePrunerTest, ListBoundaryRespectsFilterNull) {
    RuntimeState state;
    RuntimeFilterPartitionBoundary boundary;
    boundary.slot_id = 1;
    boundary.column_type = TypeDescriptor(TYPE_INT);
    boundary.contains_null = true;
    boundary.list_values = make_boundary_int_column({2}, true, /*append_null=*/true);
    boundary.physical_partition_ids = {10};

    // NULL can match even when all non-null values miss.
    ComposedRuntimeBloomFilter<TYPE_INT> filter;
    filter.membership_filter().init(16);
    filter.insert(7);
    filter.insert_null();
    EXPECT_FALSE(bloom_filter_prunes(boundary, filter, &state));
}

// Fill shards before concat(), which moves their bloom filters.
void fill_partitioned_filter(std::vector<ComposedRuntimeBloomFilter<TYPE_INT>>& shards,
                             const std::vector<int32_t>& per_shard_value,
                             ComposedRuntimeBloomFilter<TYPE_INT>* merged) {
    for (size_t i = 0; i < shards.size(); ++i) {
        shards[i].membership_filter().init(16);
        if (per_shard_value[i] >= 0) {
            shards[i].insert(per_shard_value[i]);
        }
    }
    for (auto& shard : shards) {
        merged->concat(&shard);
    }
    merged->get_membership_filter()->set_join_mode(TRuntimeFilterBuildJoinMode::PARTITIONED);
    merged->get_membership_filter()->set_global();
}

// Global filters require hash routing before membership lookup.
TEST(PartitionScanRangePrunerTest, PartitionedFilterRoutesValuesToTheirShard) {
    constexpr int kShards = 4;
    std::unordered_set<size_t> routed_shards;
    for (int32_t probe_value : {7, 8, 9, 10}) {
        SCOPED_TRACE(probe_value);
        RuntimeState state;

        RuntimeFilterPartitionBoundary boundary;
        boundary.physical_partition_ids = {10};
        boundary.slot_id = 1;
        boundary.column_type = TypeDescriptor(TYPE_INT);
        boundary.list_values = make_boundary_int_column({probe_value}, true);

        // Use the same hash routing as the pruning path.
        size_t routed_shard = 0;
        {
            ObjectPool pool;
            RuntimeFilterProbeCollector collector;
            auto* descriptor = add_bloom_descriptor(&pool, &state, &collector, 1, boundary.slot_id, nullptr);
            std::vector<ComposedRuntimeBloomFilter<TYPE_INT>> shards(kShards);
            ComposedRuntimeBloomFilter<TYPE_INT> probe;
            fill_partitioned_filter(shards, std::vector<int32_t>(kShards, -1), &probe);
            ASSERT_EQ(kShards, probe.num_hash_partitions());
            RuntimeFilter::RunningContext ctx;
            ctx.use_merged_selection = false;
            ctx.compatibility = state.func_version() <= 3 || !state.enable_pipeline_engine();
            ctx.exchange_hash_function_version = state.query_options().exchange_hash_function_version;
            ctx.selection.assign(1, 1);
            probe.compute_partition_index(descriptor->layout(), {boundary.list_values.get()}, &ctx);
            ASSERT_EQ(1, ctx.hash_values.size());
            ASSERT_LT(ctx.hash_values[0], kShards);
            routed_shard = ctx.hash_values[0];
            routed_shards.insert(routed_shard);
        }

        // Only the routed shard holds the value: judging must find it.
        {
            std::vector<int32_t> values(kShards, -1);
            values[routed_shard] = probe_value;
            std::vector<ComposedRuntimeBloomFilter<TYPE_INT>> shards(kShards);
            ComposedRuntimeBloomFilter<TYPE_INT> filter;
            fill_partitioned_filter(shards, values, &filter);
            EXPECT_FALSE(bloom_filter_prunes(boundary, filter, &state));
        }

        // A value in another shard must not count as a match.
        {
            std::vector<int32_t> values(kShards, probe_value);
            values[routed_shard] = -1;
            std::vector<ComposedRuntimeBloomFilter<TYPE_INT>> shards(kShards);
            ComposedRuntimeBloomFilter<TYPE_INT> filter;
            fill_partitioned_filter(shards, values, &filter);
            EXPECT_TRUE(bloom_filter_prunes(boundary, filter, &state));
        }
    }
    EXPECT_GT(routed_shards.size(), 1);
}

// Bitset filters use membership for LIST and min/max for RANGE.
TEST(PartitionScanRangePrunerTest, BitsetFilterJudgesListAndRange) {
    RuntimeState state;
    ComposedRuntimeBitsetFilter<TYPE_INT> filter;
    // Initialize the range before allocating the bitset.
    filter.membership_filter().set_min_max(5, 8);
    filter.membership_filter().init(4);
    filter.insert(5);
    filter.insert(8);
    ASSERT_EQ(0, filter.num_hash_partitions());

    RuntimeFilterPartitionBoundary list_boundary;
    list_boundary.physical_partition_ids = {10};
    list_boundary.slot_id = 1;
    list_boundary.column_type = TypeDescriptor(TYPE_INT);
    list_boundary.list_values = make_boundary_int_column({6, 7}, true);
    EXPECT_TRUE(bloom_filter_prunes(list_boundary, filter, &state));
    list_boundary.list_values = make_boundary_int_column({6, 8}, true);
    EXPECT_FALSE(bloom_filter_prunes(list_boundary, filter, &state));

    RuntimeFilterPartitionBoundary range_boundary;
    range_boundary.physical_partition_ids = {11};
    range_boundary.slot_id = 1;
    range_boundary.column_type = TypeDescriptor(TYPE_INT);
    range_boundary.lower_bound = make_boundary_int_column({20}, false);
    range_boundary.upper_bound = make_boundary_int_column({30}, false);
    EXPECT_TRUE(bloom_filter_prunes(range_boundary, filter, &state));
}

TEST(PartitionScanRangePrunerTest, OnlyNullPartitionFollowsFilterNullFlag) {
    RuntimeState state;
    // A partition whose value set is exactly {NULL} is decided by the filter's NULL flag alone.
    RuntimeFilterPartitionBoundary boundary;
    boundary.physical_partition_ids = {10};
    boundary.slot_id = 1;
    boundary.column_type = TypeDescriptor(TYPE_INT);
    boundary.contains_null = true;
    boundary.list_values = make_boundary_int_column({}, true, /*append_null=*/true);

    // Plain equi-join filter: NULL never matches, so the whole partition is prunable.
    ComposedRuntimeBloomFilter<TYPE_INT> without_null;
    without_null.membership_filter().init(16);
    without_null.insert(7);
    EXPECT_TRUE(bloom_filter_prunes(boundary, without_null, &state));

    // Null-safe join whose build side holds a NULL key: the partition must be kept.
    ComposedRuntimeBloomFilter<TYPE_INT> with_null;
    with_null.membership_filter().init(16);
    with_null.insert(7);
    with_null.insert_null();
    EXPECT_FALSE(bloom_filter_prunes(boundary, with_null, &state));
}

TEST(PartitionScanRangePrunerTest, ClearedBloomDegradesToMinMaxComponent) {
    RuntimeState state;
    RuntimeFilterPartitionBoundary boundary;
    boundary.slot_id = 1;
    boundary.column_type = TypeDescriptor(TYPE_INT);
    boundary.list_values = make_boundary_int_column({2, 5}, true);
    boundary.physical_partition_ids = {10};

    // Bloom would prune {2, 5} (only 1 and 10 inserted), min/max [1, 10] alone would not.
    ComposedRuntimeBloomFilter<TYPE_INT> filter;
    filter.membership_filter().init(16);
    filter.insert(1);
    filter.insert(10);
    EXPECT_TRUE(bloom_filter_prunes(boundary, filter, &state));

    // An unusable membership filter must fall back to min/max.
    filter.membership_filter().clear_bf();
    ASSERT_FALSE(filter.membership_filter().can_use_bf());
    EXPECT_FALSE(bloom_filter_prunes(boundary, filter, &state));

    // The min/max component keeps working after the degradation.
    RuntimeFilterPartitionBoundary outside;
    outside.slot_id = 1;
    outside.column_type = TypeDescriptor(TYPE_INT);
    outside.list_values = make_boundary_int_column({20, 30}, true);
    outside.physical_partition_ids = {11};
    EXPECT_TRUE(bloom_filter_prunes(outside, filter, &state));
}

TEST(PartitionScanRangePrunerTest, EmptyFilterCarrierKeepsMinMaxComponent) {
    RuntimeState state;
    // EMPTY_FILTER retains min/max after membership is dropped during merge.
    ComposedRuntimeBloomFilter<TYPE_INT> bloom;
    bloom.membership_filter().init(16);
    bloom.insert(1);
    bloom.insert(10);
    ObjectPool pool;
    RuntimeFilter* empty_carrier = RuntimeFilterFactory::to_empty_filter(&pool, &bloom);
    ASSERT_NE(nullptr, empty_carrier);
    ASSERT_EQ(RuntimeFilterSerializeType::EMPTY_FILTER, empty_carrier->type());

    // Membership has no capability: values inside min/max are kept.
    RuntimeFilterPartitionBoundary inside;
    inside.slot_id = 1;
    inside.column_type = TypeDescriptor(TYPE_INT);
    inside.list_values = make_boundary_int_column({2, 5}, true);
    inside.physical_partition_ids = {10};
    EXPECT_FALSE(bloom_filter_prunes(inside, *empty_carrier, &state));

    // The min/max component keeps pruning.
    RuntimeFilterPartitionBoundary outside;
    outside.slot_id = 1;
    outside.column_type = TypeDescriptor(TYPE_INT);
    outside.lower_bound = make_boundary_int_column({12}, true);
    outside.upper_bound = make_boundary_int_column({15}, true);
    outside.physical_partition_ids = {11};
    EXPECT_TRUE(bloom_filter_prunes(outside, *empty_carrier, &state));
}

TEST(PartitionScanRangePrunerTest, RangeBoundaryUsesMinMaxWithOpenUpperBound) {
    RuntimeState state;
    RuntimeFilterPartitionBoundary boundary;
    boundary.slot_id = 1;
    boundary.column_type = TypeDescriptor(TYPE_INT);
    boundary.lower_bound = make_boundary_int_column({5}, true);
    boundary.upper_bound = make_boundary_int_column({7}, true);
    boundary.physical_partition_ids = {10};

    // min == upper proves disjointness: the upper bound is exclusive.
    ComposedRuntimeBloomFilter<TYPE_INT> at_upper;
    at_upper.membership_filter().init(16);
    at_upper.insert(7);
    at_upper.insert(10);
    EXPECT_TRUE(bloom_filter_prunes(boundary, at_upper, &state));

    // max < lower proves disjointness.
    ComposedRuntimeBloomFilter<TYPE_INT> below_lower;
    below_lower.membership_filter().init(16);
    below_lower.insert(1);
    EXPECT_TRUE(bloom_filter_prunes(boundary, below_lower, &state));

    // RANGE uses min/max only, so gaps in bloom membership cannot prune it.
    ComposedRuntimeBloomFilter<TYPE_INT> bloom_gap;
    bloom_gap.membership_filter().init(16);
    bloom_gap.insert(1);
    bloom_gap.insert(10);
    EXPECT_FALSE(bloom_filter_prunes(boundary, bloom_gap, &state));

    // A NULL match keeps the partition even when its non-null range is disjoint.
    RuntimeFilterPartitionBoundary null_boundary = boundary;
    null_boundary.contains_null = true;
    ComposedRuntimeBloomFilter<TYPE_INT> with_null;
    with_null.membership_filter().init(16);
    with_null.insert(100);
    with_null.insert_null();
    EXPECT_FALSE(bloom_filter_prunes(null_boundary, with_null, &state));
}

TEST(PartitionScanRangePrunerTest, ExactJoinInFilterJudgesRangeAndList) {
    RuntimeState state;
    TupleDescriptor* tuple = nullptr;
    make_tuple(&state, &tuple, /*nullable=*/false);
    auto* slot = tuple->slots()[0];

    auto in_filter = make_runtime_in_filter(&state, slot->id(), {5, 8});

    // IN {5, 8} misses the exclusive range [3, 5).
    RuntimeFilterPartitionBoundary range_boundary;
    range_boundary.physical_partition_ids = {10, 11};
    range_boundary.slot_id = slot->id();
    range_boundary.column_type = TypeDescriptor(TYPE_INT);
    range_boundary.lower_bound = make_boundary_int_column({3}, false);
    range_boundary.upper_bound = make_boundary_int_column({5}, false);
    EXPECT_TRUE(in_filter_prunes(range_boundary, in_filter.get()));

    // A closed upper bound includes 5.
    range_boundary.upper_bound_closed = true;
    EXPECT_FALSE(in_filter_prunes(range_boundary, in_filter.get()));
    range_boundary.upper_bound_closed = false;

    // RANGE [1, 6) contains 5, so the partition may intersect.
    range_boundary.lower_bound = make_boundary_int_column({1}, false);
    range_boundary.upper_bound = make_boundary_int_column({6}, false);
    EXPECT_FALSE(in_filter_prunes(range_boundary, in_filter.get()));

    // LIST {2, 4}: neither value is in the IN set, prune; LIST {2, 5}: 5 matches, keep.
    RuntimeFilterPartitionBoundary list_boundary;
    list_boundary.physical_partition_ids = {12};
    list_boundary.slot_id = slot->id();
    list_boundary.column_type = TypeDescriptor(TYPE_INT);
    list_boundary.list_values = make_boundary_int_column({2, 4}, true);
    EXPECT_TRUE(in_filter_prunes(list_boundary, in_filter.get()));
    list_boundary.list_values = make_boundary_int_column({2, 5}, true);
    EXPECT_FALSE(in_filter_prunes(list_boundary, in_filter.get()));
}

// Set NULL flags as HashJoiner does: eq_null from the join, null_in_set from build keys.
TEST(PartitionScanRangePrunerTest, ExactInFilterHonoursNullSafeSemantics) {
    RuntimeState state;
    TupleDescriptor* tuple = nullptr;
    make_tuple(&state, &tuple, /*nullable=*/true);
    auto* slot = tuple->slots()[0];

    auto in_filter = make_runtime_in_filter(&state, slot->id(), {5, 8});
    auto* predicate = down_cast<VectorizedInConstPredicate<TYPE_INT>*>(in_filter->root());

    // Only NULL can match: all non-null values miss.
    RuntimeFilterPartitionBoundary boundary;
    boundary.physical_partition_ids = {10};
    boundary.slot_id = slot->id();
    boundary.column_type = TypeDescriptor(TYPE_INT);
    boundary.contains_null = true;
    boundary.list_values = make_boundary_int_column({2, 4}, true, /*append_null=*/true);

    RuntimeFilterPartitionBoundaryMap boundaries;
    boundaries[slot->id()].emplace_back(boundary);

    // Plain equi-join: NULL never matches, so the partition holds nothing joinable.
    {
        RuntimeFilterPartitionPruner pruner(boundaries);
        pruner.prune_by_in_filters({in_filter.get()});
        EXPECT_EQ(1, pruner.pruned_partition_count());
        EXPECT_TRUE(pruner.is_partition_pruned(10));
    }

    // Null-safe joins still prune NULL when the build side has no NULL key.
    predicate->set_eq_null(true);
    {
        RuntimeFilterPartitionPruner pruner(boundaries);
        pruner.prune_by_in_filters({in_filter.get()});
        EXPECT_EQ(1, pruner.pruned_partition_count());
        EXPECT_TRUE(pruner.is_partition_pruned(10));
    }

    // Null-safe join with a NULL build key: NULL matches NULL, so the partition is kept.
    predicate->set_null_in_set(true);
    {
        RuntimeFilterPartitionPruner pruner(boundaries);
        pruner.prune_by_in_filters({in_filter.get()});
        EXPECT_EQ(0, pruner.pruned_partition_count());
        EXPECT_FALSE(pruner.is_partition_pruned(10));
    }
}

TEST(PartitionScanRangePrunerTest, UpdateIgnoresInFilterOfAnotherSlot) {
    RuntimeState state;
    TupleDescriptor* tuple = nullptr;
    make_tuple(&state, &tuple, /*nullable=*/false);
    auto* slot = tuple->slots()[0];

    auto in_filter = make_runtime_in_filter(&state, slot->id(), {5, 8});

    RuntimeFilterPartitionBoundary boundary;
    boundary.physical_partition_ids = {10};
    boundary.slot_id = slot->id() + 1; // a different partition column
    boundary.column_type = TypeDescriptor(TYPE_INT);
    boundary.list_values = make_boundary_int_column({2, 4}, true);

    RuntimeFilterPartitionBoundaryMap boundaries;
    boundaries[boundary.slot_id].emplace_back(boundary);

    RuntimeFilterPartitionPruner pruner(boundaries);
    pruner.prune_by_in_filters({in_filter.get()});
    EXPECT_EQ(0, pruner.pruned_partition_count());
    EXPECT_FALSE(pruner.is_partition_pruned(10));
}

TEST(PartitionScanRangePrunerTest, ClosedUpperBoundJudgesInclusively) {
    RuntimeState state;
    // First-column projection of a multi-column RANGE partition: [5, 7] closed.
    RuntimeFilterPartitionBoundary boundary;
    boundary.slot_id = 1;
    boundary.column_type = TypeDescriptor(TYPE_INT);
    boundary.lower_bound = make_boundary_int_column({5}, true);
    boundary.upper_bound = make_boundary_int_column({7}, true);
    boundary.upper_bound_closed = true;
    boundary.physical_partition_ids = {10};

    // min == upper no longer proves disjointness under a closed upper bound.
    ComposedRuntimeBloomFilter<TYPE_INT> at_upper;
    at_upper.membership_filter().init(16);
    at_upper.insert(7);
    at_upper.insert(10);
    EXPECT_FALSE(bloom_filter_prunes(boundary, at_upper, &state));

    ComposedRuntimeBloomFilter<TYPE_INT> above_upper;
    above_upper.membership_filter().init(16);
    above_upper.insert(8);
    EXPECT_TRUE(bloom_filter_prunes(boundary, above_upper, &state));
}

TEST(PartitionScanRangePrunerTest, RepeatedInPruningDoesNotDoubleCountPhysicalPartitions) {
    RuntimeState state;
    TupleDescriptor* tuple = nullptr;
    make_tuple(&state, &tuple, /*nullable=*/false);
    auto* slot = tuple->slots()[0];

    auto in_filter = make_runtime_in_filter(&state, slot->id(), {2, 8});

    RuntimeFilterPartitionBoundary boundary;
    boundary.physical_partition_ids = {10, 11};
    boundary.slot_id = slot->id();
    boundary.column_type = TypeDescriptor(TYPE_INT);
    boundary.lower_bound = make_boundary_int_column({3}, false);
    boundary.upper_bound = make_boundary_int_column({5}, false);

    RuntimeFilterPartitionBoundaryMap boundaries;
    boundaries[slot->id()].emplace_back(boundary);

    RuntimeFilterPartitionPruner pruner(boundaries);
    pruner.prune_by_in_filters({in_filter.get()});
    // pruned counts physical partitions, and this boundary carries two of them.
    EXPECT_EQ(2, pruner.pruned_partition_count());
    // A DISJOINT verdict marks every physical partition the boundary carries.
    EXPECT_TRUE(pruner.is_partition_pruned(10));
    EXPECT_TRUE(pruner.is_partition_pruned(11));
    EXPECT_FALSE(pruner.is_partition_pruned(12));

    // Re-evaluation must not count the same physical partition again.
    pruner.prune_by_in_filters({in_filter.get()});
    EXPECT_EQ(2, pruner.pruned_partition_count());
}

TEST(PartitionScanRangePrunerTest, LateBloomFilterIsRetriedThenJudgedOnce) {
    RuntimeState state;
    ObjectPool pool;
    RuntimeFilterProbeCollector collector;
    auto* descriptor = add_bloom_descriptor(&pool, &state, &collector, 1, 1, nullptr);

    RuntimeFilterPartitionBoundary boundary;
    boundary.slot_id = 1;
    boundary.column_type = TypeDescriptor(TYPE_INT);
    boundary.list_values = make_boundary_int_column({2, 5}, true);
    boundary.physical_partition_ids = {10};
    RuntimeFilterPartitionBoundaryMap boundaries;
    boundaries[1].emplace_back(boundary);

    RuntimeFilterPartitionPruner pruner(boundaries);
    pruner.prune_by_bloom_filters(collector, &state);
    EXPECT_EQ(0, pruner.pruned_partition_count());
    EXPECT_FALSE(pruner.is_partition_pruned(10));

    ComposedRuntimeBloomFilter<TYPE_INT> filter;
    filter.membership_filter().init(16);
    filter.insert(7);
    descriptor->set_runtime_filter(&filter);
    pruner.prune_by_bloom_filters(collector, &state);
    EXPECT_EQ(1, pruner.pruned_partition_count());
    EXPECT_TRUE(pruner.is_partition_pruned(10));

    // Every filter is consumed now: later checkpoints exit on the fast path.
    pruner.prune_by_bloom_filters(collector, &state);
    EXPECT_EQ(1, pruner.pruned_partition_count());
}

// Both channels publish into the same set.
TEST(PartitionScanRangePrunerTest, ChannelsShareOnePrunedSet) {
    RuntimeState state;
    TupleDescriptor* tuple = nullptr;
    make_tuple(&state, &tuple, /*nullable=*/false);
    auto* slot = tuple->slots()[0];

    // IN prunes the first partition; bloom can prune both.
    RuntimeFilterPartitionBoundary first;
    first.physical_partition_ids = {10};
    first.slot_id = slot->id();
    first.column_type = TypeDescriptor(TYPE_INT);
    first.list_values = make_boundary_int_column({2, 4}, true);
    RuntimeFilterPartitionBoundary second;
    second.physical_partition_ids = {11};
    second.slot_id = slot->id();
    second.column_type = TypeDescriptor(TYPE_INT);
    second.list_values = make_boundary_int_column({5, 9}, true);
    RuntimeFilterPartitionBoundaryMap boundaries;
    boundaries[slot->id()].emplace_back(first);
    boundaries[slot->id()].emplace_back(second);

    auto in_filter = make_runtime_in_filter(&state, slot->id(), {5, 8});

    RuntimeFilterPartitionPruner pruner(boundaries);
    pruner.prune_by_in_filters({in_filter.get()});
    EXPECT_EQ(1, pruner.pruned_partition_count());
    EXPECT_TRUE(pruner.is_partition_pruned(10));
    EXPECT_FALSE(pruner.is_partition_pruned(11));

    // Partition 10 is already pruned, so bloom only adds partition 11.
    ObjectPool pool;
    RuntimeFilterProbeCollector collector;
    ComposedRuntimeBloomFilter<TYPE_INT> filter;
    filter.membership_filter().init(16);
    filter.insert(100);
    add_bloom_descriptor(&pool, &state, &collector, 1, slot->id(), &filter);
    pruner.prune_by_bloom_filters(collector, &state);
    EXPECT_EQ(2, pruner.pruned_partition_count());
    EXPECT_TRUE(pruner.is_partition_pruned(11));

    // Everything is pruned: the entry check turns any further call into a no-op.
    pruner.prune_by_bloom_filters(collector, &state);
    EXPECT_EQ(2, pruner.pruned_partition_count());
}

TEST(PartitionScanRangePrunerTest, InPruningCompletesWithPendingBloomFilter) {
    RuntimeState state;
    TupleDescriptor* tuple = nullptr;
    make_tuple(&state, &tuple, false);
    auto slot_id = tuple->slots()[0]->id();
    auto boundaries = parse_partition_boundaries(
            tuple, {make_list_boundary(slot_id, {10, 11}, {2}), make_list_boundary(slot_id, {12}, {3})});
    auto in_filter = make_runtime_in_filter(&state, slot_id, {7});
    ObjectPool pool;
    RuntimeFilterProbeCollector collector;
    add_bloom_descriptor(&pool, &state, &collector, 1, slot_id, nullptr);
    RuntimeFilterPartitionPruner pruner(boundaries);
    pruner.prune_by_in_filters({in_filter.get()});
    EXPECT_EQ(3, pruner.pruned_partition_count());
    EXPECT_TRUE(pruner._bloom_pruning_complete.load());
    for (int i = 0; i < 10; ++i) {
        pruner.prune_by_bloom_filters(collector, &state);
        EXPECT_EQ(3, pruner.pruned_partition_count());
        EXPECT_TRUE(pruner.is_partition_pruned(10));
        EXPECT_TRUE(pruner.is_partition_pruned(11));
        EXPECT_TRUE(pruner.is_partition_pruned(12));
        EXPECT_FALSE(pruner.is_partition_pruned(99));
    }
}

TEST(PartitionScanRangePrunerTest, AllPartitionsPrunedCompletesWithPendingBloomFilter) {
    RuntimeState state;
    TupleDescriptor* tuple = nullptr;
    make_tuple(&state, &tuple, false);
    auto slot_id = tuple->slots()[0]->id();
    auto boundaries = parse_partition_boundaries(tuple, {make_list_boundary(slot_id, {10, 11}, {2})});
    ObjectPool pool;
    RuntimeFilterProbeCollector collector;
    add_bloom_descriptor(&pool, &state, &collector, 1, slot_id, nullptr);
    ComposedRuntimeBloomFilter<TYPE_INT> filter;
    filter.membership_filter().init(16);
    filter.insert(7);
    add_bloom_descriptor(&pool, &state, &collector, 2, slot_id, &filter);
    RuntimeFilterPartitionPruner pruner(boundaries);
    pruner.prune_by_bloom_filters(collector, &state);
    EXPECT_EQ(2, pruner.pruned_partition_count());
    EXPECT_TRUE(pruner._bloom_pruning_complete.load());
    pruner.prune_by_bloom_filters(collector, &state);
    EXPECT_EQ(2, pruner.pruned_partition_count());
    EXPECT_TRUE(pruner.is_partition_pruned(10));
    EXPECT_TRUE(pruner.is_partition_pruned(11));
}

TEST(PartitionScanRangePrunerTest, CompletionCountsDistinctPartitionsAcrossColumns) {
    RuntimeState state;
    TupleDescriptor* tuple = nullptr;
    make_tuple(&state, &tuple, false);
    auto slot_id = tuple->slots()[0]->id();
    auto boundaries = parse_partition_boundaries(tuple, {make_list_boundary(slot_id, {10, 11}, {2})});
    auto second_column = boundaries.at(slot_id).front();
    second_column.slot_id = slot_id + 1;
    second_column.physical_partition_ids = {11, 12};
    boundaries[second_column.slot_id].push_back(second_column);
    RuntimeFilterPartitionPruner pruner(std::move(boundaries));
    EXPECT_EQ(3, pruner.candidate_partition_count());

    auto in_filter = make_runtime_in_filter(&state, slot_id, {7});
    pruner.prune_by_in_filters({in_filter.get()});
    EXPECT_EQ(2, pruner.pruned_partition_count());
    EXPECT_FALSE(pruner._bloom_pruning_complete.load());

    ObjectPool pool;
    RuntimeFilterProbeCollector collector;
    auto* descriptor = add_bloom_descriptor(&pool, &state, &collector, 1, second_column.slot_id, nullptr);
    pruner.prune_by_bloom_filters(collector, &state);
    EXPECT_EQ(2, pruner.pruned_partition_count());
    EXPECT_FALSE(pruner._bloom_pruning_complete.load());
    ComposedRuntimeBloomFilter<TYPE_INT> bloom;
    bloom.membership_filter().init(16);
    bloom.insert(7);
    descriptor->set_runtime_filter(&bloom);
    pruner.prune_by_bloom_filters(collector, &state);
    EXPECT_EQ(3, pruner.pruned_partition_count());
    EXPECT_TRUE(pruner._bloom_pruning_complete.load());
    EXPECT_TRUE(pruner.is_partition_pruned(10));
    EXPECT_TRUE(pruner.is_partition_pruned(11));
    EXPECT_TRUE(pruner.is_partition_pruned(12));
    EXPECT_FALSE(pruner.is_partition_pruned(99));
}

TEST(PartitionScanRangePrunerTest, CompactRangeBoundsPruneWithInAndBloomFilters) {
    auto check = []<LogicalType LT>(const std::vector<int64_t>& values, const std::vector<TExpr>& literals,
                                    const std::vector<RunTimeCppType<LT>>& expected_values) {
        RuntimeState state;
        TupleDescriptor* tuple = nullptr;
        make_tuple(&state, &tuple, true, TypeDescriptor(LT));
        const auto slot_id = tuple->slots()[0]->id();
        const auto lower = expected_values[1];
        const auto upper = expected_values[3];
        for (bool has_lower : {false, true}) {
            for (bool has_upper : {false, true}) {
                if (!has_lower && !has_upper) {
                    continue;
                }
                for (bool upper_closed : {false, true}) {
                    SCOPED_TRACE(testing::Message()
                                 << LT << "/" << has_lower << "/" << has_upper << "/" << upper_closed);
                    TPartitionBoundary thrift;
                    thrift.__set_slot_id(slot_id);
                    thrift.__set_physical_partition_ids({10});
                    if (has_lower) {
                        thrift.__set_range_lower_int(values[1]);
                    }
                    if (has_upper) {
                        thrift.__set_range_upper_int(values[3]);
                    }
                    thrift.__set_range_upper_closed(upper_closed);
                    auto boundaries = parse_partition_boundaries(tuple, {thrift});
                    ASSERT_EQ(1, boundaries.size());
                    ASSERT_EQ(1, boundaries.at(slot_id).size());
                    const auto& boundary = boundaries.at(slot_id).front();
                    ASSERT_TRUE(boundary.is_range());
                    EXPECT_EQ(nullptr, boundary.list_values);
                    if (has_lower) {
                        ASSERT_NE(nullptr, boundary.lower_bound);
                        ASSERT_EQ(1, boundary.lower_bound->size());
                        EXPECT_EQ(lower, ColumnViewer<LT>(boundary.lower_bound).value(0));
                    } else {
                        EXPECT_EQ(nullptr, boundary.lower_bound);
                    }
                    if (has_upper) {
                        ASSERT_NE(nullptr, boundary.upper_bound);
                        ASSERT_EQ(1, boundary.upper_bound->size());
                        EXPECT_EQ(upper, ColumnViewer<LT>(boundary.upper_bound).value(0));
                    } else {
                        EXPECT_EQ(nullptr, boundary.upper_bound);
                    }
                    EXPECT_EQ(upper_closed, boundary.upper_bound_closed);
                    for (size_t i = 0; i < values.size(); ++i) {
                        SCOPED_TRACE(values[i]);
                        const bool pruned =
                                (has_lower && values[i] < values[1]) ||
                                (has_upper && (values[i] > values[3] || (values[i] == values[3] && !upper_closed)));
                        auto in_filter = make_runtime_in_filter<LT>(&state, slot_id, {literals[i]});
                        EXPECT_EQ(pruned, in_filter_prunes(boundary, in_filter.get()));
                        ComposedRuntimeBloomFilter<LT> bloom;
                        bloom.membership_filter().init(16);
                        bloom.insert(expected_values[i]);
                        EXPECT_EQ(pruned, bloom_filter_prunes(boundary, bloom, &state));
                    }
                }
            }
        }
    };
    check.template operator()<TYPE_INT>(
            {-1, 0, 1, 2, 3},
            {make_int_literal(-1), make_int_literal(0), make_int_literal(1), make_int_literal(2), make_int_literal(3)},
            {-1, 0, 1, 2, 3});
    check.template operator()<TYPE_DATE>(
            {20240101, 20240102, 20240103, 20240104, 20240105},
            {make_date_literal(TYPE_DATE, "2024-01-01"), make_date_literal(TYPE_DATE, "2024-01-02"),
             make_date_literal(TYPE_DATE, "2024-01-03"), make_date_literal(TYPE_DATE, "2024-01-04"),
             make_date_literal(TYPE_DATE, "2024-01-05")},
            {DateValue{date::from_date(2024, 1, 1)}, DateValue{date::from_date(2024, 1, 2)},
             DateValue{date::from_date(2024, 1, 3)}, DateValue{date::from_date(2024, 1, 4)},
             DateValue{date::from_date(2024, 1, 5)}});
}

TEST(PartitionScanRangePrunerTest, DateAndDatetimeRangesHaveSingleValueBounds) {
    auto check = []<LogicalType LT>(const std::string& lower, const std::string& inside, const std::string& upper) {
        SCOPED_TRACE(lower);
        RuntimeState state;
        TupleDescriptor* tuple = nullptr;
        make_tuple(&state, &tuple, true, TypeDescriptor(LT));
        const auto slot_id = tuple->slots()[0]->id();
        TPartitionBoundary thrift;
        thrift.__set_slot_id(slot_id);
        thrift.__set_physical_partition_ids({10});
        thrift.__set_range_lower(make_date_literal(LT, lower));
        thrift.__set_range_upper(make_date_literal(LT, upper));
        auto boundaries = parse_partition_boundaries(tuple, {thrift});
        ASSERT_EQ(1, boundaries.size());
        ASSERT_EQ(1, boundaries.at(slot_id).size());
        auto boundary = boundaries.at(slot_id).front();
        ASSERT_TRUE(boundary.is_range());
        ASSERT_NE(nullptr, boundary.lower_bound);
        ASSERT_NE(nullptr, boundary.upper_bound);
        ASSERT_EQ(1, boundary.lower_bound->size());
        ASSERT_EQ(1, boundary.upper_bound->size());
        ASSERT_FALSE(boundary.lower_bound->is_null(0));
        ASSERT_FALSE(boundary.upper_bound->is_null(0));
        RunTimeCppType<LT> expected_lower;
        RunTimeCppType<LT> expected_upper;
        ASSERT_TRUE(expected_lower.from_string(lower.data(), lower.size()));
        ASSERT_TRUE(expected_upper.from_string(upper.data(), upper.size()));
        EXPECT_EQ(expected_lower, ColumnViewer<LT>(boundary.lower_bound).value(0));
        EXPECT_EQ(expected_upper, ColumnViewer<LT>(boundary.upper_bound).value(0));

        for (const auto& value : {lower, inside, upper}) {
            SCOPED_TRACE(value);
            auto in_filter = make_runtime_in_filter<LT>(&state, slot_id, {make_date_literal(LT, value)});
            RunTimeCppType<LT> key;
            ASSERT_TRUE(key.from_string(value.data(), value.size()));
            ComposedRuntimeBloomFilter<LT> bloom;
            bloom.membership_filter().init(16);
            bloom.insert(key);
            boundary.upper_bound_closed = false;
            EXPECT_EQ(value == upper, in_filter_prunes(boundary, in_filter.get()));
            EXPECT_EQ(value == upper, bloom_filter_prunes(boundary, bloom, &state));
            boundary.upper_bound_closed = true;
            EXPECT_FALSE(in_filter_prunes(boundary, in_filter.get()));
            EXPECT_FALSE(bloom_filter_prunes(boundary, bloom, &state));
        }
    };
    check.template operator()<TYPE_DATE>("2024-01-01", "2024-01-02", "2024-01-03");
    check.template operator()<TYPE_DATETIME>("2024-01-01 10:00:00", "2024-01-01 11:00:00", "2024-01-01 12:00:00");
}

TEST(PartitionScanRangePrunerTest, StreamBuildFiltersAreSkippedAndConsumed) {
    for (auto filter_type : {TRuntimeFilterBuildType::AGG_FILTER, TRuntimeFilterBuildType::TOPN_FILTER}) {
        SCOPED_TRACE(filter_type);
        RuntimeState state;
        TupleDescriptor* tuple = nullptr;
        make_tuple(&state, &tuple, false);
        const auto slot_id = tuple->slots()[0]->id();
        auto boundaries = parse_partition_boundaries(tuple, {make_list_boundary(slot_id, {10}, {2})});
        ASSERT_EQ(1, boundaries.size());
        InRuntimeFilter<TYPE_INT> agg_filter;
        auto build_values = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);
        build_values->append_datum(Datum(int32_t(7)));
        agg_filter.build(build_values.get());
        ASSERT_FALSE(agg_filter.always_true());
        MinMaxRuntimeFilter<TYPE_INT> topn_filter;
        topn_filter.insert(7);
        const RuntimeFilter* filter = filter_type == TRuntimeFilterBuildType::AGG_FILTER
                                              ? static_cast<const RuntimeFilter*>(&agg_filter)
                                              : static_cast<const RuntimeFilter*>(&topn_filter);
        TRuntimeFilterDescription thrift;
        thrift.__set_filter_id(1);
        thrift.__set_filter_type(filter_type);
        thrift.__set_has_remote_targets(false);
        thrift.__set_build_plan_node_id(0);
        thrift.__set_build_join_mode(TRuntimeFilterBuildJoinMode::BROADCAST);
        thrift.__set_plan_node_id_to_target_expr(
                {{0, ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(slot_id, false)}});
        ObjectPool pool;
        auto* descriptor = pool.add(new RuntimeFilterProbeDescriptor());
        ASSERT_OK(descriptor->init(&pool, thrift, 0, &state));
        ASSERT_TRUE(descriptor->is_stream_build_filter());
        descriptor->set_runtime_filter(filter);
        RuntimeFilterProbeCollector collector;
        collector.add_descriptor(descriptor);
        RuntimeFilterPartitionPruner pruner(std::move(boundaries));
        pruner.prune_by_bloom_filters(collector, &state);
        EXPECT_EQ(0, pruner.pruned_partition_count());
        EXPECT_FALSE(pruner.is_partition_pruned(10));
        EXPECT_EQ(1, pruner._processed_filter_ids.count(1));
        EXPECT_TRUE(pruner._bloom_pruning_complete.load());
        pruner.prune_by_bloom_filters(collector, &state);
        EXPECT_EQ(0, pruner.pruned_partition_count());
    }
}

// An always-true filter carries no information: it must not prune anything, but it is consumed like any
// other filter so the checkpoint reaches its terminal state instead of re-judging it forever. The empty
// range makes the guard load-bearing: judged as a plain min-max filter it would prune the RANGE partition.
TEST(PartitionScanRangePrunerTest, AlwaysTrueFilterIsConsumedWithoutPruning) {
    RuntimeState state;
    TupleDescriptor* tuple = nullptr;
    make_tuple(&state, &tuple, false);
    const auto slot_id = tuple->slots()[0]->id();
    auto boundaries = parse_partition_boundaries(
            tuple, {make_list_boundary(slot_id, {10}, {2}), make_range_boundary(slot_id, {11}, 20, 30)});
    ASSERT_EQ(1, boundaries.size());
    ObjectPool pool;
    auto* filter = MinMaxRuntimeFilter<TYPE_INT>::create_with_empty_range_without_null(&pool);
    ASSERT_TRUE(filter->always_true());
    RuntimeFilterProbeCollector collector;
    add_bloom_descriptor(&pool, &state, &collector, /*filter_id=*/1, slot_id, filter);
    RuntimeFilterPartitionPruner pruner(std::move(boundaries));
    pruner.prune_by_bloom_filters(collector, &state);
    EXPECT_EQ(0, pruner.pruned_partition_count());
    EXPECT_FALSE(pruner.is_partition_pruned(10));
    EXPECT_FALSE(pruner.is_partition_pruned(11));
    EXPECT_EQ(1, pruner._processed_filter_ids.count(1));
    EXPECT_TRUE(pruner._bloom_pruning_complete.load());
    pruner.prune_by_bloom_filters(collector, &state);
    EXPECT_EQ(0, pruner.pruned_partition_count());
}

// Every creation site goes through one entry: the counter is a high water mark merged by max, so a stale
// snapshot written after a newer one cannot lower it, and drivers/instances keep the largest snapshot.
TEST(PartitionScanRangePrunerTest, PrunedPartitionsCounterIsHighWaterMarkMergedByMax) {
    RuntimeProfile profile("scan");
    auto* counter = add_rf_partitions_pruned_counter(&profile);
    // get-or-add: a second creation site receives the same object, so its strategy cannot drift.
    ASSERT_EQ(counter, add_rf_partitions_pruned_counter(&profile));
    EXPECT_NE(nullptr, dynamic_cast<RuntimeProfile::HighWaterMarkCounter*>(counter));
    EXPECT_EQ(TCounterAggregateType::MAX, counter->strategy().aggregate_type);
    EXPECT_FALSE(counter->skip_merge());
    EXPECT_TRUE(counter->skip_min_max());
    COUNTER_SET(counter, int64_t{5});
    COUNTER_SET(counter, int64_t{3});
    EXPECT_EQ(5, counter->value());
}

TEST(PartitionScanRangePrunerTest, EmptyListValuesRepresentNullOnlyPartition) {
    RuntimeState state;
    TupleDescriptor* tuple = nullptr;
    make_tuple(&state, &tuple, true);
    const auto slot_id = tuple->slots()[0]->id();
    auto boundaries = parse_partition_boundaries(tuple, {make_list_boundary(slot_id, {10}, {}, true)});
    ASSERT_EQ(1, boundaries.size());
    ASSERT_EQ(1, boundaries.at(slot_id).size());
    const auto& boundary = boundaries.at(slot_id).front();
    ASSERT_TRUE(boundary.contains_null);
    ASSERT_NE(nullptr, boundary.list_values);
    ASSERT_EQ(0, boundary.list_values->size());
    for (bool null_safe : {false, true}) {
        for (bool build_has_null : {false, true}) {
            SCOPED_TRACE(::testing::Message() << "null_safe=" << null_safe << ", build_has_null=" << build_has_null);
            auto in_filter = make_runtime_in_filter(&state, slot_id, {7});
            auto* predicate = down_cast<VectorizedInConstPredicate<TYPE_INT>*>(in_filter->root());
            predicate->set_eq_null(null_safe);
            predicate->set_null_in_set(build_has_null);
            ComposedRuntimeBloomFilter<TYPE_INT> bloom;
            bloom.membership_filter().init(16);
            bloom.insert(7);
            const bool matches_null = null_safe && build_has_null;
            if (matches_null) {
                bloom.insert_null();
            }
            EXPECT_EQ(!matches_null, in_filter_prunes(boundary, in_filter.get()));
            EXPECT_EQ(!matches_null, bloom_filter_prunes(boundary, bloom, &state));
        }
    }
}

TEST(PartitionScanRangePrunerTest, VarcharAndCharListBoundariesUseStringFilters) {
    auto check = []<LogicalType LT>() {
        SCOPED_TRACE(LT);
        RuntimeState state;
        TupleDescriptor* tuple = nullptr;
        make_tuple(&state, &tuple, true, TypeDescriptor(LT));
        const auto slot_id = tuple->slots()[0]->id();
        TPartitionBoundary thrift;
        thrift.__set_slot_id(slot_id);
        thrift.__set_physical_partition_ids({10});
        thrift.__set_list_values({make_string_literal("beijing"), make_string_literal("shanghai")});
        auto boundaries = parse_partition_boundaries(tuple, {thrift});
        ASSERT_EQ(1, boundaries.size());
        ASSERT_EQ(1, boundaries.at(slot_id).size());
        const auto& boundary = boundaries.at(slot_id).front();
        ASSERT_NE(nullptr, boundary.list_values);
        ASSERT_EQ(2, boundary.list_values->size());
        for (const std::string value : {"beijing", "shanghai", "shenzhen"}) {
            SCOPED_TRACE(value);
            auto in_filter = make_runtime_in_filter<LT>(&state, slot_id, {make_string_literal(value)});
            ComposedRuntimeBloomFilter<LT> bloom;
            bloom.membership_filter().init(16);
            bloom.insert(Slice(value));
            const bool pruned = value == "shenzhen";
            EXPECT_EQ(pruned, in_filter_prunes(boundary, in_filter.get()));
            EXPECT_EQ(pruned, bloom_filter_prunes(boundary, bloom, &state));
        }
    };
    check.template operator()<TYPE_VARCHAR>();
    check.template operator()<TYPE_CHAR>();
}

} // namespace starrocks

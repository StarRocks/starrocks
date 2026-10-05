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

// Lives in starrocks_test rather than expr_test because it drives the real
// Analytor, which belongs to the exec layer that expr_test does not link.

#include <gtest/gtest.h>

#include <optional>

#include "base/utility/defer_op.h"
#include "column/geo_column.h"
#include "column/nullable_column.h"
#include "exec/analytor.h"
#include "geo/wkb.h"
#include "runtime/descriptor_helper.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {
TypeDescriptor coverage_type() {
    return TypeDescriptor::create_geo_type(TYPE_GEOMETRY, {GEO_LOGICAL_TYPE_GEOMETRY, GEO_COORDINATE_SYSTEM_CARTESIAN,
                                                           GEO_EDGE_ALGORITHM_PLANAR, "EPSG:3857", 3857});
}
std::string bytes(const std::string& text) {
    WkbGeometry geometry;
    EXPECT_TRUE(WkbCodec::parse_wkt(text, &geometry, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    std::string result;
    EXPECT_TRUE(WkbCodec::to_wkb(geometry, &result, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    return result;
}
ColumnPtr coverage_input(const std::vector<std::optional<std::string>>& texts) {
    auto geo = GeoColumn::create(coverage_type(), 0);
    auto nulls = NullColumn::create();
    for (const auto& text : texts) {
        if (text)
            geo->append_wkb(Slice(bytes(*text)));
        else
            geo->append_default();
        nulls->append(!text);
    }
    return NullableColumn::create(std::move(geo), std::move(nulls));
}
std::string output_text(const ColumnPtr& column, size_t row) {
    if (column->is_null(row)) return "NULL";
    const auto* geo = down_cast<const GeoColumn*>(down_cast<const NullableColumn*>(column.get())->data_column().get());
    WkbGeometry geometry;
    EXPECT_TRUE(WkbCodec::parse_wkb(geo->get_wkb(row), &geometry, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    std::string text;
    EXPECT_TRUE(WkbCodec::to_wkt(geometry, &text, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    return text;
}
const std::string left = "POLYGON ((0 0,0 8,4 8,4.1 6,3.8 4,4.2 2,4 0,0 0))";
const std::string right = "POLYGON ((4 0,4.2 2,3.8 4,4.1 6,4 8,8 8,8 0,4 0))";

// Product-path test: the actual Analytor evaluates slot refs and plan constants,
// buffers complete groups, maps each row, and publishes ordinary native chunks.
Status drive_coverage_analytor(size_t chunk_rows, const std::vector<int32_t>& keys,
                               const std::vector<std::optional<std::string>>& input, bool null_parameter,
                               std::vector<std::pair<int32_t, std::string>>* output) {
    ObjectPool pool;
    auto type = coverage_type();
    TDescriptorTableBuilder builder;
    TTupleDescriptorBuilder in_tuple;
    in_tuple.add_slot(TSlotDescriptorBuilder().type(type).nullable(true).build());
    in_tuple.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).nullable(false).build());
    in_tuple.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).nullable(false).build());
    in_tuple.build(&builder);
    TTupleDescriptorBuilder out_tuple;
    out_tuple.add_slot(TSlotDescriptorBuilder().type(type).nullable(true).build());
    out_tuple.build(&builder);
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    state.init_mem_trackers(std::make_shared<MemTracker>(-1));
    DescriptorTbl* descriptors = nullptr;
    RETURN_IF_ERROR(DescriptorTbl::create(&state, &pool, builder.desc_tbl(), &descriptors, 4096));
    state.set_desc_tbl(descriptors);
    auto* in = descriptors->get_tuple_descriptor(0);
    auto* out = descriptors->get_tuple_descriptor(1);
    auto slot = [&](size_t i) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::SLOT_REF);
        node.__set_type(in->slots()[i]->type().to_thrift());
        node.__set_num_children(0);
        node.__set_is_nullable(i == 0);
        TSlotRef ref;
        ref.__set_tuple_id(0);
        ref.__set_slot_id(in->slots()[i]->id());
        node.__set_slot_ref(ref);
        return node;
    };
    TExprNode root;
    root.__set_node_type(TExprNodeType::AGG_EXPR);
    root.__set_num_children(2);
    root.__set_type(type.to_thrift());
    root.__set_has_nullable_child(true);
    root.__set_is_nullable(true);
    TAggregateExpr agg;
    agg.__set_is_merge_agg(false);
    root.__set_agg_expr(agg);
    TFunction function;
    TFunctionName name;
    name.__set_function_name("st_coveragesimplify");
    function.__set_name(name);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);
    function.__set_has_var_args(false);
    function.__set_arg_types({type.to_thrift(), TypeDescriptor(TYPE_DOUBLE).to_thrift()});
    function.__set_ret_type(type.to_thrift());
    root.__set_fn(function);
    TExprNode literal;
    literal.__set_type(TypeDescriptor(TYPE_DOUBLE).to_thrift());
    literal.__set_num_children(0);
    literal.__set_is_nullable(null_parameter);
    literal.__set_node_type(null_parameter ? TExprNodeType::NULL_LITERAL : TExprNodeType::FLOAT_LITERAL);
    TFloatLiteral value;
    value.__set_value(0);
    literal.__set_float_literal(value);
    TExpr call;
    call.nodes = {root, slot(0), literal};
    TAnalyticNode analytic;
    analytic.__set_buffered_tuple_id(0);
    analytic.analytic_functions = {call};
    if (!keys.empty()) {
        TExpr partition;
        partition.nodes = {slot(1)};
        analytic.__set_partition_exprs({partition});
    }
    TPlanNode plan;
    plan.__set_node_id(0);
    plan.__set_node_type(TPlanNodeType::ANALYTIC_EVAL_NODE);
    plan.__set_limit(-1);
    plan.__set_analytic_node(analytic);
    RuntimeProfile profile("CoverageAnalytor");
    Analytor processor(plan, out, false);
    RETURN_IF_ERROR(processor.prepare(&state, &pool, &profile));
    RETURN_IF_ERROR(processor.open(&state));
    auto close = DeferOp([&] { processor.close(&state); });
    auto collect = [&] {
        while (auto chunk = processor.poll_chunk_buffer()) {
            auto result = chunk->get_column_by_slot_id(out->slots()[0]->id());
            auto ids = chunk->get_column_by_slot_id(in->slots()[2]->id());
            result->check_or_die();
            for (size_t row = 0; row < chunk->num_rows(); ++row)
                output->emplace_back(ids->get(row).get_int32(), output_text(result, row));
        }
    };
    for (size_t first = 0; first < input.size(); first += chunk_rows) {
        size_t count = std::min(chunk_rows, input.size() - first);
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(coverage_input({input.begin() + first, input.begin() + first + count}),
                             in->slots()[0]->id());
        auto partition = Int32Column::create(), ids = Int32Column::create();
        for (size_t row = first; row < first + count; ++row) {
            partition->append(keys.empty() ? 0 : keys[row]);
            ids->append(row);
        }
        chunk->append_column(std::move(partition), in->slots()[1]->id());
        chunk->append_column(std::move(ids), in->slots()[2]->id());
        RETURN_IF_ERROR(processor.process(&state, chunk));
        collect();
    }
    RETURN_IF_ERROR(processor.finish_process(&state));
    collect();
    return Status::OK();
}
TEST(CoverageAnalytorTest, WholePartitionsAcrossAndInsideChunksKeepRowIdentity) {
    const std::vector<std::optional<std::string>> input{left, std::nullopt, right, "POLYGON EMPTY", left, right};
    const std::vector<int32_t> keys{1, 1, 1, 1, 2, 2};
    for (size_t rows : {size_t(1), size_t(2), size_t(3), size_t(6)}) {
        std::vector<std::pair<int32_t, std::string>> output;
        auto status = drive_coverage_analytor(rows, keys, input, false, &output);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(output.size(), input.size());
        for (size_t i = 0; i < input.size(); ++i) {
            EXPECT_EQ(output[i].first, i);
            EXPECT_EQ(output[i].second, input[i] ? output_text(coverage_input({input[i]}), 0) : "NULL");
        }
    }
}
TEST(CoverageAnalytorTest, OverAllRowsAndNullParameterUseTheRealExecutor) {
    for (bool null : {false, true}) {
        std::vector<std::pair<int32_t, std::string>> output;
        auto status = drive_coverage_analytor(1, {}, {left, std::nullopt, right}, null, &output);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(output.size(), 3);
        EXPECT_EQ(output[0].second, null ? "NULL" : output_text(coverage_input({left}), 0));
        EXPECT_EQ(output[1].second, "NULL");
        EXPECT_EQ(output[2].second, null ? "NULL" : output_text(coverage_input({right}), 0));
    }
}
} // namespace
} // namespace starrocks

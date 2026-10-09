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
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/geo_column.h"
#include "column/nullable_column.h"
#include "common/config_exec_flow_fwd.h"
#include "exec/analytor.h"
#include "exprs/function_context.h"
#include "exprs/geo_functions.h"
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
StatusOr<ColumnPtr> constructed_coverage_input(const std::vector<std::optional<std::string>>& texts) {
    auto wkt = BinaryColumn::create();
    auto nulls = NullColumn::create();
    for (const auto& text : texts) {
        if (text)
            wkt->append(*text);
        else
            wkt->append_default();
        nulls->append(!text);
    }
    ColumnPtr input = NullableColumn::create(std::move(wkt), std::move(nulls));
    auto crs = ColumnHelper::create_const_column<TYPE_VARCHAR>("EPSG:3857", input->size());
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context(
            {TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR)}, coverage_type()));
    return GeoFunctions::st_geom_from_text(ctx.get(), {input, crs});
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
                               std::vector<std::pair<int32_t, std::string>>* output, double tolerance = 0,
                               bool spill = false, bool companion = false, bool exceeded_query_limit = false,
                               std::vector<std::optional<double>>* areas = nullptr, bool constructor_input = false,
                               std::optional<bool> boundary = std::nullopt) {
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
    if (companion) out_tuple.add_slot(TSlotDescriptorBuilder().type(TYPE_BIGINT).nullable(false).build());
    out_tuple.build(&builder);
    TQueryOptions options;
    options.__set_enable_spill(spill);
    RuntimeState state(TUniqueId(), options, TQueryGlobals(), nullptr);
    auto query = std::make_shared<MemTracker>(exceeded_query_limit ? 1 : -1);
    state.init_mem_trackers(query);
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
    root.__set_num_children(boundary.has_value() ? 3 : 2);
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
    if (boundary.has_value()) function.arg_types.push_back(TypeDescriptor(TYPE_BOOLEAN).to_thrift());
    function.__set_ret_type(type.to_thrift());
    root.__set_fn(function);
    TExprNode literal;
    literal.__set_type(TypeDescriptor(TYPE_DOUBLE).to_thrift());
    literal.__set_num_children(0);
    literal.__set_is_nullable(null_parameter);
    literal.__set_node_type(null_parameter ? TExprNodeType::NULL_LITERAL : TExprNodeType::FLOAT_LITERAL);
    TFloatLiteral value;
    value.__set_value(tolerance);
    literal.__set_float_literal(value);
    TExpr call;
    call.nodes = {root, slot(0), literal};
    if (boundary.has_value()) {
        TExprNode flag;
        flag.__set_node_type(TExprNodeType::BOOL_LITERAL);
        flag.__set_type(TypeDescriptor(TYPE_BOOLEAN).to_thrift());
        flag.__set_num_children(0);
        flag.__set_is_nullable(false);
        TBoolLiteral value;
        value.__set_value(*boundary);
        flag.__set_bool_literal(value);
        call.nodes.push_back(flag);
    }
    TAnalyticNode analytic;
    analytic.__set_buffered_tuple_id(0);
    analytic.analytic_functions = {call};
    if (companion) {
        TExprNode sibling = root;
        sibling.__set_num_children(1);
        sibling.__set_type(TypeDescriptor(TYPE_BIGINT).to_thrift());
        sibling.__set_has_nullable_child(false);
        sibling.__set_is_nullable(false);
        TFunction sibling_function = function;
        TFunctionName sibling_name;
        sibling_name.__set_function_name("count");
        sibling_function.__set_name(sibling_name);
        sibling_function.__set_arg_types({TypeDescriptor(TYPE_INT).to_thrift()});
        sibling_function.__set_ret_type(TypeDescriptor(TYPE_BIGINT).to_thrift());
        sibling.__set_fn(sibling_function);
        TExpr expression;
        expression.nodes = {sibling, slot(2)};
        analytic.analytic_functions.push_back(expression);
    }
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
    auto close = DeferOp([&] { processor.close(&state); });
    RETURN_IF_ERROR(processor.open(&state));
    if (exceeded_query_limit) query->consume(2);
    auto release = DeferOp([&] {
        if (exceeded_query_limit) query->release(2);
    });
    auto collect = [&]() -> Status {
        while (auto chunk = processor.poll_chunk_buffer()) {
            auto result = chunk->get_column_by_slot_id(out->slots()[0]->id());
            auto ids = chunk->get_column_by_slot_id(in->slots()[2]->id());
            result->check_or_die();
            const auto* geo =
                    down_cast<const GeoColumn*>(down_cast<const NullableColumn*>(result.get())->data_column().get());
            EXPECT_EQ(geo->descriptor().type, *type.geo_type);
            EXPECT_EQ(geo->descriptor().storage.dimension, GEO_DIMENSION_XY);
            EXPECT_EQ(geo->descriptor().storage.validation_state, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED);
            ASSIGN_OR_RETURN(auto measured, GeoFunctions::st_geometry_area(nullptr, {result}));
            ColumnViewer<TYPE_DOUBLE> area(measured);
            ASSIGN_OR_RETURN(auto rendered, GeoFunctions::st_geometry_as_text(nullptr, {result}));
            ColumnViewer<TYPE_VARCHAR> text(rendered);
            for (size_t row = 0; row < chunk->num_rows(); ++row) {
                const auto id = ids->get(row).get_int32();
                if (companion) {
                    const auto key = keys.empty() ? 0 : keys[id];
                    const auto count = keys.empty() ? input.size() : std::count(keys.begin(), keys.end(), key);
                    auto result_count = chunk->get_column_by_slot_id(out->slots()[1]->id());
                    EXPECT_EQ(result_count->get(row).get_int64(), count);
                }
                output->emplace_back(id, text.is_null(row) ? "NULL" : text.value(row).to_string());
                EXPECT_EQ(area.is_null(row), result->is_null(row));
                if (areas) areas->push_back(area.is_null(row) ? std::nullopt : std::optional<double>{area.value(row)});
            }
        }
        return Status::OK();
    };
    for (size_t first = 0; first < input.size(); first += chunk_rows) {
        size_t count = std::min(chunk_rows, input.size() - first);
        auto chunk = std::make_shared<Chunk>();
        ColumnPtr values;
        if (constructor_input) {
            ASSIGN_OR_RETURN(values,
                             constructed_coverage_input({input.begin() + first, input.begin() + first + count}));
        } else {
            values = coverage_input({input.begin() + first, input.begin() + first + count});
        }
        chunk->append_column(std::move(values), in->slots()[0]->id());
        auto partition = Int32Column::create(), ids = Int32Column::create();
        for (size_t row = first; row < first + count; ++row) {
            partition->append(keys.empty() ? 0 : keys[row]);
            ids->append(row);
        }
        chunk->append_column(std::move(partition), in->slots()[1]->id());
        chunk->append_column(std::move(ids), in->slots()[2]->id());
        RETURN_IF_ERROR(processor.process(&state, chunk));
        RETURN_IF_ERROR(collect());
    }
    RETURN_IF_ERROR(processor.finish_process(&state));
    RETURN_IF_ERROR(collect());
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
TEST(CoverageAnalytorTest, WktConstructorFeedsThreeArgumentWindowAndTextOutput) {
    const std::vector<std::optional<std::string>> input{left, right, std::nullopt, "MULTIPOLYGON EMPTY", left, right};
    const std::vector<int32_t> keys{7, 7, 7, 7, 8, 8};
    const std::vector<std::pair<int32_t, std::string>> expected{{0, "POLYGON ((0 0, 0 8, 4 8, 4 0, 0 0))"},
                                                                {1, "POLYGON ((4 0, 4 8, 8 8, 8 0, 4 0))"},
                                                                {2, "NULL"},
                                                                {3, "MULTIPOLYGON EMPTY"},
                                                                {4, "POLYGON ((0 0, 0 8, 4 8, 4 0, 0 0))"},
                                                                {5, "POLYGON ((4 0, 4 8, 8 8, 8 0, 4 0))"}};
    for (size_t rows : {size_t(1), size_t(2), size_t(3), size_t(6)}) {
        std::vector<std::pair<int32_t, std::string>> output;
        auto status = drive_coverage_analytor(rows, keys, input, false, &output, 1, false, false, false, nullptr, true,
                                              false);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(output, expected);
    }
}
TEST(CoverageAnalytorTest, WktConstructorPreservesNullPrefixAcrossChunks) {
    const std::vector<std::optional<std::string>> input{std::nullopt, std::nullopt, left, right, "MULTIPOLYGON EMPTY"};
    for (size_t rows : {size_t(1), size_t(2), size_t(5)}) {
        std::vector<std::pair<int32_t, std::string>> output;
        auto status =
                drive_coverage_analytor(rows, {}, input, false, &output, 1, false, false, false, nullptr, true, false);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(output.size(), input.size());
        EXPECT_EQ(output[0].second, "NULL");
        EXPECT_EQ(output[1].second, "NULL");
        EXPECT_EQ(output[2].second, "POLYGON ((0 0, 0 8, 4 8, 4 0, 0 0))");
        EXPECT_EQ(output[3].second, "POLYGON ((4 0, 4 8, 8 8, 8 0, 4 0))");
        EXPECT_EQ(output[4].second, "MULTIPOLYGON EMPTY");
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

TEST(CoverageAnalytorTest, WindowResultsFeedAreaAcrossNullPartitionsAndChunks) {
    const std::vector<std::optional<std::string>> input{std::nullopt, left, right, "MULTIPOLYGON EMPTY", left, right};
    const std::vector<int32_t> keys{0, 1, 1, 1, 2, 2};
    for (size_t rows : {size_t(1), size_t(3), size_t(6)}) {
        std::vector<std::pair<int32_t, std::string>> output;
        std::vector<std::optional<double>> areas;
        auto status = drive_coverage_analytor(rows, keys, input, false, &output, 1, false, false, false, &areas);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(areas.size(), input.size());
        EXPECT_FALSE(areas[0]);
        EXPECT_EQ(areas[3], 0);
        for (size_t row : {size_t(1), size_t(2), size_t(4), size_t(5)}) {
            ASSERT_TRUE(areas[row]);
            EXPECT_GT(*areas[row], 0);
        }
    }
}

TEST(CoverageAnalytorTest, EmptyInputPreservesInitializationErrors) {
    for (double tolerance : {-1.0, 1e200}) {
        std::vector<std::pair<int32_t, std::string>> output;
        auto status = drive_coverage_analytor(1, {}, {}, false, &output, tolerance);
        EXPECT_FALSE(status.ok());
        EXPECT_TRUE(output.empty());
    }
    std::vector<std::pair<int32_t, std::string>> output;
    EXPECT_TRUE(drive_coverage_analytor(1, {}, {}, false, &output, 0, true).ok());
    EXPECT_TRUE(drive_coverage_analytor(1, {}, {}, false, &output).ok());
}
TEST(CoverageAnalytorTest, SpillSettingDoesNotChangeWindowResults) {
    const std::vector<std::optional<std::string>> input{left, std::nullopt, right, "MULTIPOLYGON EMPTY", left, right};
    const std::vector<int32_t> keys{1, 1, 1, 1, 2, 2};
    std::vector<std::pair<int32_t, std::string>> expected;
    ASSERT_TRUE(drive_coverage_analytor(1, keys, input, false, &expected, 1).ok());
    ASSERT_EQ(expected.size(), input.size());
    for (size_t rows : {size_t(1), size_t(3), size_t(6)}) {
        std::vector<std::pair<int32_t, std::string>> output;
        auto status = drive_coverage_analytor(rows, keys, input, false, &output, 1, true);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(output, expected);
    }
}
TEST(CoverageAnalytorTest, LateInvalidPartitionCannotPublishItsRows) {
    std::vector<std::pair<int32_t, std::string>> output;
    auto status = drive_coverage_analytor(1, {1, 2, 2}, {left, left, left}, false, &output);
    EXPECT_FALSE(status.ok());
    for (const auto& row : output) EXPECT_EQ(row.first, 0);
}
TEST(CoverageAnalytorTest, BufferContractionAndCompanionWindowPreserveRowIdentity) {
    const auto previous = config::pipeline_analytic_removable_chunk_num;
    auto restore = DeferOp([&] { config::pipeline_analytic_removable_chunk_num = previous; });
    config::pipeline_analytic_removable_chunk_num = 1;
    std::vector<int32_t> keys;
    std::vector<std::optional<std::string>> input;
    for (int32_t partition = 0; partition < 30; ++partition) {
        keys.insert(keys.end(), {partition, partition, partition, partition});
        input.insert(input.end(), {left, std::nullopt, right, "MULTIPOLYGON EMPTY"});
    }
    for (size_t rows : {size_t(1), size_t(3), size_t(7)}) {
        std::vector<std::pair<int32_t, std::string>> output;
        auto status = drive_coverage_analytor(rows, keys, input, false, &output, 1, false, true);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(output.size(), input.size());
        for (size_t row = 0; row < output.size(); ++row) {
            EXPECT_EQ(output[row].first, row);
            EXPECT_EQ(output[row].second, row % 4 == 0
                                                  ? "POLYGON ((0 0, 0 8, 4 8, 4 0, 0 0))"
                                                  : row % 4 == 1 ? "NULL"
                                                                 : row % 4 == 2 ? "POLYGON ((4 0, 4 8, 8 8, 8 0, 4 0))"
                                                                                : "MULTIPOLYGON EMPTY");
        }
    }
}
TEST(CoverageAnalytorTest, ExceededQueryMemoryLimitUsesTheStandardStatusPath) {
    std::vector<std::pair<int32_t, std::string>> output;
    auto status = drive_coverage_analytor(1, {}, {left}, false, &output, 0, false, false, true);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(output.empty());
}
} // namespace
} // namespace starrocks

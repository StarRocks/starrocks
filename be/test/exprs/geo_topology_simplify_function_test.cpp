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

#include <atomic>
#include <cmath>
#include <limits>
#include <thread>

#include "column/column_viewer.h"
#include "column/geo_column.h"
#include "column/nullable_column.h"
#include "exprs/builtin_functions.h"
#include "exprs/expr_context.h"
#include "exprs/function_call_expr.h"
#include "exprs/geo_functions.h"
#include "exprs/mock_vectorized_expr.h"
#include "geo/geo_measurements.h"
#include "runtime/runtime_state.h"
namespace starrocks {
namespace {
constexpr const char* kSquare = "POLYGON ((0 0,1 0,2 0,2 1,2 2,1 2,0 2,0 1,0 0))";
TypeDescriptor geo_type(std::string crs = "EPSG:3857") {
    return TypeDescriptor::create_geo_type(
            TYPE_GEOMETRY, {GEO_LOGICAL_TYPE_GEOMETRY, GEO_COORDINATE_SYSTEM_CARTESIAN, GEO_EDGE_ALGORITHM_PLANAR, crs,
                            crs == "EPSG:4326" ? 4326 : 3857});
}
std::unique_ptr<FunctionContext> context(const TypeDescriptor& type = geo_type()) {
    return std::unique_ptr<FunctionContext>(
            FunctionContext::create_test_context({type, TypeDescriptor(TYPE_DOUBLE)}, type));
}
ColumnPtr geometries(std::initializer_list<const char*> values, const TypeDescriptor& type = geo_type()) {
    auto text = BinaryColumn::create();
    auto nulls = NullColumn::create();
    for (const char* value : values) {
        if (value) {
            text->append(value);
        } else {
            text->append_default();
        }
        nulls->append(value == nullptr);
    }
    ColumnPtr input = NullableColumn::create(text, nulls);
    auto crs = ColumnHelper::create_const_column<TYPE_VARCHAR>(type.geo_type->crs, input->size());
    std::unique_ptr<FunctionContext> ctx(
            FunctionContext::create_test_context({TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR)}, type));
    return GeoFunctions::st_geom_from_text(ctx.get(), {input, crs}).value();
}
ColumnPtr distances(std::initializer_list<double> values) {
    auto result = DoubleColumn::create();
    for (double value : values) result->append(value);
    return result;
}
ColumnPtr radius(double value, size_t size = 1) {
    return ColumnHelper::create_const_column<TYPE_DOUBLE>(value, size);
}
ColumnPtr raw_geometry(std::string_view wkb,
                       GeoColumnDescriptor descriptor = {
                               geo_type().geo_type.value(),
                               {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_STRUCTURALLY_VALIDATED}}) {
    auto result = GeoColumn::create(std::move(descriptor));
    result->append_wkb(Slice(wkb));
    return result;
}
WkbGeometry decoded(const ColumnPtr& column, size_t row = 0) {
    auto data = GeoFunctions::st_geometry_as_wkb(nullptr, {column});
    EXPECT_TRUE(data.ok()) << data.status();
    WkbGeometry result;
    if (data.ok()) {
        EXPECT_TRUE(WkbCodec::parse_wkb(ColumnViewer<TYPE_VARBINARY>(*data).value(row), &result,
                                        WkbCoordinateSemantics::GEOMETRY_CARTESIAN)
                            .ok());
    }
    return result;
}

} // namespace

TEST(GeoTopologySimplifyFunctionTest, FamilyCrsAndValidationSurviveVaryingTolerance) {
    for (const char* crs : {"EPSG:4326", "EPSG:3857"}) {
        auto type = geo_type(crs);
        auto ctx = context(type);
        auto source = geometries({kSquare, kSquare, kSquare}, type);
        auto result =
                GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(), {source, distances({0, 0.01, 100})});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(9, decoded(*result, 0).rings[0].size());
        EXPECT_EQ(5, decoded(*result, 1).rings[0].size());
        EXPECT_GE(decoded(*result, 2).rings[0].size(), 4);
        auto* data = down_cast<const GeoColumn*>(down_cast<const NullableColumn*>(result->get())->data_column().get());
        EXPECT_EQ(type.geo_type.value(), data->descriptor().type);
        EXPECT_EQ(GEO_DIMENSION_XY, data->descriptor().storage.dimension);
        EXPECT_EQ(GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED, data->descriptor().storage.validation_state);
        auto before = GeoFunctions::st_geometry_as_wkb(nullptr, {source}).value();
        auto after = GeoFunctions::st_geometry_as_wkb(nullptr, {*result}).value();
        EXPECT_EQ(ColumnViewer<TYPE_VARBINARY>(before).value(0), ColumnViewer<TYPE_VARBINARY>(after).value(0));
    }
}
TEST(GeoTopologySimplifyFunctionTest, SevenFamiliesTypedEmptiesAndNestedChildren) {
    auto ctx = context();
    for (const char* wkt :
         {"POINT (0 0)", "MULTIPOINT ((0 0),(0 0))", "LINESTRING (0 0,1 0,2 0)",
          "MULTILINESTRING ((0 0,1 0,2 0),(3 0,4 0))", kSquare, "MULTIPOLYGON (((0 0,1 0,2 0,2 2,0 2,0 0)))",
          "GEOMETRYCOLLECTION (POINT EMPTY,GEOMETRYCOLLECTION (LINESTRING (0 0,1 0,2 0),POLYGON EMPTY))"}) {
        auto input = geometries({wkt});
        auto source = decoded(input);
        auto result = GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(), {input, radius(0.01)});
        ASSERT_TRUE(result.ok()) << result.status();
        auto output = decoded(*result);
        EXPECT_EQ(source.type, output.type);
        EXPECT_EQ(source.children.size(), output.children.size());
        if (output.type == WkbGeometryType::GEOMETRYCOLLECTION) {
            EXPECT_TRUE(output.children[0].empty);
            EXPECT_EQ(2, output.children[1].children.size());
            EXPECT_EQ(2, output.children[1].children[0].coordinates.size());
            EXPECT_TRUE(output.children[1].children[1].empty);
        }
    }
    for (const char* family :
         {"POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"}) {
        auto input = geometries({(std::string(family) + " EMPTY").c_str()});
        auto result = GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(), {input, radius(1)});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(decoded(input).type, decoded(*result).type);
        EXPECT_TRUE(decoded(*result).empty);
    }
}
TEST(GeoTopologySimplifyFunctionTest, NullableArgumentsAndUnreachedMalformedPayloadKeepErrorTiming) {
    auto ctx = context();
    auto values = DoubleColumn::create();
    values->append(-1);
    values->append(0.01);
    values->append(0);
    auto nulls = NullColumn::create();
    nulls->append(0);
    nulls->append(0);
    nulls->append(1);
    auto output = GeoFunctions::st_geometry_simplify_preserve_topology(
            ctx.get(), {geometries({nullptr, kSquare, kSquare}), NullableColumn::create(values, nulls)});
    ASSERT_TRUE(output.ok()) << output.status();
    EXPECT_TRUE((*output)->is_null(0));
    EXPECT_FALSE((*output)->is_null(1));
    EXPECT_TRUE((*output)->is_null(2));
    auto all_null = ColumnHelper::create_const_null_column(1);
    for (const Columns& args : {Columns{raw_geometry("bad"), all_null}, Columns{all_null, radius(-1)}}) {
        auto result = GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(), args);
        ASSERT_TRUE(result.ok());
        EXPECT_TRUE((*result)->only_null());
    }
    auto bad = GeoColumn::create(down_cast<const GeoColumn*>(raw_geometry("bad").get())->descriptor());
    bad->append_wkb(Slice("bad"));
    auto good = GeoFunctions::st_geometry_as_wkb(nullptr, {geometries({kSquare})}).value();
    bad->append_wkb(ColumnViewer<TYPE_VARBINARY>(good).value(0));
    values = DoubleColumn::create();
    values->append(1);
    values->append(.01);
    nulls = NullColumn::create();
    nulls->append(1);
    nulls->append(0);
    auto used = GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(),
                                                                     {bad, NullableColumn::create(values, nulls)});
    ASSERT_TRUE(used.ok()) << used.status();
    EXPECT_TRUE((*used)->is_null(0));
    EXPECT_EQ(5, decoded(*used, 1).rings[0].size());
}
TEST(GeoTopologySimplifyFunctionTest, InvalidToleranceAndTopologyAreRejectedBeforeZeroShortcut) {
    auto ctx = context();
    for (const char* input : {"POLYGON EMPTY", kSquare})
        for (double t : {-1., std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::infinity()})
            EXPECT_FALSE(
                    GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(), {geometries({input}), radius(t)})
                            .ok());
    for (const char* wkt : {"POLYGON ((0 0,2 2,0 2,2 0,0 0))", "LINESTRING (1 1,1 1)"})
        EXPECT_FALSE(
                GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(), {geometries({wkt}), radius(0)}).ok());
    auto good = geometries({kSquare});
    auto bytes = GeoFunctions::st_geometry_as_wkb(nullptr, {good}).value();
    auto wkb = ColumnViewer<TYPE_VARBINARY>(bytes).value(0).to_string();
    GeoColumnDescriptor descriptor{geo_type().geo_type.value(),
                                   {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_STRUCTURALLY_VALIDATED}};
    for (auto dim :
         {GEO_DIMENSION_UNKNOWN, GEO_DIMENSION_MIXED, GEO_DIMENSION_XYZ, GEO_DIMENSION_XYM, GEO_DIMENSION_XYZM}) {
        auto other = descriptor;
        other.storage.dimension = dim;
        EXPECT_FALSE(
                GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(), {raw_geometry(wkb, other), radius(0)})
                        .ok());
    }
    auto other = descriptor;
    other.type.logical_type = GEO_LOGICAL_TYPE_GEOGRAPHY;
    EXPECT_FALSE(GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(), {raw_geometry(wkb, other), radius(0)})
                         .ok());
    auto wrong = context(geo_type("EPSG:4326"));
    EXPECT_FALSE(GeoFunctions::st_geometry_simplify_preserve_topology(wrong.get(), {good, radius(0)}).ok());
    EXPECT_FALSE(
            GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(), {raw_geometry("bad"), radius(0)}).ok());
    EXPECT_FALSE(GeoFunctions::st_geometry_simplify_preserve_topology(nullptr, {good, radius(1)}).ok());
}
TEST(GeoTopologySimplifyFunctionTest, ConstantAndZeroRowBatchesHaveCorrectSizes) {
    auto ctx = context();
    auto output = GeoFunctions::st_geometry_simplify_preserve_topology(
            ctx.get(), {ConstColumn::create(geometries({kSquare}), 4), radius(.01, 4)});
    ASSERT_TRUE(output.ok());
    EXPECT_TRUE((*output)->is_constant());
    EXPECT_EQ(4, (*output)->size());
    for (const Columns& input : {Columns{geometries({}), distances({})},
                                 Columns{ConstColumn::create(geometries({kSquare}), 0), radius(1, 0)}}) {
        auto result = GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(), input);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(0, (*result)->size());
    }
}
TEST(GeoTopologySimplifyFunctionTest, PlanConstantInputReusesPreparationAcrossBatchesAndConcurrentWorkers) {
    auto ctx = context();
    auto fixed = ConstColumn::create(geometries({kSquare}), 3);
    ctx->set_constant_columns({fixed, nullptr});
    ASSERT_TRUE(GeoFunctions::native_geo_simplify_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    auto* state = ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL);
    ASSERT_NE(nullptr, state);
    for (int batch = 0; batch < 2; ++batch) {
        auto result =
                GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(), {fixed, distances({0, .01, 100})});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(9, decoded(*result, 0).rings[0].size());
        EXPECT_EQ(5, decoded(*result, 1).rings[0].size());
        EXPECT_GE(decoded(*result, 2).rings[0].size(), 4);
        EXPECT_EQ(state, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    }
    std::atomic<int> failures = 0;
    std::vector<std::thread> workers;
    for (int i = 0; i < 4; ++i)
        workers.emplace_back([&, i] {
            for (int batch = 0; batch < 3; ++batch) {
                auto result = GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(),
                                                                                   {fixed, radius(i % 2 ? 0 : .01, 3)});
                if (!result.ok() || decoded(*result).rings[0].size() != (i % 2 ? 9 : 5)) ++failures;
            }
        });
    for (auto& w : workers) w.join();
    EXPECT_EQ(0, failures);
    ASSERT_TRUE(GeoFunctions::native_geo_simplify_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    EXPECT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    ASSERT_TRUE(GeoFunctions::native_geo_simplify_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}
TEST(GeoTopologySimplifyFunctionTest, ChunkConstantsAndRepeatedVaryingRowsAreNotPersistentlyCached) {
    auto ctx = context();
    ctx->set_constant_columns({nullptr, radius(.01)});
    ASSERT_TRUE(GeoFunctions::native_geo_simplify_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    EXPECT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    for (const char* wkt : {kSquare, "POLYGON EMPTY"}) {
        auto result = GeoFunctions::st_geometry_simplify_preserve_topology(
                ctx.get(), {ConstColumn::create(geometries({wkt}), 1), radius(.01)});
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(std::string_view(wkt) == "POLYGON EMPTY", decoded(*result).empty);
    }
    auto result = GeoFunctions::st_geometry_simplify_preserve_topology(
            ctx.get(), {geometries({kSquare, "POLYGON EMPTY", kSquare}), radius(.01, 3)});
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(5, decoded(*result, 0).rings[0].size());
    EXPECT_TRUE(decoded(*result, 1).empty);
    EXPECT_EQ(5, decoded(*result, 2).rings[0].size());
}
TEST(GeoTopologySimplifyFunctionTest, PreparedInvalidAndNullInputsDeferPayloadErrorsUntilUsed) {
    for (ColumnPtr fixed :
         Columns{ConstColumn::create(raw_geometry("bad"), 1), ColumnHelper::create_const_null_column(1)}) {
        auto ctx = context();
        ctx->set_constant_columns({fixed, nullptr});
        ASSERT_TRUE(GeoFunctions::native_geo_simplify_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
        auto result = GeoFunctions::st_geometry_simplify_preserve_topology(
                ctx.get(), {fixed, ColumnHelper::create_const_null_column(1)});
        ASSERT_TRUE(result.ok());
        EXPECT_TRUE((*result)->only_null());
        auto used = GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(), {fixed, radius(1)});
        if (fixed->only_null()) {
            ASSERT_TRUE(used.ok());
            EXPECT_TRUE((*used)->only_null());
        } else
            EXPECT_FALSE(used.ok());
        ASSERT_TRUE(GeoFunctions::native_geo_simplify_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    }
}
TEST(GeoTopologySimplifyFunctionTest, CancellationAndQueryMemoryLimitReturnOriginalErrors) {
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    state.init_mem_trackers(TUniqueId());
    ASSERT_NE(nullptr, state.query_mem_tracker_ptr());
    auto type = geo_type();
    std::unique_ptr<FunctionContext> ctx(
            FunctionContext::create_context(&state, nullptr, type, {type, TypeDescriptor(TYPE_DOUBLE)}));
    state.set_is_cancelled(true);
    auto result = GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(), {geometries({kSquare}), radius(1)});
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_cancelled());
    ctx->set_constant_columns({ConstColumn::create(geometries({kSquare}), 1), nullptr});
    EXPECT_TRUE(GeoFunctions::native_geo_simplify_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).is_cancelled());
    state.set_is_cancelled(false);
    ASSERT_TRUE(state.set_mem_limit_exceeded(state.instance_mem_tracker(), 1, "simplify test").is_mem_limit_exceeded());
    result = GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(), {geometries({kSquare}), radius(1)});
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_mem_limit_exceeded());
}
TEST(GeoTopologySimplifyFunctionTest, RegistryConsumesOnlyTheGeometryReservation) {
    const auto* fn = BuiltinFunctions::find_builtin_function(120411);
    ASSERT_NE(nullptr, fn);
    EXPECT_EQ(2, fn->args_nums);
    ASSERT_TRUE(fn->prepare_function);
    ASSERT_TRUE(fn->close_function);
    auto ctx = context();
    auto fixed = ConstColumn::create(geometries({kSquare}), 1);
    ctx->set_constant_columns({fixed, nullptr});
    ASSERT_TRUE(fn->prepare_function(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    auto result = fn->scalar_function(ctx.get(), {fixed, radius(.01)});
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(5, decoded(*result).rings[0].size());
    ASSERT_TRUE(fn->close_function(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    for (uint64_t id : {120400, 120410, 120420, 120421, 120430, 120431})
        EXPECT_EQ(nullptr, BuiltinFunctions::find_builtin_function(id));
}
TEST(GeoTopologySimplifyFunctionTest, ExpressionOwnerAndCloneShareImmutablePreparationAndCleanUpOnce) {
    class Operand final : public MockExpr {
    public:
        Operand(const TExprNode& node, ColumnPtr value, bool constant)
                : MockExpr(node, std::move(value)), _constant(constant) {}
        bool is_constant() const override { return _constant; }
        ColumnPtr next;
        StatusOr<ColumnPtr> evaluate_checked(ExprContext* ctx, Chunk* chunk) override {
            return next ? StatusOr<ColumnPtr>(next) : MockExpr::evaluate_checked(ctx, chunk);
        }

    private:
        bool _constant;
    };
    RuntimeState state;
    state.init_instance_mem_tracker();
    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_type(geo_type().to_thrift());
    node.__set_num_children(2);
    TFunction function;
    TFunctionName name;
    name.__set_function_name("ST_SimplifyPreserveTopology");
    function.__set_name(name);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);
    function.__set_fid(120411);
    function.__set_arg_types({geo_type().to_thrift(), TypeDescriptor(TYPE_DOUBLE).to_thrift()});
    function.__set_ret_type(geo_type().to_thrift());
    function.__set_has_var_args(false);
    node.__set_fn(function);
    VectorizedFunctionCallExpr expr(node);
    TExprNode operand_node;
    operand_node.__set_node_type(TExprNodeType::SLOT_REF);
    operand_node.__set_type(geo_type().to_thrift());
    operand_node.__set_num_children(0);
    operand_node.__set_is_nullable(true);
    Operand left(operand_node, ConstColumn::create(geometries({kSquare}), 1), true);
    operand_node.__set_type(TypeDescriptor(TYPE_DOUBLE).to_thrift());
    Operand right(operand_node, distances({0.01}), false);
    expr.add_child(&left);
    expr.add_child(&right);
    ExprContext owner(&expr);
    ASSERT_TRUE(owner.prepare(&state).ok());
    ASSERT_TRUE(owner.open(&state).ok());
    const auto* prepared = owner.fn_context(0)->get_function_state(FunctionContext::FRAGMENT_LOCAL);
    ASSERT_NE(nullptr, prepared);
    ObjectPool pool;
    ExprContext* clone = nullptr;
    ASSERT_TRUE(owner.clone(&state, &pool, &clone).ok());
    EXPECT_EQ(prepared, clone->fn_context(0)->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    auto result = expr.evaluate_checked(clone, nullptr);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(5, decoded(*result).rings[0].size());
    clone->close(&state);
    EXPECT_EQ(prepared, owner.fn_context(0)->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    right.next = distances({0});
    auto second = expr.evaluate_checked(&owner, nullptr);
    ASSERT_TRUE(second.ok()) << second.status();
    EXPECT_EQ(9, decoded(*second).rings[0].size());
    owner.close(&state);
    EXPECT_EQ(nullptr, owner.fn_context(0)->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    owner.close(&state);
}

TEST(GeoTopologySimplifyFunctionTest, DocumentedSqlResultsUseTheActualNativeTextPath) {
    auto ctx = context();
    for (const auto& sample : std::vector<std::pair<const char*, const char*>>{
                 {"LINESTRING (0 0,1 0,2 0)", "LINESTRING (0 0, 2 0)"},
                 {"MULTIPOLYGON EMPTY", "MULTIPOLYGON EMPTY"},
                 {"MULTILINESTRING ((0 0,1 0,2 0),(3 0,4 0))", "MULTILINESTRING ((0 0, 2 0), (3 0, 4 0))"},
                 {"GEOMETRYCOLLECTION (POINT EMPTY,LINESTRING (0 0,1 0,2 0))",
                  "GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING (0 0, 2 0))"}}) {
        auto result = GeoFunctions::st_geometry_simplify_preserve_topology(ctx.get(),
                                                                           {geometries({sample.first}), radius(0.1)});
        ASSERT_TRUE(result.ok()) << result.status();
        auto text = GeoFunctions::st_geometry_as_text(nullptr, {*result});
        ASSERT_TRUE(text.ok()) << text.status();
        EXPECT_EQ(sample.second, ColumnViewer<TYPE_VARCHAR>(*text).value(0).to_string());
    }
    auto result = GeoFunctions::st_geometry_simplify_preserve_topology(
            ctx.get(), {geometries({"POINT (1 2)"}), ColumnHelper::create_const_null_column(1)});
    ASSERT_TRUE(result.ok());
    EXPECT_TRUE(result->get()->only_null());
}

} // namespace starrocks

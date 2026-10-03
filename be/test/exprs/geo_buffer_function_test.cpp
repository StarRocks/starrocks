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
constexpr const char* kSquare = "POLYGON ((0 0,10 0,10 10,0 10,0 0))";
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
double area(const ColumnPtr& column, size_t row = 0) {
    auto result = GeoFunctions::st_geometry_area(nullptr, {column});
    EXPECT_TRUE(result.ok()) << result.status();
    return result.ok() ? ColumnViewer<TYPE_DOUBLE>(*result).value(row) : -1;
}
} // namespace
TEST(GeoBufferFunctionTest, SignedDistancesPreserveCrsAndValidationState) {
    for (const char* crs : {"EPSG:4326", "EPSG:3857"}) {
        auto type = geo_type(crs);
        auto ctx = context(type);
        auto result = GeoFunctions::st_geometry_buffer(
                ctx.get(), {geometries({kSquare, kSquare, kSquare}, type), distances({0, -1, 1})});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_DOUBLE_EQ(100, area(*result, 0));
        EXPECT_DOUBLE_EQ(64, area(*result, 1));
        EXPECT_GT(area(*result, 2), 140);
        auto* data = down_cast<const GeoColumn*>(down_cast<const NullableColumn*>(result->get())->data_column().get());
        EXPECT_EQ(type.geo_type.value(), data->descriptor().type);
        EXPECT_EQ(GEO_DIMENSION_XY, data->descriptor().storage.dimension);
        EXPECT_EQ(GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED, data->descriptor().storage.validation_state);
    }
}
TEST(GeoBufferFunctionTest, SixFamiliesAndTypedEmptiesExecuteThroughSqlWrapper) {
    auto ctx = context();
    for (const char* input : {"POINT (0 0)", "LINESTRING (0 0,2 0)", kSquare, "MULTIPOINT ((0 0),(10 0))",
                              "MULTILINESTRING ((0 0,2 0),(10 0,12 0))", "MULTIPOLYGON (((0 0,2 0,2 2,0 2,0 0)))"}) {
        auto result = GeoFunctions::st_geometry_buffer(ctx.get(), {geometries({input}), radius(1)});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(decoded(*result).empty);
    }
    for (const char* family : {"POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON"}) {
        auto input = std::string(family) + " EMPTY";
        for (double value : {1.0, 0.0, -1.0}) {
            auto result = GeoFunctions::st_geometry_buffer(ctx.get(), {geometries({input.c_str()}), radius(value)});
            ASSERT_TRUE(result.ok()) << result.status();
            EXPECT_EQ(WkbGeometryType::POLYGON, decoded(*result).type);
            EXPECT_TRUE(decoded(*result).empty);
        }
    }
}
TEST(GeoBufferFunctionTest, NullableRowsAndAllNullArgumentsShortCircuit) {
    auto ctx = context();
    auto values = DoubleColumn::create();
    values->append(1);
    values->append(0);
    values->append(-1);
    auto nulls = NullColumn::create();
    nulls->append(0);
    nulls->append(1);
    nulls->append(0);
    auto result = GeoFunctions::st_geometry_buffer(
            ctx.get(), {geometries({nullptr, kSquare, kSquare}), NullableColumn::create(values, nulls)});
    ASSERT_TRUE(result.ok());
    EXPECT_TRUE((*result)->is_null(0));
    EXPECT_TRUE((*result)->is_null(1));
    EXPECT_DOUBLE_EQ(64, area(*result, 2));
    auto all_null = ColumnHelper::create_const_null_column(1);
    for (const Columns& columns : {Columns{raw_geometry("bad"), all_null}, Columns{all_null, radius(1)}}) {
        auto output = GeoFunctions::st_geometry_buffer(ctx.get(), columns);
        ASSERT_TRUE(output.ok());
        EXPECT_TRUE((*output)->only_null());
    }
}
TEST(GeoBufferFunctionTest, ConstantResultAndZeroRowBatchHaveCorrectSizes) {
    auto ctx = context();
    auto result =
            GeoFunctions::st_geometry_buffer(ctx.get(), {ConstColumn::create(geometries({kSquare}), 4), radius(-1, 4)});
    ASSERT_TRUE(result.ok());
    EXPECT_TRUE((*result)->is_constant());
    EXPECT_EQ(4, (*result)->size());
    EXPECT_DOUBLE_EQ(64, area(*result));
    for (bool constant : {false, true}) {
        ColumnPtr input = geometries({});
        ColumnPtr values = distances({});
        if (constant) {
            input = ConstColumn::create(geometries({kSquare}), 0);
            values = radius(1, 0);
        }
        auto empty = GeoFunctions::st_geometry_buffer(ctx.get(), {input, values});
        ASSERT_TRUE(empty.ok()) << empty.status();
        EXPECT_EQ(0, (*empty)->size());
    }
}
TEST(GeoBufferFunctionTest, NonFiniteRadiusIsRejectedEvenForEmptyInput) {
    auto ctx = context();
    for (double value : {std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::infinity(),
                         -std::numeric_limits<double>::infinity()}) {
        auto result = GeoFunctions::st_geometry_buffer(ctx.get(), {geometries({"POLYGON EMPTY"}), radius(value)});
        ASSERT_FALSE(result.ok());
        EXPECT_TRUE(result.status().is_invalid_argument());
    }
}
TEST(GeoBufferFunctionTest, RejectsMalformedDimensionsKindsAndIncompatibleReturnDescriptor) {
    auto ctx = context();
    EXPECT_FALSE(GeoFunctions::st_geometry_buffer(ctx.get(), {raw_geometry("bad"), radius(1)}).ok());
    for (const char* input : {"GEOMETRYCOLLECTION EMPTY", "POLYGON ((0 0,4 4,0 4,4 0,0 0))"}) {
        EXPECT_FALSE(GeoFunctions::st_geometry_buffer(ctx.get(), {geometries({input}), radius(0)}).ok());
    }
    auto good = geometries({kSquare});
    auto data = GeoFunctions::st_geometry_as_wkb(nullptr, {good});
    ASSERT_TRUE(data.ok());
    const auto wkb = ColumnViewer<TYPE_VARBINARY>(*data).value(0).to_string();
    GeoColumnDescriptor descriptor{geo_type().geo_type.value(),
                                   {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_STRUCTURALLY_VALIDATED}};
    for (auto dimension :
         {GEO_DIMENSION_UNKNOWN, GEO_DIMENSION_MIXED, GEO_DIMENSION_XYZ, GEO_DIMENSION_XYM, GEO_DIMENSION_XYZM}) {
        auto other = descriptor;
        other.storage.dimension = dimension;
        EXPECT_FALSE(GeoFunctions::st_geometry_buffer(ctx.get(), {raw_geometry(wkb, other), radius(1)}).ok());
    }
    auto geography = descriptor;
    geography.type.logical_type = GEO_LOGICAL_TYPE_GEOGRAPHY;
    EXPECT_FALSE(GeoFunctions::st_geometry_buffer(ctx.get(), {raw_geometry(wkb, geography), radius(1)}).ok());
    auto wrong = context(geo_type("EPSG:4326"));
    EXPECT_FALSE(GeoFunctions::st_geometry_buffer(wrong.get(), {good, radius(1)}).ok());
    EXPECT_FALSE(GeoFunctions::st_geometry_buffer(nullptr, {good, radius(1)}).ok());
    EXPECT_FALSE(GeoFunctions::st_geometry_buffer(ctx.get(), {good}).ok());
}
TEST(GeoBufferFunctionTest, PlanConstantGeometryReusesPreparationWithVaryingRadiiAndSharedWorkers) {
    auto ctx = context();
    auto fixed = ConstColumn::create(geometries({kSquare}), 3);
    ctx->set_constant_columns({fixed, nullptr});
    ASSERT_TRUE(GeoFunctions::native_geo_buffer_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    const void* state = ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL);
    ASSERT_NE(nullptr, state);
    for (int batch = 0; batch < 2; ++batch) {
        auto result = GeoFunctions::st_geometry_buffer(ctx.get(), {fixed, distances({0, -1, -6})});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_DOUBLE_EQ(100, area(*result, 0));
        EXPECT_DOUBLE_EQ(64, area(*result, 1));
        EXPECT_TRUE(decoded(*result, 2).empty);
        EXPECT_EQ(state, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    }
    std::atomic<int> failures{0};
    std::vector<std::thread> workers;
    for (int worker = 0; worker < 4; ++worker)
        workers.emplace_back([&, worker] {
            for (int batch = 0; batch < 3; ++batch) {
                auto result = GeoFunctions::st_geometry_buffer(ctx.get(), {fixed, radius(worker % 2 ? -1 : 0, 3)});
                if (!result.ok()) {
                    ++failures;
                    continue;
                }
                auto measured = GeoFunctions::st_geometry_area(nullptr, {*result});
                if (!measured.ok() || ColumnViewer<TYPE_DOUBLE>(*measured).value(0) != (worker % 2 ? 64 : 100))
                    ++failures;
            }
        });
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(0, failures);
    ASSERT_TRUE(GeoFunctions::native_geo_buffer_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    EXPECT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    ASSERT_TRUE(GeoFunctions::native_geo_buffer_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}
TEST(GeoBufferFunctionTest, PlanConstantRadiusDoesNotCacheChangingChunkGeometry) {
    auto ctx = context();
    auto fixed = radius(-1);
    ctx->set_constant_columns({nullptr, fixed});
    ASSERT_TRUE(GeoFunctions::native_geo_buffer_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    EXPECT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    for (const char* input :
         {kSquare, "POLYGON EMPTY", "MULTIPOLYGON (((0 0,10 0,10 10,0 10,0 0)),((20 0,30 0,30 10,20 10,20 0)))"}) {
        auto result = GeoFunctions::st_geometry_buffer(ctx.get(), {ConstColumn::create(geometries({input}), 1), fixed});
        ASSERT_TRUE(result.ok()) << result.status();
        const auto value = decoded(*result);
        if (std::string_view(input) == kSquare) {
            EXPECT_DOUBLE_EQ(64, area(*result));
        } else if (std::string_view(input) == "POLYGON EMPTY") {
            EXPECT_TRUE(value.empty);
        } else {
            EXPECT_EQ(WkbGeometryType::MULTIPOLYGON, value.type);
            EXPECT_EQ(2, value.children.size());
            EXPECT_DOUBLE_EQ(128, area(*result));
        }
    }
}
TEST(GeoBufferFunctionTest, PreparedInvalidAndNullConstantsKeepErrorsLazy) {
    for (ColumnPtr fixed :
         Columns{ConstColumn::create(raw_geometry("bad"), 1), ColumnHelper::create_const_null_column(1)}) {
        auto ctx = context();
        ctx->set_constant_columns({fixed, nullptr});
        ASSERT_TRUE(GeoFunctions::native_geo_buffer_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
        auto null_result =
                GeoFunctions::st_geometry_buffer(ctx.get(), {fixed, ColumnHelper::create_const_null_column(1)});
        ASSERT_TRUE(null_result.ok());
        EXPECT_TRUE((*null_result)->only_null());
        auto used = GeoFunctions::st_geometry_buffer(ctx.get(), {fixed, radius(1)});
        if (fixed->only_null()) {
            ASSERT_TRUE(used.ok());
            EXPECT_TRUE((*used)->only_null());
        } else {
            EXPECT_FALSE(used.ok());
        }
        ASSERT_TRUE(GeoFunctions::native_geo_buffer_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    }
}
TEST(GeoBufferFunctionTest, CancellationAndRealQueryMemoryLimitReturnErrors) {
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    state.init_mem_trackers(TUniqueId());
    ASSERT_NE(nullptr, state.query_mem_tracker_ptr());
    auto type = geo_type();
    std::unique_ptr<FunctionContext> ctx(
            FunctionContext::create_context(&state, nullptr, type, {type, TypeDescriptor(TYPE_DOUBLE)}));
    state.set_is_cancelled(true);
    auto result = GeoFunctions::st_geometry_buffer(ctx.get(), {geometries({kSquare}), radius(1)});
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_cancelled());
    state.set_is_cancelled(false);
    ASSERT_TRUE(state.set_mem_limit_exceeded(state.instance_mem_tracker(), 1, "Buffer test").is_mem_limit_exceeded());
    result = GeoFunctions::st_geometry_buffer(ctx.get(), {geometries({kSquare}), radius(1)});
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_mem_limit_exceeded());
}
TEST(GeoBufferFunctionTest, RegistryActivatesOnlyGeometryBufferAndItsLifecycle) {
    const auto* fn = BuiltinFunctions::find_builtin_function(120401);
    ASSERT_NE(nullptr, fn);
    EXPECT_EQ(2, fn->args_nums);
    ASSERT_TRUE(fn->prepare_function);
    ASSERT_TRUE(fn->close_function);
    auto ctx = context();
    auto fixed = ConstColumn::create(geometries({kSquare}), 1);
    ctx->set_constant_columns({fixed, nullptr});
    ASSERT_TRUE(fn->prepare_function(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(fn->scalar_function(ctx.get(), {fixed, radius(-1)}).ok());
    ASSERT_TRUE(fn->close_function(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    EXPECT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    for (uint64_t id : {120400, 120410, 120411, 120420, 120421, 120430, 120431})
        EXPECT_EQ(nullptr, BuiltinFunctions::find_builtin_function(id));
}

TEST(GeoBufferFunctionTest, ExpressionOwnerAndCloneShareImmutablePreparationAndCleanUpOnce) {
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
    name.__set_function_name("ST_Buffer");
    function.__set_name(name);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);
    function.__set_fid(120401);
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
    Operand right(operand_node, distances({-1}), false);
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
    EXPECT_DOUBLE_EQ(64, area(*result));
    clone->close(&state);
    EXPECT_EQ(prepared, owner.fn_context(0)->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    right.next = distances({0});
    auto second = expr.evaluate_checked(&owner, nullptr);
    ASSERT_TRUE(second.ok()) << second.status();
    EXPECT_DOUBLE_EQ(100, area(*second));
    owner.close(&state);
    EXPECT_EQ(nullptr, owner.fn_context(0)->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    owner.close(&state);
}

} // namespace starrocks

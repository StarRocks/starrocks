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

#include "geo/geo_overlay.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstring>
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
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {
using OverlayFunction = StatusOr<ColumnPtr> (*)(FunctionContext*, const Columns&);
constexpr OverlayFunction functions[] = {GeoFunctions::st_geometry_union, GeoFunctions::st_geometry_difference,
                                         GeoFunctions::st_geometry_sym_difference};
constexpr const char* square = "POLYGON ((0 0,4 0,4 4,0 4,0 0))";
constexpr const char* shifted = "POLYGON ((2 0,6 0,6 4,2 4,2 0))";
constexpr const char* empty = "POLYGON EMPTY";

TypeDescriptor geo_type(std::string crs = "EPSG:3857") {
    return TypeDescriptor::create_geo_type(
            TYPE_GEOMETRY, {GEO_LOGICAL_TYPE_GEOMETRY, GEO_COORDINATE_SYSTEM_CARTESIAN, GEO_EDGE_ALGORITHM_PLANAR, crs,
                            crs == "EPSG:4326" ? 4326 : 3857});
}

ColumnPtr geometries(std::initializer_list<const char*> values, const TypeDescriptor& type = geo_type()) {
    auto text = BinaryColumn::create();
    auto nulls = NullColumn::create();
    for (const char* value : values) {
        if (value)
            text->append(value);
        else
            text->append_default();
        nulls->append(value == nullptr);
    }
    ColumnPtr input = NullableColumn::create(text, nulls);
    auto crs = ColumnHelper::create_const_column<TYPE_VARCHAR>(type.geo_type->crs, input->size());
    std::unique_ptr<FunctionContext> ctx(
            FunctionContext::create_test_context({TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR)}, type));
    return GeoFunctions::st_geom_from_text(ctx.get(), {input, crs}).value();
}

ColumnPtr raw_geometry(std::string_view wkb) {
    GeoColumnDescriptor descriptor{geo_type().geo_type.value(),
                                   {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_STRUCTURALLY_VALIDATED}};
    auto column = GeoColumn::create(descriptor);
    column->append_datum(Datum(Slice(wkb)));
    return column;
}

std::unique_ptr<FunctionContext> context(const TypeDescriptor& type = geo_type()) {
    return std::unique_ptr<FunctionContext>(FunctionContext::create_test_context({type, type}, type));
}

WkbGeometry decoded(const ColumnPtr& column, size_t row = 0) {
    WkbGeometry result;
    auto text = GeoFunctions::st_geometry_as_text(nullptr, {column});
    EXPECT_TRUE(text.ok()) << text.status();
    if (!text.ok()) return result;
    ColumnViewer<TYPE_VARCHAR> viewer(*text);
    EXPECT_FALSE(viewer.is_null(row));
    EXPECT_TRUE(WkbCodec::parse_wkt(viewer.value(row).to_string(), &result, WkbCoordinateSemantics::GEOMETRY_CARTESIAN)
                        .ok());
    return result;
}

std::vector<const WkbGeometry*> polygons(const WkbGeometry& geometry) {
    if (geometry.empty) return {};
    if (geometry.type == WkbGeometryType::POLYGON) return {&geometry};
    std::vector<const WkbGeometry*> result;
    for (const auto& child : geometry.children)
        if (!child.empty) result.push_back(&child);
    return result;
}

// Independent Euclidean boundary oracle: every vertex AND edge midpoint must
// lie on an expected segment, in both directions. Match components and holes
// individually so area/bbox equality cannot hide misplaced geometry.
double segment_distance(WkbCoordinate point, WkbCoordinate a, WkbCoordinate b) {
    const double dx = b.x - a.x, dy = b.y - a.y;
    const double length2 = dx * dx + dy * dy;
    const double t = length2 == 0 ? 0 : std::clamp(((point.x - a.x) * dx + (point.y - a.y) * dy) / length2, 0.0, 1.0);
    return std::hypot(point.x - a.x - t * dx, point.y - a.y - t * dy);
}
bool directed_ring_match(const std::vector<WkbCoordinate>& a, const std::vector<WkbCoordinate>& b, double tolerance) {
    if (a.size() < 4 || b.size() < 4 || !(a.front() == a.back()) || !(b.front() == b.back())) return false;
    for (size_t i = 1; i < a.size(); ++i) {
        for (const auto& point : {a[i], WkbCoordinate{(a[i - 1].x + a[i].x) / 2, (a[i - 1].y + a[i].y) / 2}}) {
            double distance = std::numeric_limits<double>::infinity();
            for (size_t j = 1; j < b.size(); ++j)
                distance = std::min(distance, segment_distance(point, b[j - 1], b[j]));
            if (distance > tolerance) return false;
        }
    }
    return true;
}
bool ring_match(const std::vector<WkbCoordinate>& a, const std::vector<WkbCoordinate>& b, double tolerance) {
    return directed_ring_match(a, b, tolerance) && directed_ring_match(b, a, tolerance);
}
bool polygon_match(const WkbGeometry& a, const WkbGeometry& b, double tolerance) {
    if (a.rings.size() != b.rings.size() || !ring_match(a.rings.front(), b.rings.front(), tolerance)) return false;
    std::vector<bool> matched(b.rings.size(), false);
    for (size_t i = 1; i < a.rings.size(); ++i) {
        bool found = false;
        for (size_t j = 1; j < b.rings.size(); ++j) {
            if (!matched[j] && ring_match(a.rings[i], b.rings[j], tolerance)) {
                matched[j] = found = true;
                break;
            }
        }
        if (!found) return false;
    }
    return true;
}
void expect_geometry(const ColumnPtr& output, const char* expected_wkt, double tolerance = 1e-12) {
    WkbGeometry expected;
    ASSERT_TRUE(WkbCodec::parse_wkt(expected_wkt, &expected, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    auto actual = decoded(output);
    ASSERT_EQ(expected.type, actual.type);
    ASSERT_EQ(expected.empty, actual.empty);
    const auto a = polygons(actual), b = polygons(expected);
    ASSERT_EQ(a.size(), b.size());
    std::vector<bool> matched(b.size(), false);
    for (const auto* polygon : a) {
        bool found = false;
        for (size_t j = 0; j < b.size(); ++j) {
            if (!matched[j] && polygon_match(*polygon, *b[j], tolerance)) {
                matched[j] = found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "No matching expected component";
    }
    auto validity = GeoFunctions::st_geometry_is_valid(nullptr, {output});
    ASSERT_TRUE(validity.ok()) << validity.status();
    EXPECT_TRUE(ColumnViewer<TYPE_BOOLEAN>(*validity).value(0));
}
ColumnPtr run(OverlayFunction function, const char* a, const char* b) {
    auto ctx = context();
    auto result = function(ctx.get(), {geometries({a}), geometries({b})});
    EXPECT_TRUE(result.ok()) << result.status();
    return result.ok() ? *result : ColumnHelper::create_const_null_column(1);
}
std::string multi(const char* polygon) {
    std::string value(polygon);
    return "MULTIPOLYGON (" + value.substr(value.find('(')) + ")";
}
} // namespace

TEST(GeoOverlayTest, AnalyticOverlapAndAllFamilyPairs) {
    const char* expected[] = {"POLYGON ((0 0,6 0,6 4,0 4,0 0))", "POLYGON ((0 0,2 0,2 4,0 4,0 0))",
                              "MULTIPOLYGON (((0 0,2 0,2 4,0 4,0 0)),((4 0,6 0,6 4,4 4,4 0)))"};
    for (size_t op = 0; op < 3; ++op) {
        for (bool a_multi : {false, true})
            for (bool b_multi : {false, true}) {
                const auto a = a_multi ? multi(square) : square, b = b_multi ? multi(shifted) : shifted;
                expect_geometry(run(functions[op], a.c_str(), b.c_str()), expected[op]);
            }
    }
    expect_geometry(run(functions[1], shifted, square), "POLYGON ((4 0,6 0,6 4,4 4,4 0))");
    expect_geometry(run(functions[0], shifted, square), expected[0]);
    expect_geometry(run(functions[2], shifted, square), expected[2]);
}

TEST(GeoOverlayTest, IdenticalDisjointTouchingAndEmpty) {
    const char* disjoint = "POLYGON ((8 0,9 0,9 1,8 1,8 0))";
    const char* combined = "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((8 0,9 0,9 1,8 1,8 0)))";
    expect_geometry(run(functions[0], square, square), square);
    expect_geometry(run(functions[1], square, square), empty);
    expect_geometry(run(functions[2], square, square), empty);
    for (auto fn : {functions[0], functions[2]}) expect_geometry(run(fn, square, disjoint), combined);
    expect_geometry(run(functions[1], square, disjoint), square);
    const char* adjacent = "POLYGON ((4 0,8 0,8 4,4 4,4 0))";
    for (auto fn : {functions[0], functions[2]}) {
        expect_geometry(run(fn, square, adjacent), "POLYGON ((0 0,8 0,8 4,0 4,0 0))");
    }
    expect_geometry(run(functions[1], square, adjacent), square);
    const char* point_touch = "POLYGON ((4 4,5 4,5 5,4 5,4 4))";
    expect_geometry(run(functions[0], square, point_touch),
                    "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((4 4,5 4,5 5,4 5,4 4)))");
    for (auto fn : functions) {
        expect_geometry(run(fn, square, empty), square);
        expect_geometry(run(fn, empty, empty), empty);
        expect_geometry(run(fn, empty, square), fn == functions[1] ? empty : square);
    }
}

TEST(GeoOverlayTest, HolesAndTrueMultiComponents) {
    const char* inner = "POLYGON ((1 1,3 1,3 3,1 3,1 1))";
    const char* donut = "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,1 3,3 3,3 1,1 1))";
    for (auto fn : {functions[1], functions[2]}) expect_geometry(run(fn, square, inner), donut);
    expect_geometry(run(functions[1], inner, square), empty);
    expect_geometry(run(functions[0], donut, inner), square);
    expect_geometry(run(functions[1], donut, inner), donut);
    const char* multipart = "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((8 0,9 0,9 1,8 1,8 0)))";
    expect_geometry(run(functions[1], multipart, inner),
                    "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0),(1 1,1 3,3 3,3 1,1 1)),((8 0,9 0,9 1,8 1,8 0)))");
    expect_geometry(run(functions[1], square, "MULTIPOLYGON (((1 1,2 1,2 2,1 2,1 1)),((3 1,3.5 1,3.5 2,3 2,3 1)))"),
                    "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,1 2,2 2,2 1,1 1),(3 1,3 2,3.5 2,3.5 1,3 1))");
}

TEST(GeoOverlayTest, PreservesNarrowGapsSliversAndLargeCoordinates) {
    // A gap much smaller than an integer rescale grid, but representable in double.
    expect_geometry(run(functions[0], "POLYGON ((0 0,1 0,1 1,0 1,0 0))",
                        "POLYGON ((1.0000000001 0,2 0,2 1,1.0000000001 1,1.0000000001 0))"),
                    "MULTIPOLYGON (((0 0,1 0,1 1,0 1,0 0)),"
                    "((1.0000000001 0,2 0,2 1,1.0000000001 1,1.0000000001 0)))",
                    1e-14);
    expect_geometry(run(functions[1], square, "POLYGON ((0.0000000001 0,4 0,4 4,0.0000000001 4,0.0000000001 0))"),
                    "POLYGON ((0 0,0.0000000001 0,0.0000000001 4,0 4,0 0))", 1e-14);
    const char* a =
            "POLYGON ((10000000 10000000,10000000.1 10000000,10000000.1 10000000.1,"
            "10000000 10000000.1,10000000 10000000))";
    const char* b =
            "POLYGON ((10000000.05 10000000,10000000.15 10000000,10000000.15 10000000.1,"
            "10000000.05 10000000.1,10000000.05 10000000))";
    expect_geometry(run(functions[0], empty, a), a, 2e-9);
    expect_geometry(run(functions[0], a, b),
                    "POLYGON ((10000000 10000000,10000000.15 10000000,10000000.15 10000000.1,"
                    "10000000 10000000.1,10000000 10000000))",
                    2e-9);
    expect_geometry(run(functions[1], a, b),
                    "POLYGON ((10000000 10000000,10000000.05 10000000,10000000.05 10000000.1,"
                    "10000000 10000000.1,10000000 10000000))",
                    2e-9);
}

TEST(GeoOverlayTest, RejectsTopologyLossFromCommonOriginTranslation) {
    const char* remote =
            "POLYGON ((100000000000000000000 0,100000000000000100000 0,"
            "100000000000000100000 100000,100000000000000000000 100000,100000000000000000000 0))";
    const char* combined =
            "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),"
            "((100000000000000000000 0,100000000000000100000 0,100000000000000100000 100000,"
            "100000000000000000000 100000,100000000000000000000 0)))";
    // Both operands are valid independently; translating the small one by -1e20
    // collapses its X coordinates even in the extended-precision working model.
    expect_geometry(run(functions[0], remote, empty), remote);
    expect_geometry(run(functions[0], square, empty), square);
    for (size_t op = 0; op < 3; ++op) {
        auto ctx = context();
        auto result = functions[op](ctx.get(), {geometries({remote}), geometries({square})});
        ASSERT_FALSE(result.ok());
        EXPECT_TRUE(result.status().is_invalid_argument());
        EXPECT_NE(std::string::npos, result.status().to_string().find("loses topology during coordinate translation"));
        // The reverse order does not collapse either model and preserves the
        // two disjoint components for union and symmetric difference.
        expect_geometry(run(functions[op], square, remote), op == 1 ? square : combined);
    }
}

TEST(GeoOverlayTest, RejectsTranslatedCollapsedHolesAndMultiComponents) {
    const char* remote =
            "POLYGON ((100000000000000000000 0,100000000000000100000 0,"
            "100000000000000100000 100000,100000000000000000000 100000,100000000000000000000 0))";
    const char* with_hole = "POLYGON ((0 0,32 0,32 32,0 32,0 0),(1 1,1 3,3 3,3 1,1 1))";
    const char* multipart = "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((32 0,64 0,64 32,32 32,32 0)))";
    for (const char* value : {with_hole, multipart}) {
        expect_geometry(run(functions[0], value, empty), value);
        for (auto fn : functions) {
            auto ctx = context();
            auto result = fn(ctx.get(), {geometries({remote}), geometries({value})});
            ASSERT_FALSE(result.ok());
            EXPECT_TRUE(result.status().is_invalid_argument());
            EXPECT_NE(std::string::npos,
                      result.status().to_string().find("loses topology during coordinate translation"));
        }
    }
}

TEST(GeoOverlayTest, RejectsInvalidTopologyAndUnsupportedInputsBeforeEmptyShortcut) {
    const char* invalid[] = {"POLYGON ((0 0,4 4,0 4,4 0,0 0))",
                             "POLYGON ((0 0,4 0,4 4,0 4,0 0),(8 8,9 8,9 9,8 9,8 8))",
                             "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1),(1.5 1.5,2 1.5,2 2,1.5 2,1.5 1.5))",
                             "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((2 0,6 0,6 4,2 4,2 0)))",
                             "POINT EMPTY",
                             "LINESTRING (0 0,1 1)",
                             "GEOMETRYCOLLECTION EMPTY"};
    for (auto fn : functions) {
        auto ctx = context();
        for (const char* value : invalid) {
            auto result = fn(ctx.get(), {geometries({value}), geometries({empty})});
            ASSERT_FALSE(result.ok()) << value;
            EXPECT_TRUE(result.status().is_invalid_argument()) << result.status();
        }
        auto malformed = fn(ctx.get(), {raw_geometry("not WKB"), geometries({square})});
        ASSERT_FALSE(malformed.ok());
        EXPECT_TRUE(malformed.status().is_invalid_argument());
        auto mismatch = fn(ctx.get(), {geometries({square}), geometries({square}, geo_type("EPSG:4326"))});
        ASSERT_FALSE(mismatch.ok());
        EXPECT_TRUE(mismatch.status().is_invalid_argument());
    }
}

TEST(GeoOverlayTest, NullPropagationAndDeferredConstantError) {
    for (auto fn : functions) {
        auto ctx = context();
        auto result = fn(ctx.get(), {geometries({nullptr, square}), geometries({square, nullptr})});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE((*result)->is_null(0));
        EXPECT_TRUE((*result)->is_null(1));
        auto bad = ConstColumn::create(geometries({"POINT (0 0)"}), 1);
        ctx->set_constant_columns({bad, nullptr});
        ASSERT_TRUE(GeoFunctions::native_geo_overlay_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
        auto skipped = fn(ctx.get(), {bad, ColumnHelper::create_const_null_column(1)});
        ASSERT_TRUE(skipped.ok()) << skipped.status();
        EXPECT_TRUE((*skipped)->only_null());
        EXPECT_FALSE(fn(ctx.get(), {bad, geometries({square})}).ok());
        ASSERT_TRUE(GeoFunctions::native_geo_overlay_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
        ctx->set_constant_columns({ColumnHelper::create_const_null_column(1), nullptr});
        ASSERT_TRUE(GeoFunctions::native_geo_overlay_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
        EXPECT_TRUE(
                fn(ctx.get(), {ColumnHelper::create_const_null_column(1), geometries({square})})->get()->only_null());
        ASSERT_TRUE(GeoFunctions::native_geo_overlay_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    }
}

TEST(GeoOverlayTest, PreparedConstantsAreImmutableAndOtherConstantsStayBatchLocal) {
    for (auto fn : functions)
        for (size_t slot : {0, 1}) {
            auto ctx = context();
            auto fixed = ConstColumn::create(geometries({square}), 1);
            ctx->set_constant_columns(slot == 0 ? Columns{fixed, nullptr} : Columns{nullptr, fixed});
            ASSERT_TRUE(GeoFunctions::native_geo_overlay_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
            const void* prepared = ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL);
            ASSERT_NE(nullptr, prepared);
            for (const char* value : {shifted, empty, "MULTIPOLYGON (((8 0,9 0,9 1,8 1,8 0)))"}) {
                auto changing = ConstColumn::create(geometries({value}), 1);
                Columns arguments = slot == 0 ? Columns{fixed, changing} : Columns{changing, fixed};
                auto result = fn(ctx.get(), arguments);
                auto plain_ctx = context();
                auto plain = fn(plain_ctx.get(), arguments);
                ASSERT_TRUE(result.ok()) << result.status();
                ASSERT_TRUE(plain.ok()) << plain.status();
                auto text = GeoFunctions::st_geometry_as_text(nullptr, {*plain});
                ASSERT_TRUE(text.ok());
                const auto expected = ColumnHelper::get_const_value<TYPE_VARCHAR>(*text).to_string();
                expect_geometry(*result, expected.c_str());
                EXPECT_EQ(prepared, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
            }
            std::atomic<bool> passed = true;
            std::vector<std::thread> workers;
            for (size_t worker = 0; worker < 4; ++worker)
                workers.emplace_back([&, worker] {
                    for (size_t i = 0; i < 10; ++i) {
                        const double offset = worker + 1;
                        const auto value = "POLYGON ((" + std::to_string(offset) + " 0," + std::to_string(offset + 4) +
                                           " 0," + std::to_string(offset + 4) + " 4," + std::to_string(offset) + " 4," +
                                           std::to_string(offset) + " 0))";
                        auto row = geometries({value.c_str()});
                        auto result = fn(ctx.get(), slot == 0 ? Columns{fixed, row} : Columns{row, fixed});
                        if (!result.ok() || (*result)->size() != 1) {
                            passed = false;
                            continue;
                        }
                        auto area = GeoFunctions::st_geometry_area(nullptr, {*result});
                        const double expected =
                                fn == functions[0] ? 16 + 4 * offset : fn == functions[1] ? 4 * offset : 8 * offset;
                        if (!area.ok() || std::abs(ColumnViewer<TYPE_DOUBLE>(*area).value(0) - expected) > 1e-10) {
                            passed = false;
                        }
                    }
                });
            for (auto& worker : workers) worker.join();
            EXPECT_TRUE(passed);
            ASSERT_TRUE(GeoFunctions::native_geo_overlay_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
            EXPECT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        }
}

TEST(GeoOverlayTest, RegistryUsesPrepareAndCloseAndReservesGeography) {
    for (uint64_t id : {120341, 120351, 120361, 120371}) {
        const auto* descriptor = BuiltinFunctions::find_builtin_function(id);
        ASSERT_NE(nullptr, descriptor);
        EXPECT_EQ(2, descriptor->args_nums);
        ASSERT_TRUE(descriptor->prepare_function);
        ASSERT_TRUE(descriptor->close_function);
        auto ctx = context();
        auto a = ConstColumn::create(geometries({square}), 1);
        ctx->set_constant_columns({a, nullptr});
        ASSERT_TRUE(descriptor->prepare_function(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
        EXPECT_NE(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        EXPECT_TRUE(descriptor->scalar_function(ctx.get(), {a, geometries({shifted})}).ok());
        ASSERT_TRUE(descriptor->close_function(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
        EXPECT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    }
    for (uint64_t id : {120340, 120350, 120360, 120370}) {
        EXPECT_EQ(nullptr, BuiltinFunctions::find_builtin_function(id));
    }
}

TEST(GeoOverlayTest, CancellationAndQueryMemoryLimit) {
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    state.init_mem_trackers(TUniqueId());
    ASSERT_NE(nullptr, state.runtime_profile());
    ASSERT_NE(nullptr, state.query_mem_tracker_ptr());
    ASSERT_EQ(state.query_mem_tracker_ptr().get(), state.instance_mem_tracker()->parent());
    auto type = geo_type();
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_context(&state, nullptr, type, {type, type}));
    auto a = geometries({square}), b = geometries({shifted});
    state.set_is_cancelled(true);
    for (auto fn : {functions[0], functions[1], functions[2], GeoFunctions::st_geometry_intersection}) {
        auto result = fn(ctx.get(), {a, b});
        ASSERT_FALSE(result.ok());
        EXPECT_TRUE(result.status().is_cancelled());
    }
    state.set_is_cancelled(false);
    ASSERT_TRUE(state.set_mem_limit_exceeded(state.instance_mem_tracker(), 1, "overlay test").is_mem_limit_exceeded());
    for (auto fn : {functions[0], functions[1], functions[2], GeoFunctions::st_geometry_intersection}) {
        auto result = fn(ctx.get(), {a, b});
        ASSERT_FALSE(result.ok());
        EXPECT_TRUE(result.status().is_mem_limit_exceeded());
    }
}

TEST(GeoOverlayTest, CoordinateAndPairWorkLimits) {
    WkbGeometry polygon;
    polygon.type = WkbGeometryType::POLYGON;
    auto& ring = polygon.rings.emplace_back();
    for (size_t i = 0; i < 4000; ++i) {
        const double angle = 2 * 3.14159265358979323846 * i / 4000;
        ring.push_back({std::cos(angle), std::sin(angle)});
    }
    ring.push_back(ring.front());
    std::string wkb;
    ASSERT_TRUE(WkbCodec::to_wkb(polygon, &wkb, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    auto prepared = PreparedGeoPolygon::prepare(Slice(wkb));
    ASSERT_TRUE(prepared.ok()) << prepared.status();
    auto result = (*prepared)->overlay(**prepared, GeoOverlayKind::UNION);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_invalid_argument());
    std::string oversized(256 * 1024 + 1, 'x');
    EXPECT_FALSE(PreparedGeoPolygon::prepare(Slice(oversized)).ok());
}

TEST(GeoOverlayTest, MultiComponentsAndTinyHoles) {
    const char* a = "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((8 0,9 0,9 1,8 1,8 0)))";
    const char* b = "MULTIPOLYGON (((2 0,6 0,6 4,2 4,2 0)),((10 0,11 0,11 1,10 1,10 0)))";
    expect_geometry(run(functions[0], a, b),
                    "MULTIPOLYGON (((0 0,6 0,6 4,0 4,0 0)),((8 0,9 0,9 1,8 1,8 0)),((10 0,11 0,11 1,10 1,10 0)))");
    expect_geometry(run(functions[2], a, b),
                    "MULTIPOLYGON (((0 0,2 0,2 4,0 4,0 0)),((4 0,6 0,6 4,4 4,4 0)),"
                    "((8 0,9 0,9 1,8 1,8 0)),((10 0,11 0,11 1,10 1,10 0)))");
    expect_geometry(run(functions[1], square, "POLYGON ((1 1,1.0000000001 1,1.0000000001 2,1 2,1 1))"),
                    "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,1 2,1.0000000001 2,1.0000000001 1,1 1))", 1e-14);
    expect_geometry(run(functions[0], "POLYGON ((0 0,1 0,1 1,0 1,0 0))", "POLYGON ((1 0,2 0,2 1.0000000001,1 1,1 0))"),
                    "POLYGON ((0 0,2 0,2 1.0000000001,1 1,0 1,0 0))", 1e-14);
}

TEST(GeoOverlayTest, RejectsMalformedNonFiniteDimensionsAndReturnDescriptor) {
    WkbGeometry polygon;
    ASSERT_TRUE(WkbCodec::parse_wkt(square, &polygon, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    std::string wkb;
    ASSERT_TRUE(WkbCodec::to_wkb(polygon, &wkb, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    std::vector<std::string> malformed{wkb.substr(0, wkb.size() - 1)};
    auto open_ring = wkb;
    const double unclosed = 2;
    std::memcpy(open_ring.data() + open_ring.size() - 16, &unclosed, sizeof(unclosed));
    malformed.push_back(open_ring);
    auto infinity = wkb;
    const double nonfinite = std::numeric_limits<double>::infinity();
    std::memcpy(infinity.data() + 13, &nonfinite, sizeof(nonfinite));
    malformed.push_back(infinity);
    auto z = wkb;
    const uint32_t z_type = 1003;
    std::memcpy(z.data() + 1, &z_type, sizeof(z_type));
    malformed.push_back(z);
    auto ctx = context();
    for (auto fn : functions) {
        for (const auto& payload : malformed) {
            auto result = fn(ctx.get(), {raw_geometry(payload), geometries({empty})});
            ASSERT_FALSE(result.ok());
        }
        auto bad_return = context(geo_type("EPSG:4326"));
        EXPECT_FALSE(fn(bad_return.get(), {geometries({square}), geometries({square})}).ok());
        auto wrong_kind = GeoColumn::create(
                GeoColumnDescriptor{{GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_COORDINATE_SYSTEM_SPHERICAL,
                                     GEO_EDGE_ALGORITHM_SPHERICAL, "OGC:CRS84", 4326},
                                    {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_STRUCTURALLY_VALIDATED}});
        wrong_kind->append_datum(Datum(Slice(wkb)));
        EXPECT_FALSE(fn(ctx.get(), {wrong_kind, geometries({square})}).ok());
    }
}

TEST(GeoOverlayTest, PreparedConstantWorksAcrossNullableBatches) {
    auto fixed = ConstColumn::create(geometries({square}), 2);
    for (auto fn : functions)
        for (size_t slot : {0, 1}) {
            auto ctx = context();
            ctx->set_constant_columns(slot == 0 ? Columns{fixed, nullptr} : Columns{nullptr, fixed});
            ASSERT_TRUE(GeoFunctions::native_geo_overlay_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
            for (auto batch : {geometries({nullptr, shifted}), geometries({empty, nullptr})}) {
                auto result = fn(ctx.get(), slot == 0 ? Columns{fixed, batch} : Columns{batch, fixed});
                ASSERT_TRUE(result.ok()) << result.status();
                ASSERT_EQ(2, (*result)->size());
                if (batch->is_null(0)) {
                    EXPECT_TRUE((*result)->is_null(0));
                    EXPECT_FALSE((*result)->is_null(1));
                    auto area = GeoFunctions::st_geometry_area(nullptr, {*result});
                    ASSERT_TRUE(area.ok());
                    EXPECT_DOUBLE_EQ(fn == functions[0] ? 24 : fn == functions[1] ? 8 : 16,
                                     ColumnViewer<TYPE_DOUBLE>(*area).value(1));
                } else {
                    EXPECT_FALSE((*result)->is_null(0));
                    EXPECT_TRUE((*result)->is_null(1));
                }
            }
            ASSERT_TRUE(GeoFunctions::native_geo_overlay_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
        }
}

TEST(GeoOverlayTest, OutputCoordinateLimitDoesNotTruncate) {
    auto stripes = [](bool vertical) {
        WkbGeometry geometry;
        geometry.type = WkbGeometryType::MULTIPOLYGON;
        for (size_t i = 0; i < 40; ++i) {
            WkbGeometry polygon;
            polygon.type = WkbGeometryType::POLYGON;
            const double start = i;
            if (vertical)
                polygon.rings = {{{start, 0}, {start + 0.4, 0}, {start + 0.4, 40}, {start, 40}, {start, 0}}};
            else
                polygon.rings = {{{0, start}, {40, start}, {40, start + 0.4}, {0, start + 0.4}, {0, start}}};
            geometry.children.push_back(std::move(polygon));
        }
        std::string wkb;
        EXPECT_TRUE(WkbCodec::to_wkb(geometry, &wkb, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
        return PreparedGeoPolygon::prepare(Slice(wkb));
    };
    auto a = stripes(true), b = stripes(false);
    ASSERT_TRUE(a.ok()) << a.status();
    ASSERT_TRUE(b.ok()) << b.status();
    auto result = (*a)->overlay(**b, GeoOverlayKind::UNION);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_invalid_argument());
    EXPECT_NE(std::string::npos, result.status().to_string().find("result exceeds coordinate limit"));
    auto intersection = (*a)->overlay(**b, GeoOverlayKind::INTERSECTION);
    ASSERT_FALSE(intersection.ok());
    EXPECT_NE(std::string::npos, intersection.status().to_string().find("result exceeds coordinate limit"));
    auto symmetric = (*a)->overlay(**b, GeoOverlayKind::SYMMETRIC_DIFFERENCE);
    ASSERT_FALSE(symmetric.ok());
    EXPECT_NE(std::string::npos, symmetric.status().to_string().find("intermediate exceeds coordinate limit"));
}

TEST(GeoOverlayTest, ExpressionOwnerAndCloneShareImmutablePreparationAndCleanUpOnce) {
    for (uint64_t id : {120341, 120351}) {
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
        name.__set_function_name(id == 120341 ? "ST_Intersection" : "ST_Union");
        function.__set_name(name);
        function.__set_binary_type(TFunctionBinaryType::BUILTIN);
        function.__set_fid(id);
        function.__set_arg_types({geo_type().to_thrift(), geo_type().to_thrift()});
        function.__set_ret_type(geo_type().to_thrift());
        function.__set_has_var_args(false);
        node.__set_fn(function);
        VectorizedFunctionCallExpr expr(node);
        TExprNode operand_node;
        operand_node.__set_node_type(TExprNodeType::SLOT_REF);
        operand_node.__set_type(geo_type().to_thrift());
        operand_node.__set_num_children(0);
        operand_node.__set_is_nullable(true);
        Operand left(operand_node, ConstColumn::create(geometries({square}), 1), true);
        Operand right(operand_node, geometries({shifted}), false);
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
        expect_geometry(*result, id == 120341 ? "POLYGON ((2 0,4 0,4 4,2 4,2 0))" : "POLYGON ((0 0,6 0,6 4,0 4,0 0))");
        clone->close(&state);
        EXPECT_EQ(prepared, owner.fn_context(0)->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        right.next = geometries({empty});
        auto second = expr.evaluate_checked(&owner, nullptr);
        ASSERT_TRUE(second.ok()) << second.status();
        expect_geometry(*second, id == 120341 ? empty : square);
        owner.close(&state);
        EXPECT_EQ(nullptr, owner.fn_context(0)->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        owner.close(&state);
    }
}

TEST(GeoOverlayTest, SymmetricDifferenceChecksCancellationBetweenPrimitives) {
    auto a = geometries({square}), b = geometries({shifted});
    auto wkb_a = GeoFunctions::st_geometry_as_wkb(nullptr, {a});
    auto wkb_b = GeoFunctions::st_geometry_as_wkb(nullptr, {b});
    ASSERT_TRUE(wkb_a.ok());
    ASSERT_TRUE(wkb_b.ok());
    auto left = PreparedGeoPolygon::prepare(ColumnViewer<TYPE_VARBINARY>(*wkb_a).value(0));
    auto right = PreparedGeoPolygon::prepare(ColumnViewer<TYPE_VARBINARY>(*wkb_b).value(0));
    ASSERT_TRUE(left.ok());
    ASSERT_TRUE(right.ok());
    size_t checkpoints = 0;
    // Entry and post-translation checks precede the first difference.
    auto result = (*left)->overlay(**right, GeoOverlayKind::SYMMETRIC_DIFFERENCE, [&]() {
        return ++checkpoints == 3 ? Status::Cancelled("cancel after first difference") : Status::OK();
    });
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_cancelled());
    EXPECT_EQ(3, checkpoints);
}

TEST(GeoOverlayTest, IntersectionReturnsAreaLinePointAndEmptyWithInputCrs) {
    auto fn = GeoFunctions::st_geometry_intersection;
    auto ctx = context();
    expect_geometry(run(fn, square, shifted), "POLYGON ((2 0,4 0,4 4,2 4,2 0))");
    expect_geometry(run(fn, square, empty), empty);
    auto edge = run(fn, square, "POLYGON ((4 0,8 0,8 4,4 4,4 0))");
    auto line = decoded(edge);
    ASSERT_EQ(WkbGeometryType::LINESTRING, line.type);
    ASSERT_EQ(2, line.coordinates.size());
    EXPECT_EQ(4, line.coordinates[0].x);
    EXPECT_EQ(4, line.coordinates[1].x);
    EXPECT_EQ(4, std::abs(line.coordinates[1].y - line.coordinates[0].y));
    auto point = run(fn, square, "POLYGON ((4 4,5 4,5 5,4 5,4 4))");
    ASSERT_EQ(WkbGeometryType::POINT, decoded(point).type);
    EXPECT_EQ((std::vector<WkbCoordinate>{{4, 4}}), decoded(point).coordinates);
    for (const auto& type : {geo_type(), geo_type("EPSG:4326")}) {
        auto typed_ctx = context(type);
        auto result = fn(typed_ctx.get(), {geometries({square}, type), geometries({shifted}, type)});
        ASSERT_TRUE(result.ok()) << result.status();
        const auto* data = down_cast<const GeoColumn*>(ColumnHelper::get_data_column(*result));
        EXPECT_EQ(type.geo_type.value(), data->descriptor().type);
        EXPECT_EQ(GEO_DIMENSION_XY, data->descriptor().storage.dimension);
    }
}
TEST(GeoOverlayTest, IntersectionCrossingsRemainPolygonForDownstreamOverlays) {
    const char* a = "POLYGON ((0 0,8 -7,6 2,0 0))";
    const char* b = "POLYGON ((-2 0,0 0,8 2,-2 0))";
    const char* expected =
            "POLYGON ((3 1,0 0,6.105263157894737 1.5263157894736843,"
            "6.085106382978723 1.6170212765957446,3 1))";
    for (bool reverse : {false, true})
        for (bool prepared : {false, true}) {
            auto ctx = context();
            Columns inputs{geometries({reverse ? b : a}), geometries({reverse ? a : b})};
            if (prepared) {
                for (auto& column : inputs) column = ConstColumn::create(column, 1);
                ctx->set_constant_columns(inputs);
                ASSERT_TRUE(GeoFunctions::native_geo_overlay_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
            }
            auto result = GeoFunctions::st_geometry_intersection(ctx.get(), inputs);
            ASSERT_TRUE(result.ok()) << result.status();
            expect_geometry(*result, expected);
            auto union_ctx = context();
            auto united = GeoFunctions::st_geometry_union(union_ctx.get(), {*result, geometries({empty})});
            EXPECT_TRUE(united.ok()) << united.status();
            if (united.ok()) expect_geometry(*united, expected);
            if (prepared) {
                ASSERT_TRUE(GeoFunctions::native_geo_overlay_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
            }
        }
}
TEST(GeoOverlayTest, IntersectionMixedCollectionFeedsMeasurements) {
    auto result =
            run(GeoFunctions::st_geometry_intersection,
                "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((10 0,12 0,12 2,10 2,10 0)),((20 0,22 0,22 2,20 2,20 0)))",
                "MULTIPOLYGON (((2 0,6 0,6 4,2 4,2 0)),((12 0,14 0,14 2,12 2,12 0)),((22 2,24 2,24 4,22 4,22 2)))");
    ASSERT_EQ(WkbGeometryType::GEOMETRYCOLLECTION, decoded(result).type);
    ASSERT_EQ(3, decoded(result).children.size());
    auto area = GeoFunctions::st_geometry_area(nullptr, {result});
    auto length = GeoFunctions::st_geometry_length(nullptr, {result});
    auto valid = GeoFunctions::st_geometry_is_valid(nullptr, {result});
    ASSERT_TRUE(area.ok());
    ASSERT_TRUE(length.ok());
    ASSERT_TRUE(valid.ok());
    EXPECT_DOUBLE_EQ(8, ColumnViewer<TYPE_DOUBLE>(*area).value(0));
    EXPECT_DOUBLE_EQ(2, ColumnViewer<TYPE_DOUBLE>(*length).value(0));
    EXPECT_TRUE(ColumnViewer<TYPE_BOOLEAN>(*valid).value(0));
}
TEST(GeoOverlayTest, IntersectionPreparedConstantsAcrossNullableAndBothConstantBatches) {
    auto fn = GeoFunctions::st_geometry_intersection;
    for (size_t side : {0, 1}) {
        auto ctx = context();
        auto fixed = ConstColumn::create(geometries({square}), 3);
        ctx->set_constant_columns(side == 0 ? Columns{fixed, nullptr} : Columns{nullptr, fixed});
        ASSERT_TRUE(GeoFunctions::native_geo_overlay_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
        auto batch = geometries({shifted, "POLYGON ((4 0,8 0,8 4,4 4,4 0))", nullptr});
        auto result = fn(ctx.get(), side == 0 ? Columns{fixed, batch} : Columns{batch, fixed});
        ASSERT_TRUE(result.ok()) << result.status();
        ASSERT_EQ(3, (*result)->size());
        EXPECT_TRUE((*result)->is_null(2));
        EXPECT_EQ(WkbGeometryType::POLYGON, decoded(*result, 0).type);
        EXPECT_EQ(WkbGeometryType::LINESTRING, decoded(*result, 1).type);
        auto moving = ConstColumn::create(geometries({shifted}), 3);
        auto constant = fn(ctx.get(), side == 0 ? Columns{fixed, moving} : Columns{moving, fixed});
        ASSERT_TRUE(constant.ok()) << constant.status();
        EXPECT_TRUE((*constant)->is_constant());
        EXPECT_EQ(3, (*constant)->size());
        expect_geometry(*constant, "POLYGON ((2 0,4 0,4 4,2 4,2 0))");
        ASSERT_TRUE(GeoFunctions::native_geo_overlay_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    }
}
TEST(GeoOverlayTest, IntersectionPreservesNullPolicyAndRejectsInvalidInputsAndCrs) {
    auto fn = GeoFunctions::st_geometry_intersection;
    auto ctx = context();
    auto rows = fn(ctx.get(), {geometries({nullptr, square}), geometries({square, nullptr})});
    ASSERT_TRUE(rows.ok());
    EXPECT_TRUE((*rows)->is_null(0));
    EXPECT_TRUE((*rows)->is_null(1));
    auto bad = ConstColumn::create(geometries({"POINT (0 0)"}), 1);
    ctx->set_constant_columns({bad, nullptr});
    ASSERT_TRUE(GeoFunctions::native_geo_overlay_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    auto skipped = fn(ctx.get(), {bad, ColumnHelper::create_const_null_column(1)});
    ASSERT_TRUE(skipped.ok());
    EXPECT_TRUE((*skipped)->only_null());
    EXPECT_FALSE(fn(ctx.get(), {bad, geometries({square})}).ok());
    ASSERT_TRUE(GeoFunctions::native_geo_overlay_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    for (const char* input : {"LINESTRING (0 0,1 1)", "GEOMETRYCOLLECTION EMPTY", "POLYGON ((0 0,4 4,0 4,4 0,0 0))"})
        EXPECT_FALSE(fn(ctx.get(), {geometries({input}), geometries({empty})}).ok());
    EXPECT_FALSE(fn(ctx.get(), {geometries({square}), geometries({square}, geo_type("EPSG:4326"))}).ok());
}
} // namespace starrocks

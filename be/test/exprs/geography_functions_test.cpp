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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <h3/h3api.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <random>
#include <thread>
#include <unordered_set>

#include "exprs/geo_functions.h"

#define BOOST_MATH_DISABLE_FLOAT128
#define BOOST_CSTDFLOAT_NO_LIBQUADMATH_SUPPORT
#include <boost/multiprecision/cpp_int.hpp>

#include "butil/time.h"
#include "column/column_viewer.h"
#include "column/geo_column.h"
#include "column/nullable_column.h"
#include "common/config_expr_fwd.h"
#include "exprs/builtin_functions.h"
#include "exprs/expr_context.h"
#include "exprs/function_call_expr.h"
#include "exprs/mock_vectorized_expr.h"
#include "geo/geo_measurements.h"
#include "geo/geo_types.h"
#include "geo/wkb.h"
#include "gutil/compiler_util.h"
#include "runtime/runtime_state.h"
#include "types/geo_wkb.h"

DECLARE_bool(s2debug);

namespace starrocks {

class geographyFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {}

    static TypeDescriptor geography_type() {
        return TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY,
                                               {GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_COORDINATE_SYSTEM_SPHERICAL,
                                                GEO_EDGE_ALGORITHM_SPHERICAL, "OGC:CRS84", 4326});
    }

    static TypeDescriptor geometry_type(std::string crs = "EPSG:3857", std::optional<int32_t> srid = 3857) {
        return TypeDescriptor::create_geo_type(
                TYPE_GEOMETRY, {GEO_LOGICAL_TYPE_GEOMETRY, GEO_COORDINATE_SYSTEM_CARTESIAN, GEO_EDGE_ALGORITHM_PLANAR,
                                std::move(crs), srid});
    }

    static ColumnPtr geography(std::initializer_list<const char*> values) {
        auto input = BinaryColumn::create();
        auto nulls = NullColumn::create();
        bool has_null = false;
        for (const char* value : values) {
            if (value == nullptr) {
                input->append_default();
                nulls->append(1);
                has_null = true;
            } else {
                input->append(value);
                nulls->append(0);
            }
        }
        ColumnPtr source = input;
        if (has_null) source = NullableColumn::create(input, nulls);
        std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context(
                {TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH)}, geography_type()));
        return GeoFunctions::st_geog_from_text(context.get(), {source}).value();
    }

    static ColumnPtr geometry(std::initializer_list<const char*> values, const TypeDescriptor& type = geometry_type()) {
        auto input = BinaryColumn::create();
        auto nulls = NullColumn::create();
        bool has_null = false;
        for (const char* value : values) {
            if (value == nullptr) {
                input->append_default();
                nulls->append(1);
                has_null = true;
            } else {
                input->append(value);
                nulls->append(0);
            }
        }
        ColumnPtr source = input;
        if (has_null) source = NullableColumn::create(input, nulls);
        auto crs = ColumnHelper::create_const_column<TYPE_VARCHAR>(type.geo_type->crs, source->size());
        std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context(
                {TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH),
                 TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH)},
                type));
        return GeoFunctions::st_geom_from_text(context.get(), {source, crs}).value();
    }
    static std::vector<std::string> geometry_point_wkb_cases() {
        auto encode = [](WkbCoordinate coordinate) {
            std::string bytes(21, '\0');
            bytes[0] = bytes[1] = 1;
            const std::array<double, 2> xy = {coordinate.x, coordinate.y};
            for (size_t axis = 0; axis < xy.size(); ++axis) {
                const uint64_t bits = std::bit_cast<uint64_t>(xy[axis]);
                for (size_t i = 0; i < sizeof(bits); ++i) {
                    bytes[5 + axis * sizeof(bits) + i] = static_cast<char>(bits >> (8 * i));
                }
            }
            return bytes;
        };
        const double tiny = std::numeric_limits<double>::denorm_min();
        const double huge = std::numeric_limits<double>::max();
        std::vector<WkbCoordinate> coordinates = {{0, 0},      {-0.0, 0.0}, {0.0, -0.0}, {-0.0, -0.0},  {180, 90},
                                                  {-180, -90}, {181, -91},  {1e7, -2e7}, {tiny, -tiny}, {huge, -huge}};
        std::mt19937_64 random(20261018);
        for (size_t count = 0; count < 256;) {
            const WkbCoordinate point{std::bit_cast<double>(random()), std::bit_cast<double>(random())};
            if (std::isfinite(point.x) && std::isfinite(point.y)) {
                coordinates.push_back(point);
                ++count;
            }
        }
        std::vector<std::string> values;
        for (const auto& point : coordinates) {
            auto bytes = encode(point);
            values.push_back(bytes);
            bytes[0] = 0;
            std::reverse(bytes.begin() + 1, bytes.begin() + 5);
            std::reverse(bytes.begin() + 5, bytes.begin() + 13);
            std::reverse(bytes.begin() + 13, bytes.end());
            values.push_back(bytes);
        }
        const double nan = std::bit_cast<double>(uint64_t{0xfff8000000001234});
        const double inf = std::numeric_limits<double>::infinity();
        for (const auto& point :
             {WkbCoordinate{nan, nan}, {nan, 0}, {0, nan}, {inf, 0}, {-inf, 0}, {0, inf}, {0, -inf}}) {
            values.push_back(encode(point));
        }
        const auto plain = encode({1, 2});
        for (uint32_t type : {0x20000001U, 0x40000001U, 0x80000001U, 1001U, 2001U, 3001U}) {
            auto bytes = plain;
            for (size_t i = 0; i < sizeof(type); ++i) bytes[1 + i] = static_cast<char>(type >> (8 * i));
            if (type == 0x20000001U) bytes.insert(5, "\xe6\x10\0\0", 4);
            values.push_back(bytes);
        }
        auto bad_order = plain;
        bad_order[0] = 2;
        values.insert(values.end(), {bad_order, plain.substr(0, 20), plain + '\0', std::string()});
        for (const char* wkt : {"POINT EMPTY", "LINESTRING (0 0, 1 1)", "POLYGON ((0 0, 1 0, 0 1, 0 0))"}) {
            values.push_back(geometry({wkt})->get(0).get_slice().to_string());
        }
        return values;
    }
};

TEST_F(geographyFunctionsTest, st_pointTest) {
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;

        auto str_1 = DoubleColumn::create();
        auto str_2 = DoubleColumn::create();

        str_1->append(24.7);
        str_2->append(56.7);
        columns.emplace_back(std::move(str_1));
        columns.emplace_back(std::move(str_2));
        ColumnPtr result = GeoFunctions::st_point(ctx.get(), columns).value();

        columns.clear();
        columns.emplace_back(std::move(result));
        result = GeoFunctions::st_as_wkt(ctx.get(), columns).value();

        auto v = ColumnHelper::as_column<BinaryColumn>(result);
        ASSERT_EQ("POINT (24.7 56.7)", v->get_slice(0).to_string());
    }

    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;

        auto str_1 = DoubleColumn::create();
        auto str_2 = DoubleColumn::create();

        str_1->append(24.7);
        str_2->append(56.7);
        columns.emplace_back(std::move(str_1));
        columns.emplace_back(std::move(str_2));
        ColumnPtr result = GeoFunctions::st_point(ctx.get(), columns).value();
        auto v = ColumnHelper::as_column<BinaryColumn>(result);
        auto str_value = v->get_slice(0);

        GeoPoint point;
        auto res = point.decode_from(str_value.data, str_value.size);
        ASSERT_TRUE(res);
        ASSERT_EQ(24.7, point.x());
        ASSERT_EQ(56.7, point.y());
    }
}

TEST_F(geographyFunctionsTest, st_xDoubleTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str_column = BinaryColumn::create();
    GeoPoint point;
    point.from_coord(134, 63);

    std::string buf;
    point.encode_to(&buf);
    str_column->append(Slice(buf));
    columns.emplace_back(std::move(str_column));

    auto result = GeoFunctions::st_x(ctx.get(), columns).value();
    auto v = ColumnHelper::as_column<DoubleColumn>(result);
    ASSERT_EQ(134, v->immutable_data()[0]);

    //}
}

TEST_F(geographyFunctionsTest, st_yDoubleTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str_column = BinaryColumn::create();
    GeoPoint point;
    point.from_coord(134, 63);

    std::string buf;
    point.encode_to(&buf);
    str_column->append(Slice(buf));
    columns.emplace_back(std::move(str_column));

    auto result = GeoFunctions::st_y(ctx.get(), columns).value();
    auto v = ColumnHelper::as_column<DoubleColumn>(result);
    ASSERT_EQ(63, v->immutable_data()[0]);
}

TEST_F(geographyFunctionsTest, st_distance_sphereTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto xlng = DoubleColumn::create();
    auto xlat = DoubleColumn::create();
    auto ylng = DoubleColumn::create();
    auto ylat = DoubleColumn::create();

    xlng->append(0.0);
    xlat->append(0.0);
    ylng->append(0.0);
    ylat->append(0.0);

    columns.emplace_back(std::move(xlng));
    columns.emplace_back(std::move(xlat));
    columns.emplace_back(std::move(ylng));
    columns.emplace_back(std::move(ylat));

    ColumnPtr result = GeoFunctions::st_distance_sphere(ctx.get(), columns).value();
    auto v = ColumnHelper::as_column<DoubleColumn>(result);

    ASSERT_EQ(0, v->immutable_data()[0]);
}

TEST_F(geographyFunctionsTest, as_wktTest) {
    GeoPoint point;
    point.from_coord(134, 63);

    std::string buf;
    point.encode_to(&buf);

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str_point = BinaryColumn::create();
    str_point->append(buf);
    columns.emplace_back(std::move(str_point));

    auto result = GeoFunctions::st_as_wkt(ctx.get(), columns).value();
    auto v = ColumnHelper::as_column<BinaryColumn>(result);

    ASSERT_EQ("POINT (134 63)", v->get_slice(0).to_string());
}

TEST_F(geographyFunctionsTest, st_from_wktGeneralTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    std::string wkt = "POINT (10.1 20.2)";
    auto wkt_column = BinaryColumn::create();
    wkt_column->append(wkt);

    columns.emplace_back(std::move(wkt_column));

    ctx->set_constant_columns(columns);

    GeoFunctions::st_from_wkt_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);

    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto result = GeoFunctions::st_from_wkt(ctx.get(), columns).value();
    GeoFunctions::st_from_wkt_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto v = ColumnHelper::as_column<BinaryColumn>(result);

    GeoPoint point;
    auto res = point.decode_from(v->get_slice(0).data, v->get_slice(0).size);

    ASSERT_TRUE(res);
    ASSERT_DOUBLE_EQ(10.1, point.x());
    ASSERT_DOUBLE_EQ(20.2, point.y());
}

TEST_F(geographyFunctionsTest, st_from_wktConstTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto wkt_column = ColumnHelper::create_const_column<TYPE_VARCHAR>("POINT (10.1 20.2)", 1);

    columns.emplace_back(std::move(wkt_column));

    ctx->set_constant_columns(columns);

    GeoFunctions::st_from_wkt_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);

    ASSERT_EQ(false, !ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto result = GeoFunctions::st_from_wkt(ctx.get(), columns).value();
    auto v = ColumnHelper::as_column<ConstColumn>(result);
    GeoFunctions::st_from_wkt_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);

    GeoPoint point;
    auto res = point.decode_from(v->get(0).get_slice().data, v->get(0).get_slice().size);

    ASSERT_TRUE(res);
    ASSERT_DOUBLE_EQ(10.1, point.x());
    ASSERT_DOUBLE_EQ(20.2, point.y());
}

TEST_F(geographyFunctionsTest, st_lineGeneralTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    std::string line = "LINESTRING (10.1 20.2, 21.1 30.1)";
    Columns columns;
    auto line_string = BinaryColumn::create();
    line_string->append(line);

    columns.emplace_back(std::move(line_string));
    ctx->set_constant_columns(columns);

    GeoFunctions::st_line_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto result = GeoFunctions::st_line(ctx.get(), columns).value();
    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    GeoLine line_obj;
    auto res = line_obj.decode_from(v->get_slice(0).data, v->get_slice(0).size);
    ASSERT_TRUE(res);
    GeoFunctions::st_from_wkt_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
}

TEST_F(geographyFunctionsTest, st_lineConstTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto line_string = ColumnHelper::create_const_column<TYPE_VARCHAR>("LINESTRING (10.1 20.2, 21.1 30.1)", 1);

    columns.emplace_back(std::move(line_string));
    ctx->set_constant_columns(columns);

    GeoFunctions::st_line_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    ColumnPtr result = GeoFunctions::st_line(ctx.get(), columns).value();
    auto v = ColumnHelper::get_const_value<TYPE_VARCHAR>(result);

    GeoLine line;
    auto res = line.decode_from(v.data, v.size);
    ASSERT_TRUE(res);
    GeoFunctions::st_from_wkt_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
}

TEST_F(geographyFunctionsTest, st_polygonGeneralTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    std::string wkt = "POLYGON ((10 10, 50 10, 50 50, 10 50, 10 10))";
    auto polygon_str = BinaryColumn::create();
    polygon_str->append(wkt);
    columns.emplace_back(std::move(polygon_str));

    ctx->set_constant_columns(columns);
    GeoFunctions::st_polygon_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto str2 = GeoFunctions::st_polygon(ctx.get(), columns).value();
    auto result = ColumnHelper::as_column<BinaryColumn>(str2);
    ASSERT_FALSE(str2->is_null(0));
    auto v = result->get_slice(0);

    GeoPolygon polygon;
    auto res = polygon.decode_from(v.data, v.size);
    ASSERT_TRUE(res);
    GeoFunctions::st_from_wkt_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
}

TEST_F(geographyFunctionsTest, st_polygonConstTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto const_polygon =
            ColumnHelper::create_const_column<TYPE_VARCHAR>("POLYGON ((10 10, 50 10, 50 50, 10 50, 10 10))", 1);
    columns.emplace_back(std::move(const_polygon));

    ctx->set_constant_columns(columns);
    GeoFunctions::st_polygon_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    ASSERT_NE(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto str2 = GeoFunctions::st_polygon(ctx.get(), columns).value();
    ColumnPtr result = ColumnHelper::as_column<ConstColumn>(str2);
    auto v = ColumnHelper::get_const_value<TYPE_VARCHAR>(result);

    GeoPolygon polygon;
    auto res = polygon.decode_from(v.data, v.size);
    ASSERT_TRUE(res);
    GeoFunctions::st_from_wkt_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
}

TEST_F(geographyFunctionsTest, st_circleGeneralTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto lng_column = DoubleColumn::create();
    lng_column->append(111);
    auto lat_column = DoubleColumn::create();
    lat_column->append(64);
    auto radius_column = DoubleColumn::create();
    radius_column->append(10 * 100);
    columns.emplace_back(std::move(lng_column));
    columns.emplace_back(std::move(lat_column));
    columns.emplace_back(std::move(radius_column));

    ctx->set_constant_columns(columns);
    GeoFunctions::st_circle_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto result = GeoFunctions::st_circle(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));

    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    auto cir_str = v->get_slice(0);

    GeoCircle circle;
    auto res = circle.decode_from(cir_str.data, cir_str.size);
    ASSERT_TRUE(res);
    GeoFunctions::st_from_wkt_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
}

TEST_F(geographyFunctionsTest, st_circleConstTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto lng_column = ColumnHelper::create_const_column<TYPE_DOUBLE>(111, 1);
    auto lat_column = ColumnHelper::create_const_column<TYPE_DOUBLE>(64, 1);
    auto radius_column = ColumnHelper::create_const_column<TYPE_DOUBLE>(10 * 100, 1);
    columns.emplace_back(std::move(lng_column));
    columns.emplace_back(std::move(lat_column));
    columns.emplace_back(std::move(radius_column));

    ctx->set_constant_columns(columns);
    GeoFunctions::st_circle_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    ASSERT_NE(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto result = GeoFunctions::st_circle(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));

    ColumnPtr v = ColumnHelper::as_column<ConstColumn>(result);
    auto cir_str = ColumnHelper::get_const_value<TYPE_VARCHAR>(v);

    GeoCircle circle;
    auto res = circle.decode_from(cir_str.data, cir_str.size);
    ASSERT_TRUE(res);
    GeoFunctions::st_from_wkt_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
}

TEST_F(geographyFunctionsTest, st_containsGeneralTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    std::string polygon_wkt = "POLYGON ((10 10, 50 10, 50 50, 10 50, 10 10))";
    auto polygon_column = BinaryColumn::create();
    polygon_column->append(polygon_wkt);
    columns.emplace_back(std::move(polygon_column));
    ctx->set_constant_columns(columns);
    GeoFunctions::st_from_wkt_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto result1 = GeoFunctions::st_from_wkt(ctx.get(), columns).value();

    columns.clear();
    std::string point_wkt = "POINT (25 25)";
    auto point_column = BinaryColumn::create();
    point_column->append(point_wkt);
    columns.emplace_back(std::move(point_column));
    ctx->set_constant_columns(columns);
    GeoFunctions::st_from_wkt_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto result2 = GeoFunctions::st_from_wkt(ctx.get(), columns).value();

    columns.clear();
    columns.emplace_back(std::move(result1));
    columns.emplace_back(std::move(result2));
    ctx->set_constant_columns(columns);
    GeoFunctions::st_contains_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);

    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto res = GeoFunctions::st_contains(ctx.get(), columns).value();
    auto bools = ColumnHelper::cast_to<TYPE_BOOLEAN>(res);
    ASSERT_FALSE(res->is_null(0));
    ASSERT_TRUE(bools->immutable_data()[0]);
    GeoFunctions::st_contains_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
}

TEST_F(geographyFunctionsTest, st_containsWithUnexpectedInputTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    std::string polygon_wkt = "POLYGON ((10 10, 50 10, 50 50, 10 50, 10 10))";
    auto polygon_column = BinaryColumn::create();
    polygon_column->append(polygon_wkt);
    columns.emplace_back(std::move(polygon_column));
    ctx->set_constant_columns(columns);
    GeoFunctions::st_from_wkt_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto result1 = GeoFunctions::st_from_wkt(ctx.get(), columns).value();

    columns.clear();
    std::string point_wkt = "POINT (25 25)";
    auto point_column = BinaryColumn::create();
    point_column->append(point_wkt);
    columns.emplace_back(std::move(point_column));
    ctx->set_constant_columns(columns);
    GeoFunctions::st_from_wkt_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto result2 = GeoFunctions::st_from_wkt(ctx.get(), columns).value();

    columns.clear();
    auto varchar_column = ColumnHelper::cast_to<TYPE_VARCHAR>(result1);
    ASSERT_TRUE(varchar_column->size() == 1);
    ASSERT_FALSE(varchar_column->is_null(0));
    auto value = varchar_column->get_slice(0);
    std::string string_value(value.get_data(), value.get_size());
    string_value.append("A");
    auto input_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(string_value), varchar_column->size());

    columns.emplace_back(std::move(input_column));
    columns.emplace_back(std::move(result2));
    ctx->set_constant_columns(columns);
    GeoFunctions::st_contains_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto res = GeoFunctions::st_contains(ctx.get(), columns).value();
    ASSERT_FALSE(res->only_null());
    GeoFunctions::st_contains_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
}

TEST_F(geographyFunctionsTest, st_containsConstTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    std::string polygon_wkt = "POLYGON ((10 10, 50 10, 50 50, 10 50, 10 10))";
    auto polygon_column = BinaryColumn::create();
    polygon_column->append(polygon_wkt);
    columns.emplace_back(std::move(polygon_column));
    ctx->set_constant_columns(columns);
    GeoFunctions::st_from_wkt_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto result1 = ConstColumn::create(GeoFunctions::st_from_wkt(ctx.get(), columns).value(), 1);

    columns.clear();
    std::string point_wkt = "POINT (25 25)";
    auto point_column = BinaryColumn::create();
    point_column->append(point_wkt);
    columns.emplace_back(std::move(point_column));
    ctx->set_constant_columns(columns);
    GeoFunctions::st_from_wkt_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto result2 = ConstColumn::create(GeoFunctions::st_from_wkt(ctx.get(), columns).value(), 1);

    columns.clear();
    columns.emplace_back(std::move(result1));
    columns.emplace_back(std::move(result2));
    ctx->set_constant_columns(columns);
    GeoFunctions::st_contains_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);

    ASSERT_NE(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto res = GeoFunctions::st_contains(ctx.get(), columns).value();
    ASSERT_TRUE(ColumnHelper::get_const_value<TYPE_BOOLEAN>(res));
    GeoFunctions::st_contains_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
}

TEST_F(geographyFunctionsTest, nativeGeographyWktAndWkbRoundTrip) {
    GeoTypeDescriptor descriptor{GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_COORDINATE_SYSTEM_SPHERICAL,
                                 GEO_EDGE_ALGORITHM_SPHERICAL, "OGC:CRS84", 4326};
    auto geography = TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, descriptor);
    std::unique_ptr<FunctionContext> constructor(FunctionContext::create_test_context(
            {TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH)}, geography));

    auto input = BinaryColumn::create();
    input->append("GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING (1 2, 3 4))");
    auto native = GeoFunctions::st_geog_from_text(constructor.get(), {input}).value();
    ASSERT_FALSE(native->is_null(0));

    std::unique_ptr<FunctionContext> text_serializer(FunctionContext::create_test_context(
            {geography}, TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH)));
    auto text = GeoFunctions::st_geography_as_text(text_serializer.get(), {native}).value();
    EXPECT_EQ("GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING (1 2, 3 4))", text->get(0).get_slice().to_string());

    std::unique_ptr<FunctionContext> wkb_serializer(FunctionContext::create_test_context(
            {geography}, TypeDescriptor::create_varbinary_type(TypeDescriptor::MAX_VARCHAR_LENGTH)));
    auto wkb = GeoFunctions::st_geography_as_wkb(wkb_serializer.get(), {native}).value();
    std::unique_ptr<FunctionContext> wkb_constructor(FunctionContext::create_test_context(
            {TypeDescriptor::create_varbinary_type(TypeDescriptor::MAX_VARCHAR_LENGTH)}, geography));
    auto reconstructed = GeoFunctions::st_geog_from_wkb(wkb_constructor.get(), {wkb}).value();
    auto reconstructed_text = GeoFunctions::st_geography_as_text(text_serializer.get(), {reconstructed}).value();
    EXPECT_EQ(text->get(0).get_slice().to_string(), reconstructed_text->get(0).get_slice().to_string());
}

TEST_F(geographyFunctionsTest, nativeGeographyPointWkbConstructorPreservesCanonicalBytes) {
    const double tiny = std::numeric_limits<double>::denorm_min();
    const std::array<WkbCoordinate, 9> coordinates = {{{0, 0},
                                                       {-0.0, 0.0},
                                                       {0.0, -0.0},
                                                       {-0.0, -0.0},
                                                       {180, 90},
                                                       {-180, -90},
                                                       {37.62, 55.75},
                                                       {-179.999999, 0.000001},
                                                       {tiny, -tiny}}};
    auto input = BinaryColumn::create();
    std::vector<std::string> expected;
    for (const auto& coordinate : coordinates) {
        WkbGeometry point;
        point.type = WkbGeometryType::POINT;
        point.coordinates.push_back(coordinate);
        std::string bytes;
        ASSERT_TRUE(WkbCodec::to_wkb(point, &bytes).ok());
        expected.push_back(bytes);
        input->append(Slice(bytes));
    }
    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context(
            {TypeDescriptor::create_varbinary_type(TypeDescriptor::MAX_VARCHAR_LENGTH)}, geography_type()));
    auto result = GeoFunctions::st_geog_from_wkb(context.get(), {input});
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(coordinates.size(), (*result)->size());
    for (size_t row = 0; row < coordinates.size(); ++row) {
        ASSERT_FALSE((*result)->is_null(row));
        EXPECT_EQ(expected[row], (*result)->get(row).get_slice().to_string());
    }
    auto& output = down_cast<const GeoColumn*>(down_cast<const NullableColumn*>((*result).get())->data_column().get())
                           ->descriptor();
    EXPECT_EQ(GEO_DIMENSION_XY, output.storage.dimension);
    EXPECT_EQ(GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED, output.storage.validation_state);

    auto constant_value = BinaryColumn::create();
    constant_value->append(Slice(expected[3]));
    auto constant = GeoFunctions::st_geog_from_wkb(context.get(), {ConstColumn::create(constant_value, 17)});
    ASSERT_TRUE(constant.ok()) << constant.status();
    ASSERT_TRUE((*constant)->is_constant());
    EXPECT_EQ(17, (*constant)->size());
    EXPECT_EQ(expected[3], (*constant)->get(16).get_slice().to_string());

    // The constructor must own a copy; later changes to the input cannot alter it.
    input->reset_column();
    input->append("changed source bytes");
    EXPECT_EQ(expected[0], (*result)->get(0).get_slice().to_string());
}

TEST_F(geographyFunctionsTest, nativeGeographyCanonicalPointDecoderRetainsAcceptedInputs) {
    // Reference fast-path predicate from before the shared POINT decoder.
    auto previous = [](Slice wkb) {
        constexpr std::array<uint8_t, 5> header = {1, 1, 0, 0, 0};
        if (wkb.size != header.size() + 2 * sizeof(double) ||
            std::memcmp(wkb.data, header.data(), header.size()) != 0) {
            return false;
        }
        std::array<uint64_t, 2> coordinates;
        std::memcpy(coordinates.data(), wkb.data + header.size(), sizeof(coordinates));
        if constexpr (std::endian::native != std::endian::little) {
            coordinates[0] = std::byteswap(coordinates[0]);
            coordinates[1] = std::byteswap(coordinates[1]);
        }
        return is_valid_crs84_coordinate(std::bit_cast<double>(coordinates[0]), std::bit_cast<double>(coordinates[1]));
    };
    auto values = geometry_point_wkb_cases();
    auto append_point = [&](double x, double y) {
        std::string bytes(21, '\0');
        bytes[0] = bytes[1] = 1;
        const std::array<double, 2> coordinates = {x, y};
        for (size_t axis = 0; axis < coordinates.size(); ++axis) {
            const uint64_t bits = std::bit_cast<uint64_t>(coordinates[axis]);
            for (size_t i = 0; i < sizeof(bits); ++i) {
                bytes[5 + axis * sizeof(bits) + i] = static_cast<char>(bits >> (8 * i));
            }
        }
        values.push_back(bytes);
        bytes[0] = 0;
        std::reverse(bytes.begin() + 1, bytes.begin() + 5);
        std::reverse(bytes.begin() + 5, bytes.begin() + 13);
        std::reverse(bytes.begin() + 13, bytes.end());
        values.push_back(bytes);
    };
    const double inf = std::numeric_limits<double>::infinity();
    for (double x : {-180.0, 180.0, std::nextafter(-180.0, -inf), std::nextafter(180.0, inf)}) {
        for (double y : {-90.0, 90.0, std::nextafter(-90.0, -inf), std::nextafter(90.0, inf)}) {
            append_point(x, y);
        }
    }
    for (double value : {0.0, -0.0, std::numeric_limits<double>::quiet_NaN(), inf, -inf}) {
        append_point(value, 0);
        append_point(0, value);
    }
    append_point(std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN());
    std::mt19937_64 random(20261019);
    std::uniform_real_distribution<double> longitude(-180, 180);
    std::uniform_real_distribution<double> latitude(-90, 90);
    for (size_t i = 0; i < 256; ++i) {
        const double x = longitude(random);
        const double y = latitude(random);
        append_point(x, y);
    }
    for (size_t i = 0; i < 256; ++i) {
        auto malformed = values[0];
        for (size_t byte = 0; byte < 5; ++byte) {
            malformed[byte] = static_cast<char>(random());
        }
        values.push_back(std::move(malformed));
    }
    auto input = BinaryColumn::create();
    for (const auto& bytes : values) input->append(Slice(bytes));
    std::unique_ptr<FunctionContext> context(
            FunctionContext::create_test_context({TypeDescriptor(TYPE_VARBINARY)}, geography_type()));
    auto output = GeoFunctions::st_geog_from_wkb(context.get(), {input});
    ASSERT_TRUE(output.ok()) << output.status();
    ASSERT_EQ(values.size(), (*output)->size());
    size_t canonical = 0;
    for (size_t row = 0; row < values.size(); ++row) {
        SCOPED_TRACE(row);
        const auto& bytes = values[row];
        if (previous(Slice(bytes))) {
            ++canonical;
            ASSERT_FALSE((*output)->is_null(row));
            EXPECT_EQ(bytes, (*output)->get(row).get_slice().to_string());
        } else {
            WkbGeometry point;
            auto status = WkbCodec::parse_wkb(Slice(bytes), &point);
            std::string expected;
            if (status.ok()) status = WkbCodec::to_wkb(point, &expected);
            ASSERT_EQ(!status.ok(), (*output)->is_null(row));
            if (status.ok()) EXPECT_EQ(expected, (*output)->get(row).get_slice().to_string());
        }
    }
    EXPECT_GE(canonical, 256);
}

TEST_F(geographyFunctionsTest, nativeGeographyPointWkbConstructorRetainsNormalizationAndValidation) {
    WkbGeometry point;
    ASSERT_TRUE(WkbCodec::parse_wkt("POINT (37.62 -55.75)", &point).ok());
    std::string little;
    ASSERT_TRUE(WkbCodec::to_wkb(point, &little).ok());
    auto big = little;
    big[0] = 0;
    std::reverse(big.begin() + 1, big.begin() + 5);
    std::reverse(big.begin() + 5, big.begin() + 13);
    std::reverse(big.begin() + 13, big.end());
    auto unusual_empty = little;
    const uint64_t nan_bits = 0xfff8000000001234ULL;
    for (size_t i = 0; i < sizeof(nan_bits); ++i) {
        unusual_empty[5 + i] = static_cast<char>(nan_bits >> (8 * i));
        unusual_empty[13 + i] = static_cast<char>(nan_bits >> (8 * i));
    }
    auto partial_nan = little;
    for (size_t i = 0; i < sizeof(nan_bits); ++i) partial_nan[5 + i] = static_cast<char>(nan_bits >> (8 * i));
    auto bad_order = little;
    bad_order[0] = 2;
    auto ewkb = little;
    ewkb[4] = static_cast<char>(0x20);
    auto xyz = little + std::string(sizeof(double), '\0');
    xyz[1] = static_cast<char>(1001 & 0xff);
    xyz[2] = static_cast<char>(1001 >> 8);
    std::vector<std::string> values = {little, big, unusual_empty,        partial_nan,   bad_order,
                                       ewkb,   xyz, little.substr(0, 20), little + '\0', std::string()};
    for (const char* wkt : {"POINT (181 0)", "POINT (0 -91)", "LINESTRING (1 2, 3 4)", "POLYGON ((0 0, 1 0, 0 1, 0 0))",
                            "GEOMETRYCOLLECTION (POINT EMPTY, POINT (1 2))"}) {
        WkbGeometry geometry;
        ASSERT_TRUE(WkbCodec::parse_wkt(wkt, &geometry, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
        std::string bytes;
        ASSERT_TRUE(WkbCodec::to_wkb(geometry, &bytes, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
        values.push_back(bytes);
    }
    for (double invalid : {std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity()}) {
        auto bytes = little;
        const uint64_t bits = std::bit_cast<uint64_t>(invalid);
        for (size_t i = 0; i < sizeof(bits); ++i) bytes[13 + i] = static_cast<char>(bits >> (8 * i));
        values.push_back(bytes);
    }
    auto input = BinaryColumn::create();
    for (const auto& bytes : values) input->append(Slice(bytes));
    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context(
            {TypeDescriptor::create_varbinary_type(TypeDescriptor::MAX_VARCHAR_LENGTH)}, geography_type()));
    auto result = GeoFunctions::st_geog_from_wkb(context.get(), {input});
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(values.size(), (*result)->size());
    for (size_t row = 0; row < values.size(); ++row) {
        SCOPED_TRACE(row);
        WkbGeometry geometry;
        auto status = WkbCodec::parse_wkb(Slice(values[row]), &geometry);
        std::string expected;
        if (status.ok()) status = WkbCodec::to_wkb(geometry, &expected);
        if (!status.ok()) {
            EXPECT_TRUE((*result)->is_null(row));
        } else {
            ASSERT_FALSE((*result)->is_null(row));
            EXPECT_EQ(expected, (*result)->get(row).get_slice().to_string());
        }
    }
    EXPECT_NE(unusual_empty, (*result)->get(2).get_slice().to_string());
    EXPECT_EQ(little, (*result)->get(1).get_slice().to_string());
}

TEST_F(geographyFunctionsTest, nativeGeographyPointWkbConstructorRetainsNullSridAndEmptyInput) {
    WkbGeometry point;
    ASSERT_TRUE(WkbCodec::parse_wkt("POINT (-0 90)", &point).ok());
    std::string bytes;
    ASSERT_TRUE(WkbCodec::to_wkb(point, &bytes).ok());
    auto input = BinaryColumn::create();
    for (size_t row = 0; row < 5; ++row) input->append(Slice(bytes));
    auto nulls = NullColumn::create();
    for (uint8_t flag : {0, 0, 1, 0, 0}) nulls->append(flag);
    auto srids = Int32Column::create();
    for (int32_t srid : {4326, 3857, 4326, 4326, 4326}) srids->append(srid);
    auto srid_nulls = NullColumn::create();
    for (uint8_t flag : {0, 0, 0, 1, 0}) srid_nulls->append(flag);
    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context(
            {TypeDescriptor::create_varbinary_type(TypeDescriptor::MAX_VARCHAR_LENGTH), TypeDescriptor(TYPE_INT)},
            geography_type()));
    auto result = GeoFunctions::st_geog_from_wkb(
            context.get(), {NullableColumn::create(input, nulls), NullableColumn::create(srids, srid_nulls)});
    ASSERT_TRUE(result.ok()) << result.status();
    for (size_t row : {1, 2, 3}) EXPECT_TRUE((*result)->is_null(row));
    for (size_t row : {0, 4}) {
        ASSERT_FALSE((*result)->is_null(row));
        EXPECT_EQ(bytes, (*result)->get(row).get_slice().to_string());
    }
    auto one = BinaryColumn::create();
    one->append(Slice(bytes));
    const ColumnPtr constant_input = ConstColumn::create(one, 17);
    const std::array<ColumnPtr, 2> invalid_srids = {ColumnHelper::create_const_column<TYPE_INT>(3857, 17),
                                                    ColumnHelper::create_const_null_column(17)};
    for (const auto& srid : invalid_srids) {
        auto invalid = GeoFunctions::st_geog_from_wkb(context.get(), {constant_input, srid});
        ASSERT_TRUE(invalid.ok()) << invalid.status();
        EXPECT_TRUE((*invalid)->is_constant());
        EXPECT_EQ(17, (*invalid)->size());
        EXPECT_TRUE((*invalid)->is_null(16));
    }
    auto empty = GeoFunctions::st_geog_from_wkb(context.get(), {BinaryColumn::create(), Int32Column::create()});
    ASSERT_TRUE(empty.ok()) << empty.status();
    EXPECT_EQ(0, (*empty)->size());
}

TEST_F(geographyFunctionsTest, nativeGeographyRejectsInvalidInputAndSrid) {
    GeoTypeDescriptor descriptor{GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_COORDINATE_SYSTEM_SPHERICAL,
                                 GEO_EDGE_ALGORITHM_SPHERICAL, "OGC:CRS84", 4326};
    auto geography = TypeDescriptor::create_geo_type(TYPE_GEOGRAPHY, descriptor);
    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context(
            {TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH), TypeDescriptor(TYPE_INT)},
            geography));
    auto input = BinaryColumn::create();
    input->append("POINT (181 0)");
    input->append("POINT (1 2)");
    input->append_default();
    input->append("POINT (1 2)");
    auto nulls = NullColumn::create();
    nulls->append(0);
    nulls->append(0);
    nulls->append(1);
    nulls->append(0);
    auto srid = Int32Column::create();
    srid->append(4326);
    srid->append(3857);
    srid->append(4326);
    srid->append(4326);

    auto result = GeoFunctions::st_geog_from_text(context.get(), {NullableColumn::create(input, nulls), srid}).value();
    EXPECT_TRUE(result->is_null(0));
    EXPECT_TRUE(result->is_null(1));
    EXPECT_TRUE(result->is_null(2));
    EXPECT_FALSE(result->is_null(3));
}

TEST_F(geographyFunctionsTest, nativeGeographySerializerChecksBoundaryDescriptor) {
    GeoTypeDescriptor type{GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_COORDINATE_SYSTEM_CARTESIAN, GEO_EDGE_ALGORITHM_PLANAR,
                           "OGC:CRS84", 4326};
    GeoColumnDescriptor descriptor{type,
                                   {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}};
    auto column = GeoColumn::create(std::move(descriptor));
    const char point[] = {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    column->append_wkb(Slice(point, sizeof(point)));

    auto result = GeoFunctions::st_geography_as_text(nullptr, {column});
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_not_supported());
}

TEST_F(geographyFunctionsTest, nativeGeographyPointAccessorsMatchCodec) {
    auto values = geometry_point_wkb_cases();
    const double inf = std::numeric_limits<double>::infinity();
    const double tiny = std::numeric_limits<double>::denorm_min();
    const double nan = std::bit_cast<double>(uint64_t{0xfff8000000001234});
    const std::vector<double> longitude = {0.0,
                                           -0.0,
                                           -180.0,
                                           180.0,
                                           std::nextafter(-180.0, 0.0),
                                           std::nextafter(180.0, 0.0),
                                           std::nextafter(-180.0, -inf),
                                           std::nextafter(180.0, inf),
                                           tiny,
                                           -tiny,
                                           nan,
                                           inf,
                                           -inf};
    const std::vector<double> latitude = {0.0,
                                          -0.0,
                                          -90.0,
                                          90.0,
                                          std::nextafter(-90.0, 0.0),
                                          std::nextafter(90.0, 0.0),
                                          std::nextafter(-90.0, -inf),
                                          std::nextafter(90.0, inf),
                                          tiny,
                                          -tiny,
                                          nan,
                                          inf,
                                          -inf};
    for (double x : longitude) {
        for (double y : latitude) {
            auto bytes = values.front();
            const std::array<double, 2> coordinates = {x, y};
            for (size_t axis = 0; axis < coordinates.size(); ++axis) {
                const uint64_t bits = std::bit_cast<uint64_t>(coordinates[axis]);
                for (size_t i = 0; i < sizeof(bits); ++i) {
                    bytes[5 + axis * sizeof(bits) + i] = static_cast<char>(bits >> (8 * i));
                }
            }
            values.push_back(bytes);
            bytes[0] = 0;
            std::reverse(bytes.begin() + 1, bytes.begin() + 5);
            std::reverse(bytes.begin() + 5, bytes.begin() + 13);
            std::reverse(bytes.begin() + 13, bytes.end());
            values.push_back(bytes);
        }
    }
    const GeoColumnDescriptor descriptor{geography_type().geo_type.value(),
                                         {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_UNVALIDATED}};
    for (size_t i = 0; i < values.size(); ++i) {
        SCOPED_TRACE(i);
        auto input = GeoColumn::create(descriptor);
        input->append_wkb(Slice(values[i]));
        WkbGeometry point;
        const auto parsed = WkbCodec::parse_wkb(Slice(values[i]), &point, WkbCoordinateSemantics::GEOGRAPHY_CRS84);
        for (bool x : {true, false}) {
            Status expected = parsed;
            if (expected.ok() && (point.type != WkbGeometryType::POINT || point.empty)) {
                expected = Status::InvalidArgument(x ? "ST_X requires a non-empty POINT GEOGRAPHY"
                                                     : "ST_Y requires a non-empty POINT GEOGRAPHY");
            }
            for (bool nullable : {false, true}) {
                for (bool constant : {false, true}) {
                    ColumnPtr source = input;
                    if (nullable) source = NullableColumn::create(source, NullColumn::create(1, 0));
                    if (constant) source = ConstColumn::create(source, 9);
                    auto output = x ? GeoFunctions::st_geography_x(nullptr, {source})
                                    : GeoFunctions::st_geography_y(nullptr, {source});
                    if (!expected.ok()) {
                        ASSERT_FALSE(output.ok());
                        EXPECT_EQ(expected.code(), output.status().code());
                        EXPECT_EQ(expected.message(), output.status().message());
                    } else {
                        ASSERT_TRUE(output.ok()) << output.status();
                        EXPECT_EQ(constant, (*output)->is_constant());
                        EXPECT_EQ(source->size(), (*output)->size());
                        ColumnViewer<TYPE_DOUBLE> actual(*output);
                        for (size_t row = 0; row < source->size(); ++row) {
                            ASSERT_FALSE(actual.is_null(row));
                            EXPECT_EQ(std::bit_cast<uint64_t>(x ? point.coordinates[0].x : point.coordinates[0].y),
                                      std::bit_cast<uint64_t>(actual.value(row)));
                        }
                    }
                }
            }
        }
    }
    auto data = GeoColumn::create(descriptor);
    data->append_wkb(Slice(values[2])); // (-0.0, 0.0), little-endian.
    data->append_wkb(Slice("invalid hidden NULL payload"));
    data->append_wkb(Slice(values[3])); // The same point, big-endian.
    auto nulls = NullColumn::create();
    nulls->append(0);
    nulls->append(1);
    nulls->append(0);
    ColumnPtr mixed = NullableColumn::create(data, nulls);
    for (bool x : {true, false}) {
        auto output =
                x ? GeoFunctions::st_geography_x(nullptr, {mixed}) : GeoFunctions::st_geography_y(nullptr, {mixed});
        ASSERT_TRUE(output.ok()) << output.status();
        ColumnViewer<TYPE_DOUBLE> actual(*output);
        EXPECT_TRUE(actual.is_null(1));
        for (size_t row : {0, 2}) {
            EXPECT_EQ(std::bit_cast<uint64_t>(x ? -0.0 : 0.0), std::bit_cast<uint64_t>(actual.value(row)));
        }
        auto only_null = ColumnHelper::create_const_null_column(9);
        auto all_null = x ? GeoFunctions::st_geography_x(nullptr, {only_null})
                          : GeoFunctions::st_geography_y(nullptr, {only_null});
        ASSERT_TRUE(all_null.ok()) << all_null.status();
        EXPECT_TRUE((*all_null)->only_null());
        EXPECT_EQ(9, (*all_null)->size());
    }
}

TEST_F(geographyFunctionsTest, nativeGeographyPointAccessors) {
    auto point = geography({"POINT (12.5 -8.25)"});
    ColumnViewer<TYPE_DOUBLE> x(GeoFunctions::st_geography_x(nullptr, {point}).value());
    ColumnViewer<TYPE_DOUBLE> y(GeoFunctions::st_geography_y(nullptr, {point}).value());
    EXPECT_DOUBLE_EQ(12.5, x.value(0));
    EXPECT_DOUBLE_EQ(-8.25, y.value(0));

    auto constant = ConstColumn::create(point, 3);
    auto constant_x = GeoFunctions::st_geography_x(nullptr, {constant}).value();
    EXPECT_TRUE(constant_x->is_constant());
    EXPECT_EQ(3, constant_x->size());
    EXPECT_DOUBLE_EQ(12.5, ColumnHelper::get_const_value<TYPE_DOUBLE>(constant_x));

    auto null = GeoFunctions::st_geography_x(nullptr, {geography({nullptr})}).value();
    EXPECT_TRUE(null->is_null(0));

    auto line = geography({"LINESTRING (0 0, 1 1)"});
    auto line_x = GeoFunctions::st_geography_x(nullptr, {line});
    ASSERT_FALSE(line_x.ok());
    EXPECT_TRUE(line_x.status().is_invalid_argument());

    auto empty = geography({"POINT EMPTY"});
    auto empty_y = GeoFunctions::st_geography_y(nullptr, {empty});
    ASSERT_FALSE(empty_y.ok());
    EXPECT_TRUE(empty_y.status().is_invalid_argument());
}

TEST_F(geographyFunctionsTest, nativeGeographyTypeReportsEveryFamily) {
    auto input = geography({"POINT EMPTY", "LINESTRING (0 0, 1 1)", "POLYGON ((0 0, 0 1, 1 1, 0 0))",
                            "MULTIPOINT ((0 0), (1 1))", "MULTILINESTRING ((0 0, 1 1))",
                            "MULTIPOLYGON (((0 0, 0 1, 1 1, 0 0)))", "GEOMETRYCOLLECTION (POINT (0 0))"});
    ColumnViewer<TYPE_VARCHAR> result(GeoFunctions::st_geography_type(nullptr, {input}).value());
    const char* expected[] = {"ST_Point",           "ST_LineString",   "ST_Polygon",           "ST_MultiPoint",
                              "ST_MultiLineString", "ST_MultiPolygon", "ST_GeometryCollection"};
    for (size_t row = 0; row < std::size(expected); ++row) {
        EXPECT_EQ(expected[row], result.value(row).to_string());
    }

    auto null = GeoFunctions::st_geography_type(nullptr, {geography({nullptr})}).value();
    EXPECT_TRUE(null->is_null(0));
}

TEST_F(geographyFunctionsTest, nativeGeographyPointDistance) {
    auto lhs = geography({"POINT (0 0)", "POINT (179 0)", "POINT (0 89)"});
    auto rhs = geography({"POINT (0 0)", "POINT (-179 0)", "POINT (90 89)"});
    ColumnViewer<TYPE_DOUBLE> result(GeoFunctions::st_geography_distance(nullptr, {lhs, rhs}).value());
    EXPECT_DOUBLE_EQ(0, result.value(0));
    EXPECT_NEAR(222390.202354968, result.value(1), 0.001);
    EXPECT_NEAR(157249.628092489, result.value(2), 0.001);

    auto empty = GeoFunctions::st_geography_distance(nullptr, {geography({"POINT EMPTY"}), geography({"POINT (0 0)"})})
                         .value();
    EXPECT_TRUE(empty->is_null(0));
    auto null =
            GeoFunctions::st_geography_distance(nullptr, {geography({nullptr}), geography({"POINT (0 0)"})}).value();
    EXPECT_TRUE(null->is_null(0));
}

TEST_F(geographyFunctionsTest, nativeGeometryPointLineDistanceAndDWithin) {
    auto points = geometry({"POINT (5 3)", "POINT (-2 0)", "POINT (5 1)", "POINT (5 3)"});
    auto lines = geometry({"LINESTRING (0 0, 10 0)", "LINESTRING (0 0, 10 0)",
                           "MULTILINESTRING ((0 10, 10 10), (0 0, 10 0))", "LINESTRING (0 0, 0 0, 10 0)"});
    auto distance_result = GeoFunctions::st_geometry_distance(nullptr, {points, lines});
    ASSERT_TRUE(distance_result.ok()) << distance_result.status();
    ColumnViewer<TYPE_DOUBLE> distance(*distance_result);
    EXPECT_DOUBLE_EQ(3, distance.value(0));
    EXPECT_DOUBLE_EQ(2, distance.value(1));
    EXPECT_DOUBLE_EQ(1, distance.value(2));
    EXPECT_DOUBLE_EQ(3, distance.value(3));

    auto reversed = GeoFunctions::st_geometry_distance(nullptr, {lines, points});
    ASSERT_TRUE(reversed.ok()) << reversed.status();
    ColumnViewer<TYPE_DOUBLE> reversed_distance(*reversed);
    for (size_t row = 0; row < points->size(); ++row)
        EXPECT_DOUBLE_EQ(distance.value(row), reversed_distance.value(row));

    auto equality = ColumnHelper::create_const_column<TYPE_DOUBLE>(3, points->size());
    auto within_result = GeoFunctions::st_geometry_dwithin(nullptr, {points, lines, equality});
    ASSERT_TRUE(within_result.ok()) << within_result.status();
    ColumnViewer<TYPE_BOOLEAN> within(*within_result);
    EXPECT_TRUE(within.value(0));
    EXPECT_TRUE(within.value(1));
    EXPECT_TRUE(within.value(2));
    EXPECT_TRUE(within.value(3));

    auto exact_point = geometry({"POINT (5 3)"});
    auto exact_line = geometry({"LINESTRING (0 0, 10 0)"});
    auto below = ColumnHelper::create_const_column<TYPE_DOUBLE>(std::nextafter(3.0, 0.0), 1);
    auto above = ColumnHelper::create_const_column<TYPE_DOUBLE>(std::nextafter(3.0, 4.0), 1);
    auto below_result = GeoFunctions::st_geometry_dwithin(nullptr, {exact_point, exact_line, below});
    ASSERT_TRUE(below_result.ok()) << below_result.status();
    ColumnViewer<TYPE_BOOLEAN> below_within(*below_result);
    EXPECT_FALSE(below_within.value(0));

    auto above_result = GeoFunctions::st_geometry_dwithin(nullptr, {exact_line, exact_point, above});
    ASSERT_TRUE(above_result.ok()) << above_result.status();
    ColumnViewer<TYPE_BOOLEAN> above_within(*above_result);
    EXPECT_TRUE(above_within.value(0));
}

TEST_F(geographyFunctionsTest, nativeGeometryPointLineDistancePreservesExactBoundaries) {
    auto points = geometry({"POINT (1 1)", "POINT (1 1)", "POINT (1 0)", "POINT (1 0)"});
    auto lines = geometry({"LINESTRING (0 0, 41 41)", "LINESTRING (41 41, 0 0)", "LINESTRING (-1e100 0, 0 0)",
                           "LINESTRING (0 0, -1e100 0)"});

    auto distance_result = GeoFunctions::st_geometry_distance(nullptr, {points, lines});
    ASSERT_TRUE(distance_result.ok()) << distance_result.status();
    ColumnViewer<TYPE_DOUBLE> distance(*distance_result);
    EXPECT_DOUBLE_EQ(0, distance.value(0));
    EXPECT_DOUBLE_EQ(0, distance.value(1));
    EXPECT_DOUBLE_EQ(1, distance.value(2));
    EXPECT_DOUBLE_EQ(1, distance.value(3));

    auto thresholds = DoubleColumn::create();
    thresholds->append(0);
    thresholds->append(0);
    thresholds->append(std::nextafter(1.0, 0.0));
    thresholds->append(1);
    auto within_result = GeoFunctions::st_geometry_dwithin(nullptr, {points, lines, thresholds});
    ASSERT_TRUE(within_result.ok()) << within_result.status();
    ColumnViewer<TYPE_BOOLEAN> within(*within_result);
    EXPECT_TRUE(within.value(0));
    EXPECT_TRUE(within.value(1));
    EXPECT_FALSE(within.value(2));
    EXPECT_TRUE(within.value(3));
}

TEST_F(geographyFunctionsTest, nativeGeographyPointLineDistanceAndDWithin) {
    auto points = geography({"POINT (0 1)", "POINT (180 1)", "POINT (0 89)"});
    auto lines = geography({"LINESTRING (-1 0, 1 0)", "LINESTRING (179 0, -179 0)",
                            "MULTILINESTRING ((-90 89, 90 89), (0 80, 1 80))"});
    auto distance_result = GeoFunctions::st_geography_distance(nullptr, {points, lines});
    ASSERT_TRUE(distance_result.ok()) << distance_result.status();
    ColumnViewer<TYPE_DOUBLE> distance(*distance_result);
    EXPECT_NEAR(111195.101177484, distance.value(0), 0.001);
    EXPECT_NEAR(111195.101177484, distance.value(1), 0.001);
    EXPECT_NEAR(111195.101177484, distance.value(2), 0.001);

    auto thresholds = DoubleColumn::create();
    thresholds->append(distance.value(0));
    thresholds->append(std::nextafter(distance.value(1), 0.0));
    thresholds->append(std::nextafter(distance.value(2), std::numeric_limits<double>::infinity()));
    auto within_result = GeoFunctions::st_geography_dwithin(nullptr, {lines, points, thresholds});
    ASSERT_TRUE(within_result.ok()) << within_result.status();
    ColumnViewer<TYPE_BOOLEAN> within(*within_result);
    EXPECT_TRUE(within.value(0));
    EXPECT_FALSE(within.value(1));
    EXPECT_TRUE(within.value(2));
}

TEST_F(geographyFunctionsTest, nativeGeographyPointLineDWithinPreservesMeterBoundary) {
    auto points =
            geography({"POINT (-0.02 0)", "POINT (-0.02 0)", "POINT (-0.02 0)", "POINT (180 0)", "POINT (180 0)"});
    auto lines = geography({"LINESTRING (0 0, 1 0)", "LINESTRING (0 0, 1 0)", "LINESTRING (0 0, 1 0)",
                            "LINESTRING (0 0, 0 0)", "LINESTRING (0 0, 0 0)"});

    auto distance_result = GeoFunctions::st_geography_distance(nullptr, {points, lines});
    ASSERT_TRUE(distance_result.ok()) << distance_result.status();
    ColumnViewer<TYPE_DOUBLE> distance(*distance_result);

    auto thresholds = DoubleColumn::create();
    thresholds->append(distance.value(0));
    thresholds->append(std::nextafter(distance.value(1), 0.0));
    thresholds->append(std::nextafter(distance.value(2), std::numeric_limits<double>::infinity()));
    thresholds->append(distance.value(3) - 0.05);
    thresholds->append(distance.value(4));
    auto within_result = GeoFunctions::st_geography_dwithin(nullptr, {points, lines, thresholds});
    ASSERT_TRUE(within_result.ok()) << within_result.status();
    ColumnViewer<TYPE_BOOLEAN> within(*within_result);
    EXPECT_TRUE(within.value(0));
    EXPECT_FALSE(within.value(1));
    EXPECT_TRUE(within.value(2));
    EXPECT_FALSE(within.value(3));
    EXPECT_TRUE(within.value(4));
}

TEST_F(geographyFunctionsTest, nativeGeoPointLineDistanceNullEmptyAndRejection) {
    auto point = geometry({"POINT (1 1)"});
    auto empty_line = geometry({"LINESTRING EMPTY"});
    EXPECT_TRUE(GeoFunctions::st_geometry_distance(nullptr, {point, empty_line}).value()->is_null(0));
    ColumnViewer<TYPE_BOOLEAN> empty_within(
            GeoFunctions::st_geometry_dwithin(nullptr,
                                              {point, empty_line, ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1)})
                    .value());
    EXPECT_FALSE(empty_within.value(0));
    auto empty_multiline = geometry({"MULTILINESTRING (EMPTY)"});
    EXPECT_TRUE(GeoFunctions::st_geometry_distance(nullptr, {point, empty_multiline}).value()->is_null(0));
    ColumnViewer<TYPE_BOOLEAN> empty_multi_within(
            GeoFunctions::st_geometry_dwithin(
                    nullptr, {point, empty_multiline, ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1)})
                    .value());
    EXPECT_FALSE(empty_multi_within.value(0));

    auto null_distance = GeoFunctions::st_geometry_distance(nullptr, {geometry({nullptr}), empty_line}).value();
    EXPECT_TRUE(null_distance->is_null(0));
    auto null_threshold = ColumnHelper::create_const_null_column(1);
    EXPECT_TRUE(GeoFunctions::st_geometry_dwithin(nullptr, {point, empty_line, null_threshold}).value()->is_null(0));

    for (double invalid : {-1.0, std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::infinity()}) {
        auto threshold = ColumnHelper::create_const_column<TYPE_DOUBLE>(invalid, 1);
        auto result = GeoFunctions::st_geometry_dwithin(nullptr, {point, empty_line, threshold});
        ASSERT_FALSE(result.ok());
        EXPECT_TRUE(result.status().is_invalid_argument());
    }

    auto collection = geometry({"GEOMETRYCOLLECTION (LINESTRING (0 0, 1 1))"});
    EXPECT_FALSE(GeoFunctions::st_geometry_distance(nullptr, {point, collection}).ok());
    EXPECT_FALSE(GeoFunctions::st_geometry_dwithin(
                         nullptr, {point, collection, ColumnHelper::create_const_column<TYPE_DOUBLE>(1, 1)})
                         .ok());
    EXPECT_FALSE(GeoFunctions::st_geometry_dwithin(nullptr, {point, geometry({"POINT (2 2)"}),
                                                             ColumnHelper::create_const_column<TYPE_DOUBLE>(1, 1)})
                         .ok());

    auto antipodal = GeoFunctions::st_geography_distance(
            nullptr, {geography({"POINT (0 10)"}), geography({"LINESTRING (0 0, 180 0)"})});
    ASSERT_FALSE(antipodal.ok());
    EXPECT_TRUE(antipodal.status().is_invalid_argument());
    auto near_antipodal = GeoFunctions::st_geography_distance(
            nullptr, {geography({"POINT (0 10)"}), geography({"LINESTRING (0 0, 179.999 0)"})});
    EXPECT_TRUE(near_antipodal.ok()) << near_antipodal.status();
}

TEST_F(geographyFunctionsTest, nativeGeographyComputeChecksDescriptorCapabilityAndDimension) {
    WkbGeometry point;
    ASSERT_TRUE(WkbCodec::parse_wkt("POINT (1 2)", &point).ok());
    std::string wkb;
    ASSERT_TRUE(WkbCodec::to_wkb(point, &wkb).ok());

    for (auto descriptor : {
                 GeoColumnDescriptor{
                         {GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_COORDINATE_SYSTEM_SPHERICAL, GEO_EDGE_ALGORITHM_SPHERICAL,
                          "OGC:CRS84", 4326},
                         {GEO_ENCODING_WKB, GEO_DIMENSION_XYZ, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}},
                 GeoColumnDescriptor{{GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_COORDINATE_SYSTEM_SPHERICAL,
                                      GEO_EDGE_ALGORITHM_VINCENTY, "OGC:CRS84", 4326},
                                     {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}},
         }) {
        auto input = GeoColumn::create(std::move(descriptor));
        input->append_wkb(Slice(wkb));
        auto result = GeoFunctions::st_geography_x(nullptr, {input});
        ASSERT_FALSE(result.ok());
        EXPECT_TRUE(result.status().is_not_supported());
    }
}

TEST_F(geographyFunctionsTest, nativeGeometryPointWkbConstructorMatchesCodec) {
    const auto values = geometry_point_wkb_cases();
    auto input = BinaryColumn::create();
    for (const auto& bytes : values) input->append(Slice(bytes));
    const auto type = geometry_type("EPSG:4326", 4326);
    std::unique_ptr<FunctionContext> context(
            FunctionContext::create_test_context({TypeDescriptor(TYPE_VARBINARY), TypeDescriptor(TYPE_VARCHAR)}, type));
    auto crs = ColumnHelper::create_const_column<TYPE_VARCHAR>("EPSG:4326", values.size());
    auto output = GeoFunctions::st_geom_from_wkb(context.get(), {input, crs});
    ASSERT_TRUE(output.ok()) << output.status();
    ASSERT_TRUE((*output)->is_nullable());
    ASSERT_EQ(values.size(), (*output)->size());
    for (size_t row = 0; row < values.size(); ++row) {
        SCOPED_TRACE(row);
        WkbGeometry point;
        auto status = WkbCodec::parse_wkb(Slice(values[row]), &point, WkbCoordinateSemantics::GEOMETRY_CARTESIAN);
        std::string expected;
        if (status.ok()) status = WkbCodec::to_wkb(point, &expected, WkbCoordinateSemantics::GEOMETRY_CARTESIAN);
        ASSERT_EQ(!status.ok(), (*output)->is_null(row));
        if (status.ok()) {
            EXPECT_EQ(expected, (*output)->get(row).get_slice().to_string());
        }
    }
    const auto* data = down_cast<const GeoColumn*>(ColumnHelper::get_data_column(output->get()));
    EXPECT_EQ(type.geo_type.value(), data->descriptor().type);
    EXPECT_EQ(GEO_DIMENSION_XY, data->descriptor().storage.dimension);
    auto nulls = NullColumn::create();
    for (size_t row = 0; row < values.size(); ++row) nulls->append(row % 11 == 0);
    auto mixed = GeoFunctions::st_geom_from_wkb(context.get(), {NullableColumn::create(input, nulls), crs});
    ASSERT_TRUE(mixed.ok()) << mixed.status();
    for (size_t row = 0; row < values.size(); ++row) {
        EXPECT_EQ(row % 11 == 0 || (*output)->is_null(row), (*mixed)->is_null(row));
        if (!(*mixed)->is_null(row)) {
            EXPECT_EQ((*output)->get(row).get_slice(), (*mixed)->get(row).get_slice());
        }
    }
    auto one = BinaryColumn::create();
    one->append(Slice(values[2]));
    auto constant = GeoFunctions::st_geom_from_wkb(
            context.get(),
            {ConstColumn::create(one, 17), ColumnHelper::create_const_column<TYPE_VARCHAR>("EPSG:4326", 17)});
    ASSERT_TRUE(constant.ok()) << constant.status();
    EXPECT_TRUE((*constant)->is_constant());
    EXPECT_EQ(17, (*constant)->size());
    EXPECT_EQ(values[2], (*constant)->get(16).get_slice().to_string());
    input->reset_column();
    input->append("changed source");
    EXPECT_EQ(values[0], (*output)->get(0).get_slice().to_string());
}

TEST_F(geographyFunctionsTest, nativeGeometryPointAccessorsMatchCodec) {
    const auto type = geometry_type("EPSG:4326", 4326);
    for (const auto& bytes : geometry_point_wkb_cases()) {
        SCOPED_TRACE(bytes.size());
        auto input = GeoColumn::create(GeoColumnDescriptor{
                type.geo_type.value(), {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_UNVALIDATED}});
        input->append_wkb(Slice(bytes));
        WkbGeometry point;
        const auto status = WkbCodec::parse_wkb(Slice(bytes), &point, WkbCoordinateSemantics::GEOMETRY_CARTESIAN);
        for (bool x : {true, false}) {
            for (bool constant : {false, true}) {
                ColumnPtr source = input;
                if (constant) source = ConstColumn::create(source, 9);
                auto output = x ? GeoFunctions::st_geometry_x(nullptr, {source})
                                : GeoFunctions::st_geometry_y(nullptr, {source});
                if (!status.ok()) {
                    ASSERT_FALSE(output.ok());
                    EXPECT_EQ(status.message(), output.status().message());
                } else if (point.type != WkbGeometryType::POINT || point.empty) {
                    ASSERT_FALSE(output.ok());
                    EXPECT_EQ(
                            x ? "ST_X requires a non-empty POINT GEOMETRY" : "ST_Y requires a non-empty POINT GEOMETRY",
                            output.status().message());
                } else {
                    ASSERT_TRUE(output.ok()) << output.status();
                    EXPECT_EQ(constant, (*output)->is_constant());
                    EXPECT_EQ(source->size(), (*output)->size());
                    const double actual = ColumnViewer<TYPE_DOUBLE>(*output).value(source->size() - 1);
                    EXPECT_EQ(std::bit_cast<uint64_t>(x ? point.coordinates[0].x : point.coordinates[0].y),
                              std::bit_cast<uint64_t>(actual));
                }
                if (!output.ok()) {
                    EXPECT_TRUE(output.status().is_invalid_argument());
                }
            }
        }
    }
}

TEST_F(geographyFunctionsTest, nativeGeometryPointFastPathRetainsNullsAndDescriptorChecks) {
    auto source = geometry({"POINT (-0.0 2)", nullptr, "POINT (181 -91)", "POINT (1e7 -2e7)"},
                           geometry_type("EPSG:4326", 4326));
    for (bool x : {true, false}) {
        auto result =
                x ? GeoFunctions::st_geometry_x(nullptr, {source}) : GeoFunctions::st_geometry_y(nullptr, {source});
        ASSERT_TRUE(result.ok()) << result.status();
        ColumnViewer<TYPE_DOUBLE> output(*result);
        EXPECT_TRUE(output.is_null(1));
        EXPECT_DOUBLE_EQ(x ? 181 : -91, output.value(2));
        EXPECT_DOUBLE_EQ(x ? 1e7 : -2e7, output.value(3));
        auto all_null = x ? GeoFunctions::st_geometry_x(nullptr, {ColumnHelper::create_const_null_column(17)})
                          : GeoFunctions::st_geometry_y(nullptr, {ColumnHelper::create_const_null_column(17)});
        ASSERT_TRUE(all_null.ok());
        EXPECT_EQ(17, (*all_null)->size());
        EXPECT_TRUE((*all_null)->is_null(16));
    }
    for (auto dimension : {GEO_DIMENSION_UNKNOWN, GEO_DIMENSION_MIXED, GEO_DIMENSION_XYZ}) {
        auto input = GeoColumn::create(GeoColumnDescriptor{
                geometry_type().geo_type.value(), {GEO_ENCODING_WKB, dimension, GEO_VALIDATION_STATE_UNVALIDATED}});
        input->append_wkb("not WKB");
        for (bool x : {true, false}) {
            auto output =
                    x ? GeoFunctions::st_geometry_x(nullptr, {input}) : GeoFunctions::st_geometry_y(nullptr, {input});
            ASSERT_FALSE(output.ok());
            EXPECT_TRUE(output.status().is_not_supported());
            EXPECT_EQ(dimension == GEO_DIMENSION_XYZ ? "Unsupported GEOMETRY SQL boundary descriptor"
                                                     : "GEOMETRY compute functions require XY dimension",
                      output.status().message());
        }
    }
}

TEST_F(geographyFunctionsTest, nativeGeometryInitialFunctionSet) {
    auto points = geometry({"POINT (1000000.5 -2000000.25)", nullptr});
    auto x_result = GeoFunctions::st_geometry_x(nullptr, {points});
    auto y_result = GeoFunctions::st_geometry_y(nullptr, {points});
    ASSERT_TRUE(x_result.ok()) << x_result.status();
    ASSERT_TRUE(y_result.ok()) << y_result.status();
    ColumnViewer<TYPE_DOUBLE> x(std::move(x_result).value());
    ColumnViewer<TYPE_DOUBLE> y(std::move(y_result).value());
    EXPECT_DOUBLE_EQ(1000000.5, x.value(0));
    EXPECT_DOUBLE_EQ(-2000000.25, y.value(0));
    EXPECT_TRUE(x.is_null(1));
    EXPECT_TRUE(y.is_null(1));

    auto constant = ConstColumn::create(geometry({"POINT (7 9)"}), 3);
    auto constant_x_result = GeoFunctions::st_geometry_x(nullptr, {constant});
    ASSERT_TRUE(constant_x_result.ok()) << constant_x_result.status();
    auto constant_x = std::move(constant_x_result).value();
    EXPECT_TRUE(constant_x->is_constant());
    EXPECT_EQ(3, constant_x->size());
    EXPECT_DOUBLE_EQ(7, ColumnHelper::get_const_value<TYPE_DOUBLE>(constant_x));

    auto families = geometry({"POINT EMPTY", "LINESTRING (0 0, 1 1)", "POLYGON ((0 0, 0 1, 1 1, 0 0))",
                              "MULTIPOINT ((0 0), (1 1))", "MULTILINESTRING ((0 0, 1 1))",
                              "MULTIPOLYGON (((0 0, 0 1, 1 1, 0 0)))", "GEOMETRYCOLLECTION (POINT (0 0))"});
    auto names_result = GeoFunctions::st_geometry_type(nullptr, {families});
    ASSERT_TRUE(names_result.ok()) << names_result.status();
    ColumnViewer<TYPE_VARCHAR> names(std::move(names_result).value());
    const char* expected[] = {"ST_Point",           "ST_LineString",   "ST_Polygon",           "ST_MultiPoint",
                              "ST_MultiLineString", "ST_MultiPolygon", "ST_GeometryCollection"};
    for (size_t row = 0; row < std::size(expected); ++row) {
        EXPECT_EQ(expected[row], names.value(row).to_string());
    }

    auto crs84 = geometry_type("OGC:CRS84", 4326);
    auto lhs = geometry({"POINT (0 0)", "POINT EMPTY", nullptr}, crs84);
    auto rhs = geometry({"POINT (3 4)", "POINT (3 4)", "POINT (3 4)"}, crs84);
    auto distance_result = GeoFunctions::st_geometry_distance(nullptr, {lhs, rhs});
    ASSERT_TRUE(distance_result.ok()) << distance_result.status();
    ColumnViewer<TYPE_DOUBLE> distance(std::move(distance_result).value());
    EXPECT_DOUBLE_EQ(5, distance.value(0));
    EXPECT_TRUE(distance.is_null(1));
    EXPECT_TRUE(distance.is_null(2));
}

TEST_F(geographyFunctionsTest, nativeGeometryComputeRejectsUnsupportedInputs) {
    auto line = geometry({"LINESTRING (0 0, 1 1)"});
    auto line_x = GeoFunctions::st_geometry_x(nullptr, {line});
    ASSERT_FALSE(line_x.ok());
    EXPECT_TRUE(line_x.status().is_invalid_argument());

    auto incompatible = GeoFunctions::st_geometry_distance(
            nullptr, {geometry({"POINT (0 0)"}), geometry({"POINT (3 4)"}, geometry_type("EPSG:4326", 4326))});
    ASSERT_FALSE(incompatible.ok());
    EXPECT_TRUE(incompatible.status().is_invalid_argument());

    auto mixed_kind = GeoFunctions::st_geometry_x(nullptr, {geography({"POINT (1 2)"})});
    ASSERT_FALSE(mixed_kind.ok());
    EXPECT_TRUE(mixed_kind.status().is_not_supported());

    WkbGeometry point;
    ASSERT_TRUE(WkbCodec::parse_wkt("POINT (1 2)", &point).ok());
    std::string wkb;
    ASSERT_TRUE(WkbCodec::to_wkb(point, &wkb).ok());
    GeoColumnDescriptor descriptor{
            {GEO_LOGICAL_TYPE_GEOMETRY, GEO_COORDINATE_SYSTEM_CARTESIAN, GEO_EDGE_ALGORITHM_PLANAR, "EPSG:3857", 3857},
            {GEO_ENCODING_WKB, GEO_DIMENSION_XYZ, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}};
    auto xyz = GeoColumn::create(std::move(descriptor));
    xyz->append_wkb(Slice(wkb));
    auto unsupported_dimension = GeoFunctions::st_geometry_type(nullptr, {xyz});
    ASSERT_FALSE(unsupported_dimension.ok());
    EXPECT_TRUE(unsupported_dimension.status().is_not_supported());
}

TEST_F(geographyFunctionsTest, nativeGeometryWktAndWkbRoundTrip) {
    const char* values[] = {"POINT (1000000 2000000)",
                            "LINESTRING (0 0, 1 1)",
                            "POLYGON ((0 0, 0 1, 1 1, 0 0))",
                            "MULTIPOINT (EMPTY, (1 2))",
                            "MULTILINESTRING (EMPTY, (0 0, 1 1))",
                            "MULTIPOLYGON (EMPTY, ((0 0, 0 1, 1 1, 0 0)))",
                            "GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING EMPTY)"};
    auto input = BinaryColumn::create();
    for (const char* value : values) input->append(value);
    auto crs = ColumnHelper::create_const_column<TYPE_VARCHAR>("EPSG:3857", input->size());
    auto type = geometry_type();
    std::unique_ptr<FunctionContext> constructor(FunctionContext::create_test_context(
            {TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH),
             TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH)},
            type));
    auto native = GeoFunctions::st_geom_from_text(constructor.get(), {input, crs}).value();

    const auto* nullable = down_cast<const NullableColumn*>(native.get());
    const auto* data = down_cast<const GeoColumn*>(nullable->data_column().get());
    EXPECT_EQ(type.geo_type.value(), data->descriptor().type);
    EXPECT_EQ(GEO_DIMENSION_XY, data->descriptor().storage.dimension);
    EXPECT_EQ(GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED, data->descriptor().storage.validation_state);

    std::unique_ptr<FunctionContext> text_serializer(FunctionContext::create_test_context(
            {type}, TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH)));
    auto text = GeoFunctions::st_geometry_as_text(text_serializer.get(), {native}).value();
    for (size_t row = 0; row < std::size(values); ++row) {
        EXPECT_EQ(values[row], text->get(row).get_slice().to_string());
    }

    std::unique_ptr<FunctionContext> wkb_serializer(FunctionContext::create_test_context(
            {type}, TypeDescriptor::create_varbinary_type(TypeDescriptor::MAX_VARCHAR_LENGTH)));
    auto wkb = GeoFunctions::st_geometry_as_wkb(wkb_serializer.get(), {native}).value();
    std::unique_ptr<FunctionContext> wkb_constructor(FunctionContext::create_test_context(
            {TypeDescriptor::create_varbinary_type(TypeDescriptor::MAX_VARCHAR_LENGTH),
             TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH)},
            type));
    auto reconstructed = GeoFunctions::st_geom_from_wkb(wkb_constructor.get(), {wkb, crs}).value();
    auto reconstructed_text = GeoFunctions::st_geometry_as_text(text_serializer.get(), {reconstructed}).value();
    for (size_t row = 0; row < std::size(values); ++row) {
        EXPECT_EQ(text->get(row).get_slice().to_string(), reconstructed_text->get(row).get_slice().to_string());
    }
}

TEST_F(geographyFunctionsTest, nativeGeometryNullInvalidAndCrsChecks) {
    auto input = BinaryColumn::create();
    input->append("POINT (1 2)");
    input->append_default();
    input->append("POINT (1)");
    auto nulls = NullColumn::create();
    nulls->append(0);
    nulls->append(1);
    nulls->append(0);
    auto source = NullableColumn::create(input, nulls);
    auto crs = ColumnHelper::create_const_column<TYPE_VARCHAR>("EPSG:3857", source->size());
    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context(
            {TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH),
             TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH)},
            geometry_type()));
    auto result = GeoFunctions::st_geom_from_text(context.get(), {source, crs}).value();
    EXPECT_FALSE(result->is_null(0));
    EXPECT_TRUE(result->is_null(1));
    EXPECT_TRUE(result->is_null(2));

    auto malformed_wkb = BinaryColumn::create();
    malformed_wkb->append("not WKB");
    auto one_crs = ColumnHelper::create_const_column<TYPE_VARCHAR>("EPSG:3857", 1);
    std::unique_ptr<FunctionContext> wkb_context(FunctionContext::create_test_context(
            {TypeDescriptor::create_varbinary_type(TypeDescriptor::MAX_VARCHAR_LENGTH),
             TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH)},
            geometry_type()));
    auto invalid_wkb = GeoFunctions::st_geom_from_wkb(wkb_context.get(), {malformed_wkb, one_crs}).value();
    EXPECT_TRUE(invalid_wkb->is_null(0));

    auto constant = geometry({"POINT (1 2)"});
    auto constant_input = ConstColumn::create(constant, 3);
    auto text = GeoFunctions::st_geometry_as_text(nullptr, {constant_input}).value();
    EXPECT_TRUE(text->is_constant());
    EXPECT_EQ(3, text->size());

    auto wrong_crs = ColumnHelper::create_const_column<TYPE_VARCHAR>("EPSG:4326", source->size());
    auto mismatch = GeoFunctions::st_geom_from_text(context.get(), {source, wrong_crs});
    ASSERT_FALSE(mismatch.ok());
    EXPECT_TRUE(mismatch.status().is_invalid_argument());

    auto varying_crs = BinaryColumn::create();
    varying_crs->append("EPSG:3857");
    varying_crs->append("EPSG:3857");
    varying_crs->append("EPSG:3857");
    auto nonconstant = GeoFunctions::st_geom_from_text(context.get(), {source, varying_crs});
    ASSERT_FALSE(nonconstant.ok());
    EXPECT_TRUE(nonconstant.status().is_invalid_argument());
}

TEST_F(geographyFunctionsTest, nativeGeoContainmentPredicates) {
    constexpr const char* polygon = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3))";
    auto polygons = geometry({polygon, polygon, polygon, polygon, polygon});
    auto points = geometry({"POINT (1 1)", "POINT (0 5)", "POINT (5 5)", "POINT (3 5)", "POINT (20 20)"});

    ColumnViewer<TYPE_BOOLEAN> contains(GeoFunctions::st_geometry_contains(nullptr, {polygons, points}).value());
    ColumnViewer<TYPE_BOOLEAN> within(GeoFunctions::st_geometry_within(nullptr, {points, polygons}).value());
    ColumnViewer<TYPE_BOOLEAN> covers(GeoFunctions::st_geometry_covers(nullptr, {polygons, points}).value());
    ColumnViewer<TYPE_BOOLEAN> covered_by(GeoFunctions::st_geometry_covered_by(nullptr, {points, polygons}).value());
    const bool strict_expected[] = {true, false, false, false, false};
    const bool inclusive_expected[] = {true, true, false, true, false};
    for (size_t row = 0; row < std::size(strict_expected); ++row) {
        EXPECT_EQ(strict_expected[row], contains.value(row));
        EXPECT_EQ(strict_expected[row], within.value(row));
        EXPECT_EQ(inclusive_expected[row], covers.value(row));
        EXPECT_EQ(inclusive_expected[row], covered_by.value(row));
    }

    auto multipolygon =
            geometry({"MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), "
                      "((10 10, 12 10, 12 12, 10 12, 10 10)))"});
    auto second_polygon_point = geometry({"POINT (11 11)"});
    ColumnViewer<TYPE_BOOLEAN> multipolygon_result(
            GeoFunctions::st_geometry_contains(nullptr, {multipolygon, second_polygon_point}).value());
    EXPECT_TRUE(multipolygon_result.value(0));

    auto constant_polygon = ConstColumn::create(geometry({polygon}), 3);
    auto constant_point = ConstColumn::create(geometry({"POINT (1 1)"}), 3);
    auto constant_result = GeoFunctions::st_geometry_covers(nullptr, {constant_polygon, constant_point}).value();
    EXPECT_TRUE(constant_result->is_constant());
    EXPECT_EQ(3, constant_result->size());
    EXPECT_TRUE(ColumnHelper::get_const_value<TYPE_BOOLEAN>(constant_result));

    auto dateline_polygon = geography({"POLYGON ((170 -10, -170 -10, -170 10, 170 10, 170 -10))"});
    auto dateline_points = geography({"POINT (179 0)"});
    ColumnViewer<TYPE_BOOLEAN> dateline_contains(
            GeoFunctions::st_geography_contains(nullptr, {dateline_polygon, dateline_points}).value());
    EXPECT_TRUE(dateline_contains.value(0));

    auto boundary_point = geography({"POINT (170 0)"});
    ColumnViewer<TYPE_BOOLEAN> boundary_contains(
            GeoFunctions::st_geography_contains(nullptr, {dateline_polygon, boundary_point}).value());
    ColumnViewer<TYPE_BOOLEAN> boundary_covers(
            GeoFunctions::st_geography_covers(nullptr, {dateline_polygon, boundary_point}).value());
    EXPECT_FALSE(boundary_contains.value(0));
    EXPECT_TRUE(boundary_covers.value(0));
}

TEST_F(geographyFunctionsTest, nativeGeometryContainmentIsTranslationInvariant) {
    constexpr const char* polygon = "POLYGON ((0 0, 1 1, 0 1, 0 0))";
    constexpr const char* translated_polygon =
            "POLYGON ((10000000 10000000, 10000001 10000001, 10000000 10000001, 10000000 10000000))";
    auto polygons = geometry({polygon, polygon, polygon, translated_polygon, translated_polygon, translated_polygon});
    // Interior, exterior, and boundary points, before and after the same EPSG:3857 translation.
    auto points = geometry({"POINT (0.4 0.5)", "POINT (0.5 0.4)", "POINT (0.5 0.5)", "POINT (10000000.4 10000000.5)",
                            "POINT (10000000.5 10000000.4)", "POINT (10000000.5 10000000.5)"});

    ColumnViewer<TYPE_BOOLEAN> contains(GeoFunctions::st_geometry_contains(nullptr, {polygons, points}).value());
    ColumnViewer<TYPE_BOOLEAN> within(GeoFunctions::st_geometry_within(nullptr, {points, polygons}).value());
    ColumnViewer<TYPE_BOOLEAN> covers(GeoFunctions::st_geometry_covers(nullptr, {polygons, points}).value());
    ColumnViewer<TYPE_BOOLEAN> covered_by(GeoFunctions::st_geometry_covered_by(nullptr, {points, polygons}).value());
    const bool strict_expected[] = {true, false, false, true, false, false};
    const bool inclusive_expected[] = {true, false, true, true, false, true};
    for (size_t row = 0; row < std::size(strict_expected); ++row) {
        EXPECT_EQ(strict_expected[row], contains.value(row)) << row;
        EXPECT_EQ(strict_expected[row], within.value(row)) << row;
        EXPECT_EQ(inclusive_expected[row], covers.value(row)) << row;
        EXPECT_EQ(inclusive_expected[row], covered_by.value(row)) << row;
    }
}

TEST_F(geographyFunctionsTest, nativeGeometryContainmentPreparedTranslationInvariant) {
    const auto type = geometry_type();
    auto polygon = ConstColumn::create(
            geometry({"POLYGON ((10000000 10000000, 10000001 10000001, 10000000 10000001, 10000000 10000000))"}), 3);
    auto points = geometry(
            {"POINT (10000000.4 10000000.5)", "POINT (10000000.5 10000000.4)", "POINT (10000000.5 10000000.5)"});
    const bool strict_expected[] = {true, false, false};
    const bool inclusive_expected[] = {true, false, true};
    for (bool polygon_first : {true, false}) {
        std::unique_ptr<FunctionContext> context(
                FunctionContext::create_test_context({type, type}, TypeDescriptor(TYPE_BOOLEAN)));
        context->set_constant_columns(polygon_first ? Columns{polygon, nullptr} : Columns{nullptr, polygon});
        ASSERT_TRUE(GeoFunctions::native_geo_containment_prepare(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
        auto strict_result = polygon_first ? GeoFunctions::st_geometry_contains(context.get(), {polygon, points})
                                           : GeoFunctions::st_geometry_within(context.get(), {points, polygon});
        auto inclusive_result = polygon_first ? GeoFunctions::st_geometry_covers(context.get(), {polygon, points})
                                              : GeoFunctions::st_geometry_covered_by(context.get(), {points, polygon});
        ASSERT_TRUE(strict_result.ok()) << strict_result.status();
        ASSERT_TRUE(inclusive_result.ok()) << inclusive_result.status();
        ColumnViewer<TYPE_BOOLEAN> strict(*strict_result);
        ColumnViewer<TYPE_BOOLEAN> inclusive(*inclusive_result);
        for (size_t row = 0; row < std::size(strict_expected); ++row) {
            EXPECT_EQ(strict_expected[row], strict.value(row)) << "row=" << row << " polygon_first=" << polygon_first;
            EXPECT_EQ(inclusive_expected[row], inclusive.value(row))
                    << "row=" << row << " polygon_first=" << polygon_first;
        }
        ASSERT_TRUE(GeoFunctions::native_geo_containment_close(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    }
}

TEST_F(geographyFunctionsTest, nativeGeoContainmentConstantPreparation) {
    const auto verify = [](bool spherical, const ColumnPtr& polygons, const ColumnPtr& points,
                           const std::vector<std::optional<bool>>& strict_expected,
                           const std::vector<std::optional<bool>>& inclusive_expected) {
        ASSERT_EQ(strict_expected.size(), polygons->size());
        ASSERT_EQ(strict_expected.size(), points->size());
        ASSERT_EQ(strict_expected.size(), inclusive_expected.size());

        auto contains_result = spherical ? GeoFunctions::st_geography_contains(nullptr, {polygons, points})
                                         : GeoFunctions::st_geometry_contains(nullptr, {polygons, points});
        auto within_result = spherical ? GeoFunctions::st_geography_within(nullptr, {points, polygons})
                                       : GeoFunctions::st_geometry_within(nullptr, {points, polygons});
        auto covers_result = spherical ? GeoFunctions::st_geography_covers(nullptr, {polygons, points})
                                       : GeoFunctions::st_geometry_covers(nullptr, {polygons, points});
        auto covered_by_result = spherical ? GeoFunctions::st_geography_covered_by(nullptr, {points, polygons})
                                           : GeoFunctions::st_geometry_covered_by(nullptr, {points, polygons});
        ASSERT_TRUE(contains_result.ok()) << contains_result.status();
        ASSERT_TRUE(within_result.ok()) << within_result.status();
        ASSERT_TRUE(covers_result.ok()) << covers_result.status();
        ASSERT_TRUE(covered_by_result.ok()) << covered_by_result.status();
        ColumnViewer<TYPE_BOOLEAN> contains(*contains_result);
        ColumnViewer<TYPE_BOOLEAN> within(*within_result);
        ColumnViewer<TYPE_BOOLEAN> covers(*covers_result);
        ColumnViewer<TYPE_BOOLEAN> covered_by(*covered_by_result);
        for (size_t row = 0; row < strict_expected.size(); ++row) {
            EXPECT_EQ(!strict_expected[row].has_value(), contains.is_null(row));
            EXPECT_EQ(!strict_expected[row].has_value(), within.is_null(row));
            EXPECT_EQ(!inclusive_expected[row].has_value(), covers.is_null(row));
            EXPECT_EQ(!inclusive_expected[row].has_value(), covered_by.is_null(row));
            if (strict_expected[row].has_value()) {
                EXPECT_EQ(*strict_expected[row], contains.value(row));
                EXPECT_EQ(*strict_expected[row], within.value(row));
            }
            if (inclusive_expected[row].has_value()) {
                EXPECT_EQ(*inclusive_expected[row], covers.value(row));
                EXPECT_EQ(*inclusive_expected[row], covered_by.value(row));
            }
        }
    };

    constexpr const char* polygon = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3))";
    auto varying_points = geometry({"POINT (1 1)", "POINT (0 5)", "POINT (5 5)", "POINT (20 20)", nullptr});
    auto constant_polygon = ConstColumn::create(geometry({polygon}), varying_points->size());
    verify(false, constant_polygon, varying_points, {true, false, false, false, std::nullopt},
           {true, true, false, false, std::nullopt});

    auto varying_polygons = geometry({polygon, "POLYGON ((20 20, 30 20, 30 30, 20 30, 20 20))", nullptr});
    auto constant_point = ConstColumn::create(geometry({"POINT (1 1)"}), varying_polygons->size());
    verify(false, varying_polygons, constant_point, {true, false, std::nullopt}, {true, false, std::nullopt});
    auto both_constant_polygon = ConstColumn::create(geometry({polygon}), 3);
    auto both_constant_point = ConstColumn::create(geometry({"POINT (1 1)"}), 3);
    verify(false, both_constant_polygon, both_constant_point, {true, true, true}, {true, true, true});

    constexpr const char* dateline_polygon = "POLYGON ((170 -10, -170 -10, -170 10, 170 10, 170 -10))";
    auto geography_points = geography({"POINT (179 0)", "POINT (170 0)", "POINT (0 0)", nullptr});
    auto constant_geography_polygon = ConstColumn::create(geography({dateline_polygon}), geography_points->size());
    verify(true, constant_geography_polygon, geography_points, {true, false, false, std::nullopt},
           {true, true, false, std::nullopt});

    auto geography_polygons =
            geography({dateline_polygon, "POLYGON ((-20 -10, 20 -10, 20 10, -20 10, -20 -10))", nullptr});
    auto constant_geography_point = ConstColumn::create(geography({"POINT (179 0)"}), geography_polygons->size());
    verify(true, geography_polygons, constant_geography_point, {true, false, std::nullopt},
           {true, false, std::nullopt});

    auto both_constant_geography_polygon = ConstColumn::create(geography({dateline_polygon}), 3);
    auto both_constant_geography_point = ConstColumn::create(geography({"POINT (179 0)"}), 3);
    verify(true, both_constant_geography_polygon, both_constant_geography_point, {true, true, true},
           {true, true, true});

    auto varying_geography_polygons = geography({dateline_polygon, dateline_polygon});
    auto varying_geography_points = geography({"POINT (179 0)", "POINT (0 0)"});
    verify(true, varying_geography_polygons, varying_geography_points, {true, false}, {true, false});

    constexpr const char* polar_polygon = "POLYGON ((-10 85, 10 85, 10 89, -10 89, -10 85))";
    auto polar_points = geography({"POINT (0 87)", "POINT (30 87)"});
    auto constant_polar_polygon = ConstColumn::create(geography({polar_polygon}), polar_points->size());
    verify(true, constant_polar_polygon, polar_points, {true, false}, {true, false});
}

TEST_F(geographyFunctionsTest, nativeGeoContainmentUsesPrepareCloseLifecycle) {
    const auto verify = [](bool spherical) {
        const auto geo_type = spherical ? geography_type() : geometry_type();
        auto polygon_data = spherical ? geography({"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"})
                                      : geometry({"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"});
        auto constant_polygon = ConstColumn::create(polygon_data, 2);
        std::unique_ptr<FunctionContext> context(
                FunctionContext::create_test_context({geo_type, geo_type}, TypeDescriptor(TYPE_BOOLEAN)));
        context->set_constant_columns({constant_polygon, nullptr});

        ASSERT_TRUE(GeoFunctions::native_geo_containment_prepare(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
        const auto* prepared_state = context->get_function_state(FunctionContext::FRAGMENT_LOCAL);
        ASSERT_NE(nullptr, prepared_state);

        auto first_points =
                spherical ? geography({"POINT (1 1)", "POINT (20 20)"}) : geometry({"POINT (1 1)", "POINT (20 20)"});
        auto first_result =
                spherical ? GeoFunctions::st_geography_contains(context.get(), {constant_polygon, first_points})
                          : GeoFunctions::st_geometry_contains(context.get(), {constant_polygon, first_points});
        ASSERT_TRUE(first_result.ok()) << first_result.status();
        ColumnViewer<TYPE_BOOLEAN> first(*first_result);
        EXPECT_TRUE(first.value(0));
        EXPECT_FALSE(first.value(1));

        auto second_points =
                spherical ? geography({"POINT (0 5)", "POINT (5 5)"}) : geometry({"POINT (0 5)", "POINT (5 5)"});
        auto second_result =
                spherical ? GeoFunctions::st_geography_covers(context.get(), {constant_polygon, second_points})
                          : GeoFunctions::st_geometry_covers(context.get(), {constant_polygon, second_points});
        ASSERT_TRUE(second_result.ok()) << second_result.status();
        EXPECT_EQ(prepared_state, context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        ColumnViewer<TYPE_BOOLEAN> second(*second_result);
        EXPECT_TRUE(second.value(0));
        EXPECT_TRUE(second.value(1));

        ASSERT_TRUE(GeoFunctions::native_geo_containment_close(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
        EXPECT_EQ(nullptr, context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    };

    verify(false);
    verify(true);
}

TEST_F(geographyFunctionsTest, nativeGeoContainmentKeepsChunkOnlyConstantsBatchLocal) {
    const auto geo_type = geography_type();
    constexpr const char* containing_polygon = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";

    {
        auto prepared_polygon = ConstColumn::create(geography({containing_polygon}), 1);
        std::unique_ptr<FunctionContext> context(
                FunctionContext::create_test_context({geo_type, geo_type}, TypeDescriptor(TYPE_BOOLEAN)));
        context->set_constant_columns({prepared_polygon, nullptr});
        ASSERT_TRUE(GeoFunctions::native_geo_containment_prepare(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());

        auto first_point = ConstColumn::create(geography({"POINT (1 1)"}), 1);
        auto first_result = GeoFunctions::st_geography_contains(context.get(), {prepared_polygon, first_point});
        ASSERT_TRUE(first_result.ok()) << first_result.status();
        EXPECT_TRUE(ColumnHelper::get_const_value<TYPE_BOOLEAN>(*first_result));

        auto second_point = ConstColumn::create(geography({"POINT (20 20)"}), 1);
        auto second_result = GeoFunctions::st_geography_contains(context.get(), {prepared_polygon, second_point});
        ASSERT_TRUE(second_result.ok()) << second_result.status();
        EXPECT_FALSE(ColumnHelper::get_const_value<TYPE_BOOLEAN>(*second_result));

        ASSERT_TRUE(GeoFunctions::native_geo_containment_close(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    }

    {
        auto prepared_point = ConstColumn::create(geography({"POINT (1 1)"}), 1);
        std::unique_ptr<FunctionContext> context(
                FunctionContext::create_test_context({geo_type, geo_type}, TypeDescriptor(TYPE_BOOLEAN)));
        context->set_constant_columns({nullptr, prepared_point});
        ASSERT_TRUE(GeoFunctions::native_geo_containment_prepare(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());

        auto first_polygon = ConstColumn::create(geography({containing_polygon}), 1);
        auto first_result = GeoFunctions::st_geography_contains(context.get(), {first_polygon, prepared_point});
        ASSERT_TRUE(first_result.ok()) << first_result.status();
        EXPECT_TRUE(ColumnHelper::get_const_value<TYPE_BOOLEAN>(*first_result));

        auto second_polygon = ConstColumn::create(geography({"MULTIPOLYGON (((20 20, 30 20, 30 30, 20 30, 20 20)), "
                                                             "((40 40, 50 40, 50 50, 40 50, 40 40)))"}),
                                                  1);
        auto second_result = GeoFunctions::st_geography_contains(context.get(), {second_polygon, prepared_point});
        ASSERT_TRUE(second_result.ok()) << second_result.status();
        EXPECT_FALSE(ColumnHelper::get_const_value<TYPE_BOOLEAN>(*second_result));

        ASSERT_TRUE(GeoFunctions::native_geo_containment_close(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    }
}

TEST_F(geographyFunctionsTest, nativeGeoContainmentPreparesOnlyNullConstant) {
    const auto verify = [](bool spherical) {
        const auto geo_type = spherical ? geography_type() : geometry_type();
        auto constant_null = ColumnHelper::create_const_null_column(2);
        std::unique_ptr<FunctionContext> context(
                FunctionContext::create_test_context({geo_type, geo_type}, TypeDescriptor(TYPE_BOOLEAN)));
        context->set_constant_columns({constant_null, nullptr});
        ASSERT_TRUE(GeoFunctions::native_geo_containment_prepare(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());

        auto points = spherical ? geography({"POINT (1 1)", "POINT (2 2)"}) : geometry({"POINT (1 1)", "POINT (2 2)"});
        auto result = spherical ? GeoFunctions::st_geography_contains(context.get(), {constant_null, points})
                                : GeoFunctions::st_geometry_contains(context.get(), {constant_null, points});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_TRUE((*result)->only_null());
        EXPECT_EQ(2, (*result)->size());

        ASSERT_TRUE(GeoFunctions::native_geo_containment_close(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    };

    verify(false);
    verify(true);
}

TEST_F(geographyFunctionsTest, nativeGeoContainmentPreparesSphericalComponentsLazily) {
    constexpr const char* invalid_second_component =
            "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 20 20, 20 20, 20 20)))";
    auto boundary_and_null = geography({"POINT (0 5)", nullptr});
    auto constant_polygon = ConstColumn::create(geography({invalid_second_component}), boundary_and_null->size());
    const auto geography_type_descriptor = geography_type();
    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context(
            {geography_type_descriptor, geography_type_descriptor}, TypeDescriptor(TYPE_BOOLEAN)));
    context->set_constant_columns({constant_polygon, nullptr});
    ASSERT_TRUE(GeoFunctions::native_geo_containment_prepare(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());

    auto early_result = GeoFunctions::st_geography_covers(context.get(), {constant_polygon, boundary_and_null});
    ASSERT_TRUE(early_result.ok()) << early_result.status();
    ColumnViewer<TYPE_BOOLEAN> early(*early_result);
    EXPECT_TRUE(early.value(0));
    EXPECT_TRUE(early.is_null(1));

    auto outside = geography({"POINT (40 40)"});
    auto one_constant_polygon = ConstColumn::create(geography({invalid_second_component}), 1);
    auto invalid_topology = GeoFunctions::st_geography_covers(context.get(), {one_constant_polygon, outside});
    ASSERT_FALSE(invalid_topology.ok());
    EXPECT_TRUE(invalid_topology.status().is_invalid_argument());
    ASSERT_TRUE(GeoFunctions::native_geo_containment_close(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());

    auto valid_polygon = ConstColumn::create(geography({"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"}), 1);
    auto fresh_batch = GeoFunctions::st_geography_contains(nullptr, {valid_polygon, geography({"POINT (1 1)"})});
    ASSERT_TRUE(fresh_batch.ok()) << fresh_batch.status();
    ColumnViewer<TYPE_BOOLEAN> fresh(*fresh_batch);
    EXPECT_TRUE(fresh.value(0));
}

TEST_F(geographyFunctionsTest, nativeGeoContainmentNullEmptyAndRejection) {
    auto polygon = geometry({"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"});
    auto point = geometry({"POINT (1 1)"});

    auto null_result = GeoFunctions::st_geometry_contains(nullptr, {polygon, geometry({nullptr})}).value();
    EXPECT_TRUE(null_result->is_null(0));
    auto empty_result = GeoFunctions::st_geometry_contains(nullptr, {geometry({"POLYGON EMPTY"}), point}).value();
    ColumnViewer<TYPE_BOOLEAN> empty(empty_result);
    EXPECT_FALSE(empty.value(0));

    auto incompatible = GeoFunctions::st_geometry_contains(
            nullptr, {polygon, geometry({"POINT (1 1)"}, geometry_type("EPSG:4326", 4326))});
    ASSERT_FALSE(incompatible.ok());
    EXPECT_TRUE(incompatible.status().is_invalid_argument());

    auto unsupported = GeoFunctions::st_geometry_contains(
            nullptr, {geometry({"GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)))"}), point});
    ASSERT_FALSE(unsupported.ok());
    EXPECT_TRUE(unsupported.status().is_invalid_argument());

    auto mixed_kind = GeoFunctions::st_geometry_contains(nullptr, {polygon, geography({"POINT (1 1)"})});
    ASSERT_FALSE(mixed_kind.ok());
    EXPECT_TRUE(mixed_kind.status().is_not_supported());
}

TEST_F(geographyFunctionsTest, nativeGeoIntersectsPredicates) {
    auto planar_left = geometry({"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))", "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
                                 "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))", "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
                                 "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
                                 "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3))",
                                 "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 20)))"});
    auto planar_right =
            geometry({"POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))", "POLYGON ((10 0, 20 0, 20 10, 10 10, 10 0))",
                      "POLYGON ((10 10, 12 10, 12 12, 10 12, 10 10))", "POLYGON ((20 20, 30 20, 30 30, 20 30, 20 20))",
                      "POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))", "POLYGON ((4 4, 6 4, 6 6, 4 6, 4 4))",
                      "POLYGON ((25 25, 26 25, 26 26, 25 26, 25 25))"});
    ColumnViewer<TYPE_BOOLEAN> planar(
            GeoFunctions::st_geometry_intersects(nullptr, {planar_left, planar_right}).value());
    ColumnViewer<TYPE_BOOLEAN> planar_reverse(
            GeoFunctions::st_geometry_intersects(nullptr, {planar_right, planar_left}).value());
    const bool expected[] = {true, true, true, false, true, false, true};
    for (size_t i = 0; i < std::size(expected); ++i) {
        EXPECT_EQ(expected[i], planar.value(i)) << i;
        EXPECT_EQ(expected[i], planar_reverse.value(i)) << i;
    }

    auto spherical_left =
            geography({"POLYGON ((170 -10, -170 -10, -170 10, 170 10, 170 -10))",
                       "POLYGON ((170 -10, -170 -10, -170 10, 170 10, 170 -10))",
                       "POLYGON ((170 -10, -170 -10, -170 10, 170 10, 170 -10))",
                       "POLYGON ((-45 80, 45 80, 135 80, -135 80, -45 80))",
                       "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3))",
                       "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 20)))",
                       "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"});
    auto spherical_right =
            geography({"POLYGON ((175 -5, -175 -5, -175 5, 175 5, 175 -5))",
                       "POLYGON ((-170 -10, -160 -10, -160 10, -170 10, -170 -10))",
                       "POLYGON ((-120 -5, -110 -5, -110 5, -120 5, -120 -5))",
                       "POLYGON ((-20 85, 20 85, 20 88, -20 88, -20 85))", "POLYGON ((4 4, 6 4, 6 6, 4 6, 4 4))",
                       "POLYGON ((25 25, 26 25, 26 26, 25 26, 25 25))", "POLYGON ((10 5, 15 4, 15 6, 10 5))"});
    ColumnViewer<TYPE_BOOLEAN> spherical(
            GeoFunctions::st_geography_intersects(nullptr, {spherical_left, spherical_right}).value());
    EXPECT_TRUE(spherical.value(0));
    EXPECT_TRUE(spherical.value(1));
    EXPECT_FALSE(spherical.value(2));
    EXPECT_TRUE(spherical.value(3));
    EXPECT_FALSE(spherical.value(4));
    EXPECT_TRUE(spherical.value(5));
    EXPECT_TRUE(spherical.value(6));
}

namespace intersects_reference {
// Frozen from main 8ed24335179f: no bounding-box pruning or index.
enum class PointPolygonRelation { OUTSIDE, BOUNDARY, INSIDE };

constexpr long double kPlanarBoundaryUlps = 32;

bool planar_point_on_segment(const WkbCoordinate& point, const WkbCoordinate& lhs, const WkbCoordinate& rhs) {
    const long double abx = static_cast<long double>(rhs.x) - lhs.x;
    const long double aby = static_cast<long double>(rhs.y) - lhs.y;
    const long double apx = static_cast<long double>(point.x) - lhs.x;
    const long double apy = static_cast<long double>(point.y) - lhs.y;
    const long double cross_lhs = abx * apy;
    const long double cross_rhs = aby * apx;
    const long double cross_scale = std::abs(cross_lhs) + std::abs(cross_rhs);
    const long double cross_tolerance = kPlanarBoundaryUlps * std::numeric_limits<double>::epsilon() * cross_scale;
    if (std::abs(cross_lhs - cross_rhs) > cross_tolerance) return false;

    const long double coordinate_scale = std::max({std::abs(abx), std::abs(aby), std::abs(apx), std::abs(apy)});
    const long double coordinate_tolerance =
            kPlanarBoundaryUlps * std::numeric_limits<double>::epsilon() * coordinate_scale;
    return apx >= std::min(0.0L, abx) - coordinate_tolerance && apx <= std::max(0.0L, abx) + coordinate_tolerance &&
           apy >= std::min(0.0L, aby) - coordinate_tolerance && apy <= std::max(0.0L, aby) + coordinate_tolerance;
}

PointPolygonRelation planar_ring_relation(const std::vector<WkbCoordinate>& ring, const WkbCoordinate& point) {
    bool inside = false;
    for (size_t i = 1; i < ring.size(); ++i) {
        const auto& lhs = ring[i - 1];
        const auto& rhs = ring[i];
        if (planar_point_on_segment(point, lhs, rhs)) return PointPolygonRelation::BOUNDARY;
        if ((lhs.y > point.y) != (rhs.y > point.y)) {
            const long double intersection_offset =
                    (static_cast<long double>(lhs.x) - point.x) + (static_cast<long double>(point.y) - lhs.y) *
                                                                          (static_cast<long double>(rhs.x) - lhs.x) /
                                                                          (static_cast<long double>(rhs.y) - lhs.y);
            if (intersection_offset > 0) inside = !inside;
        }
    }
    return inside ? PointPolygonRelation::INSIDE : PointPolygonRelation::OUTSIDE;
}

PointPolygonRelation planar_polygon_relation(const WkbGeometry& polygon, const WkbCoordinate& point) {
    auto relation = planar_ring_relation(polygon.rings[0], point);
    if (relation != PointPolygonRelation::INSIDE) return relation;
    for (size_t i = 1; i < polygon.rings.size(); ++i) {
        relation = planar_ring_relation(polygon.rings[i], point);
        if (relation == PointPolygonRelation::BOUNDARY) return relation;
        if (relation == PointPolygonRelation::INSIDE) return PointPolygonRelation::OUTSIDE;
    }
    return PointPolygonRelation::INSIDE;
}

PointPolygonRelation planar_polygon_family_relation(const WkbGeometry& polygon, const WkbCoordinate& point) {
    if (polygon.type == WkbGeometryType::POLYGON) return planar_polygon_relation(polygon, point);
    PointPolygonRelation result = PointPolygonRelation::OUTSIDE;
    for (const auto& child : polygon.children) {
        if (child.empty) continue;
        const auto relation = planar_polygon_relation(child, point);
        if (relation == PointPolygonRelation::BOUNDARY) return relation;
        if (relation == PointPolygonRelation::INSIDE) result = relation;
    }
    return result;
}

ALWAYS_NOINLINE int exact_planar_orientation(const WkbCoordinate& a, const WkbCoordinate& b, const WkbCoordinate& c) {
    // Finite binary64 coordinates scaled by 2^1074 have magnitude <2^2098.
    // Differences need at most 2099 bits and the determinant 4199, so
    // 4224 fixed bits suffice without heap allocations.
    using Integer = boost::multiprecision::number<
            boost::multiprecision::cpp_int_backend<4224, 4224, boost::multiprecision::signed_magnitude,
                                                   boost::multiprecision::unchecked, void>,
            boost::multiprecision::et_off>;
    const auto scaled = [](double value) -> Integer {
        const uint64_t bits = std::bit_cast<uint64_t>(value);
        const uint64_t exponent = (bits >> 52) & 0x7ff;
        Integer result = bits & ((uint64_t{1} << 52) - 1);
        if (exponent != 0) {
            result += uint64_t{1} << 52;
            result <<= exponent - 1;
        }
        return bits >> 63 ? -result : result;
    };
    const Integer ax = scaled(a.x), ay = scaled(a.y);
    const Integer bx = scaled(b.x), by = scaled(b.y);
    const Integer cx = scaled(c.x), cy = scaled(c.y);
    const Integer determinant = (bx - ax) * (cy - ay) - (by - ay) * (cx - ax);
    return (determinant > 0) - (determinant < 0);
}

int planar_orientation(const WkbCoordinate& a, const WkbCoordinate& b, const WkbCoordinate& c) {
    const long double left = (static_cast<long double>(b.x) - a.x) * (static_cast<long double>(c.y) - a.y);
    const long double right = (static_cast<long double>(b.y) - a.y) * (static_cast<long double>(c.x) - a.x);
    const long double determinant = left - right;
    // Eight epsilons bound the rounding of differences, products and subtraction.
    constexpr long double kErrorBound = 8 * std::numeric_limits<long double>::epsilon();
    const long double error = kErrorBound * (std::abs(left) + std::abs(right));
    bool certain = std::abs(determinant) > error;
    // A wide long double exponent range also covers the smallest binary64
    // product's error bound. Narrower platforms must check for underflow.
    if constexpr (std::numeric_limits<long double>::min_exponent >
                  2 * (std::numeric_limits<double>::min_exponent - std::numeric_limits<double>::digits) -
                          std::numeric_limits<long double>::digits) {
        certain &= error >= std::numeric_limits<long double>::min();
    }
    if (!certain) [[unlikely]] return exact_planar_orientation(a, b, c);
    return determinant > 0 ? 1 : -1;
}

bool planar_segments_intersect(const WkbCoordinate& a, const WkbCoordinate& b, const WkbCoordinate& c,
                               const WkbCoordinate& d) {
    if (planar_point_on_segment(a, c, d) || planar_point_on_segment(b, c, d) || planar_point_on_segment(c, a, b) ||
        planar_point_on_segment(d, a, b)) {
        return true;
    }
    const int abc = planar_orientation(a, b, c);
    const int abd = planar_orientation(a, b, d);
    if (abc * abd >= 0) return false;
    return planar_orientation(c, d, a) * planar_orientation(c, d, b) < 0;
}

bool planar_polygon_intersects(const WkbGeometry& lhs, const WkbGeometry& rhs) {
    for (const auto& lhs_ring : lhs.rings) {
        for (const auto& rhs_ring : rhs.rings) {
            for (size_t i = 1; i < lhs_ring.size(); ++i) {
                for (size_t j = 1; j < rhs_ring.size(); ++j) {
                    if (planar_segments_intersect(lhs_ring[i - 1], lhs_ring[i], rhs_ring[j - 1], rhs_ring[j])) {
                        return true;
                    }
                }
            }
        }
    }
    return planar_polygon_relation(lhs, rhs.rings[0][0]) != PointPolygonRelation::OUTSIDE ||
           planar_polygon_relation(rhs, lhs.rings[0][0]) != PointPolygonRelation::OUTSIDE;
}

bool planar_polygon_family_intersects(const WkbGeometry& lhs, const WkbGeometry& rhs) {
    const size_t lhs_count = lhs.type == WkbGeometryType::POLYGON ? 1 : lhs.children.size();
    const size_t rhs_count = rhs.type == WkbGeometryType::POLYGON ? 1 : rhs.children.size();
    for (size_t i = 0; i < lhs_count; ++i) {
        const auto& lhs_polygon = lhs.type == WkbGeometryType::POLYGON ? lhs : lhs.children[i];
        if (lhs_polygon.empty) continue;
        for (size_t j = 0; j < rhs_count; ++j) {
            const auto& rhs_polygon = rhs.type == WkbGeometryType::POLYGON ? rhs : rhs.children[j];
            if (!rhs_polygon.empty && planar_polygon_intersects(lhs_polygon, rhs_polygon)) return true;
        }
    }
    return false;
}

} // namespace intersects_reference

namespace {
WkbGeometry intersects_test_polygon(size_t vertices, double x, double y, double radius, bool concave = false) {
    WkbGeometry polygon;
    polygon.type = WkbGeometryType::POLYGON;
    auto& ring = polygon.rings.emplace_back();
    for (size_t i = 0; i < vertices; ++i) {
        const double angle = 2 * std::acos(-1.0) * i / vertices;
        const double r = concave && i % 2 ? radius / 3 : radius;
        ring.push_back({x + r * std::cos(angle), y + r * std::sin(angle)});
    }
    ring.push_back(ring[0]);
    return polygon;
}

void check_intersects_equivalence(const WkbGeometry& a, const WkbGeometry& b, const TypeDescriptor& type) {
    const bool expected = intersects_reference::planar_polygon_family_intersects(a, b);
    Columns inputs;
    for (const auto* polygon : {&a, &b}) {
        std::string bytes;
        ASSERT_TRUE(WkbCodec::to_wkb(*polygon, &bytes, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
        auto column = GeoColumn::create(
                GeoColumnDescriptor{type.geo_type.value(),
                                    {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}});
        column->append_wkb(Slice(bytes));
        inputs.emplace_back(std::move(column));
    }
    for (int form = 0; form < 4; ++form) {
        Columns columns = {form & 1 ? ConstColumn::create(inputs[0], 1) : inputs[0],
                           form & 2 ? ConstColumn::create(inputs[1], 1) : inputs[1]};
        for (int direction = 0; direction < 2; ++direction) {
            auto result = GeoFunctions::st_geometry_intersects(nullptr, columns);
            ASSERT_TRUE(result.ok()) << result.status();
            ColumnViewer<TYPE_BOOLEAN> values(*result);
            ASSERT_FALSE(values.is_null(0));
            EXPECT_EQ(expected, values.value(0)) << form << ":" << direction;
            std::swap(columns[0], columns[1]);
        }
    }
}
} // namespace

TEST_F(geographyFunctionsTest, nativeGeometryIntersectsMatchesFixedBruteForce) {
    std::mt19937_64 random(81020);
    for (size_t vertices : {4, 7, 8, 9, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257}) {
        for (bool concave : {false, true}) {
            for (double offset : {0.0, 1e7}) {
                const double jitter = static_cast<double>(random() % 1024) / 8192;
                auto lhs = intersects_test_polygon(vertices, offset, offset, 1, concave);
                for (double shift : {0.0, 0.5, 1.8, 3.0}) {
                    auto rhs =
                            intersects_test_polygon(vertices + 1, offset + shift + jitter, offset + 0.1, 1, !concave);
                    check_intersects_equivalence(lhs, rhs, geometry_type());
                }
            }
        }
    }
}

TEST_F(geographyFunctionsTest, nativeGeometryIntersectsLazySetupKeepsFirstProbe) {
    // Products below, at and above the calibrated 64-pair boundary; same-size and asymmetric rings.
    for (size_t vertices : {7, 8, 9}) {
        const auto lhs = intersects_test_polygon(vertices, 0, 0, 1, false);
        for (size_t rhs_vertices : {vertices, vertices + 1}) {
            for (double shift : {0.0, 0.3, 2.0, 3.0}) {
                const auto rhs = intersects_test_polygon(rhs_vertices, shift, 0, 1, false);
                check_intersects_equivalence(lhs, rhs, geometry_type());
            }
        }
    }
}

TEST_F(geographyFunctionsTest, nativeGeometryIntersectsIndexThresholdsAndScratchReuse) {
    const auto type = geometry_type();
    auto left = GeoColumn::create(GeoColumnDescriptor{
            type.geo_type.value(), {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}});
    auto right = GeoColumn::create(left->descriptor());
    std::vector<bool> expected;
    // Around the 128-edge threshold and the 256-pair budget (first probe skips its already checked pair).
    for (size_t n : {127, 128, 129, 255, 256, 257}) {
        auto a = intersects_test_polygon(n, 0, 0, 2);
        for (double shift : {0.0, 1.7, 4.5}) {
            auto b = intersects_test_polygon(129, shift, 0.1, 0.5, true);
            check_intersects_equivalence(a, b, type);
            for (size_t side = 0; side < 2; ++side) {
                std::string bytes;
                ASSERT_TRUE(
                        WkbCodec::to_wkb(side == 0 ? a : b, &bytes, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
                (side == 0 ? left : right)->append_wkb(Slice(bytes));
            }
            expected.push_back(intersects_reference::planar_polygon_family_intersects(a, b));
        }
    }
    auto result = GeoFunctions::st_geometry_intersects(nullptr, {left, right});
    ASSERT_TRUE(result.ok()) << result.status();
    ColumnViewer<TYPE_BOOLEAN> values(*result);
    ASSERT_EQ(expected.size(), (*result)->size());
    for (size_t row = 0; row < expected.size(); ++row) EXPECT_EQ(expected[row], values.value(row)) << row;
    auto shell = intersects_test_polygon(256, 0, 0, 2);
    shell.rings.push_back(intersects_test_polygon(128, 0, 0, 0.7).rings[0]);
    check_intersects_equivalence(shell, intersects_test_polygon(129, 0, 0, 0.3), type);
    WkbGeometry multi;
    multi.type = WkbGeometryType::MULTIPOLYGON;
    multi.children = {shell, intersects_test_polygon(257, 8, 0, 1)};
    check_intersects_equivalence(multi, intersects_test_polygon(129, 8, 0, 0.3), type);
}

TEST_F(geographyFunctionsTest, nativeGeometryIntersectsPruningKeepsHolesAndComponents) {
    const char* left[] = {"POLYGON ((0 0,10 0,10 10,0 10,0 0),(3 3,7 3,7 7,3 7,3 3))",
                          "MULTIPOLYGON (EMPTY,((0 0,2 0,2 2,0 2,0 0)),((20 20,30 20,30 30,20 30,20 20)))",
                          "POLYGON ((0 0,1 0,1 0,1 1,0 1,0 0))",
                          // Existing compute accepts a ring outside its shell; pruning must include every ring.
                          "POLYGON ((0 0,1 0,1 1,0 1,0 0),(10 10,12 10,12 12,10 12,10 10))"};
    const char* right[] = {"POLYGON ((4 4,6 4,6 6,4 6,4 4))", "POLYGON ((25 25,26 25,26 26,25 26,25 25))",
                           "POLYGON ((1 0,2 0,2 1,1 1,1 0))", "POLYGON ((11 9,11.5 9,11.5 11,11 11,11 9))"};
    for (size_t i = 0; i < std::size(left); ++i) {
        WkbGeometry a, b;
        ASSERT_TRUE(WkbCodec::parse_wkt(left[i], &a, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
        ASSERT_TRUE(WkbCodec::parse_wkt(right[i], &b, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
        check_intersects_equivalence(a, b, geometry_type());
    }
}

TEST_F(geographyFunctionsTest, nativeGeometryIntersectsPaddingFallsBackForExtremeCoordinates) {
    const double max = std::numeric_limits<double>::max();
    WkbGeometry a, b;
    a.type = b.type = WkbGeometryType::POLYGON;
    // The union span exceeds DBL_MAX: pruning must fall back even on wide-long-double hosts.
    a.rings = {{{-max, -2}, {-max / 2, -2}, {-max / 2, -1}, {-max, -2}}};
    b.rings = {{{max / 2, 1}, {max, 1}, {max / 2, 2}, {max / 2, 1}}};
    check_intersects_equivalence(a, b, geometry_type());
    // Finite span: max/2 makes the padded max overflow; the adjacent-double case remains finite.
    for (double near : {std::nextafter(max, 0.0), max / 2}) {
        a.rings = {{{near, 0}, {max, 0}, {max, 1}, {near, 0}}};
        b.rings = {{{near, 3}, {max, 3}, {near, 4}, {near, 3}}};
        check_intersects_equivalence(a, b, geometry_type());
    }
    for (int exponent : {-1070, -1000, -300, 300, 990}) {
        const double scale = std::ldexp(1.0, exponent);
        a.rings = {{{0, 0}, {scale, scale}, {0, scale}, {0, 0}}};
        for (int ulps : {0, 1, 16, 32, 64}) {
            const double p = scale * (1 + ulps * std::numeric_limits<double>::epsilon());
            b.rings = {{{p, p}, {2 * scale, p}, {p, 2 * scale}, {p, p}}};
            check_intersects_equivalence(a, b, geometry_type());
        }
    }
}

TEST_F(geographyFunctionsTest, nativeGeometryIntersectsFiltersRoundedOrientation) {
    // PostGIS 3.6.4: SELECT ST_Intersects(ST_GeomFromText(lhs), ST_GeomFromText(rhs)) is false.
    // Multiplication by powers of two preserves the counterexample, even below unit coordinates.
    constexpr const char* wkts[] = {"POLYGON ((0 0,1048576 1048577,0 1,0 0))",
                                    "POLYGON ((2097151.999999925 2097153.999999925,3145727.999999865 3145730.999999865,"
                                    "2097151.999999925 2097154.999999925,2097151.999999925 2097153.999999925))"};
    for (int exponent : {-1000, -300, -22, 0, 300, 990}) {
        for (double offset : {0.0, 1e7}) {
            if (offset != 0 && exponent != 0) continue;
            for (int reflection : {1, -1}) {
                Columns inputs;
                for (const auto* wkt : wkts) {
                    WkbGeometry polygon;
                    ASSERT_TRUE(WkbCodec::parse_wkt(wkt, &polygon, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
                    for (auto& ring : polygon.rings) {
                        for (auto& point : ring) {
                            point.x = reflection * std::ldexp(point.x, exponent) + offset;
                            point.y = std::ldexp(point.y, exponent) + offset;
                        }
                    }
                    std::string bytes;
                    ASSERT_TRUE(WkbCodec::to_wkb(polygon, &bytes, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
                    auto column = GeoColumn::create(GeoColumnDescriptor{
                            geometry_type().geo_type.value(),
                            {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}});
                    column->append_wkb(Slice(bytes));
                    inputs.emplace_back(std::move(column));
                }
                for (int form = 0; form < 4; ++form) {
                    Columns columns = {form & 1 ? ConstColumn::create(inputs[0], 1) : inputs[0],
                                       form & 2 ? ConstColumn::create(inputs[1], 1) : inputs[1]};
                    auto forward = GeoFunctions::st_geometry_intersects(nullptr, columns);
                    ASSERT_TRUE(forward.ok()) << forward.status();
                    ColumnViewer<TYPE_BOOLEAN> result(forward.value());
                    EXPECT_FALSE(result.value(0)) << exponent << ":" << reflection << ":" << offset << ":" << form;
                    std::swap(columns[0], columns[1]);
                    auto reverse = GeoFunctions::st_geometry_intersects(nullptr, columns);
                    ASSERT_TRUE(reverse.ok()) << reverse.status();
                    ColumnViewer<TYPE_BOOLEAN> reversed(reverse.value());
                    EXPECT_FALSE(reversed.value(0)) << exponent << ":" << reflection << ":" << offset << ":" << form;
                }
            }
        }
    }
}

TEST_F(geographyFunctionsTest, nativeGeometryIntersectsKeepsTolerantEndpointContacts) {
    auto left = geometry({"POLYGON ((0 0,1 1,0 1,0 0))"});
    for (int ulps : {0, 1, 16, 32, 64}) {
        const double p = 1 + ulps * std::numeric_limits<double>::epsilon();
        const double q = 2 + ulps * std::numeric_limits<double>::epsilon();
        WkbGeometry polygon;
        polygon.type = WkbGeometryType::POLYGON;
        polygon.rings = {{{p, p}, {q, p}, {p, q}, {p, p}}};
        std::string bytes;
        ASSERT_TRUE(WkbCodec::to_wkb(polygon, &bytes, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
        auto right = GeoColumn::create(
                GeoColumnDescriptor{geometry_type().geo_type.value(),
                                    {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}});
        right->append_wkb(Slice(bytes));
        auto forward = GeoFunctions::st_geometry_intersects(nullptr, {left, right});
        auto reverse = GeoFunctions::st_geometry_intersects(nullptr, {right, left});
        ASSERT_TRUE(forward.ok()) << forward.status();
        ASSERT_TRUE(reverse.ok()) << reverse.status();
        ColumnViewer<TYPE_BOOLEAN> result(forward.value()), reversed(reverse.value());
        EXPECT_EQ(ulps <= 32, result.value(0)) << ulps;
        EXPECT_EQ(ulps <= 32, reversed.value(0)) << ulps;
    }
}

TEST_F(geographyFunctionsTest, nativeGeometryIntersectsIsTranslationInvariant) {
    const auto type = geometry_type("EPSG:3857", 3857);
    auto left = geometry({"POLYGON ((0 0, 1 1, 0 1, 0 0))",
                          "POLYGON ((10000000 10000000, 10000001 10000001, 10000000 10000001, 10000000 10000000))"},
                         type);
    auto right = geometry({"POLYGON ((0.5 0.4, 0.6 0.4, 0.6 0.3, 0.5 0.4))",
                           "POLYGON ((10000000.5 10000000.4, 10000000.6 10000000.4, 10000000.6 10000000.3, "
                           "10000000.5 10000000.4))"},
                          type);

    ColumnViewer<TYPE_BOOLEAN> forward(GeoFunctions::st_geometry_intersects(nullptr, {left, right}).value());
    ColumnViewer<TYPE_BOOLEAN> reverse(GeoFunctions::st_geometry_intersects(nullptr, {right, left}).value());
    for (size_t i = 0; i < 2; ++i) {
        EXPECT_FALSE(forward.value(i)) << i;
        EXPECT_FALSE(reverse.value(i)) << i;
    }
}

TEST_F(geographyFunctionsTest, nativeGeoIntersectsLifecycleAndThreadIsolation) {
    const auto type = geography_type();
    auto prepared_left = ConstColumn::create(geography({"POLYGON ((170 -10, -170 -10, -170 10, 170 10, 170 -10))"}), 1);
    std::unique_ptr<FunctionContext> context(
            FunctionContext::create_test_context({type, type}, TypeDescriptor(TYPE_BOOLEAN)));
    context->set_constant_columns({prepared_left, nullptr});
    ASSERT_TRUE(GeoFunctions::native_geo_containment_prepare(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_NE(nullptr, context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto touching = ConstColumn::create(geography({"POLYGON ((-170 -10, -160 -10, -160 10, -170 10, -170 -10))"}), 1);
    auto disjoint = ConstColumn::create(geography({"POLYGON ((-120 -5, -110 -5, -110 5, -120 5, -120 -5))"}), 1);
    ColumnViewer<TYPE_BOOLEAN> first(
            GeoFunctions::st_geography_intersects(context.get(), {prepared_left, touching}).value());
    ColumnViewer<TYPE_BOOLEAN> second(
            GeoFunctions::st_geography_intersects(context.get(), {prepared_left, disjoint}).value());
    EXPECT_TRUE(first.value(0));
    EXPECT_FALSE(second.value(0));

    std::atomic_bool passed = true;
    std::vector<std::thread> workers;
    for (int worker = 0; worker < 4; ++worker) {
        workers.emplace_back([&, worker] {
            const char* wkt = worker % 2 == 0 ? "POLYGON ((175 -5, -175 -5, -175 5, 175 5, 175 -5))"
                                              : "POLYGON ((-120 -5, -110 -5, -110 5, -120 5, -120 -5))";
            const bool expected_value = worker % 2 == 0;
            for (int iteration = 0; iteration < 50; ++iteration) {
                auto varying = geography({wkt});
                auto value = GeoFunctions::st_geography_intersects(context.get(), {prepared_left, varying});
                if (!value.ok()) {
                    passed = false;
                    return;
                }
                ColumnViewer<TYPE_BOOLEAN> result(*value);
                if (result.value(0) != expected_value) {
                    passed = false;
                    return;
                }
            }
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_TRUE(passed);
    ASSERT_TRUE(GeoFunctions::native_geo_containment_close(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    EXPECT_EQ(nullptr, context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
}

TEST_F(geographyFunctionsTest, nativeGeoIntersectsNullEmptyAndRejection) {
    auto polygon = geometry({"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"});
    auto null_result = GeoFunctions::st_geometry_intersects(nullptr, {polygon, geometry({nullptr})}).value();
    EXPECT_TRUE(null_result->is_null(0));

    ColumnViewer<TYPE_BOOLEAN> empty(
            GeoFunctions::st_geometry_intersects(nullptr, {geometry({"POLYGON EMPTY"}), polygon}).value());
    EXPECT_FALSE(empty.value(0));

    auto incompatible = GeoFunctions::st_geometry_intersects(
            nullptr, {polygon, geometry({"POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"}, geometry_type("EPSG:4326", 4326))});
    ASSERT_FALSE(incompatible.ok());
    EXPECT_TRUE(incompatible.status().is_invalid_argument());

    auto unsupported = GeoFunctions::st_geometry_intersects(nullptr, {polygon, geometry({"POINT (1 1)"})});
    ASSERT_FALSE(unsupported.ok());
    EXPECT_TRUE(unsupported.status().is_invalid_argument());

    auto collection = GeoFunctions::st_geometry_intersects(
            nullptr, {polygon, geometry({"GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)))"})});
    ASSERT_FALSE(collection.ok());
    EXPECT_TRUE(collection.status().is_invalid_argument());

    auto mixed = GeoFunctions::st_geometry_intersects(nullptr,
                                                      {polygon, geography({"POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"})});
    ASSERT_FALSE(mixed.ok());
    EXPECT_TRUE(mixed.status().is_not_supported());
}

TEST_F(geographyFunctionsTest, nativeGeoDistanceUsesPrepareCloseAndKeepsChunkConstantsBatchLocal) {
    const auto type = geography_type();
    auto prepared_line = ConstColumn::create(geography({"LINESTRING (-1 0, 1 0)"}), 1);
    std::unique_ptr<FunctionContext> line_context(
            FunctionContext::create_test_context({type, type}, TypeDescriptor(TYPE_DOUBLE)));
    line_context->set_constant_columns({nullptr, prepared_line});
    ASSERT_TRUE(GeoFunctions::native_geo_distance_prepare(line_context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_NE(nullptr, line_context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto first_point = ConstColumn::create(geography({"POINT (0 1)"}), 1);
    auto first = GeoFunctions::st_geography_distance(line_context.get(), {first_point, prepared_line});
    ASSERT_TRUE(first.ok()) << first.status();
    EXPECT_NEAR(111195.101177484, ColumnHelper::get_const_value<TYPE_DOUBLE>(*first), 0.001);

    auto second_point = ConstColumn::create(geography({"POINT (0 2)"}), 1);
    auto second = GeoFunctions::st_geography_distance(line_context.get(), {second_point, prepared_line});
    ASSERT_TRUE(second.ok()) << second.status();
    EXPECT_NEAR(222390.202354968, ColumnHelper::get_const_value<TYPE_DOUBLE>(*second), 0.001);
    ASSERT_TRUE(GeoFunctions::native_geo_distance_close(line_context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    EXPECT_EQ(nullptr, line_context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto prepared_point = ConstColumn::create(geography({"POINT (0 1)"}), 1);
    std::unique_ptr<FunctionContext> point_context(
            FunctionContext::create_test_context({type, type}, TypeDescriptor(TYPE_DOUBLE)));
    point_context->set_constant_columns({prepared_point, nullptr});
    ASSERT_TRUE(GeoFunctions::native_geo_distance_prepare(point_context.get(), FunctionContext::FRAGMENT_LOCAL).ok());

    auto first_line = ConstColumn::create(geography({"LINESTRING (-1 0, 1 0)"}), 1);
    first = GeoFunctions::st_geography_distance(point_context.get(), {prepared_point, first_line});
    ASSERT_TRUE(first.ok()) << first.status();
    EXPECT_NEAR(111195.101177484, ColumnHelper::get_const_value<TYPE_DOUBLE>(*first), 0.001);

    auto second_line = ConstColumn::create(geography({"LINESTRING (0 -1, 0 -2)"}), 1);
    second = GeoFunctions::st_geography_distance(point_context.get(), {prepared_point, second_line});
    ASSERT_TRUE(second.ok()) << second.status();
    EXPECT_NEAR(222390.202354968, ColumnHelper::get_const_value<TYPE_DOUBLE>(*second), 0.001);
    ASSERT_TRUE(GeoFunctions::native_geo_distance_close(point_context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}

TEST_F(geographyFunctionsTest, nativeGeoDistanceIsolatesPreparedSphericalCachePerWorker) {
    const auto type = geography_type();
    auto constant_line = ConstColumn::create(geography({"LINESTRING (-1 0, 1 0)"}), 1);
    std::unique_ptr<FunctionContext> context(
            FunctionContext::create_test_context({type, type}, TypeDescriptor(TYPE_DOUBLE)));
    context->set_constant_columns({nullptr, constant_line});
    ASSERT_TRUE(GeoFunctions::native_geo_distance_prepare(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());

    std::atomic<bool> passed = true;
    std::vector<std::thread> workers;
    for (size_t worker = 0; worker < 4; ++worker) {
        workers.emplace_back([&, worker] {
            const double latitude = static_cast<double>(worker + 1);
            const std::string point_wkt = "POINT (0 " + std::to_string(latitude) + ")";
            auto points = geography({point_wkt.c_str()});
            for (size_t iteration = 0; iteration < 20; ++iteration) {
                auto result = GeoFunctions::st_geography_distance(context.get(), {points, constant_line});
                if (!result.ok()) {
                    passed = false;
                    return;
                }
                ColumnViewer<TYPE_DOUBLE> distance(*result);
                if (std::abs(distance.value(0) - 111195.101177484 * latitude) > 0.001) {
                    passed = false;
                    return;
                }
            }
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_TRUE(passed);
    ASSERT_TRUE(GeoFunctions::native_geo_distance_close(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}

TEST_F(geographyFunctionsTest, nativeGeoMeasurementsAndCollections) {
    auto planar = geometry({"POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))",
                            "MULTILINESTRING ((0 0, 3 4), (10 0, 10 5))",
                            "GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), "
                            "LINESTRING (0 0, 3 4), POINT (20 20))"});
    ColumnViewer<TYPE_DOUBLE> area(GeoFunctions::st_geometry_area(nullptr, {planar}).value());
    ColumnViewer<TYPE_DOUBLE> length(GeoFunctions::st_geometry_length(nullptr, {planar}).value());
    ColumnViewer<TYPE_DOUBLE> perimeter(GeoFunctions::st_geometry_perimeter(nullptr, {planar}).value());
    EXPECT_DOUBLE_EQ(15, area.value(0));
    EXPECT_DOUBLE_EQ(0, length.value(0));
    EXPECT_DOUBLE_EQ(20, perimeter.value(0));
    EXPECT_DOUBLE_EQ(0, area.value(1));
    EXPECT_DOUBLE_EQ(10, length.value(1));
    EXPECT_DOUBLE_EQ(0, perimeter.value(1));
    EXPECT_DOUBLE_EQ(4, area.value(2));
    EXPECT_DOUBLE_EQ(5, length.value(2));
    EXPECT_DOUBLE_EQ(8, perimeter.value(2));

    auto spherical = geography(
            {"LINESTRING (0 0, 1 0)", "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"});
    ColumnViewer<TYPE_DOUBLE> spherical_length(GeoFunctions::st_geography_length(nullptr, {spherical}).value());
    ColumnViewer<TYPE_DOUBLE> spherical_area(GeoFunctions::st_geography_area(nullptr, {spherical}).value());
    ColumnViewer<TYPE_DOUBLE> spherical_perimeter(GeoFunctions::st_geography_perimeter(nullptr, {spherical}).value());
    EXPECT_NEAR(111195.101177484, spherical_length.value(0), 0.001);
    EXPECT_DOUBLE_EQ(0, spherical_area.value(0));
    EXPECT_NEAR(12364036567.0764, spherical_area.value(1), 0.001);
    EXPECT_NEAR(spherical_area.value(1), spherical_area.value(2), 0.001);
    EXPECT_NEAR(444763.468727621, spherical_perimeter.value(1), 0.001);
    EXPECT_NEAR(spherical_perimeter.value(1), spherical_perimeter.value(2), 0.001);
}

TEST_F(geographyFunctionsTest, nativeGeographyMeasurementsRetainExactPolygonResults) {
    // Rebuild the polygon as main's second pass did, using this build's S2/libm results.
    const auto reference_polygon = [](const WkbGeometry& geometry, bool area, long double* value) {
        if (geometry.empty) {
            *value = 0;
            return GEO_PARSE_OK;
        }
        GeoCoordinateListList rings;
        for (const auto& ring : geometry.rings) {
            auto coordinates = std::make_unique<GeoCoordinateList>();
            for (const auto& point : ring) coordinates->add({point.x, point.y});
            rings.add(coordinates.release());
        }
        GeoPolygon polygon;
        const auto status = polygon.from_coords(rings);
        if (status == GEO_PARSE_OK) {
            *value = area ? polygon.area_square_meters() : polygon.perimeter_meters();
        }
        return status;
    };
    const char* cases[] = {
            "POLYGON ((0 0,1 0,1 1,0 1,0 0))",
            "POLYGON ((0 0,0 1,1 1,1 0,0 0))",
            "POLYGON ((0 0,10 0,10 10,0 10,0 0),(1 1,2 1,2 2,1 2,1 1),(6 6,8 6,8 8,6 8,6 6))",
            "MULTIPOLYGON (EMPTY,((0 0,1 0,1 1,0 1,0 0)),EMPTY,((20 10,21 10,21 11,20 11,20 10)),EMPTY)",
            "POLYGON ((-170 -70,170 -70,170 70,-170 70,-170 -70))",
            "POLYGON ((-135 80,-45 80,45 80,135 80,-135 80))",
            "POLYGON ((179 -1,-179 -1,-179 1,179 1,179 -1))",
            "POLYGON ((0 0,0.000000001 0,0.000000001 0.000000001,0 0.000000001,0 0))",
    };
    for (const auto* wkt : cases) {
        SCOPED_TRACE(wkt);
        WkbGeometry geometry;
        ASSERT_TRUE(WkbCodec::parse_wkt(wkt, &geometry, WkbCoordinateSemantics::GEOGRAPHY_CRS84).ok());
        auto input = geography({wkt, nullptr, "POLYGON EMPTY"});
        for (bool area : {true, false}) {
            long double reference = 0;
            if (geometry.type == WkbGeometryType::MULTIPOLYGON) {
                for (const auto& child : geometry.children) {
                    long double value = 0;
                    ASSERT_EQ(GEO_PARSE_OK, reference_polygon(child, area, &value));
                    reference += value;
                }
            } else {
                ASSERT_EQ(GEO_PARSE_OK, reference_polygon(geometry, area, &reference));
            }
            const auto expected = std::bit_cast<uint64_t>(static_cast<double>(reference));
            const auto evaluate = [&](const ColumnPtr& column) {
                return area ? GeoFunctions::st_geography_area(nullptr, {column})
                            : GeoFunctions::st_geography_perimeter(nullptr, {column});
            };
            auto result = evaluate(input);
            ASSERT_TRUE(result.ok()) << result.status();
            ColumnViewer<TYPE_DOUBLE> values(*result);
            EXPECT_EQ(expected, std::bit_cast<uint64_t>(values.value(0)));
            EXPECT_TRUE(values.is_null(1));
            EXPECT_EQ(0, std::bit_cast<uint64_t>(values.value(2)));

            auto constant = ConstColumn::create(geography({wkt}), 3);
            auto constant_result = evaluate(constant);
            ASSERT_TRUE(constant_result.ok()) << constant_result.status();
            EXPECT_TRUE((*constant_result)->is_constant());
            EXPECT_EQ(3, (*constant_result)->size());
            ColumnViewer<TYPE_DOUBLE> constant_values(*constant_result);
            EXPECT_EQ(expected, std::bit_cast<uint64_t>(constant_values.value(2)));
            auto null_result = evaluate(ColumnHelper::create_const_null_column(3));
            ASSERT_TRUE(null_result.ok()) << null_result.status();
            EXPECT_TRUE((*null_result)->only_null());
            EXPECT_EQ(3, (*null_result)->size());
        }
    }
}

TEST_F(geographyFunctionsTest, nativeGeographyMeasurementsValidateBeforeUsingPreparedPolygons) {
    // Match production initialization, as in h3SingleRingValidityMatchesSphericalPredicate.
    gflags::FlagSaver restore_flags;
    FLAGS_s2debug = false;
    for (const char* wkt : {"POLYGON ((0 0,2 2,0 2,2 0,0 0))", "POLYGON ((0 0,1 0,1 1,0 1,0 0),(2 2,3 2,3 3,2 3,2 2))",
                            "POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,8 2,8 8,2 8,2 2),(3 3,4 3,4 4,3 4,3 3))",
                            "MULTIPOLYGON (((0 0,2 0,2 2,0 2,0 0)),((1 1,3 1,3 3,1 3,1 1)))",
                            "MULTIPOLYGON (((0 0,1 0,1 1,0 1,0 0)),((10 10,12 12,10 12,12 10,10 10)))"}) {
        SCOPED_TRACE(wkt);
        auto input = geography({wkt});
        auto valid = GeoFunctions::st_geography_is_valid(nullptr, {input});
        ASSERT_TRUE(valid.ok()) << valid.status();
        EXPECT_FALSE(ColumnViewer<TYPE_BOOLEAN>(*valid).value(0));
        for (auto function : {GeoFunctions::st_geography_area, GeoFunctions::st_geography_perimeter}) {
            auto result = function(nullptr, {input});
            ASSERT_FALSE(result.ok());
            EXPECT_TRUE(result.status().is_invalid_argument());
            EXPECT_EQ("Measurement requires a valid GEOGRAPHY", result.status().message());
        }
    }
}

TEST_F(geographyFunctionsTest, nativeGeometryMeasurementsAreStableAfterLargeTranslation) {
    const auto type = geometry_type("EPSG:3857", 3857);
    auto polygons = geometry({"POLYGON ((0 0, 0.1 0, 0.1 0.1, 0 0.1, 0 0))",
                              "POLYGON ((10000000 10000000, 10000000.1 10000000, "
                              "10000000.1 10000000.1, 10000000 10000000.1, 10000000 10000000))"},
                             type);

    ColumnViewer<TYPE_DOUBLE> area(GeoFunctions::st_geometry_area(nullptr, {polygons}).value());
    const double represented_side = 10000000.1 - 10000000.0;
    EXPECT_NEAR(0.01, area.value(0), 1e-15);
    EXPECT_NEAR(represented_side * represented_side, area.value(1), 1e-15);

    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context({type}, type));
    auto centroids = GeoFunctions::st_geometry_centroid(context.get(), {polygons});
    ASSERT_TRUE(centroids.ok()) << centroids.status();
    ColumnViewer<TYPE_DOUBLE> x(GeoFunctions::st_geometry_x(nullptr, {*centroids}).value());
    ColumnViewer<TYPE_DOUBLE> y(GeoFunctions::st_geometry_y(nullptr, {*centroids}).value());
    EXPECT_NEAR(0.05, x.value(0), 1e-15);
    EXPECT_NEAR(0.05, y.value(0), 1e-15);
    EXPECT_NEAR(10000000.05, x.value(1), 1e-9);
    EXPECT_NEAR(10000000.05, y.value(1), 1e-9);
}

TEST_F(geographyFunctionsTest, nativeGeoCentroidPreservesKindAndUsesHighestDimension) {
    const auto planar_type = geometry_type();
    std::unique_ptr<FunctionContext> planar_context(FunctionContext::create_test_context({planar_type}, planar_type));
    auto planar = geometry({"POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))",
                            "GEOMETRYCOLLECTION (POINT (100 100), LINESTRING (0 0, 4 0))", "GEOMETRYCOLLECTION EMPTY"});
    auto planar_centroids = GeoFunctions::st_geometry_centroid(planar_context.get(), {planar}).value();
    auto planar_text = GeoFunctions::st_geometry_as_text(nullptr, {planar_centroids}).value();
    ColumnViewer<TYPE_VARCHAR> planar_values(planar_text);
    EXPECT_EQ("POINT (2 2)", planar_values.value(0).to_string());
    EXPECT_EQ("POINT (2 0)", planar_values.value(1).to_string());
    EXPECT_EQ("POINT EMPTY", planar_values.value(2).to_string());
    const Column* planar_data = planar_centroids->is_nullable()
                                        ? down_cast<const NullableColumn*>(planar_centroids.get())->data_column().get()
                                        : planar_centroids.get();
    EXPECT_EQ(planar_type.geo_type, down_cast<const GeoColumn*>(planar_data)->descriptor().type);

    auto constant_null = ColumnHelper::create_const_null_column(3);
    std::unique_ptr<FunctionContext> null_context(FunctionContext::create_test_context({planar_type}, planar_type));
    null_context->set_constant_columns({constant_null});
    ASSERT_TRUE(GeoFunctions::native_geo_unary_prepare(null_context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    auto null_centroid = GeoFunctions::st_geometry_centroid(null_context.get(), {constant_null});
    ASSERT_TRUE(null_centroid.ok()) << null_centroid.status();
    EXPECT_TRUE((*null_centroid)->only_null());
    EXPECT_EQ(3, (*null_centroid)->size());
    ASSERT_TRUE(GeoFunctions::native_geo_unary_close(null_context.get(), FunctionContext::FRAGMENT_LOCAL).ok());

    const auto spherical_type = geography_type();
    std::unique_ptr<FunctionContext> spherical_context(
            FunctionContext::create_test_context({spherical_type}, spherical_type));
    auto spherical_points = geography({"MULTIPOINT ((179 0), (-179 0))", "MULTIPOINT ((0 0), (180 0))"});
    auto spherical_centroids = GeoFunctions::st_geography_centroid(spherical_context.get(), {spherical_points}).value();
    auto spherical_text = GeoFunctions::st_geography_as_text(nullptr, {spherical_centroids}).value();
    ColumnViewer<TYPE_VARCHAR> spherical_values(spherical_text);
    EXPECT_EQ("POINT (180 0)", spherical_values.value(0).to_string());
    EXPECT_EQ("POINT EMPTY", spherical_values.value(1).to_string());
}

TEST_F(geographyFunctionsTest, nativeGeoValiditySeparatesTopologyFromBoundaryErrors) {
    auto planar = geometry({"POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))",
                            "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), "
                            "((1 1, 3 1, 3 3, 1 3, 1 1)))",
                            "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))", "POLYGON EMPTY"});
    ColumnViewer<TYPE_BOOLEAN> planar_valid(GeoFunctions::st_geometry_is_valid(nullptr, {planar}).value());
    EXPECT_FALSE(planar_valid.value(0));
    EXPECT_FALSE(planar_valid.value(1));
    EXPECT_TRUE(planar_valid.value(2));
    EXPECT_TRUE(planar_valid.value(3));

    constexpr const char* nested_holes =
            "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2), "
            "(3 3, 4 3, 4 4, 3 4, 3 3))";
    auto spherical = geography({"LINESTRING (0 0, 180 0)",
                                "MULTIPOLYGON (((0 0, 2 0, 2 2, 0 2, 0 0)), "
                                "((1 1, 3 1, 3 3, 1 3, 1 1)))",
                                "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))", nested_holes,
                                "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), "
                                "(2 2, 4 2, 4 4, 2 4, 2 2), (6 6, 8 6, 8 8, 6 8, 6 6))"});
    ColumnViewer<TYPE_BOOLEAN> spherical_valid(GeoFunctions::st_geography_is_valid(nullptr, {spherical}).value());
    EXPECT_FALSE(spherical_valid.value(0));
    EXPECT_FALSE(spherical_valid.value(1));
    EXPECT_TRUE(spherical_valid.value(2));
    EXPECT_FALSE(spherical_valid.value(3));
    EXPECT_TRUE(spherical_valid.value(4));

    auto nested_area = GeoFunctions::st_geography_area(nullptr, {geography({nested_holes})});
    ASSERT_FALSE(nested_area.ok());
    EXPECT_TRUE(nested_area.status().is_invalid_argument());

    auto null = GeoFunctions::st_geometry_is_valid(nullptr, {geometry({nullptr})}).value();
    EXPECT_TRUE(null->is_null(0));

    GeoColumnDescriptor unsupported_descriptor{
            {GEO_LOGICAL_TYPE_GEOMETRY, GEO_COORDINATE_SYSTEM_CARTESIAN, GEO_EDGE_ALGORITHM_PLANAR, "EPSG:3857", 3857},
            {GEO_ENCODING_WKB, GEO_DIMENSION_XYZ, GEO_VALIDATION_STATE_STRUCTURALLY_VALIDATED}};
    auto unsupported = GeoColumn::create(std::move(unsupported_descriptor));
    std::string point_wkb(21, 0);
    point_wkb[0] = 1;
    point_wkb[1] = 1;
    unsupported->append_wkb(Slice(point_wkb));
    auto rejected = GeoFunctions::st_geometry_is_valid(nullptr, {unsupported});
    ASSERT_FALSE(rejected.ok());
    EXPECT_TRUE(rejected.status().is_not_supported());
}

TEST_F(geographyFunctionsTest, nativeGeoUnaryLifecycleReusesPlanConstant) {
    const auto type = geography_type();
    auto constant = ConstColumn::create(geography({"POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"}), 3);
    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context({type}, TypeDescriptor(TYPE_DOUBLE)));
    context->set_constant_columns({constant});
    ASSERT_TRUE(GeoFunctions::native_geo_unary_prepare(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_NE(nullptr, context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    auto result = GeoFunctions::st_geography_area(context.get(), {constant});
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_TRUE((*result)->is_constant());
    EXPECT_EQ(3, (*result)->size());
    ASSERT_TRUE(GeoFunctions::native_geo_unary_close(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    EXPECT_EQ(nullptr, context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
}
TEST_F(geographyFunctionsTest, nativeGeoCrsFunctionsPreserveMetadataAndTransformCoordinates) {
    auto source_type = geometry_type("EPSG:4326", 4326);
    auto source = geometry({"POINT (10 20)"}, source_type);
    ColumnViewer<TYPE_INT> source_srid(GeoFunctions::st_geometry_srid(nullptr, {source}).value());
    EXPECT_EQ(4326, source_srid.value(0));

    auto target = ColumnHelper::create_const_column<TYPE_INT>(3857, 1);
    std::unique_ptr<FunctionContext> set_context(
            FunctionContext::create_test_context({source_type, TypeDescriptor(TYPE_INT)}, geometry_type()));
    set_context->set_constant_columns({nullptr, target});
    ASSERT_TRUE(GeoFunctions::native_geo_transform_prepare(set_context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    auto retagged = GeoFunctions::st_geometry_set_srid(set_context.get(), {source, target});
    ASSERT_TRUE(retagged.ok()) << retagged.status();
    ColumnViewer<TYPE_DOUBLE> retagged_x(GeoFunctions::st_geometry_x(nullptr, {*retagged}).value());
    ColumnViewer<TYPE_DOUBLE> retagged_y(GeoFunctions::st_geometry_y(nullptr, {*retagged}).value());
    EXPECT_DOUBLE_EQ(10, retagged_x.value(0));
    EXPECT_DOUBLE_EQ(20, retagged_y.value(0));
    ColumnViewer<TYPE_VARBINARY> original_wkb(GeoFunctions::st_geometry_as_wkb(nullptr, {source}).value());
    ColumnViewer<TYPE_VARBINARY> retagged_wkb(GeoFunctions::st_geometry_as_wkb(nullptr, {*retagged}).value());
    EXPECT_EQ(std::string_view(original_wkb.value(0).data, original_wkb.value(0).size),
              std::string_view(retagged_wkb.value(0).data, retagged_wkb.value(0).size));
    ColumnViewer<TYPE_INT> retagged_srid(GeoFunctions::st_geometry_srid(nullptr, {*retagged}).value());
    EXPECT_EQ(3857, retagged_srid.value(0));
    ASSERT_TRUE(GeoFunctions::native_geo_transform_close(set_context.get(), FunctionContext::FRAGMENT_LOCAL).ok());

    std::unique_ptr<FunctionContext> forward(
            FunctionContext::create_test_context({source_type, TypeDescriptor(TYPE_INT)}, geometry_type()));
    forward->set_constant_columns({nullptr, target});
    ASSERT_TRUE(GeoFunctions::native_geo_transform_prepare(forward.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    auto projected = GeoFunctions::st_geometry_transform(forward.get(), {source, target});
    ASSERT_TRUE(projected.ok()) << projected.status();
    ColumnViewer<TYPE_DOUBLE> projected_x(GeoFunctions::st_geometry_x(nullptr, {*projected}).value());
    ColumnViewer<TYPE_DOUBLE> projected_y(GeoFunctions::st_geometry_y(nullptr, {*projected}).value());
    EXPECT_NEAR(1113194.90793274, projected_x.value(0), 1e-5);
    EXPECT_NEAR(2273030.92698769, projected_y.value(0), 1e-5);
    ColumnViewer<TYPE_INT> projected_srid(GeoFunctions::st_geometry_srid(nullptr, {*projected}).value());
    EXPECT_EQ(3857, projected_srid.value(0));
    auto empty_and_null = geometry({"POINT EMPTY", nullptr}, source_type);
    auto target_two = ColumnHelper::create_const_column<TYPE_INT>(3857, 2);
    auto empty_result = GeoFunctions::st_geometry_transform(forward.get(), {empty_and_null, target_two});
    ASSERT_TRUE(empty_result.ok()) << empty_result.status();
    ColumnViewer<TYPE_VARCHAR> empty_text(GeoFunctions::st_geometry_as_text(nullptr, {*empty_result}).value());
    EXPECT_EQ("POINT EMPTY", empty_text.value(0).to_string());
    EXPECT_TRUE(empty_text.is_null(1));
    ASSERT_TRUE(GeoFunctions::native_geo_transform_close(forward.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    EXPECT_EQ(nullptr, forward->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto inverse_target = ColumnHelper::create_const_column<TYPE_INT>(4326, 1);
    std::unique_ptr<FunctionContext> inverse(
            FunctionContext::create_test_context({geometry_type(), TypeDescriptor(TYPE_INT)}, source_type));
    inverse->set_constant_columns({nullptr, inverse_target});
    ASSERT_TRUE(GeoFunctions::native_geo_transform_prepare(inverse.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    auto restored = GeoFunctions::st_geometry_transform(inverse.get(), {*projected, inverse_target});
    ASSERT_TRUE(restored.ok()) << restored.status();
    ColumnViewer<TYPE_DOUBLE> restored_x(GeoFunctions::st_geometry_x(nullptr, {*restored}).value());
    ColumnViewer<TYPE_DOUBLE> restored_y(GeoFunctions::st_geometry_y(nullptr, {*restored}).value());
    EXPECT_NEAR(10, restored_x.value(0), 1e-9);
    EXPECT_NEAR(20, restored_y.value(0), 1e-9);
    ASSERT_TRUE(GeoFunctions::native_geo_transform_close(inverse.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}

TEST_F(geographyFunctionsTest, nativeGeoCrsWebMercatorBoundaryAndCollections) {
    auto source_type = geometry_type("EPSG:4326", 4326);
    auto target = ColumnHelper::create_const_column<TYPE_INT>(3857, 1);
    std::unique_ptr<FunctionContext> forward(
            FunctionContext::create_test_context({source_type, TypeDescriptor(TYPE_INT)}, geometry_type()));
    forward->set_constant_columns({nullptr, target});
    ASSERT_TRUE(GeoFunctions::native_geo_transform_prepare(forward.get(), FunctionContext::FRAGMENT_LOCAL).ok());

    auto boundary = geometry({"POINT (180 85.0511287798066)"}, source_type);
    auto projected = GeoFunctions::st_geometry_transform(forward.get(), {boundary, target});
    ASSERT_TRUE(projected.ok()) << projected.status();
    ColumnViewer<TYPE_DOUBLE> x(GeoFunctions::st_geometry_x(nullptr, {*projected}).value());
    ColumnViewer<TYPE_DOUBLE> y(GeoFunctions::st_geometry_y(nullptr, {*projected}).value());
    EXPECT_NEAR(20037508.3427892, x.value(0), 1e-5);
    EXPECT_NEAR(20037508.3427892, y.value(0), 1e-5);

    auto collection = geometry({"GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (0 0, 1 1))"}, source_type);
    auto transformed_collection = GeoFunctions::st_geometry_transform(forward.get(), {collection, target});
    ASSERT_TRUE(transformed_collection.ok()) << transformed_collection.status();
    ColumnViewer<TYPE_VARCHAR> text(GeoFunctions::st_geometry_as_text(nullptr, {*transformed_collection}).value());
    EXPECT_NE(std::string::npos, text.value(0).to_string().find("1113194.907"));
    ASSERT_TRUE(GeoFunctions::native_geo_transform_close(forward.get(), FunctionContext::FRAGMENT_LOCAL).ok());

    auto inverse_target = ColumnHelper::create_const_column<TYPE_INT>(4326, 1);
    std::unique_ptr<FunctionContext> inverse(
            FunctionContext::create_test_context({geometry_type(), TypeDescriptor(TYPE_INT)}, source_type));
    inverse->set_constant_columns({nullptr, inverse_target});
    ASSERT_TRUE(GeoFunctions::native_geo_transform_prepare(inverse.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    auto restored = GeoFunctions::st_geometry_transform(inverse.get(), {*projected, inverse_target});
    ASSERT_TRUE(restored.ok()) << restored.status();
    ColumnViewer<TYPE_DOUBLE> restored_y(GeoFunctions::st_geometry_y(nullptr, {*restored}).value());
    EXPECT_NEAR(85.0511287798066, restored_y.value(0), 1e-9);
    ASSERT_TRUE(GeoFunctions::native_geo_transform_close(inverse.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}
TEST_F(geographyFunctionsTest, nativeGeoCrsBoundaryRoundTripsStayInsideDomain) {
    auto geographic_type = geometry_type("EPSG:4326", 4326);
    auto projected_type = geometry_type("EPSG:3857", 3857);
    auto projected_srid = ColumnHelper::create_const_column<TYPE_INT>(3857, 1);
    auto geographic_srid = ColumnHelper::create_const_column<TYPE_INT>(4326, 1);
    std::unique_ptr<FunctionContext> forward(
            FunctionContext::create_test_context({geographic_type, TypeDescriptor(TYPE_INT)}, projected_type));
    forward->set_constant_columns({nullptr, projected_srid});
    ASSERT_TRUE(GeoFunctions::native_geo_transform_prepare(forward.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    std::unique_ptr<FunctionContext> inverse(
            FunctionContext::create_test_context({projected_type, TypeDescriptor(TYPE_INT)}, geographic_type));
    inverse->set_constant_columns({nullptr, geographic_srid});
    ASSERT_TRUE(GeoFunctions::native_geo_transform_prepare(inverse.get(), FunctionContext::FRAGMENT_LOCAL).ok());

    for (const char* wkt :
         {"POINT (180 0)", "POINT (-180 0)", "POINT (0 85.0511287798066)", "POINT (0 -85.0511287798066)"}) {
        SCOPED_TRACE(wkt);
        auto source = geometry({wkt}, geographic_type);
        auto first = GeoFunctions::st_geometry_transform(forward.get(), {source, projected_srid});
        ASSERT_TRUE(first.ok()) << first.status();
        auto restored = GeoFunctions::st_geometry_transform(inverse.get(), {*first, geographic_srid});
        ASSERT_TRUE(restored.ok()) << restored.status();
        ColumnViewer<TYPE_DOUBLE> longitude(GeoFunctions::st_geometry_x(nullptr, {*restored}).value());
        ColumnViewer<TYPE_DOUBLE> latitude(GeoFunctions::st_geometry_y(nullptr, {*restored}).value());
        EXPECT_LE(std::abs(longitude.value(0)), 180.0);
        EXPECT_LE(std::abs(latitude.value(0)), 85.0511287798066);
        auto second = GeoFunctions::st_geometry_transform(forward.get(), {*restored, projected_srid});
        ASSERT_TRUE(second.ok()) << second.status();
        ColumnViewer<TYPE_DOUBLE> first_x(GeoFunctions::st_geometry_x(nullptr, {*first}).value());
        ColumnViewer<TYPE_DOUBLE> first_y(GeoFunctions::st_geometry_y(nullptr, {*first}).value());
        ColumnViewer<TYPE_DOUBLE> second_x(GeoFunctions::st_geometry_x(nullptr, {*second}).value());
        ColumnViewer<TYPE_DOUBLE> second_y(GeoFunctions::st_geometry_y(nullptr, {*second}).value());
        EXPECT_NEAR(first_x.value(0), second_x.value(0), 1e-7);
        EXPECT_NEAR(first_y.value(0), second_y.value(0), 1e-7);
    }

    for (const char* wkt : {"POINT (180.000001 0)", "POINT (0 85.051129)"}) {
        auto outside = geometry({wkt}, geographic_type);
        EXPECT_FALSE(GeoFunctions::st_geometry_transform(forward.get(), {outside, projected_srid}).ok());
    }
    auto outside_projected = geometry({"POINT (20037508.343 0)"}, projected_type);
    EXPECT_FALSE(GeoFunctions::st_geometry_transform(inverse.get(), {outside_projected, geographic_srid}).ok());
    ASSERT_TRUE(GeoFunctions::native_geo_transform_close(forward.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(GeoFunctions::native_geo_transform_close(inverse.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}

TEST_F(geographyFunctionsTest, nativeGeoCrsRejectsInvalidAndSharesPreparedContextAcrossWorkers) {
    auto source_type = geometry_type("EPSG:4326", 4326);
    auto point = geometry({"POINT (10 20)"}, source_type);
    auto target = ColumnHelper::create_const_column<TYPE_INT>(3857, 1);
    std::unique_ptr<FunctionContext> context(
            FunctionContext::create_test_context({source_type, TypeDescriptor(TYPE_INT)}, geometry_type()));
    context->set_constant_columns({nullptr, target});
    ASSERT_TRUE(GeoFunctions::native_geo_transform_prepare(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());

    std::atomic_bool passed = true;
    std::vector<std::thread> workers;
    for (int i = 0; i < 4; ++i) {
        workers.emplace_back([&] {
            for (int iteration = 0; iteration < 20; ++iteration) {
                auto output = GeoFunctions::st_geometry_transform(context.get(), {point, target});
                if (!output.ok()) {
                    passed = false;
                    return;
                }
                auto x_result = GeoFunctions::st_geometry_x(nullptr, {*output});
                if (!x_result.ok() ||
                    std::abs(ColumnViewer<TYPE_DOUBLE>(*x_result).value(0) - 1113194.90793274) > 1e-5) {
                    passed = false;
                    return;
                }
            }
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_TRUE(passed);

    auto outside_domain = geometry({"POINT (10 95)"}, source_type);
    auto rejected = GeoFunctions::st_geometry_transform(context.get(), {outside_domain, target});
    EXPECT_FALSE(rejected.ok());
    GeoColumnDescriptor malformed_descriptor{
            source_type.geo_type.value(),
            {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_STRUCTURALLY_VALIDATED}};
    auto malformed = GeoColumn::create(std::move(malformed_descriptor));
    const std::string bad_wkb = "bad";
    malformed->append_wkb(Slice(bad_wkb));
    EXPECT_FALSE(GeoFunctions::st_geometry_transform(context.get(), {malformed, target}).ok());
    ASSERT_TRUE(GeoFunctions::native_geo_transform_close(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());

    auto unknown_type = geometry_type("LOCAL:unknown", std::nullopt);
    auto unknown = geometry({"POINT (10 20)"}, unknown_type);
    ColumnViewer<TYPE_INT> srid(GeoFunctions::st_geometry_srid(nullptr, {unknown}).value());
    EXPECT_TRUE(srid.is_null(0));
    std::unique_ptr<FunctionContext> invalid_source(
            FunctionContext::create_test_context({unknown_type, TypeDescriptor(TYPE_INT)}, geometry_type()));
    invalid_source->set_constant_columns({nullptr, target});
    EXPECT_FALSE(
            GeoFunctions::native_geo_transform_prepare(invalid_source.get(), FunctionContext::FRAGMENT_LOCAL).ok());

    auto invalid_target = ColumnHelper::create_const_column<TYPE_INT>(999999, 1);
    std::unique_ptr<FunctionContext> invalid_target_context(FunctionContext::create_test_context(
            {source_type, TypeDescriptor(TYPE_INT)}, geometry_type("EPSG:999999", 999999)));
    invalid_target_context->set_constant_columns({nullptr, invalid_target});
    EXPECT_FALSE(
            GeoFunctions::native_geo_transform_prepare(invalid_target_context.get(), FunctionContext::FRAGMENT_LOCAL)
                    .ok());
}

TEST_F(geographyFunctionsTest, nativeGeoCrsRejectsThreeAxisProjectedCrs) {
    auto source_type = geometry_type("EPSG:4326", 4326);
    auto three_axis_type = geometry_type("EPSG:9895", 9895);
    auto three_axis_target = ColumnHelper::create_const_column<TYPE_INT>(9895, 1);
    std::unique_ptr<FunctionContext> invalid_target(
            FunctionContext::create_test_context({source_type, TypeDescriptor(TYPE_INT)}, three_axis_type));
    invalid_target->set_constant_columns({nullptr, three_axis_target});
    auto target_status =
            GeoFunctions::native_geo_transform_prepare(invalid_target.get(), FunctionContext::FRAGMENT_LOCAL);
    EXPECT_TRUE(target_status.is_not_supported()) << target_status;
    EXPECT_NE(std::string::npos, target_status.message().find("EPSG:4326 and EPSG:3857"));

    auto two_axis_target = ColumnHelper::create_const_column<TYPE_INT>(4326, 1);
    std::unique_ptr<FunctionContext> invalid_source(
            FunctionContext::create_test_context({three_axis_type, TypeDescriptor(TYPE_INT)}, source_type));
    invalid_source->set_constant_columns({nullptr, two_axis_target});
    auto source_status =
            GeoFunctions::native_geo_transform_prepare(invalid_source.get(), FunctionContext::FRAGMENT_LOCAL);
    EXPECT_TRUE(source_status.is_not_supported()) << source_status;
    EXPECT_NE(std::string::npos, source_status.message().find("EPSG:4326 and EPSG:3857"));
}

TEST_F(geographyFunctionsTest, nativeGeoHistoricalFunctionIds) {
    RuntimeState state;
    auto run = [&](int64_t id, bool planar, LogicalType result_type, int arguments, double expected) {
        SCOPED_TRACE(id);
        const auto type = planar ? geometry_type() : geography_type();
        auto value = planar ? geometry({"POINT (1 2)"}) : geography({"POINT (1 2)"});
        TExprNode node;
        node.__set_node_type(TExprNodeType::FUNCTION_CALL);
        node.__set_type(TypeDescriptor(result_type).to_thrift());
        node.__set_num_children(arguments);
        TFunction fn;
        TFunctionName name;
        name.__set_function_name(
                arguments == 2 ? "st_distance"
                               : (result_type == TYPE_VARCHAR ? "st_geometrytype" : planar ? "st_x" : "st_y"));
        fn.__set_name(name);
        fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
        fn.__set_fid(id);
        fn.__set_arg_types(std::vector<TTypeDesc>(arguments, type.to_thrift()));
        fn.__set_ret_type(node.type);
        fn.__set_has_var_args(false);
        node.__set_fn(fn);
        VectorizedFunctionCallExpr expr(node);
        TExprNode input;
        input.__set_node_type(TExprNodeType::SLOT_REF);
        input.__set_type(type.to_thrift());
        MockExpr first(input, value);
        MockExpr second(input, value);
        expr.add_child(&first);
        if (arguments == 2) expr.add_child(&second);
        ExprContext context(&expr);
        auto status = context.prepare(&state);
        if (status.ok()) status = context.open(&state);
        if (status.ok()) {
            auto result = context.evaluate(nullptr);
            EXPECT_TRUE(result.ok()) << result.status();
            if (result.ok()) {
                if (result_type == TYPE_VARCHAR) {
                    EXPECT_EQ("ST_Point", result.value()->get(0).get_slice().to_string());
                } else {
                    EXPECT_DOUBLE_EQ(expected, result.value()->get(0).get_double());
                }
            }
        }
        context.close(&state);
        EXPECT_TRUE(status.ok()) << status;
    };
    // Plans from both registries must work during BE-before-FE upgrades.
    for (int64_t id : {120081, 120090}) run(id, false, TYPE_DOUBLE, 1, 2.0);
    for (int64_t id : {120082, 120170}) run(id, false, TYPE_VARCHAR, 1, 0.0);
    for (int64_t id : {120083, 120180}) run(id, false, TYPE_DOUBLE, 2, 0.0);
    run(120081, true, TYPE_DOUBLE, 1, 1.0);
}

TEST_F(geographyFunctionsTest, nativeGeometryConstantMaterialization) {
    const auto type = geometry_type();
    std::unique_ptr<FunctionContext> context(
            FunctionContext::create_test_context({TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_VARCHAR)}, type));
    auto wkt = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("POINT (1 2)"), 1);
    auto crs = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("EPSG:3857"), 1);
    auto value = GeoFunctions::st_geom_from_text(context.get(), {wkt, crs});
    ASSERT_TRUE(value.ok()) << value.status();
    const auto expected = down_cast<const GeoColumn*>(ColumnHelper::get_data_column(value->get()))->descriptor();
    for (bool nullable : {false, true}) {
        // A non-nullable destination receives a non-nullable constant payload.
        ColumnPtr source =
                nullable ? value.value() : ConstColumn::create(ColumnHelper::get_data_column(value->get())->clone(), 1);
        auto unfolded = ColumnHelper::move_column(type, nullable, std::move(source), 3);
        ASSERT_EQ(3, unfolded->size());
        EXPECT_FALSE(unfolded->is_constant());
        EXPECT_EQ(nullable, unfolded->is_nullable());
        auto* geo = down_cast<const GeoColumn*>(ColumnHelper::get_data_column(unfolded.get()));
        EXPECT_EQ(expected, geo->descriptor());
        auto x = GeoFunctions::st_geometry_x(nullptr, {unfolded});
        ASSERT_TRUE(x.ok()) << x.status();
        for (size_t i = 0; i < 3; ++i) EXPECT_DOUBLE_EQ(1.0, x.value()->get(i).get_double());
    }
    // Match UNION's typed destination and copy path, with NULLs on both sides.
    auto destination = ColumnHelper::create_column(type, true);
    EXPECT_TRUE(destination->append_nulls(1));
    ColumnPtr source = value.value();
    auto row = ColumnHelper::move_column(type, true, std::move(source), 1);
    destination->append(*row, 0, 1);
    auto null_row = ColumnHelper::move_column(type, true, ColumnHelper::create_const_null_column(1), 1);
    destination->append(*null_row, 0, 1);
    destination->append(*row, 0, 1);
    EXPECT_EQ(expected, down_cast<const GeoColumn*>(ColumnHelper::get_data_column(destination.get()))->descriptor());
    auto x = GeoFunctions::st_geometry_x(nullptr, {destination});
    ASSERT_TRUE(x.ok()) << x.status();
    ASSERT_EQ(4, x.value()->size());
    EXPECT_TRUE(x.value()->is_null(0));
    EXPECT_DOUBLE_EQ(1.0, x.value()->get(1).get_double());
    EXPECT_TRUE(x.value()->is_null(2));
    EXPECT_DOUBLE_EQ(1.0, x.value()->get(3).get_double());
    EXPECT_TRUE(value.value()->is_constant());
    EXPECT_EQ(1, value.value()->size());
}

TEST_F(geographyFunctionsTest, nativeGeometryCrsIntegerSyntax) {
    const std::pair<const char*, std::optional<int32_t>> cases[] = {
            {"EPSG:4326", 4326},
            {"EPSG:004326", 4326},
            {"EPSG:-4326", -4326},
            {"EPSG:-0", 0},
            {"EPSG:2147483647", INT32_MAX},
            {"EPSG:-2147483648", INT32_MIN},
            {"OGC:CRS84", 4326},
            {"EPSG:+4326", std::nullopt},
            {"EPSG:\xef\xbc\x94\xef\xbc\x93\xef\xbc\x92\xef\xbc\x96", std::nullopt},
            {"EPSG: 4326", std::nullopt},
            {"EPSG:4326 ", std::nullopt},
            {"EPSG:", std::nullopt},
            {"EPSG:-", std::nullopt},
            {"EPSG:2147483648", std::nullopt},
            {"EPSG:-2147483649", std::nullopt},
            {"custom:local", std::nullopt}};
    for (const auto& [crs, srid] : cases) {
        SCOPED_TRACE(crs);
        const auto type = geometry_type(crs, srid);
        auto value = geometry({"POINT (1 2)"}, type);
        const auto* column = down_cast<const GeoColumn*>(ColumnHelper::get_data_column(value.get()));
        EXPECT_EQ(type.geo_type.value(), column->descriptor().type);
        auto text = GeoFunctions::st_geometry_as_text(nullptr, {value});
        ASSERT_TRUE(text.ok()) << text.status();
        EXPECT_EQ("POINT (1 2)", text.value()->get(0).get_slice().to_string());
    }
}

TEST_F(geographyFunctionsTest, nativeGeoFunctionRegistryContract) {
    struct ExpectedFunction {
        uint64_t id;
        const char* name;
        const char* return_type;
        std::vector<const char*> arg_types;
    };
    const ExpectedFunction expected[] = {
            {120020, "ST_GeogFromText", "GEOGRAPHY", {"VARCHAR"}},
            {120021, "ST_GeogFromText", "GEOGRAPHY", {"VARCHAR", "INT"}},
            {120022, "ST_GeomFromText", "GEOMETRY", {"VARCHAR", "VARCHAR"}},
            {120030, "ST_GeogFromWKB", "GEOGRAPHY", {"VARBINARY"}},
            {120031, "ST_GeogFromWKB", "GEOGRAPHY", {"VARBINARY", "INT"}},
            {120032, "ST_GeomFromWKB", "GEOMETRY", {"VARBINARY", "VARCHAR"}},
            {120040, "ST_AsText", "VARCHAR", {"GEOGRAPHY"}},
            {120041, "ST_AsText", "VARCHAR", {"GEOMETRY"}},
            {120050, "ST_AsWKT", "VARCHAR", {"GEOGRAPHY"}},
            {120051, "ST_AsWKT", "VARCHAR", {"GEOMETRY"}},
            {120060, "ST_AsBinary", "VARBINARY", {"GEOGRAPHY"}},
            {120061, "ST_AsBinary", "VARBINARY", {"GEOMETRY"}},
            {120070, "ST_AsWKB", "VARBINARY", {"GEOGRAPHY"}},
            {120071, "ST_AsWKB", "VARBINARY", {"GEOMETRY"}},
            {120080, "ST_X", "DOUBLE", {"GEOGRAPHY"}},
            {120081, "ST_X", "DOUBLE", {"GEOMETRY"}},
            {120090, "ST_Y", "DOUBLE", {"GEOGRAPHY"}},
            {120091, "ST_Y", "DOUBLE", {"GEOMETRY"}},
            {120170, "ST_GeometryType", "VARCHAR", {"GEOGRAPHY"}},
            {120171, "ST_GeometryType", "VARCHAR", {"GEOMETRY"}},
            {120180, "ST_Distance", "DOUBLE", {"GEOGRAPHY", "GEOGRAPHY"}},
            {120181, "ST_Distance", "DOUBLE", {"GEOMETRY", "GEOMETRY"}},
            {120190, "ST_Contains", "BOOLEAN", {"GEOGRAPHY", "GEOGRAPHY"}},
            {120191, "ST_Contains", "BOOLEAN", {"GEOMETRY", "GEOMETRY"}},
            {120200, "ST_Within", "BOOLEAN", {"GEOGRAPHY", "GEOGRAPHY"}},
            {120201, "ST_Within", "BOOLEAN", {"GEOMETRY", "GEOMETRY"}},
            {120210, "ST_Covers", "BOOLEAN", {"GEOGRAPHY", "GEOGRAPHY"}},
            {120211, "ST_Covers", "BOOLEAN", {"GEOMETRY", "GEOMETRY"}},
            {120220, "ST_CoveredBy", "BOOLEAN", {"GEOGRAPHY", "GEOGRAPHY"}},
            {120221, "ST_CoveredBy", "BOOLEAN", {"GEOMETRY", "GEOMETRY"}},
            {120230, "ST_DWithin", "BOOLEAN", {"GEOGRAPHY", "GEOGRAPHY", "DOUBLE"}},
            {120231, "ST_DWithin", "BOOLEAN", {"GEOMETRY", "GEOMETRY", "DOUBLE"}},
            {120240, "ST_Intersects", "BOOLEAN", {"GEOGRAPHY", "GEOGRAPHY"}},
            {120241, "ST_Intersects", "BOOLEAN", {"GEOMETRY", "GEOMETRY"}},
            {120250, "ST_Area", "DOUBLE", {"GEOGRAPHY"}},
            {120251, "ST_Area", "DOUBLE", {"GEOMETRY"}},
            {120260, "ST_Length", "DOUBLE", {"GEOGRAPHY"}},
            {120261, "ST_Length", "DOUBLE", {"GEOMETRY"}},
            {120270, "ST_Perimeter", "DOUBLE", {"GEOGRAPHY"}},
            {120271, "ST_Perimeter", "DOUBLE", {"GEOMETRY"}},
            {120280, "ST_Centroid", "GEOGRAPHY", {"GEOGRAPHY"}},
            {120281, "ST_Centroid", "GEOMETRY", {"GEOMETRY"}},
            {120290, "ST_IsValid", "BOOLEAN", {"GEOGRAPHY"}},
            {120291, "ST_IsValid", "BOOLEAN", {"GEOMETRY"}},
            {120300, "ST_SRID", "INT", {"GEOMETRY"}},
            {120305, "ST_SetSRID", "GEOMETRY", {"GEOMETRY", "INT"}},
            {120310, "ST_Transform", "GEOMETRY", {"GEOMETRY", "INT"}},
            {120500, "H3_FromGeo", "BIGINT", {"GEOGRAPHY", "INT"}},
            {120501, "H3_GridDisk", "ARRAY_BIGINT", {"BIGINT", "INT"}},
            {120502, "H3_ToParent", "BIGINT", {"BIGINT", "INT"}},
            {120503, "H3_ToChildren", "ARRAY_BIGINT", {"BIGINT", "INT"}},
            {120504, "H3_Resolution", "INT", {"BIGINT"}},
            {120505, "H3_ToBoundary", "GEOGRAPHY", {"BIGINT"}},
            {120506, "H3_PolygonToCells", "ARRAY_BIGINT", {"GEOGRAPHY", "INT"}},
    };

    for (const auto& function : expected) {
        SCOPED_TRACE(function.id);
        const auto* descriptor = BuiltinFunctions::find_builtin_function(function.id);
        ASSERT_NE(nullptr, descriptor);
        EXPECT_EQ(function.name, descriptor->name);
        EXPECT_STREQ(function.return_type, descriptor->return_type);
        ASSERT_EQ(function.arg_types.size(), descriptor->arg_types.size());
        EXPECT_EQ(function.arg_types.size(), descriptor->args_nums);
        for (size_t i = 0; i < function.arg_types.size(); ++i) {
            EXPECT_STREQ(function.arg_types[i], descriptor->arg_types[i]);
        }
        if (function.id >= 120500 && function.id < 120506) {
            EXPECT_FALSE(static_cast<bool>(descriptor->prepare_function));
            EXPECT_FALSE(static_cast<bool>(descriptor->close_function));
        } else if (function.id >= 120180 && function.id != 120300) {
            EXPECT_TRUE(static_cast<bool>(descriptor->prepare_function));
            EXPECT_TRUE(static_cast<bool>(descriptor->close_function));
        }
    }
}

TEST_F(geographyFunctionsTest, h3CellAndArrayContract) {
    constexpr int64_t expected = 0x83754efffffffffLL; // H3 4.5.0: POINT (0 0), resolution 3
    auto point = geography({"POINT (0 0)"});
    auto res3 = ColumnHelper::create_const_column<TYPE_INT>(3, 1);
    std::unique_ptr<FunctionContext> from_context(FunctionContext::create_test_context(
            {geography_type(), TypeDescriptor(TYPE_INT)}, TypeDescriptor(TYPE_BIGINT)));
    auto from = GeoFunctions::h3_from_geo(from_context.get(), {point, res3});
    ASSERT_TRUE(from.ok()) << from.status();
    ColumnViewer<TYPE_BIGINT> from_view(*from);
    EXPECT_EQ(expected, from_view.value(0));
    EXPECT_GT(from_view.value(0), 1LL << 53);

    auto cell = ColumnHelper::create_const_column<TYPE_BIGINT>(expected, 1);
    auto cell_resolution = GeoFunctions::h3_resolution(nullptr, {cell});
    ASSERT_TRUE(cell_resolution.ok()) << cell_resolution.status();
    EXPECT_EQ(3, ColumnViewer<TYPE_INT>(*cell_resolution).value(0));

    auto parent = GeoFunctions::h3_to_parent(nullptr, {cell, res3});
    ASSERT_TRUE(parent.ok()) << parent.status();
    EXPECT_EQ(expected, ColumnViewer<TYPE_BIGINT>(*parent).value(0));

    auto children = GeoFunctions::h3_to_children(nullptr, {cell, res3});
    ASSERT_TRUE(children.ok()) << children.status();
    ASSERT_EQ(1, (*children)->get(0).get_array().size());
    EXPECT_EQ(expected, (*children)->get(0).get_array()[0].get_int64());

    auto k0 = ColumnHelper::create_const_column<TYPE_INT>(0, 1);
    auto disk = GeoFunctions::h3_grid_disk(nullptr, {cell, k0});
    ASSERT_TRUE(disk.ok()) << disk.status();
    ASSERT_EQ(1, (*disk)->get(0).get_array().size());
    EXPECT_EQ(expected, (*disk)->get(0).get_array()[0].get_int64());

    EXPECT_FALSE(GeoFunctions::h3_resolution(nullptr, {ColumnHelper::create_const_column<TYPE_BIGINT>(-1, 1)}).ok());
    EXPECT_FALSE(GeoFunctions::h3_grid_disk(nullptr, {cell, ColumnHelper::create_const_column<TYPE_INT>(-1, 1)}).ok());
    EXPECT_FALSE(GeoFunctions::h3_to_parent(nullptr, {cell, ColumnHelper::create_const_column<TYPE_INT>(4, 1)}).ok());
    EXPECT_FALSE(GeoFunctions::h3_to_children(nullptr, {cell, ColumnHelper::create_const_column<TYPE_INT>(2, 1)}).ok());
}

TEST_F(geographyFunctionsTest, h3FromGeoNullableEmptyAndVaryingResolutions) {
    auto points = geography({"POINT (0 0)", nullptr, "POINT EMPTY", "POINT (180 90)", "POINT (-180 -90)"});
    auto resolutions = Int32Column::create();
    for (int resolution : {3, 9, 11, 7, 15}) resolutions->append(resolution);
    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context(
            {geography_type(), TypeDescriptor(TYPE_INT)}, TypeDescriptor(TYPE_BIGINT)));
    auto result = GeoFunctions::h3_from_geo(context.get(), {points, resolutions});
    ASSERT_TRUE(result.ok()) << result.status();
    ColumnViewer<TYPE_BIGINT> output(*result);
    EXPECT_TRUE(output.is_null(1));
    EXPECT_TRUE(output.is_null(2));
    const std::array<LatLng, 3> coordinates = {{{0, 0}, {M_PI_2, M_PI}, {-M_PI_2, -M_PI}}};
    const std::array<int, 3> rows = {0, 3, 4};
    for (size_t i = 0; i < rows.size(); ++i) {
        H3Index expected = 0;
        ASSERT_EQ(E_SUCCESS, latLngToCell(&coordinates[i], resolutions->get_data()[rows[i]], &expected));
        EXPECT_EQ(static_cast<int64_t>(expected), output.value(rows[i]));
    }
}

TEST_F(geographyFunctionsTest, h3FromGeoPreparedPointAcrossResolutions) {
    auto point = ConstColumn::create(geography({"POINT (30 60)"}), 3);
    auto resolutions = Int32Column::create();
    for (int resolution : {3, 9, 11}) resolutions->append(resolution);
    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context(
            {geography_type(), TypeDescriptor(TYPE_INT)}, TypeDescriptor(TYPE_BIGINT)));
    context->set_constant_columns({point, nullptr});
    for (int repeat = 0; repeat < 2; ++repeat) {
        auto result = GeoFunctions::h3_from_geo(context.get(), {point, resolutions});
        ASSERT_TRUE(result.ok()) << result.status();
        ASSERT_FALSE((*result)->is_constant());
        ColumnViewer<TYPE_BIGINT> output(*result);
        const LatLng coordinate{M_PI / 3, M_PI / 6};
        for (size_t row = 0; row < 3; ++row) {
            H3Index expected = 0;
            ASSERT_EQ(E_SUCCESS, latLngToCell(&coordinate, resolutions->get_data()[row], &expected));
            EXPECT_EQ(static_cast<int64_t>(expected), output.value(row));
        }
    }
    auto constant =
            GeoFunctions::h3_from_geo(context.get(), {point, ColumnHelper::create_const_column<TYPE_INT>(9, 3)});
    ASSERT_TRUE(constant.ok()) << constant.status();
    EXPECT_TRUE((*constant)->is_constant());
    EXPECT_EQ(3, (*constant)->size());
}

TEST_F(geographyFunctionsTest, h3FromGeoRetainsCoordinateAndTypeValidation) {
    for (const char* wkt : {"POINT (181 0)", "POINT (0 91)", "LINESTRING (0 0, 1 1)"}) {
        SCOPED_TRACE(wkt);
        WkbGeometry geometry;
        ASSERT_TRUE(WkbCodec::parse_wkt(wkt, &geometry, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
        std::string bytes;
        ASSERT_TRUE(WkbCodec::to_wkb(geometry, &bytes, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
        // Producer validation metadata must not bypass H3's coordinate/type checks.
        GeoColumnDescriptor descriptor{
                geography_type().geo_type.value(),
                {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}};
        auto input = GeoColumn::create(descriptor);
        input->append_wkb(Slice(bytes));
        auto result = GeoFunctions::h3_from_geo(nullptr, {input, ColumnHelper::create_const_column<TYPE_INT>(9, 1)});
        ASSERT_FALSE(result.ok());
        EXPECT_TRUE(result.status().is_invalid_argument()) << result.status();
    }
    auto empty = geography({"POINT EMPTY"});
    EXPECT_FALSE(GeoFunctions::h3_from_geo(nullptr, {empty, ColumnHelper::create_const_column<TYPE_INT>(16, 1)}).ok());
}

TEST_F(geographyFunctionsTest, h3FromGeoConstantAndVaryingPointsShareDiagnostics) {
    WkbGeometry point;
    ASSERT_TRUE(WkbCodec::parse_wkt("POINT (181 0)", &point, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    std::string bytes;
    ASSERT_TRUE(WkbCodec::to_wkb(point, &bytes, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    const double nan = std::numeric_limits<double>::quiet_NaN();
    const double inf = std::numeric_limits<double>::infinity();
    for (const auto& coordinate : std::array<WkbCoordinate, 4>{{{181, 0}, {inf, 0}, {nan, 0}, {nan, nan}}}) {
        const std::array<double, 2> axes{coordinate.x, coordinate.y};
        for (size_t axis = 0; axis < axes.size(); ++axis) {
            const uint64_t bits = std::bit_cast<uint64_t>(axes[axis]);
            for (size_t i = 0; i < sizeof(bits); ++i) {
                bytes[5 + axis * sizeof(bits) + i] = static_cast<char>(bits >> (8 * i));
            }
        }
        GeoColumnDescriptor descriptor{
                geography_type().geo_type.value(),
                {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}};
        auto input = GeoColumn::create(descriptor);
        input->append_wkb(Slice(bytes));
        auto resolution = ColumnHelper::create_const_column<TYPE_INT>(9, 1);
        auto varying = GeoFunctions::h3_from_geo(nullptr, {input, resolution});
        auto constant_input = ConstColumn::create(input, 1);
        std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context(
                {geography_type(), TypeDescriptor(TYPE_INT)}, TypeDescriptor(TYPE_BIGINT)));
        context->set_constant_columns({constant_input, resolution});
        auto constant = GeoFunctions::h3_from_geo(context.get(), {constant_input, resolution});
        ASSERT_EQ(std::isnan(coordinate.y), varying.ok());
        ASSERT_EQ(varying.ok(), constant.ok());
        if (varying.ok()) {
            EXPECT_TRUE(ColumnViewer<TYPE_BIGINT>(*varying).is_null(0));
            EXPECT_TRUE(ColumnViewer<TYPE_BIGINT>(*constant).is_null(0));
        } else {
            EXPECT_EQ(varying.status().code(), constant.status().code());
            EXPECT_EQ(varying.status().message(), constant.status().message());
            if (coordinate.x == inf) {
                EXPECT_EQ("Invalid WKB: POINT coordinates must be finite or both NaN for EMPTY",
                          varying.status().message());
            }
        }
    }
}

TEST_F(geographyFunctionsTest, h3FromGeoPointBytesInBothOrdersAndMalformedPayloads) {
    const auto from_bytes = [&](const std::string& bytes) {
        GeoColumnDescriptor descriptor{
                geography_type().geo_type.value(),
                {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}};
        auto column = GeoColumn::create(descriptor);
        column->append_wkb(Slice(bytes));
        return GeoFunctions::h3_from_geo(nullptr, {column, ColumnHelper::create_const_column<TYPE_INT>(9, 1)});
    };
    WkbGeometry point;
    ASSERT_TRUE(WkbCodec::parse_wkt("POINT (37.62 55.75)", &point).ok());
    std::string little;
    ASSERT_TRUE(WkbCodec::to_wkb(point, &little).ok());
    auto expected = from_bytes(little);
    ASSERT_TRUE(expected.ok());
    auto big = little;
    big[0] = 0;
    std::reverse(big.begin() + 1, big.begin() + 5);
    std::reverse(big.begin() + 5, big.begin() + 13);
    std::reverse(big.begin() + 13, big.end());
    auto actual = from_bytes(big);
    ASSERT_TRUE(actual.ok()) << actual.status();
    EXPECT_EQ(ColumnViewer<TYPE_BIGINT>(*expected).value(0), ColumnViewer<TYPE_BIGINT>(*actual).value(0));

    for (const auto& bytes : {little.substr(0, 20), little + '\0', std::string()}) {
        EXPECT_FALSE(from_bytes(bytes).ok());
    }
    auto bad_order = little;
    bad_order[0] = 2;
    EXPECT_FALSE(from_bytes(bad_order).ok());
    auto ewkb = little;
    ewkb[4] = static_cast<char>(0x20);
    EXPECT_FALSE(from_bytes(ewkb).ok());

    point.coordinates[0].x = std::numeric_limits<double>::quiet_NaN();
    // Encode a partial-NaN tuple directly: it must not be treated as POINT EMPTY.
    const uint64_t nan_bits = std::bit_cast<uint64_t>(point.coordinates[0].x);
    for (size_t i = 0; i < 8; ++i) little[5 + i] = static_cast<char>(nan_bits >> (8 * i));
    EXPECT_FALSE(from_bytes(little).ok());
    for (size_t i = 0; i < 8; ++i) little[13 + i] = static_cast<char>(nan_bits >> (8 * i));
    auto empty = from_bytes(little);
    ASSERT_TRUE(empty.ok()) << empty.status();
    EXPECT_TRUE(ColumnViewer<TYPE_BIGINT>(*empty).is_null(0));
}

TEST_F(geographyFunctionsTest, h3FromGeoFixedPointStructureRetainsDiagnostics) {
    const auto from_bytes = [&](const std::string& bytes) {
        GeoColumnDescriptor descriptor{
                geography_type().geo_type.value(),
                {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED}};
        auto column = GeoColumn::create(descriptor);
        column->append_wkb(Slice(bytes));
        return GeoFunctions::h3_from_geo(nullptr, {column, ColumnHelper::create_const_column<TYPE_INT>(9, 1)});
    };
    for (uint8_t order : {0, 1, 2, 255}) {
        for (uint32_t type : {0U, 1U, 2U, 7U, 1001U, 2001U, 3001U, 0x20000001U, 0x40000001U, 0x80000001U}) {
            for (size_t size : {0U, 1U, 4U, 5U, 13U, 20U, 21U, 22U, 29U, 37U}) {
                SCOPED_TRACE(::testing::Message()
                             << "order=" << unsigned(order) << " type=" << type << " size=" << size);
                std::string bytes(37, '\0');
                bytes[0] = static_cast<char>(order);
                for (size_t i = 0; i < sizeof(type); ++i) {
                    const size_t shift = order == 1 ? i : sizeof(type) - 1 - i;
                    bytes[1 + i] = static_cast<char>(type >> (8 * shift));
                }
                bytes.resize(size);
                auto structural = inspect_geo_wkb(Slice(bytes));
                auto actual = from_bytes(bytes);
                if (!structural.ok()) {
                    ASSERT_FALSE(actual.ok());
                    EXPECT_EQ(structural.status().code(), actual.status().code());
                    EXPECT_EQ(structural.status().message(), actual.status().message());
                } else if (structural->dimension != GEO_DIMENSION_XY || structural->geometry_type != 1) {
                    ASSERT_FALSE(actual.ok());
                    EXPECT_TRUE(actual.status().is_invalid_argument());
                    EXPECT_EQ("H3_FromGeo requires XY POINT", actual.status().message());
                } else {
                    ASSERT_TRUE(actual.ok()) << actual.status();
                    H3Index expected = 0;
                    const LatLng origin{0, 0};
                    ASSERT_EQ(E_SUCCESS, latLngToCell(&origin, 9, &expected));
                    EXPECT_EQ(static_cast<int64_t>(expected), ColumnViewer<TYPE_BIGINT>(*actual).value(0));
                }
            }
        }
    }
}

TEST_F(geographyFunctionsTest, h3FromGeoFixedPointStructureRetainsCoordinateBits) {
    const double payload_nan = std::bit_cast<double>(uint64_t{0xfff8000000001234});
    const std::array<std::array<double, 2>, 9> coordinates = {{{0.0, 0.0},
                                                               {-0.0, 0.0},
                                                               {0.0, -0.0},
                                                               {-0.0, -0.0},
                                                               {180.0, 90.0},
                                                               {-180.0, -90.0},
                                                               {37.62, 55.75},
                                                               {-179.999999, 0.000001},
                                                               {payload_nan, payload_nan}}};
    for (uint8_t order : {0, 1}) {
        GeoColumnDescriptor descriptor{geography_type().geo_type.value(),
                                       {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_UNVALIDATED}};
        auto points = GeoColumn::create(descriptor);
        for (const auto& coordinate : coordinates) {
            std::string bytes(21, '\0');
            bytes[0] = static_cast<char>(order);
            bytes[order == 1 ? 1 : 4] = 1;
            for (size_t axis = 0; axis < 2; ++axis) {
                const uint64_t bits = std::bit_cast<uint64_t>(coordinate[axis]);
                for (size_t i = 0; i < sizeof(bits); ++i) {
                    const size_t shift = order == 1 ? i : sizeof(bits) - 1 - i;
                    bytes[5 + axis * sizeof(bits) + i] = static_cast<char>(bits >> (8 * shift));
                }
            }
            points->append_wkb(Slice(bytes));
        }
        for (int resolution = 0; resolution <= 15; ++resolution) {
            SCOPED_TRACE(::testing::Message() << "order=" << unsigned(order) << " resolution=" << resolution);
            auto result = GeoFunctions::h3_from_geo(
                    nullptr, {points, ColumnHelper::create_const_column<TYPE_INT>(resolution, coordinates.size())});
            ASSERT_TRUE(result.ok()) << result.status();
            ColumnViewer<TYPE_BIGINT> actual(*result);
            for (size_t row = 0; row + 1 < coordinates.size(); ++row) {
                constexpr double radians = 0.017453292519943295769236907684886;
                const LatLng coordinate{coordinates[row][1] * radians, coordinates[row][0] * radians};
                H3Index expected = 0;
                ASSERT_EQ(E_SUCCESS, latLngToCell(&coordinate, resolution, &expected));
                ASSERT_FALSE(actual.is_null(row));
                EXPECT_EQ(static_cast<int64_t>(expected), actual.value(row));
            }
            EXPECT_TRUE(actual.is_null(coordinates.size() - 1));
        }
    }
}

TEST_F(geographyFunctionsTest, h3ScalarCellsAcrossChunksAndNullableInputs) {
    constexpr int64_t cell = 0x83754efffffffffLL;
    constexpr size_t size = 3073;
    auto cells = NullableColumn::create(Int64Column::create(), NullColumn::create());
    auto resolutions = NullableColumn::create(Int32Column::create(), NullColumn::create());
    for (size_t row = 0; row < size; ++row) {
        if (row % 7 == 0)
            cells->append_nulls(1);
        else
            cells->append_datum(Datum(cell));
        if (row % 11 == 0)
            resolutions->append_nulls(1);
        else
            resolutions->append_datum(Datum(static_cast<int32_t>(row % 4)));
    }
    auto actual_resolution = GeoFunctions::h3_resolution(nullptr, {cells});
    auto actual_parent = GeoFunctions::h3_to_parent(nullptr, {cells, resolutions});
    ASSERT_TRUE(actual_resolution.ok()) << actual_resolution.status();
    ASSERT_TRUE(actual_parent.ok()) << actual_parent.status();
    EXPECT_EQ(size, (*actual_resolution)->size());
    EXPECT_EQ(size, (*actual_parent)->size());
    ColumnViewer<TYPE_INT> output_resolution(*actual_resolution);
    ColumnViewer<TYPE_BIGINT> output_parent(*actual_parent);
    for (size_t row = 0; row < size; ++row) {
        EXPECT_EQ(row % 7 == 0, output_resolution.is_null(row));
        EXPECT_EQ(row % 7 == 0 || row % 11 == 0, output_parent.is_null(row));
        if (row % 7 != 0) {
            EXPECT_EQ(3, output_resolution.value(row));
        }
        if (row % 7 != 0 && row % 11 != 0) {
            H3Index expected = 0;
            ASSERT_EQ(E_SUCCESS, cellToParent(cell, static_cast<int>(row % 4), &expected));
            EXPECT_EQ(static_cast<int64_t>(expected), output_parent.value(row));
        }
    }
}

TEST_F(geographyFunctionsTest, h3ScalarCellsRetainConstantAndEmptyResults) {
    constexpr int64_t cell = 0x83754efffffffffLL;
    constexpr size_t size = 3073;
    auto cells = ColumnHelper::create_const_column<TYPE_BIGINT>(cell, size);
    auto constant_resolution = GeoFunctions::h3_resolution(nullptr, {cells});
    auto constant_parent =
            GeoFunctions::h3_to_parent(nullptr, {cells, ColumnHelper::create_const_column<TYPE_INT>(3, size)});
    ASSERT_TRUE(constant_resolution.ok());
    ASSERT_TRUE(constant_parent.ok());
    EXPECT_TRUE((*constant_resolution)->is_constant());
    EXPECT_TRUE((*constant_parent)->is_constant());
    EXPECT_EQ(size, (*constant_resolution)->size());
    EXPECT_EQ(size, (*constant_parent)->size());
    EXPECT_EQ(3, ColumnViewer<TYPE_INT>(*constant_resolution).value(0));
    EXPECT_EQ(cell, ColumnViewer<TYPE_BIGINT>(*constant_parent).value(0));

    auto varying_resolution = Int32Column::create();
    for (size_t row = 0; row < size; ++row) varying_resolution->append(row % 4);
    auto varying_parent = GeoFunctions::h3_to_parent(nullptr, {cells, varying_resolution});
    ASSERT_TRUE(varying_parent.ok()) << varying_parent.status();
    EXPECT_FALSE((*varying_parent)->is_constant());
    for (size_t row = 0; row < size; ++row) {
        H3Index expected = 0;
        ASSERT_EQ(E_SUCCESS, cellToParent(cell, static_cast<int>(row % 4), &expected));
        EXPECT_EQ(static_cast<int64_t>(expected), ColumnViewer<TYPE_BIGINT>(*varying_parent).value(row));
    }
    auto empty = Int64Column::create();
    auto empty_resolution = GeoFunctions::h3_resolution(nullptr, {empty});
    auto empty_parent = GeoFunctions::h3_to_parent(nullptr, {empty, Int32Column::create()});
    ASSERT_TRUE(empty_resolution.ok());
    ASSERT_TRUE(empty_parent.ok());
    EXPECT_EQ(0, (*empty_resolution)->size());
    EXPECT_EQ(0, (*empty_parent)->size());
}

TEST_F(geographyFunctionsTest, h3ScalarCellsRejectInvalidValuesAcrossCheckpoints) {
    constexpr int64_t cell = 0x83754efffffffffLL;
    constexpr size_t size = 3073;
    for (size_t invalid_row : {1023, 1024, 3072}) {
        SCOPED_TRACE(invalid_row);
        auto cells = Int64Column::create();
        auto resolutions = Int32Column::create();
        for (size_t row = 0; row < size; ++row) {
            cells->append(row == invalid_row ? 0 : cell);
            resolutions->append(row == invalid_row ? 4 : 3);
        }
        auto invalid_resolution = GeoFunctions::h3_resolution(nullptr, {cells});
        auto invalid_parent_cell =
                GeoFunctions::h3_to_parent(nullptr, {cells, ColumnHelper::create_const_column<TYPE_INT>(3, size)});
        auto invalid_parent_resolution = GeoFunctions::h3_to_parent(
                nullptr, {ColumnHelper::create_const_column<TYPE_BIGINT>(cell, size), resolutions});
        ASSERT_FALSE(invalid_resolution.ok());
        ASSERT_FALSE(invalid_parent_cell.ok());
        ASSERT_FALSE(invalid_parent_resolution.ok());
        EXPECT_TRUE(invalid_resolution.status().is_invalid_argument());
        EXPECT_TRUE(invalid_parent_cell.status().is_invalid_argument());
        EXPECT_TRUE(invalid_parent_resolution.status().is_invalid_argument());
    }
}

TEST_F(geographyFunctionsTest, h3ScalarOutputsMatchLibraryAtEveryResolution) {
    auto cells = Int64Column::create();
    auto parent_resolutions = Int32Column::create();
    std::vector<H3Index> originals;
    uint64_t state = 0x238ad937a41928c7ULL;
    const auto random_unit = [&]() {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        return static_cast<double>(state >> 11) * (1.0 / 9007199254740992.0);
    };
    for (int resolution = 0; resolution <= 15; ++resolution) {
        std::array<H3Index, 12> pentagons{};
        ASSERT_EQ(E_SUCCESS, getPentagons(resolution, pentagons.data()));
        std::vector<H3Index> at_resolution(pentagons.begin(), pentagons.end());
        for (size_t sample = 0; sample < 32; ++sample) {
            const LatLng location{(random_unit() * 178.0 - 89.0) * M_PI / 180.0,
                                  (random_unit() * 360.0 - 180.0) * M_PI / 180.0};
            H3Index cell = 0;
            ASSERT_EQ(E_SUCCESS, latLngToCell(&location, resolution, &cell));
            at_resolution.push_back(cell);
        }
        if (resolution == 0) {
            std::array<H3Index, 122> base_cells{};
            ASSERT_EQ(E_SUCCESS, getRes0Cells(base_cells.data()));
            at_resolution.insert(at_resolution.end(), base_cells.begin(), base_cells.end());
        }
        for (H3Index cell : at_resolution) {
            ASSERT_TRUE(isValidCell(cell));
            originals.push_back(cell);
            for (int parent_resolution = 0; parent_resolution <= resolution; ++parent_resolution) {
                cells->append(static_cast<int64_t>(cell));
                parent_resolutions->append(parent_resolution);
            }
        }
    }
    auto resolution_result = GeoFunctions::h3_resolution(nullptr, {cells});
    auto parent_result = GeoFunctions::h3_to_parent(nullptr, {cells, parent_resolutions});
    ASSERT_TRUE(resolution_result.ok()) << resolution_result.status();
    ASSERT_TRUE(parent_result.ok()) << parent_result.status();
    ASSERT_EQ(cells->size(), (*resolution_result)->size());
    ASSERT_EQ(cells->size(), (*parent_result)->size());
    EXPECT_FALSE((*resolution_result)->is_nullable());
    EXPECT_FALSE((*parent_result)->is_nullable());
    ColumnViewer<TYPE_INT> resolutions(*resolution_result);
    ColumnViewer<TYPE_BIGINT> parents(*parent_result);
    for (size_t row = 0; row < cells->size(); ++row) {
        const auto cell = static_cast<H3Index>(cells->get_data()[row]);
        H3Index expected_parent = 0;
        ASSERT_EQ(E_SUCCESS, cellToParent(cell, parent_resolutions->get_data()[row], &expected_parent));
        EXPECT_EQ(getResolution(cell), resolutions.value(row));
        EXPECT_EQ(static_cast<int64_t>(expected_parent), parents.value(row));
    }

    // Exercise the constant-parent-resolution path, including both shift endpoints.
    for (int parent_resolution = 0; parent_resolution <= 15; ++parent_resolution) {
        auto eligible = Int64Column::create();
        for (H3Index cell : originals) {
            if (getResolution(cell) >= parent_resolution) eligible->append(static_cast<int64_t>(cell));
        }
        auto result = GeoFunctions::h3_to_parent(
                nullptr, {eligible, ColumnHelper::create_const_column<TYPE_INT>(parent_resolution, eligible->size())});
        ASSERT_TRUE(result.ok()) << result.status();
        ColumnViewer<TYPE_BIGINT> actual(*result);
        for (size_t row = 0; row < eligible->size(); ++row) {
            H3Index expected = 0;
            ASSERT_EQ(E_SUCCESS,
                      cellToParent(static_cast<H3Index>(eligible->get_data()[row]), parent_resolution, &expected));
            EXPECT_EQ(static_cast<int64_t>(expected), actual.value(row));
        }
    }
}

TEST_F(geographyFunctionsTest, h3ScalarRetainsFullCellValidationForMutatedIndexes) {
    std::vector<H3Index> candidates = {0, std::numeric_limits<uint64_t>::max()};
    const LatLng location{55.75 * M_PI / 180.0, 37.62 * M_PI / 180.0};
    for (int resolution = 0; resolution <= 15; ++resolution) {
        H3Index cell = 0;
        ASSERT_EQ(E_SUCCESS, latLngToCell(&location, resolution, &cell));
        candidates.push_back(cell);
        for (int bit = 0; bit < 64; ++bit) candidates.push_back(cell ^ (uint64_t{1} << bit));
        // Base cells 122..127 are invalid despite fitting the seven-bit field.
        for (uint64_t base_cell = 122; base_cell < 128; ++base_cell) {
            candidates.push_back((cell & ~(uint64_t{127} << 45)) | (base_cell << 45));
        }
        if (resolution != 0) {
            std::array<H3Index, 12> pentagons{};
            ASSERT_EQ(E_SUCCESS, getPentagons(resolution, pentagons.data()));
            for (H3Index pentagon : pentagons) {
                candidates.push_back(pentagon);
                // A first nonzero direction of 1 is the deleted pentagon subsequence.
                candidates.push_back(pentagon | (uint64_t{1} << (3 * (15 - resolution))));
            }
        }
    }
    for (H3Index raw : candidates) {
        SCOPED_TRACE(raw);
        const int64_t value = static_cast<int64_t>(raw);
        auto cell = Int64Column::create();
        cell->append(value);
        auto resolution = GeoFunctions::h3_resolution(nullptr, {cell});
        auto parent = GeoFunctions::h3_to_parent(nullptr, {cell, ColumnHelper::create_const_column<TYPE_INT>(0, 1)});
        const bool valid = value > 0 && isValidCell(raw);
        ASSERT_EQ(valid, resolution.ok());
        ASSERT_EQ(valid, parent.ok());
        if (!valid) {
            const auto expected = Status::InvalidArgument("H3 requires a valid positive cell index");
            EXPECT_EQ(expected.code(), resolution.status().code());
            EXPECT_EQ(expected.message(), resolution.status().message());
            EXPECT_EQ(expected.code(), parent.status().code());
            EXPECT_EQ(expected.message(), parent.status().message());
        } else {
            H3Index expected = 0;
            ASSERT_EQ(E_SUCCESS, cellToParent(raw, 0, &expected));
            EXPECT_EQ(getResolution(raw), ColumnViewer<TYPE_INT>(*resolution).value(0));
            EXPECT_EQ(static_cast<int64_t>(expected), ColumnViewer<TYPE_BIGINT>(*parent).value(0));
        }
    }
}

TEST_F(geographyFunctionsTest, h3ScalarConstantOperandsRetainNullAndErrorPrecedence) {
    constexpr int64_t valid = 0x83754efffffffffLL;
    const auto invalid_cell = Status::InvalidArgument("H3 requires a valid positive cell index");
    const auto invalid_resolution = Status::InvalidArgument("H3 resolution must be between 0 and 15");
    const auto wrong_parent = Status::InvalidArgument("H3_ToParent resolution exceeds cell resolution");
    auto cell_values = Int64Column::create();
    cell_values->append(0);
    cell_values->append(valid);
    auto cell_nulls = NullColumn::create();
    cell_nulls->append(1);
    cell_nulls->append(0);
    const ColumnPtr nullable_cells = NullableColumn::create(cell_values, cell_nulls);
    auto resolution = GeoFunctions::h3_resolution(nullptr, {nullable_cells});
    ASSERT_TRUE(resolution.ok()) << resolution.status();
    EXPECT_TRUE((*resolution)->is_null(0));
    EXPECT_EQ(3, ColumnViewer<TYPE_INT>(*resolution).value(1));

    auto parent =
            GeoFunctions::h3_to_parent(nullptr, {nullable_cells, ColumnHelper::create_const_column<TYPE_INT>(2, 2)});
    ASSERT_TRUE(parent.ok()) << parent.status();
    EXPECT_TRUE((*parent)->is_null(0));
    H3Index expected_parent = 0;
    ASSERT_EQ(E_SUCCESS, cellToParent(static_cast<H3Index>(valid), 2, &expected_parent));
    EXPECT_EQ(static_cast<int64_t>(expected_parent), ColumnViewer<TYPE_BIGINT>(*parent).value(1));

    auto resolution_values = Int32Column::create();
    resolution_values->append(16);
    resolution_values->append(2);
    auto resolution_nulls = NullColumn::create();
    resolution_nulls->append(1);
    resolution_nulls->append(0);
    const ColumnPtr nullable_resolutions = NullableColumn::create(resolution_values, resolution_nulls);
    auto constant_cell = GeoFunctions::h3_to_parent(
            nullptr, {ColumnHelper::create_const_column<TYPE_BIGINT>(valid, 2), nullable_resolutions});
    ASSERT_TRUE(constant_cell.ok()) << constant_cell.status();
    EXPECT_TRUE((*constant_cell)->is_null(0));
    EXPECT_EQ(static_cast<int64_t>(expected_parent), ColumnViewer<TYPE_BIGINT>(*constant_cell).value(1));

    // Invalid physical values hidden by NULL must never be validated eagerly.
    const ColumnPtr all_null_resolutions = NullableColumn::create(Int32Column::create(3, 16), NullColumn::create(3, 1));
    auto hidden_cell = GeoFunctions::h3_to_parent(
            nullptr, {ColumnHelper::create_const_column<TYPE_BIGINT>(0, 3), all_null_resolutions});
    ASSERT_TRUE(hidden_cell.ok()) << hidden_cell.status();
    const ColumnPtr all_null_cells = NullableColumn::create(Int64Column::create(3, 0), NullColumn::create(3, 1));
    auto hidden_resolution =
            GeoFunctions::h3_to_parent(nullptr, {all_null_cells, ColumnHelper::create_const_column<TYPE_INT>(16, 3)});
    ASSERT_TRUE(hidden_resolution.ok()) << hidden_resolution.status();
    for (size_t row = 0; row < 3; ++row) {
        EXPECT_TRUE((*hidden_cell)->is_null(row));
        EXPECT_TRUE((*hidden_resolution)->is_null(row));
    }
    auto empty = GeoFunctions::h3_to_parent(
            nullptr, {ColumnHelper::create_const_column<TYPE_BIGINT>(0, 0), Int32Column::create()});
    ASSERT_TRUE(empty.ok()) << empty.status();
    EXPECT_EQ(0, (*empty)->size());

    auto both_invalid = GeoFunctions::h3_to_parent(nullptr, {ColumnHelper::create_const_column<TYPE_BIGINT>(0, 1),
                                                             ColumnHelper::create_const_column<TYPE_INT>(16, 1)});
    ASSERT_FALSE(both_invalid.ok());
    EXPECT_EQ(invalid_cell.code(), both_invalid.status().code());
    EXPECT_EQ(invalid_cell.message(), both_invalid.status().message());
    for (int target : {-1, 4, 16}) {
        auto invalid = GeoFunctions::h3_to_parent(nullptr, {ColumnHelper::create_const_column<TYPE_BIGINT>(valid, 1),
                                                            ColumnHelper::create_const_column<TYPE_INT>(target, 1)});
        ASSERT_FALSE(invalid.ok());
        const auto& expected = target == 4 ? wrong_parent : invalid_resolution;
        EXPECT_EQ(expected.code(), invalid.status().code());
        EXPECT_EQ(expected.message(), invalid.status().message());
    }
    auto first_visible_invalid = GeoFunctions::h3_to_parent(
            nullptr, {ColumnHelper::create_const_column<TYPE_BIGINT>(0, 2), nullable_resolutions});
    ASSERT_FALSE(first_visible_invalid.ok());
    EXPECT_EQ(invalid_cell.code(), first_visible_invalid.status().code());
    EXPECT_EQ(invalid_cell.message(), first_visible_invalid.status().message());

    // An all-nonnull nullable input must retain the ordinary nonnullable output shape.
    const ColumnPtr nonnull = NullableColumn::create(Int64Column::create(3, valid), NullColumn::create(3, 0));
    auto nonnull_resolution = GeoFunctions::h3_resolution(nullptr, {nonnull});
    auto nonnull_parent =
            GeoFunctions::h3_to_parent(nullptr, {nonnull, ColumnHelper::create_const_column<TYPE_INT>(3, 3)});
    ASSERT_TRUE(nonnull_resolution.ok()) << nonnull_resolution.status();
    ASSERT_TRUE(nonnull_parent.ok()) << nonnull_parent.status();
    EXPECT_FALSE((*nonnull_resolution)->is_nullable());
    EXPECT_FALSE((*nonnull_parent)->is_nullable());
}

TEST_F(geographyFunctionsTest, h3ResolutionValidatesEveryRow) {
    constexpr size_t size = 3073;
    for (int resolution = 0; resolution <= 15; ++resolution) {
        SCOPED_TRACE(resolution);
        std::array<H3Index, 12> pentagons{};
        ASSERT_EQ(E_SUCCESS, getPentagons(resolution, pentagons.data()));
        auto cells = Int64Column::create();
        for (size_t row = 0; row < size; ++row) {
            cells->append(static_cast<int64_t>(pentagons[row % pentagons.size()]));
        }
        ASSERT_FALSE(cells->is_constant());
        auto same_resolution = GeoFunctions::h3_resolution(nullptr, {cells});
        ASSERT_TRUE(same_resolution.ok()) << same_resolution.status();
        EXPECT_FALSE((*same_resolution)->is_constant());
        EXPECT_EQ(size, (*same_resolution)->size());
        ColumnViewer<TYPE_INT> output(*same_resolution);
        for (size_t row : {size_t{0}, size_t{1023}, size_t{1024}, size - 1}) {
            EXPECT_EQ(resolution, output.value(row));
        }

        std::array<H3Index, 12> other_resolution{};
        ASSERT_EQ(E_SUCCESS, getPentagons((resolution + 1) % 16, other_resolution.data()));
        cells->get_data()[size - 1] = static_cast<int64_t>(other_resolution[0]);
        auto mixed = GeoFunctions::h3_resolution(nullptr, {cells});
        ASSERT_TRUE(mixed.ok()) << mixed.status();
        EXPECT_FALSE((*mixed)->is_constant());
        EXPECT_EQ(resolution, ColumnViewer<TYPE_INT>(*mixed).value(0));
        EXPECT_EQ((resolution + 1) % 16, ColumnViewer<TYPE_INT>(*mixed).value(size - 1));

        // The invalid cell has the same resolution bits as every preceding row.
        cells->get_data()[size - 1] = static_cast<int64_t>(pentagons[0] | (H3Index{1} << 56));
        auto invalid = GeoFunctions::h3_resolution(nullptr, {cells});
        ASSERT_FALSE(invalid.ok());
        const auto expected = Status::InvalidArgument("H3 requires a valid positive cell index");
        EXPECT_EQ(expected.code(), invalid.status().code());
        EXPECT_EQ(expected.message(), invalid.status().message());

        auto nulls = NullColumn::create(size, 0);
        nulls->get_data()[size - 1] = 1;
        auto with_null = GeoFunctions::h3_resolution(nullptr, {NullableColumn::create(cells, nulls)});
        ASSERT_TRUE(with_null.ok()) << with_null.status();
        EXPECT_FALSE((*with_null)->is_constant());
        EXPECT_TRUE((*with_null)->is_null(size - 1));
        EXPECT_EQ(resolution, ColumnViewer<TYPE_INT>(*with_null).value(0));
    }
}

TEST_F(geographyFunctionsTest, h3ParentConstantTargetPreservesEmptyAndInvalidCellPrecedence) {
    auto empty = GeoFunctions::h3_to_parent(
            nullptr, {Int64Column::create(), ColumnHelper::create_const_column<TYPE_INT>(16, 0)});
    ASSERT_TRUE(empty.ok()) << empty.status();
    EXPECT_EQ(0, (*empty)->size());
    for (int target : {-1, 0, 15, 16}) {
        auto cells = Int64Column::create();
        cells->append(0);
        auto invalid =
                GeoFunctions::h3_to_parent(nullptr, {cells, ColumnHelper::create_const_column<TYPE_INT>(target, 1)});
        ASSERT_FALSE(invalid.ok());
        const auto expected = Status::InvalidArgument("H3 requires a valid positive cell index");
        EXPECT_EQ(expected.code(), invalid.status().code());
        EXPECT_EQ(expected.message(), invalid.status().message());
    }
}

TEST_F(geographyFunctionsTest, h3BoundaryAndPolygonFill) {
    constexpr int64_t cell_value = 0x83754efffffffffLL;
    auto cell = ColumnHelper::create_const_column<TYPE_BIGINT>(cell_value, 1);
    std::unique_ptr<FunctionContext> boundary_context(
            FunctionContext::create_test_context({TypeDescriptor(TYPE_BIGINT)}, geography_type()));
    auto boundary = GeoFunctions::h3_to_boundary(boundary_context.get(), {cell});
    ASSERT_TRUE(boundary.ok()) << boundary.status();
    EXPECT_TRUE((*boundary)->is_constant());
    const auto* data =
            down_cast<const GeoColumn*>(down_cast<const ConstColumn*>((*boundary).get())->data_column().get());
    WkbGeometry geometry;
    ASSERT_TRUE(WkbCodec::parse_wkb(data->get_wkb(0), &geometry).ok());
    ASSERT_EQ(WkbGeometryType::POLYGON, geometry.type);
    ASSERT_EQ(1, geometry.rings.size());
    CellBoundary library_boundary{};
    ASSERT_EQ(E_SUCCESS, cellToBoundary(static_cast<H3Index>(cell_value), &library_boundary));
    EXPECT_EQ(static_cast<size_t>(library_boundary.numVerts + 1), geometry.rings[0].size());
    EXPECT_EQ(geometry.rings[0].front(), geometry.rings[0].back());

    auto polygon = geography({"POLYGON ((-5 -5, 5 -5, 5 5, -5 5, -5 -5))"});
    auto res3 = ColumnHelper::create_const_column<TYPE_INT>(3, 1);
    auto cells = GeoFunctions::h3_polygon_to_cells(nullptr, {polygon, res3});
    ASSERT_TRUE(cells.ok()) << cells.status();
    ASSERT_FALSE((*cells)->get(0).get_array().empty());
    std::unordered_set<int64_t> unique;
    const auto filled = (*cells)->get(0).get_array();
    for (const auto& element : filled) {
        const int64_t value = element.get_int64();
        EXPECT_TRUE(isValidCell(static_cast<H3Index>(value)));
        EXPECT_TRUE(unique.emplace(value).second);
    }
    EXPECT_TRUE(unique.contains(cell_value));

    auto with_hole =
            geography({"POLYGON ((-5 -5, 5 -5, 5 5, -5 5, -5 -5), "
                       "(-1 -1, -1 1, 1 1, 1 -1, -1 -1))"});
    auto hole_cells = GeoFunctions::h3_polygon_to_cells(nullptr, {with_hole, res3});
    ASSERT_TRUE(hole_cells.ok()) << hole_cells.status();
    const auto holed = (*hole_cells)->get(0).get_array();
    for (const auto& element : holed) {
        EXPECT_NE(cell_value, element.get_int64());
    }

    auto bad_resolution = ColumnHelper::create_const_column<TYPE_INT>(16, 1);
    auto empty = geography({"POLYGON EMPTY"});
    EXPECT_FALSE(GeoFunctions::h3_polygon_to_cells(nullptr, {empty, bad_resolution}).ok());
    auto empty_result = GeoFunctions::h3_polygon_to_cells(nullptr, {empty, res3});
    ASSERT_TRUE(empty_result.ok()) << empty_result.status();
    EXPECT_TRUE((*empty_result)->get(0).get_array().empty());
}

TEST_F(geographyFunctionsTest, h3ArraysAcrossNullableInputs) {
    constexpr int64_t cell = 0x83754efffffffffLL;
    constexpr size_t size = 100;
    std::array<H3Index, 12> pentagons{};
    ASSERT_EQ(E_SUCCESS, getPentagons(3, pentagons.data()));
    for (bool children : {false, true}) {
        auto cells = NullableColumn::create(Int64Column::create(), NullColumn::create());
        auto parameters = NullableColumn::create(Int32Column::create(), NullColumn::create());
        for (size_t row = 0; row < size; ++row) {
            if (row % 7 == 0)
                cells->append_nulls(1);
            else
                cells->append_datum(Datum(row % 2 == 0 ? cell : static_cast<int64_t>(pentagons[0])));
            if (row % 11 == 0)
                parameters->append_nulls(1);
            else
                parameters->append_datum(Datum(static_cast<int32_t>((children ? 3 : 0) + row % 4)));
        }
        auto actual = children ? GeoFunctions::h3_to_children(nullptr, {cells, parameters})
                               : GeoFunctions::h3_grid_disk(nullptr, {cells, parameters});
        ASSERT_TRUE(actual.ok()) << actual.status();
        ASSERT_EQ(size, (*actual)->size());
        EXPECT_FALSE((*actual)->is_constant());
        for (size_t row = 0; row < size; ++row) {
            SCOPED_TRACE(row);
            const auto datum = (*actual)->get(row);
            const bool is_null = row % 7 == 0 || row % 11 == 0;
            ASSERT_EQ(is_null, datum.is_null());
            if (is_null) continue;
            const H3Index origin = row % 2 == 0 ? cell : pentagons[0];
            const int parameter = (children ? 3 : 0) + row % 4;
            int64_t count = 0;
            ASSERT_EQ(E_SUCCESS,
                      children ? cellToChildrenSize(origin, parameter, &count) : maxGridDiskSize(parameter, &count));
            std::vector<H3Index> expected(count, 0);
            ASSERT_EQ(E_SUCCESS, children ? cellToChildren(origin, parameter, expected.data())
                                          : gridDisk(origin, parameter, expected.data()));
            expected.erase(std::remove(expected.begin(), expected.end(), H3_NULL), expected.end());
            const auto values = datum.get_array();
            ASSERT_EQ(expected.size(), values.size());
            for (size_t i = 0; i < expected.size(); ++i)
                EXPECT_EQ(static_cast<int64_t>(expected[i]), values[i].get_int64());
        }
    }
}

TEST_F(geographyFunctionsTest, h3ArraysRetainConstantAndEmptyResults) {
    constexpr int64_t cell = 0x83754efffffffffLL;
    constexpr size_t size = 3;
    std::array<H3Index, 12> pentagons{};
    ASSERT_EQ(E_SUCCESS, getPentagons(3, pentagons.data()));
    for (bool children : {false, true}) {
        for (H3Index origin : {static_cast<H3Index>(cell), pentagons[0]}) {
            const int parameter = children ? 7 : 19;
            auto cells = ColumnHelper::create_const_column<TYPE_BIGINT>(static_cast<int64_t>(origin), size);
            auto parameters = ColumnHelper::create_const_column<TYPE_INT>(parameter, size);
            auto actual = children ? GeoFunctions::h3_to_children(nullptr, {cells, parameters})
                                   : GeoFunctions::h3_grid_disk(nullptr, {cells, parameters});
            ASSERT_TRUE(actual.ok()) << actual.status();
            ASSERT_EQ(size, (*actual)->size());
            EXPECT_TRUE((*actual)->is_constant());
            int64_t count = 0;
            ASSERT_EQ(E_SUCCESS,
                      children ? cellToChildrenSize(origin, parameter, &count) : maxGridDiskSize(parameter, &count));
            std::vector<H3Index> expected(count, 0);
            ASSERT_EQ(E_SUCCESS, children ? cellToChildren(origin, parameter, expected.data())
                                          : gridDisk(origin, parameter, expected.data()));
            expected.erase(std::remove(expected.begin(), expected.end(), H3_NULL), expected.end());
            const auto values = (*actual)->get(0).get_array();
            ASSERT_EQ(expected.size(), values.size());
            for (size_t i = 0; i < expected.size(); ++i)
                EXPECT_EQ(static_cast<int64_t>(expected[i]), values[i].get_int64());
        }
        auto empty = children ? GeoFunctions::h3_to_children(nullptr, {Int64Column::create(), Int32Column::create()})
                              : GeoFunctions::h3_grid_disk(nullptr, {Int64Column::create(), Int32Column::create()});
        ASSERT_TRUE(empty.ok());
        EXPECT_EQ(0, (*empty)->size());
    }
}

TEST_F(geographyFunctionsTest, h3ArraysRejectInvalidValues) {
    constexpr int64_t cell = 0x83754efffffffffLL;
    constexpr size_t size = 16;
    for (bool children : {false, true}) {
        for (size_t invalid_row : {0, 7, 15}) {
            SCOPED_TRACE(invalid_row);
            auto cells = Int64Column::create();
            auto parameters = Int32Column::create();
            for (size_t row = 0; row < size; ++row) {
                cells->append(row == invalid_row ? 0 : cell);
                parameters->append(row == invalid_row ? -1 : (children ? 4 : 1));
            }
            auto invalid_cell =
                    children ? GeoFunctions::h3_to_children(
                                       nullptr, {cells, ColumnHelper::create_const_column<TYPE_INT>(4, size)})
                             : GeoFunctions::h3_grid_disk(
                                       nullptr, {cells, ColumnHelper::create_const_column<TYPE_INT>(1, size)});
            auto invalid_parameter =
                    children
                            ? GeoFunctions::h3_to_children(
                                      nullptr, {ColumnHelper::create_const_column<TYPE_BIGINT>(cell, size), parameters})
                            : GeoFunctions::h3_grid_disk(
                                      nullptr,
                                      {ColumnHelper::create_const_column<TYPE_BIGINT>(cell, size), parameters});
            ASSERT_FALSE(invalid_cell.ok());
            ASSERT_FALSE(invalid_parameter.ok());
            EXPECT_TRUE(invalid_cell.status().is_invalid_argument());
            EXPECT_TRUE(invalid_parameter.status().is_invalid_argument());
        }
    }
}

TEST_F(geographyFunctionsTest, h3ArraysMatchLibraryOrderAcrossResolutions) {
    for (bool children : {false, true}) {
        auto cells = Int64Column::create();
        auto parameters = Int32Column::create();
        std::vector<std::vector<H3Index>> expected_rows;
        for (int resolution = 0; resolution <= 15; ++resolution) {
            std::array<H3Index, 13> origins{};
            const LatLng coordinate{0.659296379611606, -2.136621598315946};
            ASSERT_EQ(E_SUCCESS, latLngToCell(&coordinate, resolution, &origins[0]));
            ASSERT_EQ(E_SUCCESS, getPentagons(resolution, origins.data() + 1));
            for (H3Index origin : origins) {
                for (int step : {0, 1, 2}) {
                    if (children && resolution + step > 15) continue;
                    const int parameter = children ? resolution + step : (step == 2 ? 3 : step);
                    int64_t count = 0;
                    ASSERT_EQ(E_SUCCESS, children ? cellToChildrenSize(origin, parameter, &count)
                                                  : maxGridDiskSize(parameter, &count));
                    std::vector<H3Index> expected(count, H3_NULL);
                    ASSERT_EQ(E_SUCCESS, children ? cellToChildren(origin, parameter, expected.data())
                                                  : gridDisk(origin, parameter, expected.data()));
                    expected.erase(std::remove(expected.begin(), expected.end(), H3_NULL), expected.end());
                    expected_rows.emplace_back(std::move(expected));
                    cells->append(static_cast<int64_t>(origin));
                    parameters->append(parameter);
                }
            }
        }
        auto actual = children ? GeoFunctions::h3_to_children(nullptr, {cells, parameters})
                               : GeoFunctions::h3_grid_disk(nullptr, {cells, parameters});
        ASSERT_TRUE(actual.ok()) << actual.status();
        ASSERT_EQ(expected_rows.size(), (*actual)->size());
        for (size_t row = 0; row < expected_rows.size(); ++row) {
            SCOPED_TRACE(row);
            const auto datum = (*actual)->get(row);
            ASSERT_FALSE(datum.is_null());
            const auto values = datum.get_array();
            ASSERT_EQ(expected_rows[row].size(), values.size());
            for (size_t i = 0; i < values.size(); ++i) {
                ASSERT_FALSE(values[i].is_null());
                EXPECT_EQ(static_cast<int64_t>(expected_rows[row][i]), values[i].get_int64());
            }
        }
    }
}

TEST_F(geographyFunctionsTest, h3ArraysRetainValuesAcrossGrowthCompactionAndNullRows) {
    constexpr H3Index normal = 0x83754efffffffffULL;
    std::array<H3Index, 12> pentagons{};
    ASSERT_EQ(E_SUCCESS, getPentagons(3, pentagons.data()));
    for (bool children : {false, true}) {
        auto cells = Int64Column::create();
        auto parameters = Int32Column::create();
        auto cell_nulls = NullColumn::create();
        auto parameter_nulls = NullColumn::create();
        std::vector<std::vector<H3Index>> expected_rows;
        std::vector<bool> expected_nulls;
        // Shrinking a pentagon disk leaves capacity containing earlier cells. Later rows
        // must overwrite that storage, including H3_NULL holes, without changing prior rows.
        for (size_t cycle = 0; cycle < 4; ++cycle) {
            for (size_t row = 0; row < 9; ++row) {
                const bool null_cell = row == 3;
                const bool null_parameter = row == 7;
                const H3Index origin = row == 1 || row == 4 || row == 6 ? pentagons[cycle] : normal;
                const int step = row == 0 || row == 1 || row == 2 || row == 8 ? 4 : (row == 4 ? 2 : (row == 5 ? 1 : 0));
                const int parameter = children ? 3 + step : (step == 4 ? 19 : (step == 2 ? 3 : step));
                // Invalid payloads in NULL rows must not be evaluated.
                cells->append(null_parameter ? 0 : static_cast<int64_t>(origin));
                parameters->append(null_cell ? -1 : parameter);
                cell_nulls->append(null_cell);
                parameter_nulls->append(null_parameter);
                expected_nulls.push_back(null_cell || null_parameter);
                expected_rows.emplace_back();
                if (null_cell || null_parameter) continue;
                int64_t count = 0;
                ASSERT_EQ(E_SUCCESS, children ? cellToChildrenSize(origin, parameter, &count)
                                              : maxGridDiskSize(parameter, &count));
                auto& expected = expected_rows.back();
                expected.resize(count, H3_NULL);
                ASSERT_EQ(E_SUCCESS, children ? cellToChildren(origin, parameter, expected.data())
                                              : gridDisk(origin, parameter, expected.data()));
                if (!children && row == 1) {
                    EXPECT_NE(expected.end(), std::find(expected.begin(), expected.end(), H3_NULL));
                }
                expected.erase(std::remove(expected.begin(), expected.end(), H3_NULL), expected.end());
            }
        }
        Columns columns{NullableColumn::create(cells, cell_nulls), NullableColumn::create(parameters, parameter_nulls)};
        auto actual = children ? GeoFunctions::h3_to_children(nullptr, columns)
                               : GeoFunctions::h3_grid_disk(nullptr, columns);
        ASSERT_TRUE(actual.ok()) << actual.status();
        ASSERT_EQ(expected_rows.size(), (*actual)->size());
        for (size_t row = 0; row < expected_rows.size(); ++row) {
            SCOPED_TRACE(row);
            const auto datum = (*actual)->get(row);
            ASSERT_EQ(expected_nulls[row], datum.is_null());
            if (expected_nulls[row]) continue;
            const auto values = datum.get_array();
            ASSERT_EQ(expected_rows[row].size(), values.size());
            for (size_t i = 0; i < values.size(); ++i) {
                ASSERT_FALSE(values[i].is_null());
                EXPECT_EQ(static_cast<int64_t>(expected_rows[row][i]), values[i].get_int64());
            }
        }
    }
}

TEST_F(geographyFunctionsTest, h3ArraysRetainErrorPrecedenceAndEmptyBatches) {
    constexpr int64_t cell = 0x83754efffffffffLL;
    for (bool children : {false, true}) {
        for (bool invalid_cell : {false, true}) {
            auto cells = Int64Column::create();
            auto parameters = Int32Column::create();
            cells->append(cell);
            parameters->append(children ? 7 : 19);
            cells->append(invalid_cell ? 0 : cell);
            parameters->append(-1);
            auto actual = children ? GeoFunctions::h3_to_children(nullptr, {cells, parameters})
                                   : GeoFunctions::h3_grid_disk(nullptr, {cells, parameters});
            ASSERT_FALSE(actual.ok());
            EXPECT_TRUE(actual.status().is_invalid_argument());
            EXPECT_EQ(invalid_cell ? "H3 requires a valid positive cell index"
                                   : (children ? "H3 resolution must be between 0 and 15"
                                               : "H3_GridDisk requires non-negative k"),
                      actual.status().message());
        }
        for (bool constant_cell : {false, true}) {
            Columns columns;
            if (constant_cell) {
                columns = {ColumnHelper::create_const_column<TYPE_BIGINT>(0, 0), Int32Column::create()};
            } else {
                columns = {Int64Column::create(), ColumnHelper::create_const_column<TYPE_INT>(-1, 0)};
            }
            auto empty = children ? GeoFunctions::h3_to_children(nullptr, columns)
                                  : GeoFunctions::h3_grid_disk(nullptr, columns);
            ASSERT_TRUE(empty.ok()) << empty.status();
            EXPECT_EQ(0, (*empty)->size());
        }
    }
}

TEST_F(geographyFunctionsTest, h3BoundaryBuffersMatchCanonicalWkbAcrossCells) {
    constexpr size_t size = 400;
    auto cells = NullableColumn::create(Int64Column::create(), NullColumn::create());
    std::vector<H3Index> origins;
    origins.reserve(size);
    for (size_t row = 0; row < size; ++row) {
        const int resolution = row % 16;
        H3Index cell = 0;
        if (row % 19 == 0) {
            std::array<H3Index, 12> pentagons{};
            ASSERT_EQ(E_SUCCESS, getPentagons(resolution, pentagons.data()));
            cell = pentagons[row % pentagons.size()];
        } else {
            const double latitude = row % 13 == 0 ? 89.9 : static_cast<double>(row % 161) - 80;
            const double longitude = row % 17 == 0 ? 179.9 : static_cast<double>(row % 359) - 179;
            const LatLng point{latitude * M_PI / 180, longitude * M_PI / 180};
            ASSERT_EQ(E_SUCCESS, latLngToCell(&point, resolution, &cell));
        }
        origins.push_back(cell);
        if (row % 7 == 0)
            cells->append_nulls(1);
        else
            cells->append_datum(Datum(static_cast<int64_t>(cell)));
    }
    std::unique_ptr<FunctionContext> context(
            FunctionContext::create_test_context({TypeDescriptor(TYPE_BIGINT)}, geography_type()));
    auto actual = GeoFunctions::h3_to_boundary(context.get(), {cells});
    ASSERT_TRUE(actual.ok()) << actual.status();
    ASSERT_EQ(size, (*actual)->size());
    EXPECT_FALSE((*actual)->is_constant());
    const auto* geo =
            down_cast<const GeoColumn*>(down_cast<const NullableColumn*>((*actual).get())->data_column().get());
    EXPECT_EQ(GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED, geo->descriptor().storage.validation_state);
    for (size_t row = 0; row < size; ++row) {
        SCOPED_TRACE(row);
        ASSERT_EQ(row % 7 == 0, (*actual)->get(row).is_null());
        if (row % 7 == 0) continue;
        CellBoundary boundary{};
        ASSERT_EQ(E_SUCCESS, cellToBoundary(origins[row], &boundary));
        WkbGeometry expected;
        expected.type = WkbGeometryType::POLYGON;
        auto& ring = expected.rings.emplace_back();
        constexpr double degrees_per_radian = 1.0 / 0.017453292519943295769236907684886;
        for (int i = 0; i < boundary.numVerts; ++i)
            ring.push_back({boundary.verts[i].lng * degrees_per_radian, boundary.verts[i].lat * degrees_per_radian});
        ring.push_back(ring.front());
        std::string wkb;
        ASSERT_TRUE(WkbCodec::to_wkb(expected, &wkb, WkbCoordinateSemantics::GEOGRAPHY_CRS84).ok());
        EXPECT_EQ(Slice(wkb), geo->get_wkb(row));
    }
}

TEST_F(geographyFunctionsTest, h3BoundaryBuffersRetainConstantEmptyAndInvalidInputs) {
    constexpr int64_t cell = 0x83754efffffffffLL;
    constexpr size_t size = 16;
    std::unique_ptr<FunctionContext> context(
            FunctionContext::create_test_context({TypeDescriptor(TYPE_BIGINT)}, geography_type()));
    auto constant =
            GeoFunctions::h3_to_boundary(context.get(), {ColumnHelper::create_const_column<TYPE_BIGINT>(cell, size)});
    ASSERT_TRUE(constant.ok());
    EXPECT_TRUE((*constant)->is_constant());
    EXPECT_EQ(size, (*constant)->size());
    auto empty = GeoFunctions::h3_to_boundary(context.get(), {Int64Column::create()});
    ASSERT_TRUE(empty.ok());
    EXPECT_EQ(0, (*empty)->size());
    for (size_t invalid_row : {0, 7, 15}) {
        auto varying = Int64Column::create();
        for (size_t row = 0; row < size; ++row) varying->append(row == invalid_row ? 0 : cell);
        auto invalid = GeoFunctions::h3_to_boundary(context.get(), {varying});
        ASSERT_FALSE(invalid.ok());
        EXPECT_TRUE(invalid.status().is_invalid_argument());
    }
    auto nulls = GeoFunctions::h3_to_boundary(context.get(), {ColumnHelper::create_const_null_column(size)});
    ASSERT_TRUE(nulls.ok());
    EXPECT_TRUE((*nulls)->only_null());
    EXPECT_EQ(size, (*nulls)->size());
}

TEST_F(geographyFunctionsTest, h3BoundaryDirectWkbMatchesTopologyCorpus) {
    std::array<H3Index, 122> base_cells{};
    ASSERT_EQ(E_SUCCESS, getRes0Cells(base_cells.data()));
    std::vector<H3Index> origins(base_cells.begin(), base_cells.end());
    for (H3Index cell : base_cells) {
        int64_t count = 0;
        ASSERT_EQ(E_SUCCESS, cellToChildrenSize(cell, 1, &count));
        std::vector<H3Index> children(count, H3_NULL);
        ASSERT_EQ(E_SUCCESS, cellToChildren(cell, 1, children.data()));
        origins.insert(origins.end(), children.begin(), children.end());
    }
    for (int resolution = 0; resolution <= 15; ++resolution) {
        std::array<H3Index, 12> pentagons{};
        ASSERT_EQ(E_SUCCESS, getPentagons(resolution, pentagons.data()));
        for (H3Index pentagon : pentagons) {
            std::array<H3Index, 7> disk{};
            ASSERT_EQ(E_SUCCESS, gridDisk(pentagon, 1, disk.data()));
            for (H3Index cell : disk) {
                if (cell != H3_NULL) origins.push_back(cell);
            }
        }
    }
    std::sort(origins.begin(), origins.end());
    origins.erase(std::unique(origins.begin(), origins.end()), origins.end());
    ASSERT_EQ(1972, origins.size());
    // Force maximum/shorter/maximum payload transitions before the exhaustive corpus.
    constexpr H3Index max_boundary_cell = 0x8f0800000000000ULL;
    origins.insert(origins.begin(), {max_boundary_cell, 0x8e0800000000007ULL, max_boundary_cell, 0x81023ffffffffffULL,
                                     0x8e0800000000017ULL, max_boundary_cell});
    std::vector<std::string> expected;
    std::array<bool, MAX_CELL_BNDRY_VERTS + 1> observed_vertices{};
    for (H3Index cell : origins) {
        CellBoundary boundary{};
        ASSERT_EQ(E_SUCCESS, cellToBoundary(cell, &boundary));
        ASSERT_GE(boundary.numVerts, 3);
        ASSERT_LE(boundary.numVerts, MAX_CELL_BNDRY_VERTS);
        observed_vertices[boundary.numVerts] = true;
        WkbGeometry polygon;
        polygon.type = WkbGeometryType::POLYGON;
        auto& ring = polygon.rings.emplace_back();
        constexpr double degrees_per_radian = 1.0 / 0.017453292519943295769236907684886;
        for (int i = 0; i < boundary.numVerts; ++i) {
            ring.push_back({boundary.verts[i].lng * degrees_per_radian, boundary.verts[i].lat * degrees_per_radian});
        }
        ring.push_back(ring.front());
        expected.emplace_back();
        ASSERT_TRUE(WkbCodec::to_wkb(polygon, &expected.back(), WkbCoordinateSemantics::GEOGRAPHY_CRS84).ok());
        ASSERT_EQ(13 + 16 * (boundary.numVerts + 1), expected.back().size());
    }
    for (int vertices : {5, 6, 7, 8, 10}) EXPECT_TRUE(observed_vertices[vertices]);
    ASSERT_EQ(189, expected.front().size());
    std::unique_ptr<FunctionContext> context(
            FunctionContext::create_test_context({TypeDescriptor(TYPE_BIGINT)}, geography_type()));
    // Cover ordinary, all-valid nullable, and nullable input with invalid hidden payloads.
    for (int mode : {0, 1, 2}) {
        auto cells = Int64Column::create();
        auto nulls = NullColumn::create();
        for (size_t row = 0; row < origins.size(); ++row) {
            const bool is_null = mode == 2 && row % 17 == 0;
            cells->append(is_null ? 0 : static_cast<int64_t>(origins[row]));
            nulls->append(is_null);
        }
        ColumnPtr input = cells;
        if (mode != 0) input = NullableColumn::create(cells, nulls);
        auto actual = GeoFunctions::h3_to_boundary(context.get(), {input});
        ASSERT_TRUE(actual.ok()) << actual.status();
        ASSERT_EQ(origins.size(), (*actual)->size());
        const auto* geo = down_cast<const GeoColumn*>(ColumnHelper::get_data_column(actual->get()));
        EXPECT_EQ(geography_type().geo_type.value(), geo->descriptor().type);
        EXPECT_EQ(GEO_ENCODING_WKB, geo->descriptor().storage.encoding);
        EXPECT_EQ(GEO_DIMENSION_XY, geo->descriptor().storage.dimension);
        EXPECT_EQ(GEO_VALIDATION_STATE_SEMANTICALLY_VALIDATED, geo->descriptor().storage.validation_state);
        for (size_t row = 0; row < origins.size(); ++row) {
            SCOPED_TRACE(row);
            const bool is_null = mode == 2 && row % 17 == 0;
            ASSERT_EQ(is_null, (*actual)->is_null(row));
            if (is_null) continue;
            const Slice wkb = geo->get_wkb(row);
            ASSERT_EQ(Slice(expected[row]), wkb);
            EXPECT_EQ(1, static_cast<unsigned char>(wkb.data[0]));
            EXPECT_EQ(Slice(wkb.data + 13, 16), Slice(wkb.data + wkb.size - 16, 16));
        }
    }
    auto constant = GeoFunctions::h3_to_boundary(
            context.get(), {ColumnHelper::create_const_column<TYPE_BIGINT>(max_boundary_cell, 3)});
    ASSERT_TRUE(constant.ok()) << constant.status();
    ASSERT_TRUE((*constant)->is_constant());
    ASSERT_EQ(3, (*constant)->size());
    const auto* geo = down_cast<const GeoColumn*>(ColumnHelper::get_data_column(constant->get()));
    EXPECT_EQ(Slice(expected.front()), geo->get_wkb(0));
}

TEST_F(geographyFunctionsTest, h3BoundaryDirectWkbRetainsDescriptorAndNullPrecedence) {
    auto invalid_cell = ColumnHelper::create_const_column<TYPE_BIGINT>(0, 1);
    auto missing_context = GeoFunctions::h3_to_boundary(nullptr, {invalid_cell});
    ASSERT_FALSE(missing_context.ok());
    EXPECT_TRUE(missing_context.status().is_not_supported());
    EXPECT_EQ("H3_ToBoundary requires GEOGRAPHY return descriptor", missing_context.status().message());
    for (int variant = 0; variant < 6; ++variant) {
        auto type = geography_type();
        if (variant == 0) type = TypeDescriptor(TYPE_BIGINT);
        if (variant == 1) type.geo_type.reset();
        if (variant == 2) type.geo_type->logical_type = GEO_LOGICAL_TYPE_GEOMETRY;
        if (variant == 3) type.geo_type->coordinate_system = GEO_COORDINATE_SYSTEM_CARTESIAN;
        if (variant == 4) type.geo_type->edge_algorithm = GEO_EDGE_ALGORITHM_PLANAR;
        if (variant == 5) type.geo_type->crs = "EPSG:4326";
        std::unique_ptr<FunctionContext> context(
                FunctionContext::create_test_context({TypeDescriptor(TYPE_BIGINT)}, type));
        auto actual = GeoFunctions::h3_to_boundary(context.get(), {invalid_cell});
        ASSERT_FALSE(actual.ok());
        EXPECT_TRUE(actual.status().is_not_supported());
        EXPECT_EQ(variant < 2 ? "H3_ToBoundary requires GEOGRAPHY return descriptor"
                              : "H3_ToBoundary requires CRS84 spherical GEOGRAPHY result",
                  actual.status().message());
        auto nulls = GeoFunctions::h3_to_boundary(context.get(), {ColumnHelper::create_const_null_column(17)});
        ASSERT_TRUE(nulls.ok());
        EXPECT_TRUE((*nulls)->only_null());
        EXPECT_EQ(17, (*nulls)->size());
    }
    auto nulls = GeoFunctions::h3_to_boundary(nullptr, {ColumnHelper::create_const_null_column(17)});
    ASSERT_TRUE(nulls.ok());
    EXPECT_TRUE((*nulls)->only_null());
    EXPECT_EQ(17, (*nulls)->size());
}

TEST_F(geographyFunctionsTest, h3InvalidCellsAndLimits) {
    constexpr int64_t cell_value = 0x83754efffffffffLL;
    auto cell = ColumnHelper::create_const_column<TYPE_BIGINT>(cell_value, 1);
    auto res3 = ColumnHelper::create_const_column<TYPE_INT>(3, 1);
    auto k1 = ColumnHelper::create_const_column<TYPE_INT>(1, 1);
    EXPECT_FALSE(GeoFunctions::h3_resolution(nullptr, {ColumnHelper::create_const_column<TYPE_BIGINT>(0, 1)}).ok());
    EXPECT_FALSE(GeoFunctions::h3_from_geo(
                         nullptr, {geography({"POINT (0 0)"}), ColumnHelper::create_const_column<TYPE_INT>(16, 1)})
                         .ok());
    EXPECT_FALSE(
            GeoFunctions::h3_to_children(nullptr, {cell, ColumnHelper::create_const_column<TYPE_INT>(-1, 1)}).ok());

    std::array<H3Index, 7> neighbors{};
    ASSERT_EQ(E_SUCCESS, gridDisk(cell_value, 1, neighbors.data()));
    const auto found = std::find_if(neighbors.begin(), neighbors.end(),
                                    [cell_value](H3Index value) { return value != H3_NULL && value != cell_value; });
    ASSERT_NE(neighbors.end(), found);
    H3Index edge = 0;
    ASSERT_EQ(E_SUCCESS, cellsToDirectedEdge(cell_value, *found, &edge));
    EXPECT_FALSE(GeoFunctions::h3_resolution(
                         nullptr, {ColumnHelper::create_const_column<TYPE_BIGINT>(static_cast<int64_t>(edge), 1)})
                         .ok());
    H3Index vertex = 0;
    ASSERT_EQ(E_SUCCESS, cellToVertex(cell_value, 0, &vertex));
    EXPECT_FALSE(GeoFunctions::h3_resolution(
                         nullptr, {ColumnHelper::create_const_column<TYPE_BIGINT>(static_cast<int64_t>(vertex), 1)})
                         .ok());

    auto negative_k = GeoFunctions::h3_grid_disk(nullptr, {cell, ColumnHelper::create_const_column<TYPE_INT>(-1, 1)});
    ASSERT_FALSE(negative_k.ok());
    EXPECT_EQ("H3_GridDisk requires non-negative k", negative_k.status().message());
    EXPECT_TRUE(GeoFunctions::h3_grid_disk(nullptr, {cell, ColumnHelper::create_const_column<TYPE_INT>(0, 1)}).ok());
    EXPECT_TRUE(GeoFunctions::h3_grid_disk(nullptr, {cell, k1}).ok());
    auto polygon = geography({"POLYGON ((-5 -5, 5 -5, 5 5, -5 5, -5 -5))"});
    EXPECT_TRUE(GeoFunctions::h3_polygon_to_cells(nullptr, {polygon, res3}).ok());
}

TEST_F(geographyFunctionsTest, h3ArraySizeLimits) {
    constexpr int64_t cell_value = 0x83754efffffffffLL;
    auto cell = ColumnHelper::create_const_column<TYPE_BIGINT>(cell_value, 1);
    auto k1 = ColumnHelper::create_const_column<TYPE_INT>(1, 1);
    auto res4 = ColumnHelper::create_const_column<TYPE_INT>(4, 1);
    auto accepted_disk = GeoFunctions::h3_grid_disk(nullptr, {cell, k1});
    ASSERT_TRUE(accepted_disk.ok()) << accepted_disk.status();
    EXPECT_EQ(7, (*accepted_disk)->get(0).get_array().size());
    auto accepted_children = GeoFunctions::h3_to_children(nullptr, {cell, res4});
    ASSERT_TRUE(accepted_children.ok()) << accepted_children.status();
    EXPECT_EQ(7, (*accepted_children)->get(0).get_array().size());

    auto huge_disk = GeoFunctions::h3_grid_disk(
            nullptr, {cell, ColumnHelper::create_const_column<TYPE_INT>(std::numeric_limits<int32_t>::max(), 1)});
    ASSERT_FALSE(huge_disk.ok());
    EXPECT_EQ("H3 array offset limit exceeded", huge_disk.status().message());
    auto huge_children =
            GeoFunctions::h3_to_children(nullptr, {cell, ColumnHelper::create_const_column<TYPE_INT>(15, 1)});
    ASSERT_FALSE(huge_children.ok());
    EXPECT_EQ("H3 array offset limit exceeded", huge_children.status().message());
}

TEST_F(geographyFunctionsTest, h3SeamPolarAndPentagon) {
    auto points = geography({"POINT (179.9 0)", "POINT (0 89.9)"});
    auto resolution = ColumnHelper::create_const_column<TYPE_INT>(1, 2);
    std::unique_ptr<FunctionContext> from_context(FunctionContext::create_test_context(
            {geography_type(), TypeDescriptor(TYPE_INT)}, TypeDescriptor(TYPE_BIGINT)));
    auto cells = GeoFunctions::h3_from_geo(from_context.get(), {points, resolution});
    ASSERT_TRUE(cells.ok()) << cells.status();
    ColumnViewer<TYPE_BIGINT> cell_view(*cells);
    std::unique_ptr<FunctionContext> boundary_context(
            FunctionContext::create_test_context({TypeDescriptor(TYPE_BIGINT)}, geography_type()));
    for (int i = 0; i < 2; ++i) {
        const H3Index cell = static_cast<H3Index>(cell_view.value(i));
        ASSERT_TRUE(isValidCell(cell));
        auto boundary = GeoFunctions::h3_to_boundary(
                boundary_context.get(),
                {ColumnHelper::create_const_column<TYPE_BIGINT>(static_cast<int64_t>(cell), 1)});
        ASSERT_TRUE(boundary.ok()) << boundary.status();
    }

    std::array<H3Index, 12> pentagons{};
    ASSERT_EQ(E_SUCCESS, getPentagons(1, pentagons.data()));
    ASSERT_TRUE(isValidCell(pentagons[0]));
    auto disk = GeoFunctions::h3_grid_disk(
            nullptr, {ColumnHelper::create_const_column<TYPE_BIGINT>(static_cast<int64_t>(pentagons[0]), 1),
                      ColumnHelper::create_const_column<TYPE_INT>(1, 1)});
    ASSERT_TRUE(disk.ok()) << disk.status();
    const auto ring = (*disk)->get(0).get_array();
    std::unordered_set<int64_t> unique;
    for (const auto& item : ring) {
        ASSERT_GT(item.get_int64(), 0);
        ASSERT_TRUE(isValidCell(static_cast<H3Index>(item.get_int64())));
        ASSERT_TRUE(unique.insert(item.get_int64()).second);
    }
    EXPECT_TRUE(unique.contains(static_cast<int64_t>(pentagons[0])));
}

TEST_F(geographyFunctionsTest, h3RecursiveFillMatchesStandardCenterFill) {
    for (int scenario = 0; scenario < 32; ++scenario) {
        const size_t vertex_count = scenario % 3 == 0 ? 128 : (scenario % 3 == 1 ? 256 : 512);
        const double longitude = -140 + scenario * 8;
        const double latitude = -50 + (scenario % 11) * 10;
        WkbGeometry geometry;
        geometry.type = WkbGeometryType::POLYGON;
        auto& ring = geometry.rings.emplace_back();
        for (size_t i = 0; i < vertex_count; ++i) {
            const double angle = i * 2 * M_PI / vertex_count;
            const double radius = scenario % 2 == 0 && i % 2 == 0 ? 0.012 : 0.02;
            ring.push_back({longitude + radius * std::cos(angle), latitude + radius * std::sin(angle)});
        }
        ring.push_back(ring.front());
        if (scenario % 4 == 0) {
            auto& hole = geometry.rings.emplace_back();
            for (size_t i = 0; i < 32; ++i) {
                const double angle = -static_cast<double>(i) * 2 * M_PI / 32;
                hole.push_back({longitude + 0.003 * std::cos(angle), latitude + 0.003 * std::sin(angle)});
            }
            hole.push_back(hole.front());
        }
        std::string wkb;
        ASSERT_TRUE(WkbCodec::to_wkb(geometry, &wkb).ok());
        auto input = GeoColumn::create(
                GeoColumnDescriptor{geography_type().geo_type.value(),
                                    {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_UNVALIDATED}});
        input->append_wkb(Slice(wkb));
        std::vector<std::vector<LatLng>> vertices;
        for (const auto& source : geometry.rings) {
            auto& target = vertices.emplace_back();
            for (size_t i = 0; i + 1 < source.size(); ++i)
                target.push_back({source[i].y * M_PI / 180, source[i].x * M_PI / 180});
        }
        std::vector<::GeoLoop> holes;
        for (size_t i = 1; i < vertices.size(); ++i)
            holes.push_back({static_cast<int>(vertices[i].size()), vertices[i].data()});
        ::GeoPolygon polygon{{static_cast<int>(vertices[0].size()), vertices[0].data()},
                             static_cast<int>(holes.size()),
                             holes.data()};
        for (int resolution : {7, 8, 9}) {
            SCOPED_TRACE(scenario);
            SCOPED_TRACE(resolution);
            int64_t count = 0;
            ASSERT_EQ(E_SUCCESS, maxPolygonToCellsSize(&polygon, resolution, 0, &count));
            std::vector<H3Index> expected(count, 0);
            ASSERT_EQ(E_SUCCESS, polygonToCells(&polygon, resolution, 0, expected.data()));
            std::unordered_set<int64_t> expected_cells;
            for (H3Index cell : expected)
                if (cell != H3_NULL) expected_cells.insert(static_cast<int64_t>(cell));
            auto actual = GeoFunctions::h3_polygon_to_cells(
                    nullptr, {input, ColumnHelper::create_const_column<TYPE_INT>(resolution, 1)});
            ASSERT_TRUE(actual.ok()) << actual.status();
            std::unordered_set<int64_t> actual_cells;
            const auto actual_datum = (*actual)->get(0);
            for (const auto& cell : actual_datum.get_array()) EXPECT_TRUE(actual_cells.insert(cell.get_int64()).second);
            EXPECT_EQ(expected_cells, actual_cells);
        }
    }
}

TEST_F(geographyFunctionsTest, h3RecursiveFillRetainsSensitiveGeometryAndMultipolygonSets) {
    for (int scenario = 0; scenario < 5; ++scenario) {
        const double longitude = scenario == 0 ? 179.95 : (scenario == 4 ? 35 : 0);
        const double latitude = scenario == 1 ? 85 : (scenario == 4 ? 55 : 0);
        const double radius = scenario == 2 ? 60 : 0.1;
        const size_t count = scenario == 3 ? 32 : 256;
        WkbGeometry polygon;
        polygon.type = WkbGeometryType::POLYGON;
        auto& ring = polygon.rings.emplace_back();
        for (size_t i = 0; i < count; ++i) {
            const double angle = i * 2 * M_PI / count;
            double x = longitude + radius * std::cos(angle);
            if (x > 180) x -= 360;
            ring.push_back({x, latitude + radius * std::sin(angle)});
        }
        ring.push_back(ring.front());
        std::string wkb;
        ASSERT_TRUE(WkbCodec::to_wkb(polygon, &wkb).ok());
        auto input = GeoColumn::create(
                GeoColumnDescriptor{geography_type().geo_type.value(),
                                    {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_UNVALIDATED}});
        input->append_wkb(Slice(wkb));
        std::vector<LatLng> vertices;
        for (size_t i = 0; i + 1 < ring.size(); ++i)
            vertices.push_back({ring[i].y * M_PI / 180, ring[i].x * M_PI / 180});
        ::GeoPolygon library{{static_cast<int>(vertices.size()), vertices.data()}, 0, nullptr};
        const int resolution = scenario == 2 ? 1 : 6;
        int64_t slots = 0;
        ASSERT_EQ(E_SUCCESS, maxPolygonToCellsSize(&library, resolution, 0, &slots));
        std::vector<H3Index> cells(slots, 0);
        ASSERT_EQ(E_SUCCESS, polygonToCells(&library, resolution, 0, cells.data()));
        std::unordered_set<int64_t> expected;
        for (H3Index c : cells)
            if (c != H3_NULL) expected.insert(static_cast<int64_t>(c));
        auto actual = GeoFunctions::h3_polygon_to_cells(
                nullptr, {input, ColumnHelper::create_const_column<TYPE_INT>(resolution, 1)});
        ASSERT_TRUE(actual.ok()) << actual.status();
        std::unordered_set<int64_t> returned;
        const auto actual_datum = (*actual)->get(0);
        for (const auto& c : actual_datum.get_array()) EXPECT_TRUE(returned.insert(c.get_int64()).second);
        EXPECT_EQ(expected, returned);
        if (scenario == 3) continue;
        WkbGeometry multi;
        multi.type = WkbGeometryType::MULTIPOLYGON;
        multi.children.push_back(polygon);
        if (scenario == 4) {
            auto second = polygon;
            for (auto& vertex : second.rings[0]) vertex.x += 1;
            multi.children.push_back(second);
            std::vector<LatLng> second_vertices;
            for (size_t i = 0; i + 1 < second.rings[0].size(); ++i)
                second_vertices.push_back({second.rings[0][i].y * M_PI / 180, second.rings[0][i].x * M_PI / 180});
            ::GeoPolygon second_library{{static_cast<int>(second_vertices.size()), second_vertices.data()}, 0, nullptr};
            ASSERT_EQ(E_SUCCESS, maxPolygonToCellsSize(&second_library, resolution, 0, &slots));
            cells.assign(slots, H3_NULL);
            ASSERT_EQ(E_SUCCESS, polygonToCells(&second_library, resolution, 0, cells.data()));
            for (H3Index cell : cells)
                if (cell != H3_NULL) expected.insert(static_cast<int64_t>(cell));
        }
        WkbGeometry empty;
        empty.type = WkbGeometryType::POLYGON;
        empty.empty = true;
        multi.children.push_back(empty);
        ASSERT_TRUE(WkbCodec::to_wkb(multi, &wkb).ok());
        auto multiple = GeoColumn::create(
                GeoColumnDescriptor{geography_type().geo_type.value(),
                                    {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_UNVALIDATED}});
        multiple->append_wkb(Slice(wkb));
        auto result = GeoFunctions::h3_polygon_to_cells(
                nullptr, {multiple, ColumnHelper::create_const_column<TYPE_INT>(resolution, 1)});
        ASSERT_TRUE(result.ok()) << result.status();
        returned.clear();
        const auto result_datum = (*result)->get(0);
        for (const auto& c : result_datum.get_array()) EXPECT_TRUE(returned.insert(c.get_int64()).second);
        EXPECT_EQ(expected, returned);
    }
}

TEST_F(geographyFunctionsTest, h3RecursiveFillRetainsNullsConstantInputsAndLimits) {
    WkbGeometry polygon;
    polygon.type = WkbGeometryType::POLYGON;
    auto& ring = polygon.rings.emplace_back();
    for (size_t i = 0; i < 256; ++i) {
        const double angle = i * 2 * M_PI / 256;
        ring.push_back({35 + 0.015 * std::cos(angle), 55 + 0.012 * std::sin(angle)});
    }
    ring.push_back(ring.front());
    std::string wkb;
    ASSERT_TRUE(WkbCodec::to_wkb(polygon, &wkb).ok());
    auto data = GeoColumn::create(GeoColumnDescriptor{
            geography_type().geo_type.value(), {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_UNVALIDATED}});
    data->append_wkb(Slice(wkb));
    constexpr size_t rows = 17;
    ColumnPtr input = ConstColumn::create(data, rows);
    auto resolutions = NullableColumn::create(Int32Column::create(), NullColumn::create());
    for (size_t row = 0; row < rows; ++row) {
        if (row % 5 == 0)
            resolutions->append_nulls(1);
        else
            resolutions->append_datum(Datum(static_cast<int32_t>(7 + row % 3)));
    }
    auto result = GeoFunctions::h3_polygon_to_cells(nullptr, {input, resolutions});
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(rows, (*result)->size());
    EXPECT_FALSE((*result)->is_constant());
    for (size_t row = 0; row < rows; ++row) EXPECT_EQ(row % 5 == 0, (*result)->get(row).is_null());
    auto constant =
            GeoFunctions::h3_polygon_to_cells(nullptr, {input, ColumnHelper::create_const_column<TYPE_INT>(9, rows)});
    ASSERT_TRUE(constant.ok());
    EXPECT_TRUE((*constant)->is_constant());
    EXPECT_EQ(rows, (*constant)->size());
    RuntimeState state;
    state.init_instance_mem_tracker();
    std::unique_ptr<FunctionContext> context(FunctionContext::create_context(
            &state, nullptr, TypeDescriptor::create_array_type(TypeDescriptor(TYPE_BIGINT)),
            {geography_type(), TypeDescriptor(TYPE_INT)}));
    const auto evaluate = [&]() {
        return GeoFunctions::h3_polygon_to_cells(context.get(),
                                                 {input, ColumnHelper::create_const_column<TYPE_INT>(9, rows)});
    };
    EXPECT_TRUE(evaluate().ok());
}

TEST_F(geographyFunctionsTest, h3PolygonFillRetainsOrderedCellsAndVaryingRowOffsets) {
    constexpr double radians_per_degree = 0.017453292519943295769236907684886;
    constexpr int resolution = 9;
    std::vector<WkbGeometry> shapes(8);
    std::vector<std::vector<int64_t>> expected(8);
    for (size_t scenario = 0; scenario < 5; ++scenario) {
        const size_t vertices = scenario == 1 ? 256 : 32;
        const double radius = scenario == 2 ? 1e-7 : (scenario == 3 ? 0.015 : 0.12);
        const double longitude = scenario == 4 ? 36 : 35;
        auto& shape = shapes[scenario];
        shape.type = WkbGeometryType::POLYGON;
        auto& ring = shape.rings.emplace_back();
        std::vector<LatLng> library_vertices;
        for (size_t i = 0; i < vertices; ++i) {
            const double angle = i * 2 * M_PI / vertices;
            ring.push_back({longitude + radius * std::cos(angle), 55 + radius * std::sin(angle)});
            library_vertices.push_back({ring.back().y * radians_per_degree, ring.back().x * radians_per_degree});
        }
        ring.push_back(ring.front());
        ::GeoPolygon polygon{{static_cast<int>(vertices), library_vertices.data()}, 0, nullptr};
        int64_t slots = 0;
        ASSERT_EQ(E_SUCCESS, maxPolygonToCellsSize(&polygon, resolution, 0, &slots));
        std::vector<H3Index> cells(slots, H3_NULL);
        // These fixed fixtures exercise the two pinned H3 center-fill APIs.
        if (scenario == 1) {
            ASSERT_EQ(E_SUCCESS,
                      polygonToCellsExperimental(&polygon, resolution, CONTAINMENT_CENTER, slots, cells.data()));
        } else {
            ASSERT_EQ(E_SUCCESS, polygonToCells(&polygon, resolution, 0, cells.data()));
        }
        for (H3Index cell : cells) {
            if (cell == H3_NULL) continue;
            ASSERT_TRUE(isValidCell(cell));
            expected[scenario].push_back(static_cast<int64_t>(cell));
        }
    }
    for (size_t scenario : {0, 1}) {
        ASSERT_FALSE(expected[scenario].empty());
    }
    ASSERT_TRUE(expected[2].empty());
    ASSERT_GT(expected[3].size(), 0);
    WkbGeometry empty;
    empty.type = WkbGeometryType::POLYGON;
    empty.empty = true;
    shapes[5].type = WkbGeometryType::MULTIPOLYGON;
    shapes[5].children = {empty, shapes[0], empty};
    expected[5] = expected[0];
    shapes[6].type = WkbGeometryType::MULTIPOLYGON;
    shapes[6].children = {shapes[0], shapes[4]};
    std::unordered_set<int64_t> seen;
    for (size_t component : {0, 4}) {
        for (int64_t cell : expected[component]) {
            if (seen.insert(cell).second) expected[6].push_back(cell);
        }
    }
    shapes[7] = empty;

    const std::array<size_t, 9> row_shapes{0, 1, 7, 3, 5, 6, 0, 1, 2};
    auto data = GeoColumn::create(GeoColumnDescriptor{
            geography_type().geo_type.value(), {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_UNVALIDATED}});
    auto nulls = NullColumn::create();
    auto resolutions = NullableColumn::create(Int32Column::create(), NullColumn::create());
    for (size_t row = 0; row < row_shapes.size(); ++row) {
        std::string wkb;
        ASSERT_TRUE(WkbCodec::to_wkb(shapes[row_shapes[row]], &wkb).ok());
        data->append_wkb(Slice(wkb));
        nulls->append(row == 1);
        if (row == 6)
            resolutions->append_nulls(1);
        else
            resolutions->append_datum(Datum(static_cast<int32_t>(resolution)));
    }
    auto result = GeoFunctions::h3_polygon_to_cells(nullptr, {NullableColumn::create(data, nulls), resolutions});
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(row_shapes.size(), (*result)->size());
    EXPECT_FALSE((*result)->is_constant());
    for (size_t row = 0; row < row_shapes.size(); ++row) {
        SCOPED_TRACE(row);
        const auto datum = (*result)->get(row);
        if (row == 1 || row == 6) {
            EXPECT_TRUE(datum.is_null());
            continue;
        }
        ASSERT_FALSE(datum.is_null());
        const auto& actual = datum.get_array();
        const auto& reference = expected[row_shapes[row]];
        ASSERT_EQ(reference.size(), actual.size());
        seen.clear();
        for (size_t i = 0; i < actual.size(); ++i) {
            SCOPED_TRACE(i);
            const int64_t cell = actual[i].get_int64();
            EXPECT_EQ(reference[i], cell);
            EXPECT_GT(cell, 0);
            EXPECT_TRUE(isValidCell(static_cast<H3Index>(cell)));
            EXPECT_TRUE(seen.insert(cell).second);
        }
    }
}

TEST_F(geographyFunctionsTest, h3PreparedPolygonAcrossBatchesAndThreads) {
    auto polygon = ConstColumn::create(geography({"POLYGON ((-20 -20, 20 -20, 20 20, -20 20, -20 -20))"}), 2);
    std::unique_ptr<FunctionContext> context(
            FunctionContext::create_test_context({geography_type(), TypeDescriptor(TYPE_INT)},
                                                 TypeDescriptor::create_array_type(TypeDescriptor(TYPE_BIGINT))));
    context->set_constant_columns({polygon, nullptr});
    ASSERT_TRUE(GeoFunctions::h3_polygon_to_cells_prepare(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_NE(nullptr, context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    auto at_two = GeoFunctions::h3_polygon_to_cells(context.get(),
                                                    {polygon, ColumnHelper::create_const_column<TYPE_INT>(2, 2)});
    ASSERT_TRUE(at_two.ok()) << at_two.status();
    auto at_three = GeoFunctions::h3_polygon_to_cells(context.get(),
                                                      {polygon, ColumnHelper::create_const_column<TYPE_INT>(3, 2)});
    ASSERT_TRUE(at_three.ok()) << at_three.status();
    EXPECT_NE((*at_two)->get(0).get_array().size(), (*at_three)->get(0).get_array().size());
    EXPECT_EQ(2, (*at_three)->size());

    std::atomic<bool> valid{true};
    const auto run = [&](int resolution) {
        for (int i = 0; i < 4; ++i) {
            auto result = GeoFunctions::h3_polygon_to_cells(
                    context.get(), {polygon, ColumnHelper::create_const_column<TYPE_INT>(resolution, 2)});
            if (!result.ok() || (*result)->get(0).get_array().empty()) valid.store(false);
        }
    };
    std::thread first(run, 2);
    std::thread second(run, 3);
    first.join();
    second.join();
    EXPECT_TRUE(valid.load());
    ASSERT_TRUE(GeoFunctions::h3_polygon_to_cells_close(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    EXPECT_EQ(nullptr, context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
}

TEST_F(geographyFunctionsTest, h3SingleRingValidityMatchesSphericalPredicate) {
    // Match production S2 initialization so malformed loops return a verdict
    // instead of aborting in the constructor before explicit validation.
    gflags::FlagSaver restore_flags;
    FLAGS_s2debug = false;
    std::vector<WkbGeometry> cases;
    const auto add = [&](std::vector<WkbCoordinate> ring) {
        WkbGeometry polygon;
        polygon.type = WkbGeometryType::POLYGON;
        polygon.rings.emplace_back(std::move(ring));
        cases.emplace_back(std::move(polygon));
    };
    add({{0, 0}, {2, 0}, {1, 2}, {0, 0}});
    add({{0, 0}, {0, 0}, {2, 0}, {2, 0}, {1, 2}, {0, 0}, {0, 0}});
    add({{0, 0}, {2, 2}, {0, 2}, {2, 0}, {0, 0}});         // bowtie
    add({{0, 0}, {2, 0}, {1, 1}, {2, 0}, {0, 2}, {0, 0}}); // non-adjacent duplicate
    add({{0, 0}, {0, 0}, {0, 0}, {0, 0}});
    add({{0, 0}, {2, 0}, {2, 0}, {0, 0}});     // only two mapped vertices
    add({{0, 0}, {1, 0}, {2, 0}, {0, 0}});     // collinear
    add({{0, 0}, {180, 0}, {90, 30}, {0, 0}}); // antipodal angular endpoints
    add({{0, 90}, {180, -90}, {45, 0}, {0, 90}});
    add({{179, -1}, {-179, -1}, {-179, 1}, {179, 1}, {179, -1}});       // seam
    add({{180, 0}, {179, 2}, {-179, 2}, {-180, 0}});                    // mapped closure aliases
    add({{-135, 80}, {-45, 80}, {45, 80}, {135, 80}, {-135, 80}});      // north pole
    add({{-135, -80}, {135, -80}, {45, -80}, {-45, -80}, {-135, -80}}); // south pole
    add({{-135, -10}, {-45, -10}, {45, -10}, {135, -10}, {-135, -10}}); // large loop
    add({{-170, -70}, {-10, -70}, {150, -70}, {170, 50}, {0, 80}, {-170, -70}});
    add({{-0.0, -0.0}, {1, 0}, {0, 1}, {+0.0, +0.0}});
    add({{0, 0}, {2, 0}, {1, 2}, {0, 1}});     // unclosed
    add({{181, 0}, {2, 0}, {1, 2}, {181, 0}}); // bounds
    add({{0, 91}, {2, 0}, {1, 2}, {0, 91}});
    add({{std::numeric_limits<double>::quiet_NaN(), 0}, {2, 0}, {1, 2}, {std::numeric_limits<double>::quiet_NaN(), 0}});

    add({});
    add({{0, 0}});
    add({{0, 0}, {0, 0}});

    std::mt19937_64 random(0x80396);
    std::uniform_real_distribution<double> unit(0, 1);
    constexpr size_t vertex_counts[] = {3, 4, 8, 32, 127, 128, 256, 512};
    for (size_t scenario = 0; scenario < 128; ++scenario) {
        const size_t vertices = vertex_counts[scenario % std::size(vertex_counts)];
        const double longitude = -160 + 320 * unit(random);
        const double latitude = -60 + 120 * unit(random);
        const double radius = 0.001 + 0.2 * unit(random);
        std::vector<WkbCoordinate> ring;
        for (size_t i = 0; i < vertices; ++i) {
            const double angle = 2 * M_PI * i / vertices;
            const double length = radius * (0.65 + 0.35 * unit(random));
            ring.push_back({longitude + length * std::cos(angle), latitude + length * std::sin(angle)});
        }
        ring.push_back(ring.front());
        add(ring);
        std::reverse(ring.begin(), ring.end());
        add(ring);
        const size_t duplicate = 1 + random() % (vertices - 1);
        const auto duplicate_point = ring[duplicate];
        ring.insert(ring.begin() + duplicate, duplicate_point);
        add(ring);
        if (vertices > 3) {
            ring[1 + random() % (vertices - 2)] = ring.front();
            add(ring);
        }
        ring.pop_back();
        std::shuffle(ring.begin(), ring.end(), random);
        ring.push_back(ring.front());
        add(ring);
    }

    size_t accepted = 0, rejected = 0;
    for (size_t scenario = 0; scenario < cases.size(); ++scenario) {
        SCOPED_TRACE(scenario);
        auto expected = spherical_is_valid(cases[scenario]);
        GeoCoordinateList coordinates;
        for (const auto& point : cases[scenario].rings.front()) coordinates.add({point.x, point.y});
        const bool actual = starrocks::GeoPolygon::is_valid_ring(coordinates);
        GeoCoordinateListList rings;
        rings.add(new GeoCoordinateList(coordinates));
        starrocks::GeoPolygon original;
        const bool original_valid = original.from_coords(rings) == GEO_PARSE_OK;
        ASSERT_TRUE(expected.ok()) << expected.status();
        EXPECT_EQ(expected.value(), original_valid);
        EXPECT_EQ(expected.value(), actual);
        accepted += expected.value();
        rejected += !expected.value();
    }
    EXPECT_GT(accepted, 0);
    EXPECT_GT(rejected, 0);
}

TEST_F(geographyFunctionsTest, h3SingleRingValidityRetainsOrderedFillAndTopologyErrors) {
    constexpr double radians_per_degree = 0.017453292519943295769236907684886;
    for (size_t vertices : {32, 256}) {
        WkbGeometry polygon;
        polygon.type = WkbGeometryType::POLYGON;
        auto& ring = polygon.rings.emplace_back();
        for (size_t i = 0; i < vertices; ++i) {
            const double angle = 2 * M_PI * i / vertices;
            ring.push_back({35 + 0.12 * std::cos(angle), 55 + 0.12 * std::sin(angle)});
        }
        ring.push_back(ring.front());
        std::string wkb;
        ASSERT_TRUE(WkbCodec::to_wkb(polygon, &wkb).ok());
        auto data = GeoColumn::create(
                GeoColumnDescriptor{geography_type().geo_type.value(),
                                    {GEO_ENCODING_WKB, GEO_DIMENSION_XY, GEO_VALIDATION_STATE_UNVALIDATED}});
        data->append_wkb(Slice(wkb));

        std::vector<LatLng> coordinates;
        for (size_t i = 0; i < vertices; ++i)
            coordinates.push_back({ring[i].y * radians_per_degree, ring[i].x * radians_per_degree});
        ::GeoPolygon library{{static_cast<int>(coordinates.size()), coordinates.data()}, 0, nullptr};
        int64_t slots = 0;
        ASSERT_EQ(E_SUCCESS, maxPolygonToCellsSize(&library, 9, 0, &slots));
        std::vector<H3Index> slab(slots, H3_NULL);
        const H3Error fill_error =
                vertices >= 128 ? polygonToCellsExperimental(&library, 9, CONTAINMENT_CENTER, slots, slab.data())
                                : polygonToCells(&library, 9, 0, slab.data());
        ASSERT_EQ(E_SUCCESS, fill_error);
        std::vector<int64_t> expected;
        std::unordered_set<H3Index> seen;
        for (H3Index cell : slab)
            if (cell != H3_NULL && seen.insert(cell).second) expected.push_back(static_cast<int64_t>(cell));
        ASSERT_FALSE(expected.empty());
        auto actual =
                GeoFunctions::h3_polygon_to_cells(nullptr, {data, ColumnHelper::create_const_column<TYPE_INT>(9, 1)});
        ASSERT_TRUE(actual.ok()) << actual.status();
        std::vector<int64_t> returned;
        const auto datum = (*actual)->get(0);
        for (const auto& cell : datum.get_array()) returned.push_back(cell.get_int64());
        EXPECT_EQ(expected, returned);
    }

    // Ordinary invalid topology fails before H3 admission/fill: no assertion
    // equates globally valid spherical polygons with H3 library success.
    for (const char* wkt : {"POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))", "POLYGON ((0 0, 2 0, 1 1, 2 0, 0 2, 0 0))"}) {
        auto actual = GeoFunctions::h3_polygon_to_cells(
                nullptr, {geography({wkt}), ColumnHelper::create_const_column<TYPE_INT>(1, 1)});
        ASSERT_FALSE(actual.ok());
        EXPECT_TRUE(actual.status().is_invalid_argument());
        EXPECT_NE(std::string::npos, actual.status().to_string().find("valid spherical GEOGRAPHY topology"));
    }
}

} // namespace starrocks

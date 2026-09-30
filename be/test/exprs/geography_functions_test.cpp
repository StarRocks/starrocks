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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cmath>
#include <limits>
#include <thread>

#include "butil/time.h"
#include "column/column_viewer.h"
#include "column/geo_column.h"
#include "column/nullable_column.h"
#include "exprs/builtin_functions.h"
#include "exprs/geo_functions.h"
#include "exprs/mock_vectorized_expr.h"
#include "geo/geo_types.h"
#include "geo/wkb.h"

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
        if (function.id >= 120180) {
            EXPECT_TRUE(static_cast<bool>(descriptor->prepare_function));
            EXPECT_TRUE(static_cast<bool>(descriptor->close_function));
        }
    }
}

} // namespace starrocks

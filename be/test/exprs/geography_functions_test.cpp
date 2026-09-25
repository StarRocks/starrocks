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

#include "butil/time.h"
#include "column/column_viewer.h"
#include "column/geo_column.h"
#include "column/nullable_column.h"
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

    auto unsupported = GeoFunctions::st_geography_distance(
            nullptr, {geography({"LINESTRING (0 0, 1 1)"}), geography({"POINT (0 0)"})});
    ASSERT_FALSE(unsupported.ok());
    EXPECT_TRUE(unsupported.status().is_invalid_argument());
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

    auto non_point = GeoFunctions::st_geometry_distance(nullptr, {line, geometry({"POINT (0 0)"})});
    ASSERT_FALSE(non_point.ok());
    EXPECT_TRUE(non_point.status().is_invalid_argument());

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

} // namespace starrocks

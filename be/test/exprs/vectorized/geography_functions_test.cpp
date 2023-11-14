// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "butil/time.h"
#include "exprs/vectorized/geo_functions.h"
#include "exprs/vectorized/mock_vectorized_expr.h"
#include "geo/geo_types.h"

namespace starrocks::vectorized {

class geographyFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {}
};

TEST_F(geographyFunctionsTest, st_pointTest) {
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;

        auto str_1 = DoubleColumn::create();
        auto str_2 = DoubleColumn::create();

        str_1->append(24.7);
        str_2->append(56.7);
        columns.emplace_back(str_1);
        columns.emplace_back(str_2);
        ColumnPtr result = GeoFunctions::st_point(ctx.get(), columns).value();

        columns.clear();
        columns.emplace_back(result);
        result = GeoFunctions::st_as_wkt(ctx.get(), columns).value();

        auto v = ColumnHelper::as_column<BinaryColumn>(result);
        ASSERT_EQ("POINT (24.7 56.7)", v->get_data()[0].to_string());
    }

    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;

        auto str_1 = DoubleColumn::create();
        auto str_2 = DoubleColumn::create();

        str_1->append(24.7);
        str_2->append(56.7);
        columns.emplace_back(str_1);
        columns.emplace_back(str_2);
        ColumnPtr result = GeoFunctions::st_point(ctx.get(), columns).value();
        auto v = ColumnHelper::as_column<BinaryColumn>(result);
        auto str_value = v->get_data()[0];

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
    columns.emplace_back(str_column);

    auto result = GeoFunctions::st_x(ctx.get(), columns).value();
    auto v = ColumnHelper::as_column<DoubleColumn>(result);
    ASSERT_EQ(134, v->get_data()[0]);

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
    columns.emplace_back(str_column);

    auto result = GeoFunctions::st_y(ctx.get(), columns).value();
    auto v = ColumnHelper::as_column<DoubleColumn>(result);
    ASSERT_EQ(63, v->get_data()[0]);
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

    columns.emplace_back(xlng);
    columns.emplace_back(xlat);
    columns.emplace_back(ylng);
    columns.emplace_back(ylat);

    ColumnPtr result = GeoFunctions::st_distance_sphere(ctx.get(), columns).value();
    auto v = ColumnHelper::as_column<DoubleColumn>(result);

    ASSERT_EQ(0, v->get_data()[0]);
}

TEST_F(geographyFunctionsTest, as_wktTest) {
    GeoPoint point;
    point.from_coord(134, 63);

    std::string buf;
    point.encode_to(&buf);

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str_point = BinaryColumn::create();
    columns.emplace_back(str_point);
    str_point->append(buf);

    auto result = GeoFunctions::st_as_wkt(ctx.get(), columns).value();
    auto v = ColumnHelper::as_column<BinaryColumn>(result);

    ASSERT_EQ("POINT (134 63)", v->get_data()[0].to_string());
}

TEST_F(geographyFunctionsTest, st_from_wktGeneralTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    std::string wkt = "POINT (10.1 20.2)";
    auto wkt_column = BinaryColumn::create();
    wkt_column->append(wkt);

    columns.emplace_back(wkt_column);

    ctx->impl()->set_constant_columns(columns);

    GeoFunctions::st_from_wkt_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);

    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto result = GeoFunctions::st_from_wkt(ctx.get(), columns).value();
    GeoFunctions::st_from_wkt_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto v = ColumnHelper::as_column<BinaryColumn>(result);

    GeoPoint point;
    auto res = point.decode_from(v->get_data()[0].data, v->get_data()[0].size);

    ASSERT_TRUE(res);
    ASSERT_DOUBLE_EQ(10.1, point.x());
    ASSERT_DOUBLE_EQ(20.2, point.y());
}

TEST_F(geographyFunctionsTest, st_from_wktConstTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto wkt_column = ColumnHelper::create_const_column<TYPE_VARCHAR>("POINT (10.1 20.2)", 1);

    columns.emplace_back(wkt_column);

    ctx->impl()->set_constant_columns(columns);

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

    columns.emplace_back(line_string);
    ctx->impl()->set_constant_columns(columns);

    GeoFunctions::st_line_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto result = GeoFunctions::st_line(ctx.get(), columns).value();
    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    GeoLine line_obj;
    auto res = line_obj.decode_from(v->get_data()[0].data, v->get_data()[0].size);
    ASSERT_TRUE(res);
    GeoFunctions::st_from_wkt_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
}

TEST_F(geographyFunctionsTest, st_lineConstTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto line_string = ColumnHelper::create_const_column<TYPE_VARCHAR>("LINESTRING (10.1 20.2, 21.1 30.1)", 1);

    columns.emplace_back(line_string);
    ctx->impl()->set_constant_columns(columns);

    GeoFunctions::st_line_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto result = GeoFunctions::st_line(ctx.get(), columns).value();
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
    columns.emplace_back(polygon_str);

    ctx->impl()->set_constant_columns(columns);
    GeoFunctions::st_polygon_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto str2 = GeoFunctions::st_polygon(ctx.get(), columns).value();
    auto result = ColumnHelper::as_column<BinaryColumn>(str2);
    ASSERT_FALSE(str2->is_null(0));
    auto v = result->get_data()[0];

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
    columns.emplace_back(const_polygon);

    ctx->impl()->set_constant_columns(columns);
    GeoFunctions::st_polygon_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    ASSERT_NE(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto str2 = GeoFunctions::st_polygon(ctx.get(), columns).value();
    auto result = ColumnHelper::as_column<ConstColumn>(str2);
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
    columns.emplace_back(lng_column);
    columns.emplace_back(lat_column);
    columns.emplace_back(radius_column);

    ctx->impl()->set_constant_columns(columns);
    GeoFunctions::st_circle_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto result = GeoFunctions::st_circle(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));

    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    auto cir_str = v->get_data()[0];

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
    columns.emplace_back(lng_column);
    columns.emplace_back(lat_column);
    columns.emplace_back(radius_column);

    ctx->impl()->set_constant_columns(columns);
    GeoFunctions::st_circle_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    ASSERT_NE(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto result = GeoFunctions::st_circle(ctx.get(), columns).value();
    ASSERT_FALSE(result->is_null(0));

    auto v = ColumnHelper::as_column<ConstColumn>(result);
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
    columns.emplace_back(polygon_column);
    ctx->impl()->set_constant_columns(columns);
    GeoFunctions::st_from_wkt_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto result1 = GeoFunctions::st_from_wkt(ctx.get(), columns).value();

    columns.clear();
    std::string point_wkt = "POINT (25 25)";
    auto point_column = BinaryColumn::create();
    point_column->append(point_wkt);
    columns.emplace_back(point_column);
    ctx->impl()->set_constant_columns(columns);
    GeoFunctions::st_from_wkt_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto result2 = GeoFunctions::st_from_wkt(ctx.get(), columns).value();

    columns.clear();
    columns.emplace_back(result1);
    columns.emplace_back(result2);
    ctx->impl()->set_constant_columns(columns);
    GeoFunctions::st_contains_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);

    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto res = GeoFunctions::st_contains(ctx.get(), columns).value();
    auto bools = ColumnHelper::cast_to<TYPE_BOOLEAN>(res);
    ASSERT_FALSE(res->is_null(0));
    ASSERT_TRUE(bools->get_data()[0]);
    GeoFunctions::st_contains_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
}

TEST_F(geographyFunctionsTest, st_containsWithUnexpectedInputTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    std::string polygon_wkt = "POLYGON ((10 10, 50 10, 50 50, 10 50, 10 10))";
    auto polygon_column = BinaryColumn::create();
    polygon_column->append(polygon_wkt);
    columns.emplace_back(polygon_column);
    ctx->impl()->set_constant_columns(columns);
    GeoFunctions::st_from_wkt_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto result1 = GeoFunctions::st_from_wkt(ctx.get(), columns).value();

    columns.clear();
    std::string point_wkt = "POINT (25 25)";
    auto point_column = BinaryColumn::create();
    point_column->append(point_wkt);
    columns.emplace_back(point_column);
    ctx->impl()->set_constant_columns(columns);
    GeoFunctions::st_from_wkt_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto result2 = GeoFunctions::st_from_wkt(ctx.get(), columns).value();

    columns.clear();
    auto varchar_column = ColumnHelper::cast_to<TYPE_VARCHAR>(result1);
    ASSERT_TRUE(varchar_column->size() == 1);
    ASSERT_FALSE(varchar_column->is_null(0));
    auto value = varchar_column->get_data()[0];
    std::string string_value(value.get_data(), value.get_size());
    string_value.append("A");
    auto input_column = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(string_value), varchar_column->size());

    columns.emplace_back(input_column);
    columns.emplace_back(result2);
    ctx->impl()->set_constant_columns(columns);
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
    columns.emplace_back(polygon_column);
    ctx->impl()->set_constant_columns(columns);
    GeoFunctions::st_from_wkt_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto result1 = ConstColumn::create(GeoFunctions::st_from_wkt(ctx.get(), columns).value(), 1);

    columns.clear();
    std::string point_wkt = "POINT (25 25)";
    auto point_column = BinaryColumn::create();
    point_column->append(point_wkt);
    columns.emplace_back(point_column);
    ctx->impl()->set_constant_columns(columns);
    GeoFunctions::st_from_wkt_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto result2 = ConstColumn::create(GeoFunctions::st_from_wkt(ctx.get(), columns).value(), 1);

    columns.clear();
    columns.emplace_back(result1);
    columns.emplace_back(result2);
    ctx->impl()->set_constant_columns(columns);
    GeoFunctions::st_contains_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);

    ASSERT_NE(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    auto res = GeoFunctions::st_contains(ctx.get(), columns).value();
    ASSERT_TRUE(ColumnHelper::get_const_value<TYPE_BOOLEAN>(res));
    GeoFunctions::st_contains_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
}

} // namespace starrocks::vectorized
